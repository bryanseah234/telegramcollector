import logging
import asyncio
from telethon import TelegramClient, events
from telethon.errors import (
    FloodWaitError, 
    SessionPasswordNeededError, 
    SessionRevokedError, 
    AuthKeyError, 
    UserDeactivatedError
)
import os
from enum import Enum
from database import get_db_connection

logger = logging.getLogger(__name__)


class ConnectionState(Enum):
    """States for Telegram client connection state machine."""
    DISCONNECTED = 'disconnected'      # Not connected to Telegram
    CONNECTING = 'connecting'          # Attempting to connect
    CONNECTED = 'connected'            # Connected and authorized
    RECONNECTING = 'reconnecting'      # Lost connection, trying to recover
    FLOOD_WAIT = 'flood_wait'          # Being rate limited
    INVALID_SESSION = 'invalid_session'  # Session is revoked/invalid
    PAUSED = 'paused'                  # Manually paused by user


class TelegramClientManager:
    """
    Manages the Telethon client connection with state machine pattern.
    Handles initialization, authorization, health monitoring, and cleanup.
    
    State Transitions:
        DISCONNECTED -> CONNECTING -> CONNECTED
        CONNECTED -> DISCONNECTED (network loss)
        CONNECTED -> RECONNECTING -> CONNECTED (auto-recovery)
        CONNECTED -> FLOOD_WAIT -> CONNECTED (after wait)
        * -> INVALID_SESSION (session revoked)
    """
    
    def __init__(self, session_name: str = 'user_session', api_id: int = None, api_hash: str = None):
        from config import settings
        self.api_id = api_id or settings.TG_API_ID
        self.api_hash = api_hash or settings.TG_API_HASH
        self.session_name = session_name
        self._health_task = None
        self._is_healthy = False
        self._state = ConnectionState.DISCONNECTED
        self._state_change_callbacks = []
        self._flood_wait_until = None
        
        # Ensure session directory exists
        os.makedirs('sessions', exist_ok=True)
        session_path = os.path.join('sessions', session_name)
        
        logger.debug(f"Initializing client with session path: {os.path.abspath(session_path)}")
        
        # Use SQLiteSession with increased timeout
        from telethon.sessions import SQLiteSession
        session = SQLiteSession(session_path)
        
        import sqlite3
        if hasattr(session, '_conn') and session._conn:
            session._conn.execute("PRAGMA busy_timeout = 30000")
        
        self.client = TelegramClient(session, self.api_id, self.api_hash)
    
    @property
    def state(self) -> ConnectionState:
        """Returns the current connection state."""
        return self._state
    
    async def _set_state(self, new_state: ConnectionState, reason: str = None):
        """Transitions to a new state with logging and notifications."""
        if self._state == new_state:
            return
        
        old_state = self._state
        self._state = new_state
        
        state_emoji = {
            ConnectionState.DISCONNECTED: '🔴',
            ConnectionState.CONNECTING: '🟡',
            ConnectionState.CONNECTED: '🟢',
            ConnectionState.RECONNECTING: '🟠',
            ConnectionState.FLOOD_WAIT: '⏳',
            ConnectionState.INVALID_SESSION: '❌',
            ConnectionState.PAUSED: '⏸️'
        }
        
        emoji = state_emoji.get(new_state, '❓')
        logger.info(f"{emoji} Session {self.session_name}: {old_state.value} -> {new_state.value}" + 
                   (f" ({reason})" if reason else ""))
        
        # Notify Hub for important state changes
        critical_states = [ConnectionState.INVALID_SESSION, ConnectionState.FLOOD_WAIT, ConnectionState.DISCONNECTED]
        if new_state in critical_states or old_state in critical_states:
            try:
                from hub_notifier import notify
                message = f"{emoji} **{self.session_name}**: {new_state.value}"
                if reason:
                    message += f" - {reason}"
                priority = 2 if new_state == ConnectionState.INVALID_SESSION else 1
                await notify('system', message, priority=priority)
            except Exception as e:
                logger.debug(f"Could not notify Hub of state change: {e}")
        
        # Call registered callbacks
        for callback in self._state_change_callbacks:
            try:
                if asyncio.iscoroutinefunction(callback):
                    await callback(old_state, new_state)
                else:
                    callback(old_state, new_state)
            except Exception as e:
                logger.error(f"State change callback error: {e}")
    
    def on_state_change(self, callback):
        """Registers a callback for state changes."""
        self._state_change_callbacks.append(callback)
    
    async def start(self, phone: str = None):
        """
        Starts the Telegram client and verifies authorization.
        
        Args:
            phone: Phone number for first-time authorization (optional if session exists)
        """
        await self._set_state(ConnectionState.CONNECTING)
        
        try:
            await self.client.connect()
            
            # Check authorization status
            if not await self.client.is_user_authorized():
                if phone:
                    logger.info(f"Authorizing with phone: {phone}")
                    await self.client.start(phone=phone)
                else:
                    await self.client.start()
            
            # Verify we're logged in
            me = await self.client.get_me()
            if me:
                logger.info(f"Telegram client started. Logged in as: {me.first_name} (@{me.username or 'N/A'})")
                self._is_healthy = True
                await self._set_state(ConnectionState.CONNECTED)
            else:
                raise RuntimeError("Failed to get user info after authorization")
            
            # Start health monitoring
            self._health_task = asyncio.create_task(self._health_monitor())
            
            # Schedule auto-cleanup of login messages
            asyncio.create_task(self._cleanup_login_messages())
            
            return me

        except (SessionRevokedError, AuthKeyError, UserDeactivatedError) as e:
            await self._set_state(ConnectionState.INVALID_SESSION, str(e))
            await self._handle_invalid_session()
            raise
        
        except FloodWaitError as e:
            await self._set_state(ConnectionState.FLOOD_WAIT, f"Waiting {e.seconds}s")
            self._flood_wait_until = asyncio.get_event_loop().time() + e.seconds
            await asyncio.sleep(e.seconds)
            return await self.start(phone)
            
        except Exception as e:
            await self._set_state(ConnectionState.DISCONNECTED, str(e))
            raise

    async def _handle_invalid_session(self):
        """
        Handles cleanup for invalid sessions:
        1. Updates DB status to 'paused' (not 'invalid') to preserve checkpoints
        2. Does NOT delete session file - user can re-login via Login Bot
        """
        try:
            session_file_path = os.path.join('sessions', f"{self.session_name}.session")
            
            async with get_db_connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("""
                        UPDATE telegram_accounts 
                        SET status = 'paused', 
                            last_error = 'Session logged out - use Login Bot to re-authenticate'
                        WHERE session_file_path = %s 
                           OR session_file_path = %s
                    """, (session_file_path, os.path.join('sessions', self.session_name)))
                    await conn.commit()
            
            logger.warning(f"⚠️ Session {self.session_name} paused. Checkpoints preserved.")
            
            # Notify Hub about invalid session
            try:
                from hub_notifier import notify
                await notify('error', f"❌ Session **{self.session_name}** logged out - requires re-authentication", priority=2)
            except Exception:
                pass

        except Exception as e:
            logger.error(f"Error during session pause handling: {e}")
    
    async def _health_monitor(self):
        """
        Periodically checks connection health and reconnects if needed.
        Uses exponential backoff for reconnection attempts.
        """
        MAX_RECONNECT_ATTEMPTS = 5
        BASE_RECONNECT_DELAY = 5
        reconnect_attempts = 0
        
        while True:
            try:
                await asyncio.sleep(60)
                
                if not self.client.is_connected():
                    self._is_healthy = False
                    await self._set_state(ConnectionState.RECONNECTING, f"Attempt {reconnect_attempts + 1}/{MAX_RECONNECT_ATTEMPTS}")
                    
                    # Exponential backoff
                    delay = BASE_RECONNECT_DELAY * (2 ** reconnect_attempts)
                    if reconnect_attempts > 0:
                        await asyncio.sleep(delay)
                    
                    try:
                        await self.client.connect()
                        
                        if await self.client.is_user_authorized():
                            self._is_healthy = True
                            reconnect_attempts = 0
                            await self._set_state(ConnectionState.CONNECTED, "Reconnected")
                        else:
                            reconnect_attempts += 1
                            await self._set_state(ConnectionState.INVALID_SESSION, "Not authorized after reconnect")
                    except FloodWaitError as e:
                        await self._set_state(ConnectionState.FLOOD_WAIT, f"Waiting {e.seconds}s")
                        await asyncio.sleep(e.seconds)
                    except Exception as conn_err:
                        reconnect_attempts += 1
                        
                        if reconnect_attempts >= MAX_RECONNECT_ATTEMPTS:
                            await self._set_state(ConnectionState.DISCONNECTED, f"Max attempts reached: {conn_err}")
                            try:
                                from database import log_processing_error
                                await log_processing_error(
                                    error_type='ClientDisconnected',
                                    error_message=f"Max reconnect attempts reached: {conn_err}",
                                    error_context={'session': self.session_name}
                                )
                            except Exception:
                                pass
                            break
                else:
                    # Connected - ensure state is correct
                    if self._state != ConnectionState.CONNECTED:
                        await self._set_state(ConnectionState.CONNECTED)
                    self._is_healthy = True
                    reconnect_attempts = 0
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Health check error: {e}")
                self._is_healthy = False
    
    @property
    def is_healthy(self) -> bool:
        """Returns True if the client is connected and authorized."""
        return self._is_healthy and self.client.is_connected()
    
    async def is_authorized(self) -> bool:
        """Check if the client is authorized."""
        return await self.client.is_user_authorized()

    async def _cleanup_login_messages(self):
        """
        Waits 2 minutes after startup, then deletes messages exchanged with
        the user-defined login bot (e.g., a bot used to facilitate the login process).
        
        The bot ID/username is configured via the LOGIN_BOT_ID environment variable.
        This clears sensitive OTP/login codes from the chat history.
        """
        from config import settings
        login_bot = settings.LOGIN_BOT_ID
        if not login_bot:
            logger.debug("LOGIN_BOT_ID not set. Skipping login message cleanup.")
            return
        
        logger.info(f"Scheduling login message cleanup with bot '{login_bot}' in 2 minutes...")
        await asyncio.sleep(120)  # Wait 2 minutes
        
        try:
            # Get the bot entity (can be username like @MyBot or numeric ID)
            bot_entity = await self.client.get_entity(login_bot)
            
            # Fetch recent messages with this bot
            messages = await self.client.get_messages(bot_entity, limit=20)
            
            if messages:
                # Delete our messages to the bot (we can only delete our own side)
                our_messages = [m for m in messages if m.out]  # m.out = sent by us
                if our_messages:
                    await self.client.delete_messages(bot_entity, our_messages)
                    logger.info(f"Deleted {len(our_messages)} of our messages from chat with '{login_bot}'.")
                
                # Attempt to delete bot's messages (may fail if we lack permissions)
                try:
                    bot_messages = [m for m in messages if not m.out]
                    if bot_messages:
                        await self.client.delete_messages(bot_entity, bot_messages)
                        logger.info(f"Deleted {len(bot_messages)} bot messages from chat with '{login_bot}'.")
                except Exception:
                    logger.debug("Could not delete bot's messages (permission denied or not allowed).")
            else:
                logger.debug(f"No messages found with bot '{login_bot}' to delete.")
                
        except Exception as e:
            logger.warning(f"Failed to cleanup login messages with bot '{login_bot}': {e}")

    async def stop(self):
        """Gracefully disconnect the client."""
        if self._health_task:
            self._health_task.cancel()
            try:
                await self._health_task
            except asyncio.CancelledError:
                pass
        
        await self.client.disconnect()
        self._is_healthy = False
        logger.info("Telegram client disconnected.")


# Convenience function for simple initialization
async def initialize_client(session_name: str = 'user_session') -> TelegramClient:
    """
    Convenience function to initialize and return a connected Telegram client.
    
    Returns:
        A connected and authorized TelegramClient instance.
    """
    manager = TelegramClientManager(session_name=session_name)
    await manager.start()
    return manager.client
