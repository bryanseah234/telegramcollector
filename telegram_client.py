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
from database import get_db_connection

logger = logging.getLogger(__name__)


class TelegramClientManager:
    """
    Manages the Telethon client connection and session.
    Handles initialization, authorization, health monitoring, and cleanup.
    """
    
    def __init__(self, session_name: str = 'user_session', api_id: int = None, api_hash: str = None):
        from config import settings
        self.api_id = api_id or settings.TG_API_ID
        self.api_hash = api_hash or settings.TG_API_HASH
        self.session_name = session_name
        self._health_task = None
        self._is_healthy = False
        
        # Ensure session directory exists (use absolute path to match login_bot if possible, but relative works if CWD is /app)
        # Note: login_bot now uses /app/sessions. worker.py also runs in /app.
        # But let's log to be sure.
        os.makedirs('sessions', exist_ok=True)
        session_path = os.path.join('sessions', session_name)
        
        # Debug path
        logger.debug(f"Initializing client with session path: {os.path.abspath(session_path)}")
        
        self.client = TelegramClient(session_path, self.api_id, self.api_hash)
    
    async def start(self, phone: str = None):
        """
        Starts the Telegram client and verifies authorization.
        
        Args:
            phone: Phone number for first-time authorization (optional if session exists)
        """
        try:
            await self.client.connect()
            
            # Check authorization status
            if not await self.client.is_user_authorized():
                if phone:
                    logger.info(f"Authorizing with phone: {phone}")
                    await self.client.start(phone=phone)
                else:
                    # Prompt for phone in interactive mode
                    await self.client.start()
            
            # Verify we're logged in
            me = await self.client.get_me()
            if me:
                logger.info(f"Telegram client started. Logged in as: {me.first_name} (@{me.username or 'N/A'})")
                self._is_healthy = True
            else:
                raise RuntimeError("Failed to get user info after authorization")
            
            # Start health monitoring
            self._health_task = asyncio.create_task(self._health_monitor())
            
            # Schedule auto-cleanup of login messages
            asyncio.create_task(self._cleanup_login_messages())
            
            return me

        except (SessionRevokedError, AuthKeyError, UserDeactivatedError) as e:
            logger.error(f"Session is invalid/revoked: {e}")
            await self._handle_invalid_session()
            raise  # Re-raise so the caller (worker.py) knows to skip this account
            
        except Exception as e:
            logger.error(f"Failed to start telegram client: {e}")
            raise

    async def _handle_invalid_session(self):
        """
        Handles cleanup for invalid sessions:
        1. Updates DB status to 'invalid'
        2. Deletes the local .session file
        """
        try:
            # 1. Update Database
            # We need to construct the likely path stored in DB to match it.
            # self.session_name is just the name (e.g., 'account_123')
            # The DB stores 'sessions/account_123.session' usually.
            
            # Construct standard path
            session_file_path = os.path.join('sessions', f"{self.session_name}.session")
            
            # Also try without extension just in case
            
            async with get_db_connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("""
                        UPDATE telegram_accounts 
                        SET status = 'invalid', last_error = 'Session revoked/invalid'
                        WHERE session_file_path = %s 
                           OR session_file_path = %s
                    """, (session_file_path, os.path.join('sessions', self.session_name)))
                    await conn.commit()
            
            logger.info("Marked account as invalid in database.")

            # 2. Delete Local File
            full_path = f"{session_file_path}"
            if os.path.exists(full_path):
                os.remove(full_path)
                logger.info(f"Deleted invalid session file: {full_path}")
            else:
                # Telethon might append .session automatically if we passed a path without it
                # check if self.client.session.filename is available? 
                # For now, just try deleting with .session extension
                pass

        except Exception as e:
            logger.error(f"Error during invalid session cleanup: {e}")
    
    async def _health_monitor(self):
        """
        Periodically checks connection health and reconnects if needed.
        """
        while True:
            try:
                await asyncio.sleep(60)  # Check every minute
                
                if not self.client.is_connected():
                    logger.warning("Connection lost. Attempting to reconnect...")
                    self._is_healthy = False
                    await self.client.connect()
                    
                    if await self.client.is_user_authorized():
                        self._is_healthy = True
                        logger.info("Reconnected successfully.")
                    else:
                        logger.error("Reconnected but not authorized. Session may have expired.")
                else:
                    self._is_healthy = True
                    
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
