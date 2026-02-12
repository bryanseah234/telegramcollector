"""
Bot Client - Singleton Telegram bot client for topic management and media publishing.

Uses the BOT_TOKEN to authenticate as the bot (not user accounts).
User accounts are only used for scanning; the bot handles all publishing.
"""
import os
import asyncio
import logging
from dotenv import load_dotenv
from telethon import TelegramClient

load_dotenv()

logger = logging.getLogger(__name__)

from config import settings

# Configuration
BOT_TOKEN = settings.BOT_TOKEN
API_ID = settings.TG_API_ID
API_HASH = settings.TG_API_HASH


class BotClientManager:
    """
    Singleton manager for the Telegram bot client.
    
    This bot is used for:
    - Creating forum topics in the hub group
    - Uploading media to topic threads
    - Managing topic metadata
    
    User sessions are only used for scanning chats.
    """
    
    _instance = None
    _client = None
    _initialized = False
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    @classmethod
    def reset_instance(cls):
        """Resets the singleton instance (used for restarts)."""
        if cls._instance:
            if cls._instance._client and cls._instance._client.is_connected():
                # We can't await here (sync method), but we assume disconnect() was called
                pass
            cls._instance = None
            logger.info("BotClientManager singleton reset")
    
    @property
    def client(self) -> TelegramClient:
        """Returns the bot client."""
        if self._client is None:
            raise RuntimeError("Bot client not initialized. Call start() first.")
        return self._client
    
    async def start(self):
        """Initializes and connects the bot client."""
        if self._initialized:
            return
        
        if not BOT_TOKEN:
            raise ValueError("BOT_TOKEN environment variable is required")
        
        if not API_ID or not API_HASH:
            raise ValueError("TG_API_ID and TG_API_HASH environment variables are required")
        
        from telethon.errors import FloodWaitError
        
        # Create bot client
        session_name = os.getenv('BOT_SESSION_NAME', 'bot_publisher')
        self._client = TelegramClient(session_name, API_ID, API_HASH)
        
        try:
            await self._client.start(bot_token=BOT_TOKEN)
            me = await self._client.get_me()
            logger.info(f"Bot client connected: @{me.username}")
            self._initialized = True
            
        except FloodWaitError as e:
            wait_time = e.seconds
            logger.warning(f"⏳ Bot hit rate limit. Waiting {wait_time}s ({wait_time//60}min) before retry...")
            await asyncio.sleep(wait_time)
            # Retry after waiting
            await self._client.start(bot_token=BOT_TOKEN)
            me = await self._client.get_me()
            logger.info(f"Bot client connected after FloodWait: @{me.username}")
            self._initialized = True
            
        except Exception as e:
            logger.error(f"Failed to start bot client: {e}")
            raise
    
    async def disconnect(self):
        """Disconnects the bot client."""
        if self._client:
            await self._client.disconnect()
            self._initialized = False
            logger.info("Bot client disconnected")
    
    def is_ready(self) -> bool:
        """Returns True if the bot client is connected and ready."""
        return self._initialized and self._client and self._client.is_connected()

    def register_worker(self, worker_instance):
        """Registers the main worker instance to handle commands."""
        if not self._client:
            logger.error("Cannot register worker: Bot client not initialized")
            return

        # Avoid circular imports
        import bot_commands
        from telethon import events

        logger.info("Registering bot command handlers...")

        @self._client.on(events.NewMessage(pattern='/status'))
        async def status_handler(event):
            await bot_commands.handle_status(event, worker_instance)

        @self._client.on(events.NewMessage(pattern='/pause'))
        async def pause_handler(event):
            await bot_commands.handle_pause(event, worker_instance)

        @self._client.on(events.NewMessage(pattern='/resume'))
        async def resume_handler(event):
            await bot_commands.handle_resume(event, worker_instance)

        @self._client.on(events.NewMessage(pattern='/restart'))
        async def restart_handler(event):
            await bot_commands.handle_restart(event, worker_instance)
        
        @self._client.on(events.NewMessage(pattern=r'^/(commands|help)'))
        async def help_handler(event):
            await bot_commands.handle_help(event, worker_instance)
            
        logger.info("Bot commands registered successfully")


# Global singleton instance
bot_client_manager = BotClientManager()


async def get_bot_client() -> TelegramClient:
    """
    Returns the initialized bot client.
    
    Raises RuntimeError if bot is not initialized.
    Use bot_client_manager.start() first.
    """
    if not bot_client_manager.is_ready():
        await bot_client_manager.start()
    return bot_client_manager.client
