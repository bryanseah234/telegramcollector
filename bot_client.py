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
        self._client = TelegramClient('bot_publisher', API_ID, API_HASH)
        
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
