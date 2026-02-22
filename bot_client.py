"""
Bot Client - Telegram bot client manager using the Bot Pool for rotation.

Uses the BotPool to transparently rotate between multiple bot credentials.
User accounts are only used for scanning; the bot handles all publishing.
"""
import os
import asyncio
import logging
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

from config import settings


class BotClientManager:
    """
    Manager for the Telegram bot client with pool rotation.
    
    This bot is used for:
    - Creating forum topics in the hub group
    - Uploading media to topic threads
    - Managing topic metadata
    - Bot commands (/status, /pause, etc.)
    
    The pool transparently rotates between healthy bots.
    User sessions are only used for scanning chats.
    """
    
    _instance = None
    _initialized = False
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    @classmethod
    def reset_instance(cls):
        """Resets the singleton instance (used for restarts)."""
        cls._instance = None
        logger.info("BotClientManager singleton reset")
    
    @property
    def client(self):
        """Returns a healthy bot client from the pool."""
        from bot_pool import bot_pool
        try:
            bot_entry = bot_pool.get_bot()
            return bot_entry.client
        except RuntimeError:
            raise RuntimeError("No healthy bots available. Check BotPool status.")
    
    async def start(self):
        """Initializes the bot pool with all configured tokens."""
        if self._initialized:
            return
        
        from bot_pool import bot_pool
        
        bot_configs = settings.parsed_bot_tokens
        if not bot_configs:
            raise ValueError("No bot tokens configured. Set BOT_TOKEN or BOT_TOKENS in .env")
        
        logger.info(f"Starting bot client manager with {len(bot_configs)} bot(s)...")
        await bot_pool.initialize(bot_configs)
        
        self._initialized = True
        logger.info(f"Bot client manager ready with {bot_pool.bot_count} bot(s)")
    
    async def disconnect(self):
        """Disconnects all bots in the pool."""
        from bot_pool import bot_pool
        await bot_pool.shutdown()
        self._initialized = False
        logger.info("Bot client manager disconnected")
    
    def is_ready(self) -> bool:
        """Returns True if at least one bot is connected and healthy."""
        if not self._initialized:
            return False
        from bot_pool import bot_pool
        return len(bot_pool.get_healthy_bots()) > 0

    def register_worker(self, worker_instance):
        """Registers the main worker instance to handle commands on ALL bots."""
        from bot_pool import bot_pool
        
        if not bot_pool.bots:
            logger.error("Cannot register worker: Bot pool not initialized")
            return

        import bot_commands
        from telethon import events

        logger.info(f"Registering bot command handlers on {len(bot_pool.bots)} bot(s)...")

        for bot_entry in bot_pool.bots:
            if not bot_entry.client:
                continue
            
            client = bot_entry.client
            bot_name = bot_entry.name

            @client.on(events.NewMessage(pattern='/status'))
            async def status_handler(event, _name=bot_name):
                await bot_commands.handle_status(event, worker_instance)

            @client.on(events.NewMessage(pattern='/pause'))
            async def pause_handler(event, _name=bot_name):
                await bot_commands.handle_pause(event, worker_instance)

            @client.on(events.NewMessage(pattern='/resume'))
            async def resume_handler(event, _name=bot_name):
                await bot_commands.handle_resume(event, worker_instance)

            @client.on(events.NewMessage(pattern='/restart'))
            async def restart_handler(event, _name=bot_name):
                await bot_commands.handle_restart(event, worker_instance)
            
            @client.on(events.NewMessage(pattern=r'^/(commands|help)'))
            async def help_handler(event, _name=bot_name):
                await bot_commands.handle_help(event, worker_instance)
            
            logger.info(f"  ✓ Commands registered on @{bot_entry.username or bot_name}")
        
        logger.info("Bot commands registered on all bots")


# Global singleton instance
bot_client_manager = BotClientManager()


async def get_bot_client():
    """
    Returns a healthy bot client from the pool.
    
    Raises RuntimeError if no bots are available.
    Use bot_client_manager.start() first.
    """
    if not bot_client_manager.is_ready():
        await bot_client_manager.start()
    return bot_client_manager.client
