"""
Bot Pool - Manages multiple Telegram bot credentials with rotation and health tracking.

Features:
- Round-robin rotation across healthy bots
- Automatic lockout on FloodWait/errors with timed recovery
- Fallback recommendations when a bot is locked (for login flow)
- Background health monitor to auto-unlock expired lockouts
"""
import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import List, Optional, Dict
from enum import Enum

logger = logging.getLogger(__name__)


class BotStatus(Enum):
    HEALTHY = 'healthy'
    LOCKED = 'locked'
    ERROR = 'error'
    DISCONNECTED = 'disconnected'


@dataclass
class BotEntry:
    """Represents a single bot in the pool."""
    name: str
    token: str
    client: object = None  # TelegramClient instance
    username: str = None   # @username resolved after connect
    status: BotStatus = BotStatus.DISCONNECTED
    locked_until: float = 0.0  # Unix timestamp
    lock_reason: str = ""
    error_count: int = 0
    
    @property
    def is_available(self) -> bool:
        """Returns True if the bot is healthy or its lockout has expired."""
        if self.status == BotStatus.HEALTHY:
            return True
        if self.status == BotStatus.LOCKED and time.time() >= self.locked_until:
            return True
        return False


class BotPool:
    """
    Singleton pool manager for multiple Telegram bots.
    
    Provides transparent rotation — callers get a healthy bot client
    without needing to know about the pool internals.
    """
    
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    @classmethod
    def reset_instance(cls):
        """Resets the singleton (used for testing/restarts)."""
        cls._instance = None
    
    def __init__(self):
        if self._initialized:
            return
        self._bots: List[BotEntry] = []
        self._current_index: int = 0
        self._lock = asyncio.Lock()
        self._health_task: Optional[asyncio.Task] = None
        self._running = False
        self._initialized = True
    
    @property
    def bots(self) -> List[BotEntry]:
        return self._bots
    
    @property
    def bot_count(self) -> int:
        return len(self._bots)
    
    async def initialize(self, bot_configs: List[Dict[str, str]]):
        """
        Initializes all bots from config and connects them.
        
        Args:
            bot_configs: List of {'name': ..., 'token': ...} dicts
        """
        from telethon import TelegramClient
        from config import settings
        
        api_id = settings.TG_API_ID
        api_hash = settings.TG_API_HASH
        
        for i, config in enumerate(bot_configs):
            name = config['name']
            token = config['token']
            
            # Create unique session name per bot
            session_name = f"bot_{name.lower().replace('@', '').replace(' ', '_')}"
            
            entry = BotEntry(name=name, token=token)
            
            try:
                client = TelegramClient(session_name, api_id, api_hash)
                await client.start(bot_token=token)
                
                me = await client.get_me()
                entry.client = client
                entry.username = me.username
                entry.status = BotStatus.HEALTHY
                
                logger.info(f"✓ Bot pool: Connected @{me.username} ({name})")
                
            except Exception as e:
                logger.error(f"✗ Bot pool: Failed to connect {name}: {e}")
                entry.status = BotStatus.ERROR
                entry.lock_reason = str(e)
            
            self._bots.append(entry)
        
        healthy_count = sum(1 for b in self._bots if b.status == BotStatus.HEALTHY)
        logger.info(f"Bot pool initialized: {healthy_count}/{len(self._bots)} bots healthy")
        
        if healthy_count == 0:
            raise RuntimeError("No bots could connect. Check your BOT_TOKENS configuration.")
        
        # Start background health monitor
        self._running = True
        self._health_task = asyncio.create_task(self._health_monitor())
    
    def get_bot(self) -> BotEntry:
        """
        Returns the next healthy bot using round-robin rotation.
        
        Raises:
            RuntimeError: If no healthy bots are available
        """
        if not self._bots:
            raise RuntimeError("Bot pool not initialized")
        
        # Auto-unlock expired lockouts
        now = time.time()
        for bot in self._bots:
            if bot.status == BotStatus.LOCKED and now >= bot.locked_until:
                logger.info(f"🔓 Auto-unlocked bot @{bot.username} ({bot.name})")
                bot.status = BotStatus.HEALTHY
                bot.lock_reason = ""
                bot.error_count = 0
        
        # Try round-robin starting from current index
        attempts = len(self._bots)
        for _ in range(attempts):
            bot = self._bots[self._current_index]
            self._current_index = (self._current_index + 1) % len(self._bots)
            
            if bot.is_available and bot.client:
                return bot
        
        raise RuntimeError(
            "All bots are locked or errored. "
            f"Bots: {[f'{b.name}({b.status.value})' for b in self._bots]}"
        )
    
    def get_bot_by_name(self, name: str) -> Optional[BotEntry]:
        """Returns a specific bot by name."""
        for bot in self._bots:
            if bot.name == name:
                return bot
        return None
    
    def get_bot_by_username(self, username: str) -> Optional[BotEntry]:
        """Returns a specific bot by @username."""
        clean = username.lstrip('@')
        for bot in self._bots:
            if bot.username and bot.username.lower() == clean.lower():
                return bot
        return None
    
    def mark_locked(self, name: str, duration: int = 60, reason: str = ""):
        """
        Marks a bot as locked for a specified duration.
        
        Args:
            name: Bot name or username
            duration: Lockout duration in seconds
            reason: Human-readable lock reason
        """
        bot = self.get_bot_by_name(name)
        if not bot:
            bot = self.get_bot_by_username(name)
        
        if bot:
            bot.status = BotStatus.LOCKED
            bot.locked_until = time.time() + duration
            bot.lock_reason = reason
            bot.error_count += 1
            logger.warning(
                f"🔒 Bot @{bot.username} ({bot.name}) locked for {duration}s: {reason}"
            )
    
    def mark_healthy(self, name: str):
        """Marks a bot as healthy."""
        bot = self.get_bot_by_name(name)
        if not bot:
            bot = self.get_bot_by_username(name)
        
        if bot:
            bot.status = BotStatus.HEALTHY
            bot.locked_until = 0.0
            bot.lock_reason = ""
            logger.info(f"✅ Bot @{bot.username} ({bot.name}) marked healthy")
    
    def get_healthy_bots(self) -> List[BotEntry]:
        """Returns all currently available bots."""
        now = time.time()
        return [
            b for b in self._bots
            if b.status == BotStatus.HEALTHY 
            or (b.status == BotStatus.LOCKED and now >= b.locked_until)
        ]
    
    def get_recommendation(self, exclude_name: str = None) -> Optional[str]:
        """
        Returns the @username of another healthy bot (for fallback messages).
        
        Args:
            exclude_name: Name or username of bot to exclude from recommendations
            
        Returns:
            "@username" string or None if no alternatives available
        """
        for bot in self._bots:
            if bot.is_available and bot.username:
                # Skip the excluded bot
                if exclude_name:
                    if (bot.name == exclude_name or 
                        bot.username.lower() == exclude_name.lstrip('@').lower()):
                        continue
                return f"@{bot.username}"
        return None
    
    def get_all_tokens(self) -> List[Dict[str, str]]:
        """Returns all bot configs as list of name/token dicts."""
        return [{'name': b.name, 'token': b.token, 'username': b.username} for b in self._bots]
    
    async def _health_monitor(self):
        """Background task that auto-unlocks expired lockouts and checks bot health."""
        while self._running:
            try:
                await asyncio.sleep(30)  # Check every 30 seconds
                
                now = time.time()
                for bot in self._bots:
                    # Auto-unlock expired lockouts
                    if bot.status == BotStatus.LOCKED and now >= bot.locked_until:
                        logger.info(f"🔓 Health monitor: Auto-unlocked @{bot.username} ({bot.name})")
                        bot.status = BotStatus.HEALTHY
                        bot.lock_reason = ""
                    
                    # Check if healthy bot is still connected
                    if bot.status == BotStatus.HEALTHY and bot.client:
                        try:
                            if not bot.client.is_connected():
                                logger.warning(f"⚠️ Bot @{bot.username} disconnected, reconnecting...")
                                await bot.client.connect()
                                if bot.client.is_connected():
                                    logger.info(f"✅ Bot @{bot.username} reconnected")
                                else:
                                    bot.status = BotStatus.ERROR
                                    bot.lock_reason = "Failed to reconnect"
                        except Exception as e:
                            logger.error(f"Health check failed for @{bot.username}: {e}")
                            
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Bot pool health monitor error: {e}")
    
    async def shutdown(self):
        """Disconnects all bots and stops the health monitor."""
        self._running = False
        
        if self._health_task:
            self._health_task.cancel()
            try:
                await self._health_task
            except asyncio.CancelledError:
                pass
        
        for bot in self._bots:
            if bot.client:
                try:
                    await bot.client.disconnect()
                    logger.info(f"Disconnected bot @{bot.username} ({bot.name})")
                except Exception as e:
                    logger.debug(f"Error disconnecting bot {bot.name}: {e}")
        
        self._bots.clear()
        self._initialized = False
        logger.info("Bot pool shutdown complete")
    
    def get_status_report(self) -> str:
        """Returns a formatted status report of all bots."""
        lines = [f"**Bot Pool** ({len(self._bots)} bots):"]
        for bot in self._bots:
            status_icon = {
                BotStatus.HEALTHY: '✅',
                BotStatus.LOCKED: '🔒',
                BotStatus.ERROR: '❌',
                BotStatus.DISCONNECTED: '⚪',
            }.get(bot.status, '❓')
            
            line = f"  {status_icon} @{bot.username or '???'} ({bot.name})"
            if bot.status == BotStatus.LOCKED:
                remaining = max(0, int(bot.locked_until - time.time()))
                line += f" - locked {remaining}s: {bot.lock_reason}"
            elif bot.status == BotStatus.ERROR:
                line += f" - {bot.lock_reason}"
            lines.append(line)
        
        return "\n".join(lines)


# Global singleton
bot_pool = BotPool()
