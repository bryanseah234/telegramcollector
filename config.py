from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional, Any, List, Dict

class Settings(BaseSettings):
    # Telegram API
    TG_API_ID: int
    TG_API_HASH: str
    BOT_TOKEN: str = ""  # Optional if BOT_TOKENS is set
    BOT_TOKENS: str = ""  # Semicolon-separated name:token pairs
    
    # Hub Configuration
    HUB_GROUP_ID: int
    
    # Database (Postgres)
    DB_HOST: str = "postgres"
    DB_PORT: int = 5432
    DB_NAME: str = "face_archiver"
    DB_USER: str = "postgres"
    DB_PASSWORD: str
    
    # Redis (New)
    REDIS_HOST: str = "redis"
    REDIS_PORT: int = 6379
    REDIS_DB: int = 0
    REDIS_PASSWORD: Optional[str] = None
    
    # Processing
    SIMILARITY_THRESHOLD: float = 0.55
    MIN_QUALITY_THRESHOLD: float = 0.67  # Stricter quality check (was 0.5)
    MAX_MEDIA_SIZE_MB: int = 50
    NUM_WORKERS: int = 6
    WORKER_TASK_TIMEOUT: int = 300
    QUEUE_MAX_SIZE: int = 4000  # Backpressure limit (items) - increases with RAM
    USE_GPU: bool = True
    
    # Operational
    RUN_MODE: str = "both"  # backfill, realtime, both
    HEALTH_CHECK_INTERVAL: int = 300
    LOGIN_BOT_ID: Optional[str] = None
    SESSIONS_DIR: str = "sessions"  # Directory for session files
    
    # Story Scanning
    STORY_SCAN_INTERVAL: int = 300      # 5 minutes (stories expire in 24h)
    STORY_SCAN_ENABLED: bool = True     # Can disable via env
    STORY_PRIORITY_BOOST: int = 10      # Priority offset for story tasks
    
    # Resilience
    CIRCUIT_BREAKER_THRESHOLD: int = 5  # Failures before opening circuit
    CIRCUIT_BREAKER_TIMEOUT: int = 60   # Seconds before retry (OPEN -> HALF_OPEN)
    MAX_RETRY_ATTEMPTS: int = 3
    RETRY_BASE_DELAY: float = 1.0
    
    # Hub Notifications
    HUB_NOTIFY_BATCH_INTERVAL: int = 30  # Seconds between batched notifications
    HUB_NOTIFY_RATE_LIMIT: int = 100     # Max messages per minute
    NOTIFY_ON_NEW_IDENTITY: bool = True
    NOTIFY_ON_SCAN_MILESTONE: bool = True
    NOTIFY_MILESTONE_INTERVAL: int = 500  # Messages between milestones
    
    # Observability
    ENABLE_PROMETHEUS: bool = True
    PROMETHEUS_PORT: int = 8000
    LOG_FORMAT: str = "json"  # "json" or "text"
    
    # Compatibility aliases
    @property
    def TELEGRAM_API_ID(self) -> int:
        return self.TG_API_ID
    
    @property
    def TELEGRAM_API_HASH(self) -> str:
        return self.TG_API_HASH
    
    @property
    def parsed_bot_tokens(self) -> List[Dict[str, str]]:
        """Parses BOT_TOKENS into a list of {'name': ..., 'token': ...} dicts.
        Falls back to BOT_TOKEN if BOT_TOKENS is empty."""
        if self.BOT_TOKENS and self.BOT_TOKENS.strip():
            result = []
            for entry in self.BOT_TOKENS.split(';'):
                entry = entry.strip()
                if not entry:
                    continue
                # Format: BotName:bot_id:bot_secret (name:token where token contains a colon)
                parts = entry.split(':', 1)
                if len(parts) == 2:
                    result.append({'name': parts[0].strip(), 'token': parts[1].strip()})
                else:
                    # Just a raw token without name
                    result.append({'name': f'bot_{len(result)+1}', 'token': entry})
            if result:
                return result
        # Fallback to single BOT_TOKEN
        return [{'name': 'default', 'token': self.BOT_TOKEN}]
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
        extra="ignore"  # Ignore extra env vars (like DOTENV_KEY)
    )

# Global settings instance
try:
    settings = Settings()
except Exception as e:
    import sys
    print("\n" + "="*60)
    print("CRITICAL CONFIGURATION ERROR")
    print("="*60)
    print(f"Error details: {e}")
    # ... error handling ...
    sys.exit(1)

# Global Redis client for dynamic settings
_redis_config_client = None

def _get_redis_client():
    """Returns a cached Redis client for config lookups."""
    global _redis_config_client
    if _redis_config_client is None:
        try:
            import redis
            _redis_config_client = redis.Redis(
                host=settings.REDIS_HOST,
                port=settings.REDIS_PORT,
                db=settings.REDIS_DB,
                password=settings.REDIS_PASSWORD,
                decode_responses=True,
                socket_timeout=1,
                socket_connect_timeout=1
            )
        except ImportError:
            pass
    return _redis_config_client

def get_dynamic_setting(key: str, default: Any = None) -> Any:
    """
    Fetches a setting from Redis (dynamic) or falls back to env/default.
    Uses a cached Redis connection to check for overrides.
    """
    # 1. Check Redis for override
    try:
        redis_client = _get_redis_client()
        if redis_client:
            redis_key = f"config:{key}"
            value = redis_client.get(redis_key)
            if value is not None:
                # Attempt type conversion if default is provided
                if default is not None:
                    if isinstance(default, bool):
                        return str(value).lower() == 'true'
                    if isinstance(default, int):
                        return int(value)
                    if isinstance(default, float):
                        return float(value)
                return value
    except Exception:
        pass  # Fallback to static setting
        
    # 2. Return Static/Env Value
    return getattr(settings, key, default)

def set_dynamic_setting(key: str, value: Any):
    """Sets a dynamic setting in Redis."""
    try:
        redis_client = _get_redis_client()
        if redis_client:
            redis_key = f"config:{key}"
            redis_client.set(redis_key, str(value))
            return True
    except Exception:
        return False
    return False
