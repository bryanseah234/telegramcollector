from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional, Any

class Settings(BaseSettings):
    # Telegram API
    TG_API_ID: int
    TG_API_HASH: str
    BOT_TOKEN: str
    
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
    MIN_QUALITY_THRESHOLD: float = 0.5
    MAX_MEDIA_SIZE_MB: int = 50
    NUM_WORKERS: int = 3
    QUEUE_MAX_SIZE: int = 500  # Backpressure limit (items) - increases with RAM
    USE_GPU: bool = False
    
    # Operational
    RUN_MODE: str = "both"  # backfill, realtime, both
    HEALTH_CHECK_INTERVAL: int = 1800
    LOGIN_BOT_ID: Optional[str] = None
    SESSIONS_DIR: str = "sessions"  # Directory for session files
    
    # Resilience
    CIRCUIT_BREAKER_THRESHOLD: int = 5  # Failures before opening circuit
    CIRCUIT_BREAKER_TIMEOUT: int = 60   # Seconds before retry (OPEN -> HALF_OPEN)
    MAX_RETRY_ATTEMPTS: int = 3
    RETRY_BASE_DELAY: float = 1.0
    
    # Hub Notifications
    HUB_NOTIFY_BATCH_INTERVAL: int = 60  # Seconds between batched notifications
    HUB_NOTIFY_RATE_LIMIT: int = 10      # Max messages per minute
    NOTIFY_ON_NEW_IDENTITY: bool = True
    NOTIFY_ON_SCAN_MILESTONE: bool = True
    NOTIFY_MILESTONE_INTERVAL: int = 1000  # Messages between milestones
    
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

try:
    import redis
except ImportError:
    redis = None

def get_dynamic_setting(key: str, default: Any) -> Any:
    """
    Fetches a setting with priority: Redis > Env/Config
    """
    if redis is None:
        return default
        
    try:
        # Use a separate client or reuse one if possible, but for config
        # a new short-lived connection is safer to avoid circular imports of existing clients
        r = redis.Redis(
            host=settings.REDIS_HOST,
            port=settings.REDIS_PORT,
            db=settings.REDIS_DB,
            password=settings.REDIS_PASSWORD,
            decode_responses=True,
            socket_connect_timeout=1
        )
        val = r.get(f"config:{key}")
        if val is not None:
            # Convert type based on default
            if isinstance(default, bool):
                return val.lower() == "true"
            if isinstance(default, int):
                return int(val)
            if isinstance(default, float):
                return float(val)
            return val
    except Exception:
        pass
    return default

def set_dynamic_setting(key: str, value: Any):
    """Sets a dynamic setting in Redis."""
    if redis is None:
        print("Redis not installed, cannot set dynamic setting")
        return

    try:
        r = redis.Redis(
            host=settings.REDIS_HOST,
            port=settings.REDIS_PORT,
            db=settings.REDIS_DB,
            password=settings.REDIS_PASSWORD,
            decode_responses=True
        )
        r.set(f"config:{key}", str(value))
    except Exception as e:
        print(f"Failed to set dynamic setting: {e}")
