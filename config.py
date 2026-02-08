from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional

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
    USE_GPU: bool = False
    
    # Operational
    RUN_MODE: str = "both"  # backfill, realtime, both
    HEALTH_CHECK_INTERVAL: int = 1800
    LOGIN_BOT_ID: Optional[str] = None
    
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
    print("\nThis usually means a required environment variable is missing.")
    print("Please check your .env file against .env.template.")
    print("Common missing variables: TG_API_ID, TG_API_HASH, BOT_TOKEN")
    print("="*60 + "\n")
    # Choosing to exit here is safer than running with broken config
    sys.exit(1)
