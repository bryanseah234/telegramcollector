
import logging
import contextlib
import psycopg
from psycopg_pool import AsyncConnectionPool
from config import settings

logger = logging.getLogger(__name__)

class DatabaseManager:
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DatabaseManager, cls).__new__(cls)
            cls._instance.pool = None
        return cls._instance
    
    async def initialize(self):
        """Initializes the async connection pool if not already initialized."""
        if self.pool is None:
            await self._initialize_pool()
    
    async def _initialize_pool(self):
        """Creates an async connection pool."""
        try:
            # Connection string
            conn_str = (
                f"host={settings.DB_HOST} "
                f"port={settings.DB_PORT} "
                f"dbname={settings.DB_NAME} "
                f"user={settings.DB_USER} "
                f"password={settings.DB_PASSWORD}"
            )
            
            self.pool = AsyncConnectionPool(
                conninfo=conn_str,
                min_size=5,
                max_size=20,
                open=False, # Don't open immediately, open in context or below
                kwargs={
                    'autocommit': True
                }
            )
            await self.pool.open()
            logger.info("Async database connection pool initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize database pool: {e}")
            raise

    async def close(self):
        """Closes the connection pool."""
        if self.pool:
            await self.pool.close()
            logger.info("Database pool closed")

# Global instance
db_manager = DatabaseManager()

@contextlib.asynccontextmanager
async def get_db_connection():
    """
    Async context manager for getting a database connection.
    Usage:
        async with get_db_connection() as conn:
            await conn.execute(...)
            
    Note: In psycopg 3, usage is slightly different:
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(...)
            result = await cur.fetchone()
    """
    if db_manager.pool is None:
        await db_manager.initialize()
        
    async with db_manager.pool.connection() as conn:
        yield conn

async def init_db():
    """Initializes the database schema."""
    async with get_db_connection() as conn:
        try:
            with open('init-db.sql', 'r') as f:
                schema = f.read()
            
            # Execute schema
            await conn.execute(schema)
            logger.info("Database schema initialized.")
        except Exception as e:
            logger.error(f"Database initialization failed: {e}")

async def check_db_health():
    """Checks if database is responsive."""
    try:
        async with get_db_connection() as conn:
            await conn.execute("SELECT 1")
        return True
    except Exception:
        return False
