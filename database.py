
import logging
import contextlib
import asyncio
import psycopg
from psycopg_pool import AsyncConnectionPool
from config import settings
from resilience import retry_with_jitter, get_circuit_breaker, CircuitOpenError

logger = logging.getLogger(__name__)

class DatabaseManager:
    """
    Database connection pool manager with health monitoring and circuit breaker.
    """
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DatabaseManager, cls).__new__(cls)
            cls._instance.pool = None
            cls._instance._health_task = None
            cls._instance._circuit_breaker = None
        return cls._instance
    
    async def initialize(self):
        """Initializes the async connection pool if not already initialized."""
        if self.pool is None:
            await self._initialize_pool()
            # Start health monitor
            self._health_task = asyncio.create_task(self._pool_health_monitor())
            # Initialize circuit breaker
            self._circuit_breaker = get_circuit_breaker('database')
            # FORCE RESET: Ensure we start with a closed circuit on new initialization
            # This prevents "Death Spiral" where a restart inherits an OPEN state
            if self._circuit_breaker:
                self._circuit_breaker.reset()
    
    async def _initialize_pool(self):
        """Creates an async connection pool."""
        try:
            conn_str = (
                f"host={settings.DB_HOST} "
                f"port={settings.DB_PORT} "
                f"dbname={settings.DB_NAME} "
                f"user={settings.DB_USER} "
                f"password={settings.DB_PASSWORD}"
            )
            
            self.pool = AsyncConnectionPool(
                conninfo=conn_str,
                min_size=1,
                max_size=10,
                open=False,
                kwargs={'autocommit': True}
            )
            await self.pool.open()
            logger.info("Async database connection pool initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize database pool: {e}")
            raise
    
    async def _pool_health_monitor(self):
        """
        Periodically monitors pool health and recovers stale connections.
        Runs every 5 minutes.
        """
        while True:
            try:
                await asyncio.sleep(300)  # Check every 5 minutes
                
                if self.pool is None:
                    continue
                
                # Check pool statistics
                stats = self.pool.get_stats()
                pool_size = stats.get('pool_size', 0)
                pool_available = stats.get('pool_available', 0)
                requests_waiting = stats.get('requests_waiting', 0)
                
                logger.debug(
                    f"Pool health: size={pool_size}, available={pool_available}, "
                    f"waiting={requests_waiting}"
                )
                
                # If too many waiting requests, try to recover
                if requests_waiting > 5:
                    logger.warning(f"High connection wait queue ({requests_waiting}). Attempting pool recovery...")
                    await self._recover_pool()
                
                # Test a connection
                try:
                    async with self.pool.connection() as conn:
                        await asyncio.wait_for(conn.execute("SELECT 1"), timeout=5.0)
                    logger.debug("Pool health check passed")
                except Exception as e:
                    logger.warning(f"Pool health check failed: {e}. Attempting recovery...")
                    await self._recover_pool()
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                # Break loop if event loop is closed to prevent infinite log spam
                if "no running event loop" in str(e) or isinstance(e, RuntimeError):
                    logger.error("Pool health monitor stopping: no running event loop")
                    break
                logger.error(f"Pool health monitor error: {e}")
                # Wait before retrying to verify it's not a tight loop
                try:
                    await asyncio.sleep(5)
                except:
                    break
    
    async def _recover_pool(self):
        """Attempts to recover the connection pool by resizing or recreating."""
        try:
            if self.pool:
                # Try to resize the pool to force new connections
                logger.info("Attempting pool recovery...")
                
                # Close idle connections and let pool recreate them
                # This is a soft recovery approach
                await self.pool.check()
                
                logger.info("Pool recovery completed")
        except Exception as e:
            logger.error(f"Pool recovery failed: {e}")
            # If recovery fails, try full reinitialization
            try:
                if self.pool:
                    await self.pool.close()
                    self.pool = None
                await self._initialize_pool()
                logger.info("Pool reinitialized after recovery failure")
            except Exception as reinit_err:
                logger.error(f"Pool reinitialization failed: {reinit_err}")

    async def close(self):
        """Closes the connection pool and stops health monitor."""
        if self._health_task:
            self._health_task.cancel()
            try:
                await self._health_task
            except asyncio.CancelledError:
                pass
        
        if self.pool:
            try:
                await self.pool.close()
            except asyncio.CancelledError:
                logger.info("Database pool close was cancelled (expected during shutdown)")
            except Exception as e:
                logger.error(f"Error closing DB pool: {e}")
            finally:
                self.pool = None
            logger.info("Database pool closed")

# Global instance
db_manager = DatabaseManager()

@contextlib.asynccontextmanager
async def get_db_connection(max_retries: int = 3, retry_delay: float = 1.0):
    """
    Async context manager for getting a database connection with circuit breaker protection.
    
    Args:
        max_retries: Maximum number of connection attempts
        retry_delay: Base delay between retries (exponential backoff applied)
    
    Usage:
        async with get_db_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(...)
                result = await cur.fetchone()
    
    Raises:
        CircuitOpenError: If database circuit breaker is open
        psycopg.OperationalError: If connection fails after retries
    """
    if db_manager.pool is None:
        await db_manager.initialize()
    
    # Check circuit breaker
    circuit = db_manager._circuit_breaker
    if circuit:
        from resilience import CircuitState
        if circuit.state == CircuitState.OPEN:
            logger.warning("Database circuit breaker is OPEN - failing fast")
            raise CircuitOpenError("Database circuit breaker is open - too many failures")
    
    last_error = None
    for attempt in range(max_retries):
        try:
            async with db_manager.pool.connection() as conn:
                # Record success with circuit breaker
                if circuit:
                    await circuit._on_success()
                yield conn
                return
        except psycopg.OperationalError as e:
            last_error = e
            # Record failure with circuit breaker
            if circuit:
                await circuit._on_failure()
            
            if attempt < max_retries - 1:
                delay = retry_delay * (2 ** attempt)
                logger.warning(f"DB connection failed (attempt {attempt + 1}/{max_retries}): {e}. Retrying in {delay}s...")
                await asyncio.sleep(delay)
            else:
                logger.error(f"DB connection failed after {max_retries} attempts: {e}")
                raise
        except Exception as e:
            # Record failure for unexpected errors too
            if circuit:
                await circuit._on_failure()
            logger.error(f"DB error (not retrying): {e}")
            raise
    
    if last_error:
        raise last_error

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

async def log_processing_error(error_type: str, error_message: str, error_context: dict = None):
    """
    Logs a processing error to the database for dashboard display.
    
    Args:
        error_type: Category of error (e.g., 'FaceDetection', 'MediaDownload')
        error_message: Detailed error description
        error_context: Optional dictionary with extra context (chat_id, message_id, etc.)
    """
    try:
        import json
        context_json = json.dumps(error_context) if error_context else None
        
        async with get_db_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("""
                    INSERT INTO processing_errors 
                        (error_type, error_message, error_context)
                    VALUES (%s, %s, %s)
                """, (error_type, str(error_message), context_json))
    except Exception as e:
        # Fallback to logger if DB logging fails
        logger.error(f"Failed to log error to DB: {e}")

