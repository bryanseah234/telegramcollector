import asyncio
import logging
from database import db_manager, get_db_connection

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def verify_database():
    logger.info("Starting Async Database Verification...")
    
    try:
        # 1. Initialize Pool
        await db_manager.initialize()
        logger.info("✅ Database pool initialized")
        
        # 2. Test Connection and Query
        async with get_db_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT 1")
                result = await cur.fetchone()
                if result and result[0] == 1:
                    logger.info("✅ Simple SELECT 1 query successful")
                else:
                    logger.error("❌ SELECT 1 query failed or returned unexpected result")
                    
                # 3. Test Table Access (optional, check if tables exist)
                await cur.execute("""
                    SELECT count(*) FROM information_schema.tables 
                    WHERE table_schema = 'public'
                """)
                table_count = (await cur.fetchone())[0]
                logger.info(f"✅ Found {table_count} tables in public schema")
                
    except Exception as e:
        logger.error(f"❌ Verification failed: {e}")
    finally:
        # 4. cleanup
        await db_manager.close()
        logger.info("✅ Database pool closed")

if __name__ == "__main__":
    asyncio.run(verify_database())
