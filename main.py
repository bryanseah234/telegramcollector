import asyncio
import logging
import os
import signal
import sys
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=os.getenv('LOG_LEVEL', 'INFO')
)
logger = logging.getLogger(__name__)

# Import components
from database import validate_configuration, get_db_connection, db_pool
from telegram_client import TelegramClientManager
from message_scanner import MessageScanner
from media_processor import MediaManager
from processing_queue import ProcessingQueue
from face_processor import FaceProcessor
from clustering import FaceClusterer
from worker import queue_worker

async def main():
    # 1. Validate Configuration
    validate_configuration()

    # 2. Initialize Components
    logger.info("Initializing components...")
    
    # Telegram Client
    tg_manager = TelegramClientManager()
    
    # Media & Processing
    media_manager = MediaManager()
    processing_queue = ProcessingQueue()
    face_processor = FaceProcessor()
    clusterer = FaceClusterer()
    
    # Scanner
    scanner = MessageScanner(tg_manager.client, processing_queue, media_manager)
    
    # Worker
    worker = queue_worker(processing_queue, face_processor, clusterer, tg_manager.client)

    # 3. Start Services
    logger.info("Starting services...")
    
    # Start Telegram Client
    await tg_manager.start()
    
    # Start Worker (as background task)
    worker_task = asyncio.create_task(worker.run())
    
    # 4. Discovery & Scanning
    # Get own account ID (or create one in DB)
    me = await tg_manager.client.get_me()
    account_id = await _register_account(me)
    
    # Trigger Discovery (Feature: Scrape All Dialogs)
    logger.info("Triggering dialog discovery...")
    asyncio.create_task(scanner.discover_and_scan_all_chats(account_id))
    
    # Start Realtime Scanning (for all chats in DB)
    # scanner.start_realtime(account_id) # To be implemented or just rely on backfill for now
    
    # 5. Keep Alive
    logger.info("Application is running. Press Ctrl+C to stop.")
    
    try:
        # Wait until disconnected or signal
        await tg_manager.client.run_until_disconnected()
    except (KeyboardInterrupt, SystemExit):
        pass
    finally:
        logger.info("Shutting down...")
        worker_task.cancel()
        if db_pool:
            db_pool.close_all()

async def _register_account(user):
    """
    Registers the current Telegram account in the database.
    """
    async with get_db_connection() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute("""
                INSERT INTO telegram_accounts (phone_number, session_file_path, status)
                VALUES (%s, %s, 'active')
                ON CONFLICT (phone_number) DO UPDATE SET last_active = NOW()
                RETURNING id
            """, (user.phone or 'unknown', 'session'))
            row = await cursor.fetchone()
            # conn.commit() # Not needed if autocommit=True, catch likely true in pool
            # But let's check validation - database.py has autocommit=True.
            return row[0]

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
