import asyncio
import logging
import os
import signal
import sys
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging and observability
from config import settings
from observability import setup_logging, start_metrics_server

# Setup structured logging
setup_logging(log_level=os.getenv('LOG_LEVEL', 'INFO'))
logger = logging.getLogger(__name__)

# Start Prometheus metrics server
if settings.ENABLE_PROMETHEUS:
    start_metrics_server(settings.PROMETHEUS_PORT)

# Import components
from database import validate_configuration, get_db_connection, db_pool
from telegram_client import TelegramClientManager
from message_scanner import MessageScanner
from media_processor import MediaManager
from processing_queue import ProcessingQueue
from face_processor import FaceProcessor
from clustering import FaceClusterer
from dlq_processor import DLQProcessor

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
    
    # Connect dependencies
    # Note: ProcessingQueue internally needs these if we passed them, but default init is None?
    # Checking ProcessingQueue init: it takes them as args. 
    # access them via setters or re-init?
    # processing_queue = ProcessingQueue(face_processor, ...)
    # Let's re-initialize properly.
    
    # Re-init ProcessingQueue with dependencies
    from topic_manager import TopicManager
    from media_uploader import MediaUploader
    from video_extractor import VideoFrameExtractor
    from identity_matcher import IdentityMatcher
    
    # We need bot client for TopicManager/MediaUploader
    from bot_client import bot_client_manager
    await bot_client_manager.start()
    
    topic_manager = TopicManager()
    media_uploader = MediaUploader(topic_manager)
    video_extractor = VideoFrameExtractor()
    identity_matcher = IdentityMatcher(topic_manager)
    
    processing_queue = ProcessingQueue(
        face_processor=face_processor,
        video_extractor=video_extractor,
        identity_matcher=identity_matcher,
        media_uploader=media_uploader,
        topic_manager=topic_manager,
        num_workers=settings.NUM_WORKERS,
        high_watermark=settings.QUEUE_MAX_SIZE
    )
    
    # DLQ Processor
    dlq_processor = DLQProcessor(processing_queue.redis_client, processing_queue)
    
    # Scanner
    scanner = MessageScanner(tg_manager.client, processing_queue, media_manager)

    # 3. Start Services
    logger.info("Starting services...")
    
    # Start Telegram Client
    await tg_manager.start()
    
    # Start Processing Queue Workers
    await processing_queue.start()
    
    # Start DLQ Processor
    await dlq_processor.start()
    
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
        await processing_queue.stop()
        await dlq_processor.stop()
        if db_pool:
            db_pool.close_all()

async def _register_account(user):
    """
    Registers the current Telegram account in the database.
    """
    async with get_db_connection() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute("""
                INSERT INTO telegram_accounts (phone_number, session_file_path, status, last_error)
                VALUES (%s, %s, 'active', NULL)
                ON CONFLICT (phone_number) DO UPDATE SET 
                    last_active = NOW(),
                    last_error = NULL
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
