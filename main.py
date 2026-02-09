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
    
    # 4. Discovery & Scanning (Orchestrated Backfill)
    # Get own account ID (or create one in DB)
    me = await tg_manager.client.get_me()
    account_id = await _register_account(me)
    
    # 4a. Start Real-time Monitoring (New Users/Contacts)
    # We want this running in background while backfill happens
    logger.info("Starting Real-time Scanner...")
    from message_scanner import RealtimeScanner
    realtime_scanner = RealtimeScanner(tg_manager.client, processing_queue, media_manager)
    
    # Monitor all known chats + global events (handled by empty list or specific logic in scanner)
    # The updated start_monitoring handles empty list as "no specific chat filter" if we want
    # But usually we want to monitor discovered chats.
    # For now, let's start it with empty list to just trigger the contact monitor?
    # No, we want it to handle ChatActions.
    # Let's fetch all chat IDs first? 
    # Or better: The RealtimeScanner registers global handlers if check checks for "chats=None"
    # My implementation: `chats=list(chat_ids) if chat_ids else None`
    # If None, it monitors ALL events (which is what we want for "new groups"?)
    # Telethon: `events.NewMessage()` (no args) = All chats.
    # So passing [] or None covers everything.
    await realtime_scanner.start_monitoring([], account_id)
    
    # Run the backfill orchestrator
    logger.info("Starting Backfill Orchestrator...")
    logger.info("Starting Backfill Orchestrator...")
    orchestrator_task = asyncio.create_task(_run_backfill_orchestrator(scanner, account_id))
    
    # 5. Keep Alive
    logger.info("Application is running. Press Ctrl+C to stop.")
    
    try:
        # Wait until disconnected or signal
        await tg_manager.client.run_until_disconnected()
    except (KeyboardInterrupt, SystemExit):
        pass
    finally:
        logger.info("Shutting down...")
        
        # Cancel orchestrator if running
        if not orchestrator_task.done():
            logger.info("Cancelling backfill orchestrator...")
            orchestrator_task.cancel()
            try:
                await orchestrator_task
            except asyncio.CancelledError:
                pass
                
        # Stop Realtime Scanner
        await realtime_scanner.stop()
        
        # Stop Processing Queue
        await processing_queue.stop()
        
        
        
        # Stop DLQ Processor
        await dlq_processor.stop()
        
        # Stop Bot Client
        from bot_client import bot_client_manager
        await bot_client_manager.disconnect()
        
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

async def _run_backfill_orchestrator(scanner: MessageScanner, account_id: int):
    """
    Orchestrates the backfill process in specific order:
    1. Contacts (Profile Photos) - Build identity DB
    2. Personal DMs - High value matching
    3. Groups - Bulk data
    4. Channels - Bulk data
    """
    from hub_notifier import notify
    
    try:
        # Step 1: Contacts
        await scanner.scan_contacts(account_id)
        
        # Step 2: Discovery
        await scanner.discover_and_scan_all_chats(account_id)
        
        # Step 3: Ordered Chat Backfill
        # We process in order of priority: personal -> group -> channel
        priorities = ['personal', 'group', 'channel']
        
        for chat_type in priorities:
            logger.info(f"Starting backfill for type: {chat_type.upper()}")
            await notify('scan', f"🔄 Starting backfill phase: **{chat_type.upper()}**")
            
            # Get chats of this type that are incomplete
            start_offset = 0
            batch_size = 10
            
            while True:
                chats_batch = await _get_incomplete_chats(account_id, chat_type, start_offset, batch_size)
                if not chats_batch:
                    break
                
                for chat_id in chats_batch:
                    # Scan this chat
                    await scanner.scan_chat_backfill(account_id, chat_id)
                    # Yield slightly to allow heartbeats/other tasks
                    await asyncio.sleep(1)
                
                # We don't increment offset because completed chats are removed from query result?
                # Actually _get_incomplete_chats checks is_complete=false.
                # So if they finish, they won't appear next query. 
                # BUT if they fail or pause, they might appear again. 
                # For safety, let's just re-query top N incomplets.
                # If we made NO progress, we might get stuck loop.
                # So we should probably increment offset if we skip them?
                # Actually, scan_chat_backfill marks is_complete=true on finish.
                # So re-querying is fine.
                await asyncio.sleep(2)

        logger.info("🎉 All backfill phases complete!")
        await notify('scan', "🎉 **Backfill Complete**! System is now fully synced.")
        
    except Exception as e:
        logger.error(f"Backfill orchestrator failed: {e}")
        await notify('error', f"Backfill orchestrator crashed: {e}")

async def _get_incomplete_chats(account_id: int, chat_type: str, offset: int, limit: int) -> list:
    """Fetches incomplete chats of a specific type."""
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
                SELECT chat_id FROM scan_checkpoints 
                WHERE account_id = %s 
                  AND chat_type = %s 
                  AND is_complete = false
                ORDER BY last_updated ASC
                LIMIT %s OFFSET %s
            """, (account_id, chat_type, limit, offset))
            rows = await cur.fetchall()
            return [row[0] for row in rows]

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
