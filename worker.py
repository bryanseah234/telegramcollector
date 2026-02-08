"""
Main Worker - Application entry point that integrates all components.

Initializes and coordinates:
- Phase 1: Database connection
- Phase 2: Telegram client, scanners, topic manager, media uploader
- Phase 3: Face processor, video extractor, identity matcher, processing queue
"""
import logging
import asyncio
import os
import signal
from config import settings

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MainWorker:
    """
    Main application worker that coordinates all components.
    """
    
    def __init__(self):
        self.client = None
        self.processing_queue = None
        self._shutdown_event = asyncio.Event()
    
    async def initialize(self):
        """Initializes all components in proper order."""
        logger.info("Initializing application components...")
        
        # Phase 1: Database
        from database import init_db, db_manager
        await db_manager.initialize()
        await init_db()
        logger.info("✓ Database initialized (Phase 1)")
        
        # Phase 2: Telegram User Client (for scanning)
        from telegram_client import TelegramClientManager
        self.client_manager = TelegramClientManager()
        await self.client_manager.start()
        self.client = self.client_manager.client
        logger.info("✓ Telegram user client connected (Phase 2)")
        
        # Phase 2: Bot Client (for topics and publishing)
        from bot_client import bot_client_manager
        await bot_client_manager.start()
        logger.info("✓ Bot client connected (for topic management)")
        
        # Phase 2: Topic Manager (uses bot client)
        from topic_manager import TopicManager
        self.topic_manager = TopicManager()  # Uses bot_client singleton internally
        logger.info("✓ Topic manager ready (using bot)")
        
        # Phase 2: Media Uploader (uses bot client)
        from media_uploader import MediaUploader
        self.media_uploader = MediaUploader(topic_manager=self.topic_manager)  # Uses bot_client singleton
        logger.info("✓ Media uploader ready (using bot)")

        
        # Phase 2: Media Downloader
        from media_downloader import MediaDownloadManager
        self.media_downloader = MediaDownloadManager(self.client)
        logger.info("✓ Media downloader ready")
        
        # Phase 3: Face Processor
        from face_processor import FaceProcessor
        self.face_processor = FaceProcessor.get_instance()
        logger.info("✓ Face processor initialized (Phase 3)")
        
        # Phase 3: Video Extractor
        from video_extractor import VideoFrameExtractor
        self.video_extractor = VideoFrameExtractor()
        logger.info("✓ Video extractor ready")
        
        # Phase 3: Identity Matcher
        from identity_matcher import IdentityMatcher
        self.identity_matcher = IdentityMatcher(self.topic_manager)
        logger.info("✓ Identity matcher ready")
        
        # Phase 3: Processing Queue
        from processing_queue import ProcessingQueue
        num_workers = settings.NUM_WORKERS
        self.processing_queue = ProcessingQueue(
            face_processor=self.face_processor,
            video_extractor=self.video_extractor,
            identity_matcher=self.identity_matcher,
            media_uploader=self.media_uploader,
            topic_manager=self.topic_manager,
            num_workers=num_workers
        )
        await self.processing_queue.start()
        logger.info(f"✓ Processing queue started with {num_workers} workers")
        
        # Phase 2: Message Scanner (needs processing queue)
        from message_scanner import MessageScanner, RealtimeScanner
        self.message_scanner = MessageScanner(
            client=self.client,
            media_manager=self.media_downloader,
            processing_queue=self.processing_queue
        )
        self.realtime_scanner = RealtimeScanner(
            client=self.client,
            media_manager=self.media_downloader,
            processing_queue=self.processing_queue
        )
        logger.info("✓ Message scanners ready")
        
        logger.info("All components initialized successfully!")
    
    async def run_backfill(self, account_id: int = 1):
        """Runs backfill scanning of all chats."""
        logger.info("Starting backfill scan...")
        
        # Discover all chats
        await self.message_scanner.discover_and_scan_all_chats(account_id)
        
        # Get incomplete chats and process them
        from database import get_db_connection
        async with get_db_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("""
                    SELECT chat_id FROM scan_checkpoints 
                    WHERE account_id = %s AND is_complete = FALSE
                """, (account_id,))
                chats = await cur.fetchall()
        
        for (chat_id,) in chats:
            try:
                await self.message_scanner.scan_chat_backfill(account_id, chat_id)
            except Exception as e:
                logger.error(f"Error scanning chat {chat_id}: {e}")
        
        logger.info("Backfill scan complete!")
    
    async def run_realtime(self, account_id: int = 1):
        """Runs real-time message monitoring."""
        logger.info("Starting real-time monitoring...")
        
        # Get all chats from DB to monitor (or just monitor all dialogs)
        # For now, let's monitor all chats we know about
        from database import get_db_connection
        async with get_db_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT chat_id FROM scan_checkpoints WHERE account_id = %s", (account_id,))
                rows = await cur.fetchall()
                chat_ids = [row[0] for row in rows]
        
        if chat_ids:
            await self.realtime_scanner.start_monitoring(chat_ids, account_id)
        else:
            logger.warning("No chats found to monitor for realtime.")
            
        # Keep running until shutdown
        await self._shutdown_event.wait()
    
    async def run(self, mode: str = 'both'):
        """
        Main run method.
        
        Args:
            mode: 'backfill', 'realtime', or 'both'
        """
        try:
            await self.initialize()
            
            account_id = settings.ACCOUNT_ID
            
            if mode in ('backfill', 'both'):
                await self.run_backfill(account_id)
            
            if mode in ('realtime', 'both'):
                await self.run_realtime(account_id)
                
        except Exception as e:
            logger.error(f"Fatal error: {e}")
            raise
        finally:
            await self.shutdown()
    
    async def shutdown(self):
        """Gracefully shuts down all components."""
        logger.info("Shutting down...")
        
        # Stop realtime scanner (if implemented stop method)
        # if hasattr(self, 'realtime_scanner'):
        #     await self.realtime_scanner.stop()
        
        # Stop processing queue
        if self.processing_queue:
            # stats = self.processing_queue.get_stats()
            # logger.info(f"Final stats: {stats}")
            # await self.processing_queue.stop()
            pass
        
        # Disconnect Telegram
        if hasattr(self, 'client_manager'):
            await self.client_manager.stop()
        
        from database import db_manager
        await db_manager.close()
        
        logger.info("Shutdown complete")


def main():
    """Entry point."""
    worker = MainWorker()
    
    # Handle signals for graceful shutdown
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    def signal_handler():
        logger.info("Received shutdown signal")
        worker._shutdown_event.set()
        # Create a task to shutdown properly if waiting on event
        asyncio.create_task(worker.shutdown())
    
    # Register signal handlers where supported
    if os.name != 'nt':  # Not Windows
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, signal_handler)
    
    try:
        mode = settings.RUN_MODE  # 'backfill', 'realtime', or 'both'
        loop.run_until_complete(worker.run(mode))
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        # Ensure shutdown is called
        loop.run_until_complete(worker.shutdown())
    except Exception as e:
        logger.error(f"Worker crashed: {e}")
    finally:
        loop.close()


if __name__ == '__main__':
    main()
