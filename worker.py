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
from typing import Dict, List
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
    Supports multiple Telegram accounts running in parallel.
    """
    
    def __init__(self):
        self.clients: Dict[int, 'TelegramClientManager'] = {}  # account_id -> client_manager
        self.processing_queue = None
        self._shutdown_event = asyncio.Event()
    
    async def initialize(self):
        """Initializes all components in proper order."""
        logger.info("Initializing application components...")
        
        # Phase 1: Database
        from database import init_db, db_manager, get_db_connection
        await db_manager.initialize()
        await init_db()
        logger.info("✓ Database initialized (Phase 1)")
        
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

        # Phase 2: Load User Accounts (Multiple)
        from telegram_client import TelegramClientManager
        from media_downloader import MediaDownloadManager
        from message_scanner import MessageScanner, RealtimeScanner

        async with get_db_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT id, phone_number, session_file_path FROM telegram_accounts WHERE status = 'active'")
                rows = await cur.fetchall()

        if not rows:
            logger.warning("⚠️  No active Telegram accounts found in database! Please register with the Login Bot.")
        
        self.scanners = {} # account_id -> (MessageScanner, RealtimeScanner)

        for account_id, phone, session_path in rows:
            try:
                # Extract session name from path (e.g., "sessions/account_123.session" -> "account_123")
                session_name = os.path.splitext(os.path.basename(session_path))[0]
                
                logger.info(f"Connecting account {account_id} ({phone})...")
                manager = TelegramClientManager(session_name=session_name)
                await manager.start()
                
                self.clients[account_id] = manager
                
                # Create Scanners for this account
                # Note: MediaDownloadManager needs a client. We create one per account.
                media_downloader = MediaDownloadManager(manager.client)
                
                scanner = MessageScanner(
                    client=manager.client,
                    media_manager=media_downloader,
                    processing_queue=self.processing_queue
                )
                
                rt_scanner = RealtimeScanner(
                    client=manager.client,
                    media_manager=media_downloader,
                    processing_queue=self.processing_queue
                )
                
                self.scanners[account_id] = (scanner, rt_scanner)
                logger.info(f"✓ Account {account_id} connected and scanners ready")
                
            except Exception as e:
                logger.error(f"Failed to connect account {account_id} ({phone}): {e}")

        logger.info(f"✓ All {len(self.clients)} accounts initialized successfully!")
    
    async def run_backfill(self):
        """Runs backfill scanning for ALL connected accounts."""
        if not self.clients:
            logger.warning("No accounts connected. Skipping backfill.")
            return

        logger.info("Starting backfill scan for all accounts...")
        
        tasks = []
        for account_id in self.clients:
            tasks.append(self._run_single_backfill(account_id))
        
        await asyncio.gather(*tasks)
        logger.info("Backfill scan complete for all accounts!")

    async def _run_single_backfill(self, account_id: int):
        """Runs backfill for a single account."""
        scanner, _ = self.scanners[account_id]
        logger.info(f"Running backfill for Account {account_id}...")
        
        # Discover all chats
        await scanner.discover_and_scan_all_chats(account_id)
        
        # Resume incomplete chats
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
                await scanner.scan_chat_backfill(account_id, chat_id)
            except Exception as e:
                logger.error(f"Error scanning chat {chat_id} (Account {account_id}): {e}")
    
    async def run_realtime(self):
        """Runs real-time monitoring for ALL connected accounts."""
        if not self.clients:
            logger.warning("No accounts connected. Waiting for shutdown...")
            await self._shutdown_event.wait()
            return

        logger.info("Starting real-time monitoring for all accounts...")
        
        tasks = []
        for account_id in self.clients:
            tasks.append(self._run_single_realtime(account_id))
        
        # Also wait for shutdown event
        tasks.append(self._shutdown_event.wait())
        
        # Run until shutdown
        await asyncio.gather(*tasks)

    async def _run_single_realtime(self, account_id: int):
        """Runs realtime monitor for a single account."""
        _, rt_scanner = self.scanners[account_id]
        
        # Get chats to monitor
        from database import get_db_connection
        async with get_db_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT chat_id FROM scan_checkpoints WHERE account_id = %s", (account_id,))
                rows = await cur.fetchall()
                chat_ids = [row[0] for row in rows]
        
        if chat_ids:
            logger.info(f"Account {account_id}: Monitoring {len(chat_ids)} chats")
            # start_monitoring is usually non-blocking (sets up event handlers), 
            # but if it blocks, this approach is fine as we use gather.
            # However, looking at previous implementation, it sets up handlers.
            # We just need to ensure we don't block here if start_monitoring blocks.
            # Assuming start_monitoring just adds event handlers.
            await rt_scanner.start_monitoring(chat_ids, account_id)
        else:
            logger.warning(f"Account {account_id}: No chats found to monitor.")
    
    async def run(self, mode: str = 'both'):
        """Main run method."""
        try:
            await self.initialize()
            
            if mode in ('backfill', 'both'):
                await self.run_backfill()
            
            if mode in ('realtime', 'both'):
                await self.run_realtime()
                
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
        
        # Disconnect all Telegram clients
        for account_id, manager in self.clients.items():
            await manager.stop()
            logger.info(f"Disconnected account {account_id}")
        
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
        loop.run_until_complete(worker.shutdown())
    except Exception as e:
        logger.error(f"Worker crashed: {e}")
    finally:
        loop.close()


if __name__ == '__main__':
    main()
