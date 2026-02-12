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
from hub_notifier import HubNotifier
from observability import start_metrics_server

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
        self._running = False

    async def initialize(self):
        """Initializes all components in proper order."""
        self._running = True
        logger.info("Initializing application components...")
        
        # Start metrics server early for health checks
        start_metrics_server(8000)
        
        # Phase 1: Database
        from database import init_db, db_manager, get_db_connection
        await db_manager.initialize()
        await init_db()
        logger.info("✓ Database initialized (Phase 1)")
        
        # Phase 2: Bot Client (for topics and publishing)
        from bot_client import bot_client_manager
        await bot_client_manager.start()
        bot_client_manager.register_worker(self)
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
        high_watermark = settings.QUEUE_MAX_SIZE
        low_watermark = max(1, int(high_watermark * 0.2)) # 20% of max
        
        self.processing_queue = ProcessingQueue(
            face_processor=self.face_processor,
            video_extractor=self.video_extractor,
            identity_matcher=self.identity_matcher,
            media_uploader=self.media_uploader,
            topic_manager=self.topic_manager,
            num_workers=num_workers,
            high_watermark=high_watermark,
            low_watermark=low_watermark
        )
        await self.processing_queue.start()
        logger.info(f"✓ Processing queue started with {num_workers} workers")
        
        # Phase 4: Hub Notifier (for status updates)
        self.hub_notifier = HubNotifier.get_instance()
        await self.hub_notifier.start()
        logger.info("✓ Hub notifier started")

        # Phase 5: Health Checker with Self-Healing
        from health_checker import HealthChecker
        self.health_checker = HealthChecker(
            face_processor=self.face_processor,
            processing_queue=self.processing_queue,
            check_interval=settings.HEALTH_CHECK_INTERVAL
        )
        
        # Register recovery actions
        self.health_checker.register_recovery('telegram', self._recover_telegram)
        self.health_checker.register_recovery('hub_access', self._recover_hub_access)
        
        await self.health_checker.start()
        logger.info("✓ Health checker started with self-healing enabled")

        # Phase 2: Load User Accounts (Multiple)
        from telegram_client import TelegramClientManager
        from media_downloader import MediaDownloadManager
        from message_scanner import MessageScanner, RealtimeScanner

        # AUTO-DISCOVERY: Scan sessions directory and register any existing sessions
        await self._auto_discover_sessions()

        async with get_db_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT id, phone_number, session_file_path FROM telegram_accounts WHERE status = 'active'")
                rows = await cur.fetchall()
                
                # Debug: check total accounts
                await cur.execute("SELECT COUNT(*), status FROM telegram_accounts GROUP BY status")
                stats = await cur.fetchall()
                if stats:
                    logger.info(f"Account stats in DB: {stats}")
                else:
                    logger.info("Account stats in DB: No accounts found.")

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
        
        # Log status to Hub Group
        await self.log_startup_status()

    async def log_startup_status(self):
        """Logs system startup status to the Hub Group."""
        try:
            from bot_client import bot_client_manager
            client = bot_client_manager.client
            hub_id = settings.HUB_GROUP_ID
            
            if not hub_id:
                return

            active_accounts = len(self.clients)
            mode = settings.RUN_MODE.upper()
            workers = settings.NUM_WORKERS
            version = "1.0.0" # Could be dynamic
            
            message = (
                f"🚀 **Face Archiver System Online**\n\n"
                f"📊 **Status Report:**\n"
                f"• **Active Accounts:** `{active_accounts}`\n"
                f"• **Run Mode:** `{mode}`\n"
                f"• **Workers:** `{workers}`\n"
                f"• **System:** `Operational`\n"
                f"\n"
                f"🔍 *Monitoring started for all connected accounts.*"
            )
            
            # Send to Hub Group (General Topic by default if no thread_id specified)
            await client.send_message(hub_id, message)
            logger.info(f"Sent startup status to Hub Group {hub_id}")
            
        except Exception as e:
            logger.warning(f"Failed to send startup status: {e}")
    
    async def _send_shutdown_notification(self):
        """Sends shutdown notification to Hub."""
        try:
            from bot_client import bot_client_manager
            client = bot_client_manager.client
            hub_id = settings.HUB_GROUP_ID
            
            if client and hub_id:
                await client.send_message(
                    hub_id, 
                    "🛑 **System Shutting Down**\n\n_Graceful shutdown initiated..._"
                )
        except Exception as e:
            logger.warning(f"Failed to send shutdown notification: {e}")
    
    async def _auto_discover_sessions(self):
        """
        Auto-discovers and registers existing session files.
        This enables self-healing after database wipes - existing sessions
        are automatically registered without needing the Login Bot.
        """
        from telethon import TelegramClient
        from telethon.errors import SessionPasswordNeededError, FloodWaitError
        from database import get_db_connection
        
        sessions_dir = settings.SESSIONS_DIR
        if not os.path.exists(sessions_dir):
            logger.info(f"Sessions directory does not exist: {sessions_dir}")
            return
        
        # Find all .session files
        session_files = [f for f in os.listdir(sessions_dir) if f.endswith('.session')]
        
        if not session_files:
            logger.info("No session files found for auto-discovery.")
            return
        
        logger.info(f"🔍 Auto-discovery: Found {len(session_files)} session file(s)")
        
        for session_file in session_files:
            session_name = session_file.replace('.session', '')
            session_path = os.path.join(sessions_dir, session_file)
            
            try:
                # Check if already registered in database
                async with get_db_connection() as conn:
                    async with conn.cursor() as cur:
                        await cur.execute(
                            "SELECT id FROM telegram_accounts WHERE session_file_path = %s",
                            (session_path,)
                        )
                        existing = await cur.fetchone()
                        
                        if existing:
                            logger.debug(f"Session {session_name} already registered (ID: {existing[0]})")
                            continue
                
                # Try to connect and validate the session
                logger.info(f"🔄 Validating session: {session_name}")
                
                client = TelegramClient(
                    os.path.join(sessions_dir, session_name),
                    settings.TELEGRAM_API_ID,
                    settings.TELEGRAM_API_HASH
                )
                
                await client.connect()
                
                if not await client.is_user_authorized():
                    logger.warning(f"⚠️ Session {session_name} is not authorized (needs re-login)")
                    await client.disconnect()
                    continue
                
                # Get user info for the phone number
                me = await client.get_me()
                phone = me.phone or f"unknown_{session_name}"
                
                await client.disconnect()
                
                # Register in database
                async with get_db_connection() as conn:
                    async with conn.cursor() as cur:
                        await cur.execute("""
                            INSERT INTO telegram_accounts (phone_number, session_file_path, status)
                            VALUES (%s, %s, 'active')
                            ON CONFLICT (phone_number) DO UPDATE SET 
                                session_file_path = EXCLUDED.session_file_path,
                                status = 'active',
                                last_active = NOW()
                            RETURNING id
                        """, (phone, session_path))
                        result = await cur.fetchone()
                        account_id = result[0] if result else None
                
                logger.info(f"✅ Auto-registered session: {phone} (ID: {account_id})")
                
            except SessionPasswordNeededError:
                logger.warning(f"⚠️ Session {session_name} requires 2FA password - use Login Bot")
            except FloodWaitError as e:
                logger.warning(f"⏳ FloodWait validating {session_name}. Waiting {e.seconds}s...")
                await asyncio.sleep(e.seconds)
                # Re-try this session on next startup
            except Exception as e:
                logger.error(f"❌ Failed to validate session {session_name}: {e}")
    
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
        
        # Resume incomplete chats - ORDERED BY PRIORITY (personal → group → channel)
        from database import get_db_connection
        async with get_db_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("""
                    SELECT chat_id, chat_type FROM scan_checkpoints 
                    WHERE account_id = %s AND scan_mode = 'backfill'
                    ORDER BY 
                        CASE chat_type 
                            WHEN 'personal' THEN 1 
                            WHEN 'group' THEN 2 
                            WHEN 'channel' THEN 3 
                            ELSE 4 
                        END
                """, (account_id,))
                chats = await cur.fetchall()
        
        logger.info(f"Account {account_id}: Scanning {len(chats)} chats in priority order (personal → group → channel)")
        
        for chat_id, chat_type in chats:
            try:
                logger.info(f"Scanning {chat_type} chat {chat_id}...")
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
            
            # Start health check scheduler as background task
            health_task = asyncio.create_task(self._health_check_scheduler())
            
            if mode in ('backfill', 'both'):
                await self.run_backfill()
            
            if mode in ('realtime', 'both'):
                await self.run_realtime()
                
        except Exception as e:
            logger.error(f"Fatal error: {e}")
            raise
        finally:
            await self.shutdown()
    
    async def _health_check_scheduler(self):
        """
        Periodically performs comprehensive health checks and logs to Hub's general topic.
        Runs every HEALTH_CHECK_INTERVAL seconds (default: 30 min).
        """
        interval = settings.HEALTH_CHECK_INTERVAL
        logger.info(f"Health check scheduler started (interval: {interval}s)")
        
        while not self._shutdown_event.is_set():
            try:
                # Wait for the interval or shutdown
                try:
                    await asyncio.wait_for(self._shutdown_event.wait(), timeout=interval)
                    break  # Shutdown requested
                except asyncio.TimeoutError:
                    pass  # Time to run health check
                
                await self._run_health_checks()
                
            except Exception as e:
                logger.error(f"Health check scheduler error: {e}")
                await asyncio.sleep(60)  # Wait a bit before retrying
    
    async def _run_health_checks(self):
        """Runs all health checks and posts report to Hub."""
        from datetime import datetime
        from database import get_db_connection, db_manager
        from bot_client import bot_client_manager
        import redis
        
        checks = {}
        warnings = []
        redis_client = None  # Initialize to None
        
        # 1. Database Health
        try:
            async with get_db_connection() as conn:
                async with conn.cursor() as cur:
                    await asyncio.wait_for(cur.execute("SELECT 1"), timeout=5.0)
            checks['Database'] = '✅ Connected'
        except Exception as e:
            checks['Database'] = f'❌ Error: {str(e)[:50]}'
            warnings.append('Database')
        
        # 2. Redis Health
        try:
            redis_client = redis.Redis(
                host=settings.REDIS_HOST,
                port=settings.REDIS_PORT,
                db=settings.REDIS_DB,
                password=settings.REDIS_PASSWORD,
                socket_timeout=5  # Add timeout
            )
            redis_client.ping()
            checks['Redis'] = '✅ Connected'
        except Exception as e:
            checks['Redis'] = f'❌ Error: {str(e)[:50]}'
            warnings.append('Redis')
            redis_client = None # Ensure it is None if failed
        
        # 3. Queue Health
        if self.processing_queue:
            try:
                queue_size = self.processing_queue.get_queue_size()
                queue_stats = self.processing_queue.get_stats()
                bp_state = self.processing_queue.get_backpressure_state().value
                
                if queue_size >= settings.QUEUE_MAX_SIZE * 0.8:
                    checks['Queue'] = f'⚠️ High load ({queue_size}/{settings.QUEUE_MAX_SIZE})'
                    warnings.append('Queue (High Load)')
                else:
                    checks['Queue'] = f'✅ {queue_size} items ({bp_state})'
                
                checks['Processed'] = f'📊 {queue_stats.get("total_processed", 0)} total, {queue_stats.get("faces_found", 0)} faces'
            except Exception as e:
                checks['Queue'] = f'❌ Error: {str(e)[:50]}'
                warnings.append('Queue')
        else:
            checks['Queue'] = '⏳ Not initialized'
        
        # 4. Telegram Accounts Health
        active_accounts = 0
        paused_accounts = 0
        try:
            async with get_db_connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("SELECT status, COUNT(*) FROM telegram_accounts GROUP BY status")
                    rows = await cur.fetchall()
                    status_counts = {row[0]: row[1] for row in rows}
                    active_accounts = status_counts.get('active', 0)
                    paused_accounts = status_counts.get('paused', 0)
            
            if paused_accounts > 0:
                checks['Accounts'] = f'⚠️ {active_accounts} active, {paused_accounts} paused'
                warnings.append('Paused Accounts')
            else:
                checks['Accounts'] = f'✅ {active_accounts} active'
        except Exception as e:
            checks['Accounts'] = f'❌ Error: {str(e)[:50]}'
        
        # 5. Scan Progress
        try:
            async with get_db_connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("""
                        SELECT 
                            COUNT(*) as total,
                            SUM(CASE WHEN is_complete = true THEN 1 ELSE 0 END) as complete
                        FROM scan_checkpoints
                    """)
                    row = await cur.fetchone()
                    total_chats = row[0] or 0
                    complete_chats = row[1] or 0
                    if total_chats > 0:
                        progress = (complete_chats / total_chats) * 100
                        checks['Scan Progress'] = f'📈 {complete_chats}/{total_chats} chats ({progress:.1f}%)'
                    else:
                        checks['Scan Progress'] = '⏳ No chats scanned yet'
        except Exception as e:
            checks['Scan Progress'] = f'❌ Error: {str(e)[:50]}'
        
        # 6. Topics/Identities
        try:
            async with get_db_connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("SELECT COUNT(*) FROM telegram_topics")
                    topic_count = (await cur.fetchone())[0]
                    await cur.execute("SELECT COUNT(*) FROM face_embeddings")
                    face_count = (await cur.fetchone())[0]
            checks['Identities'] = f'👥 {topic_count} topics, {face_count} embeddings'
        except Exception as e:
            checks['Identities'] = f'❌ Error: {str(e)[:50]}'
        
        # 7. Dead Letter Queue (failed tasks)
        try:
            if redis_client and self.processing_queue:
                dlq_size = redis_client.llen(self.processing_queue.dead_letter_key)
                if dlq_size > 100:
                    checks['Dead Letter Queue'] = f'⚠️ {dlq_size} failed tasks'
                    warnings.append('DLQ High')
                elif dlq_size > 0:
                    checks['Dead Letter Queue'] = f'📋 {dlq_size} failed tasks'
                else:
                    checks['Dead Letter Queue'] = '✅ Empty'
            else:
                 checks['Dead Letter Queue'] = '⚠️ Redis/Queue unavailable'
        except Exception as e:
            checks['Dead Letter Queue'] = f'❌ Error: {str(e)[:50]}'
        
        # 8. Memory Usage (if psutil available)
        try:
            import psutil
            process = psutil.Process()
            mem_mb = process.memory_info().rss / 1024 / 1024
            if mem_mb > 3000:  # > 3GB
                checks['Memory'] = f'⚠️ {mem_mb:.0f} MB'
                warnings.append('High Memory')
            else:
                checks['Memory'] = f'✅ {mem_mb:.0f} MB'
        except ImportError:
            pass  # psutil not available, skip memory check
        except Exception:
            pass
        
        # Build report
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        if warnings:
            status_emoji = '⚠️'
            status_text = f'Issues: {", ".join(warnings)}'
        else:
            status_emoji = '✅'
            status_text = 'All systems operational'
        
        report = f"{status_emoji} **Health Check Report** - `{now}`\n\n"
        report += f"**Status:** {status_text}\n\n"
        
        for key, value in checks.items():
            report += f"• **{key}:** {value}\n"
        
        # Send to Hub's general topic (reply_to=None means general thread)
        try:
            client = bot_client_manager.client
            hub_id = settings.HUB_GROUP_ID
            if client and hub_id:
                await client.send_message(hub_id, report)
                logger.info(f"Health check report sent to Hub Group")
        except Exception as e:
            logger.error(f"Failed to send health check report: {e}")

        # SELF HEALING: Check HubNotifier
        try:
            if hasattr(self, 'hub_notifier'):
                if not self.hub_notifier._running or (self.hub_notifier._flush_task and self.hub_notifier._flush_task.done()):
                    logger.warning("⚠️ Hub Notifier stopped unexpectedly. Restarting...")
                    await self.hub_notifier.stop() # Ensure clean
                    self.hub_notifier = HubNotifier.get_instance() # Should get same or new
                    # If instance was reset externally, this gets new. 
                    # If same instance, we just call start().
                    # But wait, if instance is same, start() checks _running.
                    # We set _running=False in stop().
                    await self.hub_notifier.start()
                    logger.info("✅ Hub Notifier self-healed")
        except Exception as e:
            logger.error(f"Failed to self-heal HubNotifier: {e}")

    async def _recover_telegram(self):
        """Attempts to recover Telegram connections."""
        logger.warning("Recovery: Checking Telegram clients...")
        for account_id, manager in self.clients.items():
            if not manager.client.is_connected():
                logger.info(f"Reconnecting account {account_id}...")
                await manager.start()
    
    async def _recover_hub_access(self):
        """Attempts to recover Hub access."""
        logger.warning("Recovery: Checking Bot client...")
        from bot_client import bot_client_manager
        if not bot_client_manager.client or not bot_client_manager.client.is_connected():
            logger.info("Reconnecting bot client...")
            await bot_client_manager.start()
    
    async def shutdown(self):
        """Gracefully shuts down all components and cleans up tasks."""
        self._running = False
        logger.info("Graceful shutdown initiated...")
        
        # 1. Send shutdown notification (before services stop)
        await self._send_shutdown_notification()
        
        # 2. Stop Scanners (to stop receiving new events)
        for account_id, (scanner, rt_scanner) in self.scanners.items():
            try:
                await rt_scanner.stop()
                logger.info(f"Stopped realtime scanner for account {account_id}")
            except Exception as e:
                logger.debug(f"Error stopping scanner: {e}")

        # 3. Stop hub notifier (flushes pending events)
        if hasattr(self, 'hub_notifier'):
            try:
                await self.hub_notifier.stop()
            except Exception as e:
                logger.debug(f"Error stopping hub notifier: {e}")
        
        # 4. Stop health checker
        if hasattr(self, 'health_checker'):
            try:
                await self.health_checker.stop()
            except Exception as e:
                logger.debug(f"Error stopping health checker: {e}")

        # 5. Stop processing queue
        if self.processing_queue:
            try:
                await self.processing_queue.stop()
                logger.info("Stopped processing queue")
            except Exception as e:
                logger.debug(f"Error stopping queue: {e}")
        
        # 6. Disconnect all Telegram clients
        for account_id, manager in self.clients.items():
            try:
                await manager.stop()
                logger.info(f"Disconnected account {account_id}")
            except Exception as e:
                logger.debug(f"Error disconnecting account {account_id}: {e}")
        
        # 7. Disconnect Bot Client
        try:
            from bot_client import bot_client_manager
            if bot_client_manager.is_ready():
                await bot_client_manager.disconnect()
                logger.info("Disconnected bot client")
        except Exception as e:
            logger.debug(f"Failed to disconnect bot client: {e}")

        # 8. Close Database
        try:
            from database import db_manager
            await db_manager.close()
            logger.info("Closed database connections")
        except Exception as e:
            logger.debug(f"Error closing database: {e}")
        
        # 9. Final Task Cleanup (The most critical part for "Event loop is closed" errors)
        await self._cancel_all_tasks()
        
        logger.info("Shutdown complete")

    async def _cancel_all_tasks(self):
        """Cancels all remaining background tasks on the current loop."""
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return

        current_task = asyncio.current_task()
        tasks = [t for t in asyncio.all_tasks(loop) if t is not current_task]
        
        if not tasks:
            return

        logger.info(f"Cancelling {len(tasks)} remaining background tasks...")
        for task in tasks:
            task.cancel()

        # Wait for all tasks to acknowledge cancellation (with timeout)
        try:
            await asyncio.wait_for(
                asyncio.gather(*tasks, return_exceptions=True),
                timeout=5.0
            )
        except asyncio.TimeoutError:
            logger.warning("Timed out waiting for tasks to cancel")
        except Exception as e:
            logger.debug(f"Error during final task cleanup: {e}")


def main():
    """Entry point with crash recovery and auto-restart."""
    MAX_RESTART_ATTEMPTS = 5
    BASE_RESTART_DELAY = 10  # seconds
    restart_count = 0
    
    while restart_count < MAX_RESTART_ATTEMPTS:
        worker = MainWorker()
        
        # Handle signals for graceful shutdown
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        def signal_handler():
            logger.info("Received shutdown signal")
            worker._shutdown_event.set()
            asyncio.create_task(worker.shutdown())
        
        # Register signal handlers where supported
        if os.name != 'nt':  # Not Windows
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(sig, signal_handler)
        
        try:
            mode = settings.RUN_MODE  # 'backfill', 'realtime', or 'both'
            
            # Reset HubNotifier singleton to ensure fresh event loop connection
            try:
                from hub_notifier import HubNotifier
                HubNotifier.reset_instance()
            except Exception as e:
                logger.warning(f"Failed to reset HubNotifier: {e}")
                
            logger.info(f"Starting worker (attempt {restart_count + 1}/{MAX_RESTART_ATTEMPTS})")
            loop.run_until_complete(worker.run(mode))
            
            # If we get here normally (not exception), don't restart
            logger.info("Worker completed normally")
            break
            
        except KeyboardInterrupt:
            logger.info("Interrupted by user")
            loop.run_until_complete(worker.shutdown())
            break  # Don't restart on user interrupt
            
        except Exception as e:
            restart_count += 1
            delay = BASE_RESTART_DELAY * (2 ** (restart_count - 1))  # Exponential backoff
            
            logger.error(f"Worker crashed (attempt {restart_count}/{MAX_RESTART_ATTEMPTS}): {e}")
            
            # Log error to database (async in sync context)
            try:
                from database import log_processing_error
                loop.run_until_complete(log_processing_error(
                    error_type='WorkerCrash',
                    error_message=str(e),
                    error_context={'restart_count': restart_count, 'mode': settings.RUN_MODE}
                ))
            except Exception as log_err:
                logger.warning(f"Failed to log crash to DB: {log_err}")
            
            # Cleanup
            try:
                loop.run_until_complete(worker.shutdown())
            except Exception:
                pass
            
            if restart_count < MAX_RESTART_ATTEMPTS:
                logger.info(f"Restarting in {delay} seconds...")
                import time
                time.sleep(delay)
            else:
                logger.error("Max restart attempts reached. Exiting.")
                
        finally:
            try:
                # 1. Ensure worker is shut down if it hasn't been
                if 'worker' in locals() and worker._running:
                    loop.run_until_complete(worker.shutdown())
                
                # 2. Reset singletons to prevent stale loop references
                from hub_notifier import HubNotifier
                from face_processor import FaceProcessor
                from bot_client import bot_client_manager, BotClientManager
                
                HubNotifier.reset_instance()
                FaceProcessor.reset_instance()
                BotClientManager.reset_instance()
                
                # 3. Final loop drain to let library tasks (like Telethon loops) finish
                tasks = asyncio.all_tasks(loop)
                if tasks:
                    loop.run_until_complete(asyncio.gather(*tasks, return_exceptions=True))
                
                loop.close()
            except Exception as e:
                logger.debug(f"Final cleanup error: {e}")
    
    logger.info("Worker process exiting")


if __name__ == '__main__':
    main()

