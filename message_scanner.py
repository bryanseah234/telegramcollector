"""
Message Scanner - Handles backfill and real-time message scanning.

Implements checkpointing for resumable processing and media filtering.
"""
import logging
import asyncio
import io
from datetime import datetime, timezone
import os
from telethon import events
from telethon.tl.types import User, Chat, Channel, MessageMediaPhoto, MessageMediaDocument
from telethon.errors import FloodWaitError
from database import get_db_connection
from hub_notifier import notify, increment_stat
from config import settings

logger = logging.getLogger(__name__)


class MessageScanner:
    """
    Handles scanning of Telegram chats with backfill support.
    Implements checkpointing for resumable processing.
    """
    
    def __init__(self, client, processing_queue, media_manager):
        self.client = client
        self.processing_queue = processing_queue
        self.media_manager = media_manager
        self.batch_size = 100  # Checkpoint every 100 messages
        self.stats = {}

    async def scan_contacts(self, account_id: int):
        """
        Scrapes profile photos of all contacts.
        Priority Step 1: Establish identities from contacts before scanning chats.
        """
        logger.info("Starting contact scan (Step 1/4)...")
        count = 0
        new_photos = 0
        
        try:
            # metadata=True gets photos too
            contacts = await self.client.get_contacts()
            total_contacts = len(contacts)
            logger.info(f"Found {total_contacts} contacts to scan")
            
            for user in contacts:
                if isinstance(user, User):
                    try:
                        async for photo in self.client.iter_profile_photos(user):
                            photo_id = getattr(photo, 'id', 0)
                            if not photo_id:
                                continue
                                
                            if not await self._is_historical_photo_processed(user.id, photo_id):
                                logger.debug(f"Fetching historical profile photo {photo_id} for contact {user.id} ({user.first_name})")
                                
                                photo_bytes = await self.media_manager.download_specific_profile_photo(user, photo)
                                
                                if photo_bytes:
                                    await self.processing_queue.enqueue_profile_photo(
                                        user_id=user.id,
                                        content=io.BytesIO(photo_bytes)
                                    )
                                    await self._mark_historical_photo_processed(user.id, photo_id)
                                    new_photos += 1
                    except Exception as e:
                        logger.error(f"Failed to process photos for contact {user.id}: {e}")
                            
                    count += 1
                    if count % 50 == 0:
                        logger.info(f"Scanned {count}/{total_contacts} contacts...")
                        
            logger.info(f"Contact scan complete. Processed {count} contacts, {new_photos} new profile photos enqueued.")
            await notify('scan', f"✅ Contact Scan Complete: {count} contacts checked, {new_photos} new photos.")
            
        except Exception as e:
            logger.error(f"Error during contact scan: {e}")
            await notify('error', f"Contact scan failed: {e}")

    async def discover_and_scan_all_chats(self, account_id: int):
        """
        Iterates through ALL dialogs (groups, channels, DMs) and adds them to the scan list.
        Categorizes by type: 'personal', 'group', 'channel'
        Feature: "scraping of all existing groups, channels and DMs"
        
        EXCLUDES: Hub Group (HUB_GROUP_ID) to prevent self-scraping
        """
        from config import settings
        hub_group_id = settings.HUB_GROUP_ID
        
        logger.info("Starting discovery of all dialogs...")
        count = 0
        skipped = 0
        stats = {'personal': 0, 'group': 0, 'channel': 0}
        
        try:
            async for dialog in self.client.iter_dialogs():
                chat_id = dialog.id
                chat_title = dialog.title or f"Chat_{chat_id}"
                
                # SKIP the hub group to prevent self-scraping
                if chat_id == hub_group_id or chat_id == -hub_group_id or abs(chat_id) == abs(hub_group_id):
                    logger.info(f"⏭️ Skipping Hub Group: {chat_title} (ID: {chat_id})")
                    skipped += 1
                    continue
                
                # Determine chat type for priority ordering
                entity = dialog.entity
                if isinstance(entity, User):
                    chat_type = 'personal'
                elif isinstance(entity, Channel):
                    # Channels include supergroups (megagroups) and broadcast channels
                    if entity.megagroup:
                        chat_type = 'group'  # Supergroups are treated as groups
                    else:
                        chat_type = 'channel'  # Broadcast channels
                elif isinstance(entity, Chat):
                    chat_type = 'group'  # Basic groups
                else:
                    chat_type = 'group'  # Default to group
                
                stats[chat_type] += 1
                
                # Add to checkpoint table if not exists (with chat_type)
                await self._init_checkpoint(account_id, chat_id, chat_title, chat_type)
                count += 1
                
                if count % 10 == 0:
                    logger.info(f"Discovered {count} dialogs so far...")
            
            logger.info(f"Discovery complete. Found {count} dialogs: {stats['personal']} personal, {stats['group']} groups, {stats['channel']} channels")
            return count
            
        except FloodWaitError as e:
            logger.warning(f"FloodWait during discovery. Waiting {e.seconds}s...")
            await asyncio.sleep(e.seconds)
            return await self.discover_and_scan_all_chats(account_id)
        except Exception as e:
            logger.error(f"Error discovering dialogs: {e}")
            return count

    async def _init_checkpoint(self, account_id: int, chat_id: int, title: str, chat_type: str = 'group'):
        """Initializes a checkpoint for a chat if it doesn't exist."""
        async with get_db_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("""
                    INSERT INTO scan_checkpoints 
                        (account_id, chat_id, chat_title, chat_type, last_processed_message_id, scan_mode)
                    VALUES (%s, %s, %s, %s, 0, 'backfill')
                    ON CONFLICT (account_id, chat_id) DO UPDATE SET chat_type = EXCLUDED.chat_type
                """, (account_id, chat_id, title, chat_type))

    async def scan_chat_backfill(self, account_id: int, chat_id: int):
        """
        Backfills a single chat from the last checkpoint.
        Saves progress every batch_size messages.
        """
        checkpoint = await self._get_checkpoint(account_id, chat_id)
        if not checkpoint:
            logger.warning(f"No checkpoint for chat {chat_id}")
            return
        
        chat_title = checkpoint.get('chat_title', f'Chat {chat_id}')
        chat_type = checkpoint.get('chat_type', 'group')
        
        # Notify Hub about scan start
        await notify('scan', f"Started scanning {chat_type}: **{chat_title[:30]}**")
        
        last_id = (checkpoint.get('last_processed_message_id') or 0) if checkpoint else 0
        
        # Get total message count for progress tracking
        total_messages = await self._get_chat_message_count(chat_id)
        processed_count = 0
        
        logger.info(f"Backfilling chat {chat_id} from message {last_id} (total: {total_messages})")
        
        try:
            async for message in self.client.iter_messages(
                chat_id, 
                min_id=last_id, 
                reverse=True
            ):
                # Filter: only process messages with media
                if self._should_process_message(message):
                    await self.process_message(message, account_id, chat_id)
                
                processed_count += 1
                
                # Update checkpoint every batch
                if processed_count % self.batch_size == 0:
                    await self._update_checkpoint(account_id, chat_id, message.id, processed_count)
                    logger.info(f"Chat {chat_id}: Processed {processed_count}/{total_messages} messages")
                
                # Yield to event loop periodically
                if processed_count % 10 == 0:
                    await asyncio.sleep(0)
                
                # CHECK BACKPRESSURE
                # If queue is full (CRITICAL), pause scanning to prevent Redis overload
                while self.processing_queue.should_pause():
                    logger.warning(f"Combined Queue limit reached. Pausing scan for chat {chat_id} (backpressure)...")
                    # Force a check to update the state, otherwise we might be stuck here forever if nothing else updates it
                    self.processing_queue.check_backpressure()
                    if not self.processing_queue.should_pause():
                        logger.info("Combined Queue limit dropped below critical. Resuming scan...")
                        break
                    await asyncio.sleep(5)  # Wait 5s before checking again
            
            # Final checkpoint update
            await self._mark_chat_complete(account_id, chat_id, processed_count)
            logger.info(f"Backfill complete for chat {chat_id}. Processed {processed_count} messages.")
            
        except FloodWaitError as e:
            logger.warning(f"FloodWait during backfill. Waiting {e.seconds}s...")
            await asyncio.sleep(e.seconds + 1)  # Add 1 second buffer
            # Resume from current position
            return await self.scan_chat_backfill(account_id, chat_id)
        except Exception as e:
            logger.error(f"Error backfilling chat {chat_id}: {e}")
            raise

    def _should_process_message(self, message) -> bool:
        """
        Determines if a message contains media that should be processed.
        
        Returns True for:
        - Photos
        - Video files
        - Documents that are images/videos
        """
        if message.photo:
            return True
        
        if message.video:
            return True
        
        if message.document:
            # Check MIME type for image/video documents
            mime = getattr(message.document, 'mime_type', '') or ''
            if mime.startswith('image/') or mime.startswith('video/'):
                return True
        
        return False

    async def process_message(self, message, account_id: int, chat_id: int):
        """
        Processes a single message: downloads media and queues for face detection.
        Also fetches profile photos of senders.
        
        Includes file_unique_id deduplication to skip forwarded/duplicate media.
        """
        # 1. Process Message Media (Photos/Videos)
        if message.photo or message.video or message.document:
            try:
                # Get file_unique_id for deduplication
                file_unique_id = self._get_file_unique_id(message)
                
                # Check if this exact file has already been processed
                if file_unique_id and await self._is_file_processed(file_unique_id):
                    logger.debug(f"Skipping duplicate media (file_unique_id: {file_unique_id[:20]}...)")
                    self.stats['duplicates_skipped'] = self.stats.get('duplicates_skipped', 0) + 1
                    return
                
                # Download and process
                media_content = await self.media_manager.download_media(message)
                if media_content:
                    await self.processing_queue.enqueue_media(
                        chat_id=chat_id,
                        message_id=message.id,
                        content=media_content,
                        media_type='video' if message.video else 'photo',
                        file_unique_id=file_unique_id  # Pass for tracking after processing
                    )
            except Exception as e:
                logger.warning(f"Failed to download media from message {message.id}: {e}")

        # 2. Process Sender Profile Photo (Feature: profile photo scraping)
        # Scrape all historical profile photos for the user
        try:
            sender = await message.get_sender()
            if sender and isinstance(sender, User):
                async for photo in self.client.iter_profile_photos(sender):
                    photo_id = getattr(photo, 'id', 0)
                    if not photo_id: continue
                    
                    if not await self._is_historical_photo_processed(sender.id, photo_id):
                        logger.debug(f"Fetching historical profile photo {photo_id} for user {sender.id}")
                        photo_bytes = await self.media_manager.download_specific_profile_photo(sender, photo)
                        if photo_bytes:
                            await self.processing_queue.enqueue_profile_photo(
                                user_id=sender.id,
                                content=io.BytesIO(photo_bytes)
                            )
                            await self._mark_historical_photo_processed(sender.id, photo_id)
        except Exception as e:
            logger.debug(f"Could not process profile photo for message {message.id}: {e}")

    async def _get_checkpoint(self, account_id: int, chat_id: int) -> dict:
        """Retrieves the checkpoint for a chat."""
        async with get_db_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("""
                    SELECT last_processed_message_id, processed_messages, chat_title, chat_type
                    FROM scan_checkpoints 
                    WHERE account_id = %s AND chat_id = %s
                """, (account_id, chat_id))
                row = await cur.fetchone()
                if row:
                    return {
                        'last_processed_message_id': row[0] or 0,
                        'processed_messages': row[1] or 0,
                        'chat_title': row[2],
                        'chat_type': row[3]
                    }
                return None

    async def _update_checkpoint(self, account_id: int, chat_id: int, message_id: int, processed: int):
        """Updates the checkpoint with current progress."""
        async with get_db_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("""
                    UPDATE scan_checkpoints 
                    SET last_processed_message_id = %s, 
                        processed_messages = %s,
                        last_updated = NOW()
                    WHERE account_id = %s AND chat_id = %s
                """, (message_id, processed, account_id, chat_id))
        
        # Milestone notification (every N messages)
        milestone_interval = getattr(settings, 'NOTIFY_MILESTONE_INTERVAL', 1000)
        if processed > 0 and processed % milestone_interval == 0:
            await notify('scan', f"📊 Progress: {processed} messages scanned")
        
        # Update stats
        await increment_stat('messages_scanned', 1)

    async def _mark_chat_complete(self, account_id: int, chat_id: int, total_processed: int):
        """Marks a chat as fully scanned."""
        async with get_db_connection() as conn:
            async with conn.cursor() as cur:
                # Get chat title for notification
                await cur.execute(
                    "SELECT chat_title FROM scan_checkpoints WHERE account_id = %s AND chat_id = %s",
                    (account_id, chat_id)
                )
                row = await cur.fetchone()
                chat_title = row[0] if row else f'Chat {chat_id}'
                
                await cur.execute("""
                    UPDATE scan_checkpoints 
                    SET scan_mode = 'realtime', is_complete = true, last_updated = NOW()
                    WHERE account_id = %s AND chat_id = %s
                """, (account_id, chat_id))
        
        logger.info(f"Chat {chat_id} marked complete ({total_processed} messages)")
        
        # Notify Hub about completion
        await notify('scan', f"✅ Completed **{chat_title[:30]}**: {total_processed} messages scanned")

    async def _get_chat_message_count(self, chat_id: int) -> int:
        """Gets total message count for a chat (for progress tracking)."""
        try:
            # Get a single message to access total count
            messages = await self.client.get_messages(chat_id, limit=1)
            return messages.total if messages else 0
        except Exception:
            return 0

    async def _is_historical_photo_processed(self, user_id: int, photo_id: int) -> bool:
        """
        Checks if a user's specific historical photo_id has been processed.
        """
        async with get_db_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("""
                    SELECT photo_id FROM processed_profile_photos 
                    WHERE user_id = %s AND photo_id = %s
                """, (user_id, photo_id))
                row = await cur.fetchone()
                return row is not None

    async def _mark_historical_photo_processed(self, user_id: int, photo_id: int):
        """
        Records that a user's specific historical photo has been processed.
        """
        async with get_db_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("""
                    INSERT INTO processed_profile_photos (user_id, photo_id, processed_at) 
                    VALUES (%s, %s, NOW())
                    ON CONFLICT (user_id, photo_id) DO NOTHING
                """, (user_id, photo_id))
    
    def _get_file_unique_id(self, message) -> str:
        """Extracts file_unique_id from a message's media."""
        try:
            if message.photo:
                return message.photo.file_unique_id
            
            if message.video:
                return message.video.file_unique_id
            
            if message.document:
                return message.document.file_unique_id
            
            return None
        except AttributeError:
            return None
    
    async def _is_file_processed(self, file_unique_id: str) -> bool:
        """Checks if a file has already been processed."""
        async with get_db_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("""
                    SELECT 1 FROM processed_media WHERE file_unique_id = %s
                """, (file_unique_id,))
                return await cur.fetchone() is not None


class RealtimeScanner:
    """
    Monitors chats for new messages in real-time using Telethon event handlers.
    Also handles new user joins and contact updates.
    """
    
    def __init__(self, client, processing_queue, media_manager):
        self.client = client
        self.processing_queue = processing_queue
        self.media_manager = media_manager
        self.monitored_chats = set()
        self._handler_added = False
        self._contact_monitor_task = None
    
    async def stop(self):
        """Stops the realtime scanner and background tasks."""
        if self._contact_monitor_task:
            self._contact_monitor_task.cancel()
            try:
                await self._contact_monitor_task
            except asyncio.CancelledError:
                pass
            self._contact_monitor_task = None
        logger.info("Realtime scanner stopped.")

    async def start_monitoring(self, chat_ids: list, account_id: int):
        """
        Begins real-time monitoring of specified chats.
        Registers event handlers that trigger when new messages arrive.
        """
        # Filter out the Hub Group to prevent infinite scraping loops
        from config import settings
        hub_group_id = settings.HUB_GROUP_ID
        
        filtered_chat_ids = [
            cid for cid in chat_ids
            if cid != hub_group_id and cid != -hub_group_id and abs(cid) != abs(hub_group_id)
        ]
        
        self.monitored_chats = set(filtered_chat_ids)
        self._account_id = account_id
        
        if not self._handler_added:
            # 1. New Message Handler
            @self.client.on(events.NewMessage(chats=list(filtered_chat_ids) if filtered_chat_ids else None))
            async def handle_new_message(event):
                await self._process_new_message(event)
            
            # 2. Chat Action Handler (User Joins)
            @self.client.on(events.ChatAction(chats=list(filtered_chat_ids) if filtered_chat_ids else None))
            async def handle_chat_action(event):
                await self._process_chat_action(event)
                
            self._handler_added = True
        
        # Start periodic contact monitor
        if not self._contact_monitor_task:
            self._contact_monitor_task = asyncio.create_task(self._periodic_contact_scan())
            
        logger.info(f"Real-time monitoring started for {len(self.monitored_chats)} chats")
    
    async def _process_new_message(self, event):
        """Handles incoming messages from monitored chats."""
        message = event.message
        chat_id = event.chat_id
        
        # Failsafe: Ignore Hub Group to prevent infinite scraping loops
        from config import settings
        hub_group_id = settings.HUB_GROUP_ID
        if chat_id == hub_group_id or chat_id == -hub_group_id or abs(chat_id) == abs(hub_group_id):
            return
        
        # Check if message contains processable media
        if not (message.photo or message.video):
            return
        
        logger.info(f"New media detected in chat {chat_id}, message {message.id}")
        
        try:
            # Download and queue media
            media_content = await self.media_manager.download_media(message)
            if media_content:
                await self.processing_queue.enqueue_media(
                    chat_id=chat_id,
                    message_id=message.id,
                    content=media_content,
                    media_type='video' if message.video else 'photo'
                )
            
            # Update checkpoint
            await self._update_realtime_checkpoint(chat_id, message.id)
            
        except FloodWaitError as e:
            logger.warning(f"FloodWait in realtime handler. Waiting {e.seconds}s...")
            await asyncio.sleep(e.seconds)
        except Exception as e:
            logger.error(f"Error processing realtime message: {e}")
            
    async def _process_chat_action(self, event):
        """Handles user joins/adds to scan their profile photos immediately."""
        # Failsafe: Ignore Hub Group
        from config import settings
        hub_group_id = settings.HUB_GROUP_ID
        if getattr(event, 'chat_id', None):
            chat_id = event.chat_id
            if chat_id == hub_group_id or chat_id == -hub_group_id or abs(chat_id) == abs(hub_group_id):
                return
                
        try:
            if event.user_joined or event.user_added:
                users = await event.get_users()
                chat_id = event.chat_id
                logger.info(f"👥 Users joined/added to chat {chat_id}: {[u.id for u in users]}")
                
                for user in users:
                    if isinstance(user, User):
                        logger.info(f"  → Fetching profile photos for new member {user.id}")
                        try:
                            async for photo in self.client.iter_profile_photos(user):
                                photo_id = getattr(photo, 'id', 0)
                                if not photo_id: continue
                                
                                if not await self._is_historical_photo_processed(user.id, photo_id):
                                    photo_bytes = await self.media_manager.download_specific_profile_photo(user, photo)
                                    
                                    if photo_bytes:
                                        await self.processing_queue.enqueue_profile_photo(
                                            user_id=user.id,
                                            content=io.BytesIO(photo_bytes)
                                        )
                                        await self._mark_historical_photo_processed(user.id, photo_id)
                        except Exception as e:
                            logger.error(f"Failed to fetch photos for new member {user.id}: {e}")
        except Exception as e:
            logger.error(f"Error processing chat action: {e}")

    async def _periodic_contact_scan(self):
        """Periodically rescans contacts to pick up new ones."""
        logger.info("🔄 Periodic contact monitor started (Interval: 60m)")
        while True:
            try:
                # Wait 60 minutes
                await asyncio.sleep(3600)
                
                logger.info("⏰ Running periodic contact scan...")
                
                contacts = await self.client.get_contacts()
                new_count = 0
                
                for user in contacts:
                    if isinstance(user, User):
                        try:
                            async for photo in self.client.iter_profile_photos(user):
                                pid = getattr(photo, 'id', 0)
                                if not pid: continue
                                
                                if not await self._check_if_historical_photo_processed(user.id, pid):
                                    photo_bytes = await self.media_manager.download_specific_profile_photo(user, photo)
                                    if photo_bytes:
                                        await self.processing_queue.enqueue_profile_photo(
                                            user_id=user.id,
                                            content=io.BytesIO(photo_bytes)
                                        )
                                        await self._mark_historical_photo_processed(user.id, pid)
                                        new_count += 1
                        except Exception as e:
                            logger.warning(f"Failed to download contact photo: {e}")
                                    
                if new_count > 0:
                    logger.info(f"✅ Periodic contact scan: Found {new_count} new profile photos")
                    
            except Exception as e:
                logger.error(f"Periodic contact scan failed: {e}")

    async def _check_if_historical_photo_processed(self, user_id: int, photo_id: int) -> bool:
        """Checks if a user's historical photo has been processed."""
        async with get_db_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT photo_id FROM processed_profile_photos WHERE user_id = %s AND photo_id = %s", (user_id, photo_id))
                row = await cur.fetchone()
                return row is not None

    async def _mark_historical_photo_processed(self, user_id: int, photo_id: int):
        """Records that a user's historical photo has been processed."""
        async with get_db_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("""
                    INSERT INTO processed_profile_photos (user_id, photo_id, processed_at) 
                    VALUES (%s, %s, NOW())
                    ON CONFLICT (user_id, photo_id) DO NOTHING
                """, (user_id, photo_id))
    
    async def _update_realtime_checkpoint(self, chat_id: int, message_id: int):
        """Updates checkpoint for realtime-scanned message."""
        async with get_db_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("""
                    UPDATE scan_checkpoints 
                    SET last_processed_message_id = %s, last_updated = NOW()
                    WHERE chat_id = %s
                """, (message_id, chat_id))
    
    def add_chat(self, chat_id: int):
        """Adds a chat to the monitoring list."""
        self.monitored_chats.add(chat_id)
    
    def remove_chat(self, chat_id: int):
        """Removes a chat from the monitoring list."""
        self.monitored_chats.discard(chat_id)
