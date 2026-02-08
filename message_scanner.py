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

    async def discover_and_scan_all_chats(self, account_id: int):
        """
        Iterates through ALL dialogs (groups, channels, DMs) and adds them to the scan list.
        Categorizes by type: 'personal', 'group', 'channel'
        Feature: "scraping of all existing groups, channels and DMs"
        """
        logger.info("Starting discovery of all dialogs...")
        count = 0
        stats = {'personal': 0, 'group': 0, 'channel': 0}
        
        try:
            async for dialog in self.client.iter_dialogs():
                chat_id = dialog.id
                chat_title = dialog.title or f"Chat_{chat_id}"
                
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
        last_id = checkpoint.get('last_processed_message_id', 0) if checkpoint else 0
        
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
        # Compares photo_id to detect when user changes their profile photo
        try:
            sender = await message.get_sender()
            if sender and isinstance(sender, User) and sender.photo:
                current_photo_id = getattr(sender.photo, 'photo_id', 0)
                
                # Check if this specific photo_id has been processed
                # If user changed photo (new photo_id), we'll process it again
                if not await self._is_photo_processed(sender.id, current_photo_id):
                    logger.debug(f"Fetching profile photo for user {sender.id} (photo_id: {current_photo_id})")
                    photo_bytes = await self.client.download_profile_photo(sender, file=bytes)
                    if photo_bytes:
                        await self.processing_queue.enqueue_profile_photo(
                            user_id=sender.id,
                            content=io.BytesIO(photo_bytes)
                        )
                        await self._mark_user_processed(sender.id, current_photo_id)
        except Exception as e:
            logger.debug(f"Could not process profile photo for message {message.id}: {e}")

    async def _get_checkpoint(self, account_id: int, chat_id: int) -> dict:
        """Retrieves the checkpoint for a chat."""
        async with get_db_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("""
                    SELECT last_processed_message_id, processed_messages 
                    FROM scan_checkpoints 
                    WHERE account_id = %s AND chat_id = %s
                """, (account_id, chat_id))
                row = await cur.fetchone()
                if row:
                    return {
                        'last_processed_message_id': row[0] or 0,
                        'processed_messages': row[1] or 0
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

    async def _mark_chat_complete(self, account_id: int, chat_id: int, total_processed: int):
        """Marks a chat as fully scanned."""
        async with get_db_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("""
                    UPDATE scan_checkpoints 
                    SET scan_mode = 'realtime',
                        processed_messages = %s,
                        last_updated = NOW()
                    WHERE account_id = %s AND chat_id = %s
                """, (total_processed, account_id, chat_id))

    async def _get_chat_message_count(self, chat_id: int) -> int:
        """Gets total message count for a chat (for progress tracking)."""
        try:
            # Get a single message to access total count
            messages = await self.client.get_messages(chat_id, limit=1)
            return messages.total if messages else 0
        except Exception:
            return 0

    async def _is_photo_processed(self, user_id: int, photo_id: int) -> bool:
        """
        Checks if a user's specific photo_id has been processed.
        """
        async with get_db_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("""
                    SELECT photo_id FROM processed_users WHERE user_id = %s
                """, (user_id,))
                row = await cur.fetchone()
                
                if row is None:
                    return False  # Never seen this user
                
                # Compare stored photo_id with current one
                stored_photo_id = row[0]
                return stored_photo_id == photo_id

    async def _mark_user_processed(self, user_id: int, photo_id: int):
        """
        Records that a user's profile photo (identified by photo_id) has been processed.
        """
        async with get_db_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("""
                    INSERT INTO processed_users (user_id, photo_id, last_scan) 
                    VALUES (%s, %s, NOW())
                    ON CONFLICT (user_id) DO UPDATE SET 
                        photo_id = EXCLUDED.photo_id,
                        last_scan = NOW()
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
    """
    
    def __init__(self, client, processing_queue, media_manager):
        self.client = client
        self.processing_queue = processing_queue
        self.media_manager = media_manager
        self.monitored_chats = set()
        self._handler_added = False
    
    async def start_monitoring(self, chat_ids: list, account_id: int):
        """
        Begins real-time monitoring of specified chats.
        Registers event handlers that trigger when new messages arrive.
        """
        self.monitored_chats = set(chat_ids)
        self._account_id = account_id
        
        if not self._handler_added:
            @self.client.on(events.NewMessage(chats=list(chat_ids)))
            async def handle_new_message(event):
                await self._process_new_message(event)
            
            self._handler_added = True
        
        logger.info(f"Real-time monitoring started for {len(chat_ids)} chats")
    
    async def _process_new_message(self, event):
        """Handles incoming messages from monitored chats."""
        message = event.message
        chat_id = event.chat_id
        
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
