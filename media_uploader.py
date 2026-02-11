"""
Media Uploader - Handles uploading media to Telegram forum topics.

Uses the BOT client (not user client) for uploads.
Implements retry logic, duplicate prevention, and rate limiting.
"""
import logging
import asyncio
import io
import os
from telethon.tl.types import InputMediaUploadedPhoto, InputMediaUploadedDocument
from telethon.errors import FloodWaitError
from database import get_db_connection
from hub_notifier import notify, increment_stat

logger = logging.getLogger(__name__)


class MediaUploader:
    """
    Handles media upload to Telegram forum topics with retry and deduplication.
    
    Uses the bot client for all uploads - user accounts are only for scanning.
    """
    
    def __init__(self, client=None, topic_manager=None):
        """
        Initialize MediaUploader.
        
        Args:
            client: Optional client for backwards compatibility.
                   If None, uses the bot_client singleton.
            topic_manager: Optional TopicManager instance.
        """
        from config import settings
        self._client = client
        self.topic_manager = topic_manager
        self.hub_group_id = settings.HUB_GROUP_ID
        
        # Semaphore to limit concurrent uploads
        self._upload_semaphore = asyncio.Semaphore(3)
        
        # Retry configuration
        self.max_retries = 3
        self.base_retry_delay = 2  # seconds
    
    async def _get_client(self):
        """Gets the Telegram client - prefers bot client."""
        if self._client is not None:
            return self._client
        
        # Use bot client singleton
        from bot_client import get_bot_client
        return await get_bot_client()
    
    async def upload_to_topic(
        self, 
        db_topic_id: int, 
        media_buffer: io.BytesIO, 
        source_message_id: int,
        source_chat_id: int,
        caption: str = None,
        media_type: str = 'photo'
    ) -> int:
        """
        Uploads media to a specific forum topic.
        
        Args:
            db_topic_id: Database ID of the target topic
            media_buffer: BytesIO buffer containing the media
            source_message_id: Original message ID (for deduplication)
            source_chat_id: Original chat ID (for caption)
            caption: Optional caption text
            media_type: 'photo', 'video', or 'video_note'
            
        Returns:
            The message_id of the uploaded media, or 0 on failure
        """
        # 0. Validate input buffer
        if not media_buffer:
            logger.error(f"❌ Empty media buffer provided for msg {source_message_id}")
            return 0
            
        try:
            current_pos = media_buffer.tell()
            media_buffer.seek(0, os.SEEK_END)
            size = media_buffer.tell()
            media_buffer.seek(current_pos)
            
            if size == 0:
                logger.error(f"❌ Zero-byte media buffer for msg {source_message_id}")
                return 0
                
            if size > 50 * 1024 * 1024:  # 50MB safety limit (Telegram bot limit)
                logger.warning(f"⚠️ Large media ({size/1024/1024:.2f}MB) for msg {source_message_id}. Upload might fail.")
        except Exception as e:
            logger.error(f"❌ Failed to validate media buffer: {e}")
            return 0

        # Check for duplicate
        if await self._is_duplicate(source_message_id, source_chat_id, db_topic_id):
            logger.info(f"⏭️ Skipping duplicate upload: msg {source_message_id} from chat {source_chat_id} to topic {db_topic_id}")
            return -1  # Return special code for duplicate/skip
        
        # Get topic info
        topic_info = await self.topic_manager.get_topic_info(db_topic_id)
        if not topic_info:
            logger.error(f"❌ Topic {db_topic_id} not found in DB - cannot upload media!")
            return 0
        
        telegram_topic_id = topic_info['telegram_topic_id']
        
        # Generate caption if not provided
        if caption is None:
            caption = await self._generate_caption(source_chat_id, source_message_id)
        
        # Acquire semaphore for rate limiting
        async with self._upload_semaphore:
            return await self._upload_with_retry(
                telegram_topic_id,
                media_buffer,
                source_message_id,
                source_chat_id,
                db_topic_id,
                caption,
                media_type
            )
    
    async def _upload_with_retry(
        self,
        telegram_topic_id: int,
        media_buffer: io.BytesIO,
        source_message_id: int,
        source_chat_id: int,
        db_topic_id: int,
        caption: str,
        media_type: str = 'photo'
    ) -> int:
        """Uploads with exponential backoff retry logic. Preserves media format."""
        
        client = await self._get_client()
        
        for attempt in range(self.max_retries):
            try:
                # Reset buffer position
                media_buffer.seek(0)
                
                # Determine file extension based on media type
                extension_map = {
                    'photo': '.jpg',
                    'video': '.mp4',
                    'video_note': '.mp4',
                }
                ext = extension_map.get(media_type, '.bin')
                filename = f"media_{source_chat_id}_{source_message_id}{ext}"
                
                # Upload the file with proper filename
                uploaded_file = await client.upload_file(
                    media_buffer,
                    file_name=filename
                )
                
                # Prepare send_file kwargs based on media type
                send_kwargs = {
                    'caption': caption,
                    'reply_to': telegram_topic_id,
                }
                
                if media_type == 'video_note':
                    # Send as regular video (video notes don't support captions)
                    send_kwargs['force_document'] = False
                elif media_type == 'video':
                    # Regular video (not as document)
                    send_kwargs['force_document'] = False
                elif media_type == 'photo':
                    # Photo (not as document)
                    send_kwargs['force_document'] = False
                
                # Send to the hub group, targeting the topic
                message = await client.send_file(
                    self.hub_group_id,
                    uploaded_file,
                    **send_kwargs
                )
                
                # Record successful upload
                await self._record_upload(
                    source_message_id,
                    source_chat_id,
                    db_topic_id,
                    message.id
                )
                
                # Update topic message count
                await self.topic_manager.increment_message_count(db_topic_id)
                
                logger.info(f"Uploaded media to topic {telegram_topic_id}, message {message.id}")
            
                # Track successful upload for Hub stats
                await increment_stat('uploads_completed', 1)
            
                return message.id
                
            except FloodWaitError as e:
                wait_time = e.seconds + 1
                if wait_time > 300:
                    raise  # Re-raise to be caught by caller as "Long FloodWait"
                    
                logger.warning(f"FloodWait during upload. Waiting {wait_time}s...")
                await asyncio.sleep(wait_time)
                # Don't count this as an attempt
                continue
                
            except Exception as e:
                delay = self.base_retry_delay * (2 ** attempt)
                logger.warning(f"Upload failed (attempt {attempt + 1}/{self.max_retries}): {e}")
                
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(delay)
        
        logger.error(f"Failed to upload after {self.max_retries} attempts")
    
        # Notify Hub about upload failure (immediate alert)
        await notify('error', f"Upload failed after {self.max_retries} retries", priority=1)
        await increment_stat('errors_count', 1)
    
        return 0
    
    async def _is_duplicate(self, source_message_id: int, source_chat_id: int, topic_id: int) -> bool:
        """Checks if this media has already been uploaded to this specific topic."""
        async with get_db_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("""
                    SELECT 1 FROM uploaded_media 
                    WHERE source_message_id = %s AND source_chat_id = %s AND topic_id = %s
                """, (source_message_id, source_chat_id, topic_id))
                return await cur.fetchone() is not None
    
    async def _record_upload(
        self, 
        source_message_id: int, 
        source_chat_id: int, 
        topic_id: int, 
        hub_message_id: int
    ):
        """Records a successful upload to prevent duplicates."""
        async with get_db_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("""
                    INSERT INTO uploaded_media 
                        (source_message_id, source_chat_id, topic_id, hub_message_id)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (source_chat_id, source_message_id, topic_id) DO NOTHING
                """, (source_message_id, source_chat_id, topic_id, hub_message_id))
    
    async def _generate_caption(self, source_chat_id: int, source_message_id: int) -> str:
        """Generates a caption with source metadata."""
        # Try to get chat title from database
        chat_title = await self._get_chat_title(source_chat_id)
        
        if chat_title:
            return f"📍 From: {chat_title}\n🔗 Message ID: {source_message_id}"
        else:
            return f"📍 Chat: {source_chat_id}\n🔗 Message: {source_message_id}"
    
    async def _get_chat_title(self, chat_id: int) -> str:
        """Gets chat title from checkpoint table."""
        async with get_db_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("""
                    SELECT chat_title FROM scan_checkpoints 
                    WHERE chat_id = %s LIMIT 1
                """, (chat_id,))
                row = await cur.fetchone()
                return row[0] if row else None
    
    async def bulk_upload(
        self, 
        db_topic_id: int, 
        media_items: list
    ) -> list:
        """
        Uploads multiple media items to a topic.
        
        Args:
            db_topic_id: Database ID of the target topic
            media_items: List of dicts with 'buffer', 'message_id', 'chat_id'
            
        Returns:
            List of uploaded message IDs
        """
        results = []
        
        for item in media_items:
            message_id = await self.upload_to_topic(
                db_topic_id=db_topic_id,
                media_buffer=item['buffer'],
                source_message_id=item['message_id'],
                source_chat_id=item['chat_id'],
                caption=item.get('caption'),
                media_type=item.get('media_type', 'photo')
            )
            results.append(message_id)
            
            # Small delay between uploads to avoid rate limiting
            await asyncio.sleep(0.5)
        
        return results
