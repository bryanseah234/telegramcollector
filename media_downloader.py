"""
Media Download Manager - Handles in-memory media downloads from Telegram.

Implements size limits, streaming downloads, and memory efficiency.
Integrates with Phase 2's TelegramClientManager.
"""
import logging
import asyncio
import io
import os
from typing import Optional

logger = logging.getLogger(__name__)


class MediaDownloadManager:
    """
    Handles downloading media from Telegram messages into memory buffers.
    Implements size limits and streaming downloads.
    """
    
    def __init__(self, client, max_size_mb: int = None):
        """
        Args:
            client: Telethon TelegramClient instance (from Phase 2)
            max_size_mb: Maximum file size in MB (from env or default 200)
        """
        self.client = client
        
        # Get max size from environment or use default
        max_size = max_size_mb or int(os.getenv('MAX_MEDIA_SIZE_MB', 200))
        self.max_size_bytes = max_size * 1024 * 1024
        
        # Limit concurrent downloads to prevent memory spikes
        # 3 concurrent 5MB photos is fine, but 3 concurrent 50MB videos is heavy.
        self._download_semaphore = asyncio.Semaphore(5)
        
        logger.info(f"MediaDownloadManager initialized. Max size: {max_size}MB")
    
    async def download_media(self, message) -> Optional[io.BytesIO]:
        """
        Downloads media from a Telegram message into memory.
        
        Args:
            message: Telethon Message object with photo/video/document
            
        Returns:
            BytesIO buffer containing the media, or None if failed/too large
        """
        try:
            # Check file size before downloading
            if not self._check_size(message):
                return None
            
            async with self._download_semaphore:
                # Create buffer for download
                buffer = io.BytesIO()
                
                # Download directly to buffer
                await message.download_media(file=buffer)
                
                # Reset buffer position for reading
                buffer.seek(0)
            
            # Verify we got content
            
            # Verify we got content
            if buffer.getbuffer().nbytes == 0:
                logger.warning(f"Downloaded empty content from message {message.id}")
                return None
            
            logger.debug(f"Downloaded {buffer.getbuffer().nbytes} bytes from message {message.id}")
            return buffer
            
        except Exception as e:
            logger.error(f"Failed to download media from message {message.id}: {e}")
            return None
    
    def _check_size(self, message) -> bool:
        """
        Checks if the message's media is within size limits.
        
        Returns:
            True if within limits or size unknown, False if too large
        """
        try:
            # Get file size from message attributes
            file_size = None
            
            if message.photo:
                # Photos don't have explicit size, but are usually small
                # Get from the largest photo size
                if hasattr(message.photo, 'sizes') and message.photo.sizes:
                    # Photos are generally within limits
                    return True
                    
            elif message.document:
                file_size = message.document.size
                
            elif message.video:
                # Video is a document type
                if hasattr(message.video, 'size'):
                    file_size = message.video.size
            
            if file_size and file_size > self.max_size_bytes:
                size_mb = file_size / (1024 * 1024)
                max_mb = self.max_size_bytes / (1024 * 1024)
                logger.warning(f"File too large ({size_mb:.1f}MB > {max_mb:.0f}MB limit). Skipping message {message.id}")
                return False
            
            return True
            
        except Exception as e:
            logger.debug(f"Could not determine file size: {e}")
            # If we can't determine size, try to download anyway
            return True
    
    async def download_profile_photo(self, user) -> Optional[bytes]:
        """
        Downloads a user's profile photo.
        
        Args:
            user: Telethon User object
            
        Returns:
            Bytes of the photo, or None if failed
        """
        try:
            photo_bytes = await self.client.download_profile_photo(user, file=bytes)
            if photo_bytes:
                logger.debug(f"Downloaded profile photo for user {user.id}: {len(photo_bytes)} bytes")
            return photo_bytes
        except Exception as e:
            logger.warning(f"Failed to download profile photo for user {user.id}: {e}")
            return None
    
    def get_media_type(self, message) -> str:
        """
        Determines the type of media in a message.
        
        Returns:
            'photo', 'video', 'video_note' (round video), 'document', or 'unknown'
        """
        if message.photo:
            return 'photo'
        
        if message.video_note:
            return 'video_note'  # Round videos
        
        if message.video:
            return 'video'
        
        if message.document:
            mime = getattr(message.document, 'mime_type', '') or ''
            if mime.startswith('image/'):
                return 'photo'
            elif mime.startswith('video/'):
                return 'video'
            return 'document'
        
        return 'unknown'
    
    def is_round_video(self, message) -> bool:
        """Checks if the message contains a round video (video note)."""
        return bool(message.video_note)
