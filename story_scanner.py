"""
Story Scanner - Discovers and downloads Telegram Stories for face processing.

Stories expire after 24 hours, so this scanner runs on a tight polling interval
(default 5 minutes). Only user accounts can access stories — bots cannot.

Uses Telethon's raw API:
- GetAllStoriesRequest: Discovers all visible stories
- GetPeerStoriesRequest: Gets stories from a specific peer

Stories are enqueued with higher priority than regular media tasks.
"""
import logging
import asyncio
import io
from datetime import datetime, timezone
from typing import Optional, Set

from telethon.tl.functions.stories import GetAllStoriesRequest, GetPeerStoriesRequest
from telethon.tl.types import (
    stories,
    StoryItem,
    StoryItemSkipped,
    StoryItemDeleted,
    MessageMediaPhoto,
    MessageMediaDocument,
)
from telethon.errors import FloodWaitError

from database import get_db_connection
from hub_notifier import notify, increment_stat
from config import settings

logger = logging.getLogger(__name__)


class StoryScanner:
    """
    Scans Telegram Stories from all visible peers.
    
    Features:
    - Polls all visible stories on a configurable interval
    - Deduplicates via processed_stories DB table
    - Enqueues story media with elevated priority
    - Handles photo and video stories
    - Gracefully handles FloodWait errors
    """
    
    def __init__(self, client, processing_queue, media_manager):
        """
        Args:
            client: Telethon TelegramClient (user account, NOT bot)
            processing_queue: ProcessingQueue instance for enqueuing tasks
            media_manager: MediaDownloadManager for downloading story media
        """
        self.client = client
        self.processing_queue = processing_queue
        self.media_manager = media_manager
        self._running = False
        self._poll_task = None
        
        # In-memory cache of recently processed story IDs to reduce DB lookups
        self._recent_cache: Set[str] = set()
        self._cache_max_size = 10000
        
        # Stats
        self.stats = {
            'stories_checked': 0,
            'stories_downloaded': 0,
            'stories_skipped': 0,
            'errors': 0,
            'last_scan': None,
        }
        
        logger.info("StoryScanner initialized")
    
    async def start_polling(self, account_id: int, interval: int = None):
        """
        Starts the background polling loop for stories.
        
        Args:
            account_id: Database account ID for this user session
            interval: Poll interval in seconds (default from config)
        """
        if interval is None:
            interval = settings.STORY_SCAN_INTERVAL
        
        self._running = True
        self._poll_task = asyncio.create_task(
            self._polling_loop(account_id, interval)
        )
        logger.info(f"Story scanner started for account {account_id} (interval={interval}s)")
    
    async def stop(self):
        """Stops the polling loop."""
        self._running = False
        if self._poll_task:
            self._poll_task.cancel()
            try:
                await self._poll_task
            except asyncio.CancelledError:
                pass
        logger.info("Story scanner stopped")
    
    async def _polling_loop(self, account_id: int, interval: int):
        """Background loop that periodically scans for new stories."""
        while self._running:
            try:
                await self.scan_all_stories(account_id)
            except asyncio.CancelledError:
                break
            except FloodWaitError as e:
                wait_time = min(e.seconds + 5, 600)  # Cap at 10 min
                logger.warning(f"Story scanner FloodWait: waiting {wait_time}s")
                await asyncio.sleep(wait_time)
            except Exception as e:
                logger.error(f"Story scan error: {e}")
                self.stats['errors'] += 1
                await asyncio.sleep(30)  # Brief cooldown on error
            
            # Wait for next poll
            try:
                await asyncio.sleep(interval)
            except asyncio.CancelledError:
                break
    
    async def scan_all_stories(self, account_id: int):
        """
        Fetches all currently visible stories and processes new ones.
        
        Uses GetAllStoriesRequest which returns stories from all peers
        the user can see (contacts, followed channels, etc).
        """
        logger.debug(f"Scanning stories for account {account_id}...")
        
        try:
            result = await self.client(GetAllStoriesRequest(
                next=False,
                hidden=False,
                state=""
            ))
        except Exception as e:
            logger.error(f"GetAllStoriesRequest failed: {e}")
            self.stats['errors'] += 1
            return
        
        # result.peer_stories is a list of PeerStories objects
        peer_stories_list = getattr(result, 'peer_stories', [])
        
        if not peer_stories_list:
            logger.debug("No stories found")
            self.stats['last_scan'] = datetime.now(timezone.utc)
            return
        
        new_count = 0
        skip_count = 0
        
        for peer_stories in peer_stories_list:
            peer_id = None
            
            # Extract peer ID
            peer = getattr(peer_stories, 'peer', None)
            if peer:
                peer_id = getattr(peer, 'user_id', None) or getattr(peer, 'channel_id', None)
            
            if not peer_id:
                continue
            
            story_items = getattr(peer_stories, 'stories', [])
            
            for story in story_items:
                # Skip non-content items
                if isinstance(story, (StoryItemSkipped, StoryItemDeleted)):
                    continue
                
                if not isinstance(story, StoryItem):
                    continue
                
                story_id = story.id
                cache_key = f"{peer_id}:{story_id}:{account_id}"
                
                # Fast in-memory dedup check
                if cache_key in self._recent_cache:
                    skip_count += 1
                    continue
                
                # DB dedup check
                if await self._is_story_processed(story_id, peer_id, account_id):
                    self._recent_cache.add(cache_key)
                    skip_count += 1
                    continue
                
                # Process this story
                success = await self._download_and_enqueue(
                    story, account_id, peer_id
                )
                
                if success:
                    new_count += 1
                    self._recent_cache.add(cache_key)
                    
                    # Trim cache if too large
                    if len(self._recent_cache) > self._cache_max_size:
                        # Remove oldest half
                        to_remove = list(self._recent_cache)[:self._cache_max_size // 2]
                        for key in to_remove:
                            self._recent_cache.discard(key)
        
        self.stats['stories_checked'] += new_count + skip_count
        self.stats['stories_downloaded'] += new_count
        self.stats['stories_skipped'] += skip_count
        self.stats['last_scan'] = datetime.now(timezone.utc)
        
        if new_count > 0:
            logger.info(f"📖 Stories: {new_count} new, {skip_count} skipped (account {account_id})")
            await notify('scan', f"📖 {new_count} new stories found", priority=1)
        else:
            logger.debug(f"Stories: {skip_count} already processed (account {account_id})")
    
    async def _download_and_enqueue(
        self,
        story: StoryItem,
        account_id: int,
        peer_id: int
    ) -> bool:
        """
        Downloads a story's media and enqueues it for face processing.
        
        Args:
            story: Telethon StoryItem object
            account_id: Database account ID
            peer_id: Telegram user/channel ID of the story owner
            
        Returns:
            True if successfully enqueued
        """
        try:
            media = story.media
            if not media:
                return False
            
            # Determine media type
            if isinstance(media, MessageMediaPhoto):
                media_type = 'photo'
            elif isinstance(media, MessageMediaDocument):
                doc = media.document
                mime = getattr(doc, 'mime_type', '') or ''
                if mime.startswith('video/'):
                    media_type = 'video'
                elif mime.startswith('image/'):
                    media_type = 'photo'
                else:
                    logger.debug(f"Skipping story {story.id}: unsupported mime {mime}")
                    return False
            else:
                logger.debug(f"Skipping story {story.id}: unsupported media type {type(media)}")
                return False
            
            # Download media into memory buffer
            buffer = io.BytesIO()
            await self.client.download_media(media, file=buffer)
            buffer.seek(0)
            
            if buffer.getbuffer().nbytes == 0:
                logger.warning(f"Downloaded empty story {story.id} from peer {peer_id}")
                return False
            
            # Get expiry date if available
            expire_date = getattr(story, 'expire_date', None)
            
            # Enqueue with story priority
            await self.processing_queue.enqueue_story(
                peer_id=peer_id,
                story_id=story.id,
                content=buffer,
                media_type=media_type,
                account_id=account_id
            )
            
            # Mark as processed in DB
            await self._mark_story_processed(
                story_id=story.id,
                peer_id=peer_id,
                account_id=account_id,
                media_type=media_type,
                expire_date=expire_date
            )
            
            logger.debug(f"Enqueued story {story.id} ({media_type}) from peer {peer_id}")
            return True
            
        except FloodWaitError:
            raise  # Let the polling loop handle this
        except Exception as e:
            logger.error(f"Failed to process story {story.id}: {e}")
            self.stats['errors'] += 1
            return False
    
    async def _is_story_processed(
        self, story_id: int, peer_id: int, account_id: int
    ) -> bool:
        """Checks if a story has already been processed."""
        try:
            async with get_db_connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        """SELECT 1 FROM processed_stories 
                           WHERE story_id = %s AND peer_id = %s AND account_id = %s""",
                        (story_id, peer_id, account_id)
                    )
                    return await cur.fetchone() is not None
        except Exception as e:
            logger.error(f"DB check for story {story_id} failed: {e}")
            return False  # Process it if we can't check
    
    async def _mark_story_processed(
        self,
        story_id: int,
        peer_id: int,
        account_id: int,
        media_type: str = 'photo',
        expire_date=None
    ):
        """Records that a story has been processed."""
        try:
            async with get_db_connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        """INSERT INTO processed_stories 
                           (story_id, peer_id, account_id, media_type, expire_date)
                           VALUES (%s, %s, %s, %s, %s)
                           ON CONFLICT (story_id, peer_id, account_id) DO NOTHING""",
                        (story_id, peer_id, account_id, media_type, expire_date)
                    )
                await conn.commit()
        except Exception as e:
            logger.error(f"Failed to mark story {story_id} as processed: {e}")
    
    def get_status(self) -> dict:
        """Returns current scanner status for health reports."""
        return {
            'running': self._running,
            'stories_checked': self.stats['stories_checked'],
            'stories_downloaded': self.stats['stories_downloaded'],
            'stories_skipped': self.stats['stories_skipped'],
            'errors': self.stats['errors'],
            'last_scan': self.stats['last_scan'].isoformat() if self.stats['last_scan'] else None,
            'cache_size': len(self._recent_cache),
        }
