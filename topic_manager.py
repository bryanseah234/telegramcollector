"""
Topic Manager - Handles Telegram Forum Topic creation and management.

Uses the BOT client (not user client) for topic operations.
Creates new topics for identities, uploads media to topics,
and manages topic metadata in database.
"""
import logging
import asyncio
import os
import random
from database import get_db_connection
try:
    from telethon.tl.functions.channels import CreateForumTopicRequest, EditForumTopicRequest
except ImportError:
    # Fallback or mock for environments where telethon might be different version
    pass

logger = logging.getLogger(__name__)

# Forum topic icon colors (Telegram palette)
TOPIC_ICON_COLORS = [
    0x6FB9F0,  # Blue
    0xFFD67E,  # Yellow
    0xCB86DB,  # Purple
    0x8EEE98,  # Green
    0xFF93B2,  # Pink
    0xFB6F5F,  # Red
]


class TopicManager:
    """
    Manages Telegram Forum Topics for identity organization.
    
    Uses the bot client for all Telegram operations (creating/renaming topics).
    This ensures the bot is the one managing the hub group, not user accounts.
    """
    
    def __init__(self, client=None):
        """
        Initialize Topic Manager.
        
        Args:
            client: Optional client for backwards compatibility.
                   If None, uses the bot_client singleton.
        """
        self._client = client
        self.hub_group_id = int(os.getenv('HUB_GROUP_ID', 0))
        self._label_counter = None
        
        if not self.hub_group_id:
            raise ValueError("HUB_GROUP_ID environment variable is required")
        
        # Cache for topic lookups to reduce DB hits
        # Map: identity_id -> db_topic_id
        self.topic_cache = {}
    
    async def _get_client(self):
        """Gets the Telegram client - prefers bot client."""
        if self._client is not None:
            return self._client
        
        # Use bot client singleton
        from bot_client import get_bot_client
        return await get_bot_client()
    
    async def _get_next_label_number(self) -> int:
        """DEPRECATED: Use DB auto-increment instead."""
        # This method is kept for backwards compatibility but should not be used
        async with get_db_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM telegram_topics")
                result = await cur.fetchone()
                return result[0] if result else 1
    
    async def create_topic(self, label: str = None) -> dict:
        """
        Creates a new forum topic in the hub group.
        
        Uses atomic DB insert to get unique ID, avoiding race conditions.
        
        Args:
            label: Optional label for the topic. If None, uses "Person N" format.
            
        Returns:
            Dict with 'db_id' and 'telegram_topic_id'
        """
        client = await self._get_client()
        
        # Select random icon color
        icon_color = random.choice(TOPIC_ICON_COLORS)
        
        # Generate a temporary unique label to avoid Telegram duplicate title issues
        import uuid
        temp_label = f"Person_{uuid.uuid4().hex[:8]}"
        
        from telethon.errors import FloodWaitError
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                # 1. First, reserve a DB ID to know the "Person X" number in advance
                # Use a random negative number to avoid collision on the unique constraint for topic_id
                # (Postgres constraint is on topic_id, which must be unique)
                import uuid
                temp_topic_id_reservation = -1 * random.randint(1, 2**31 - 1)
                temp_label_reservation = "Reserved_Topic" 
                
                # Verify we don't accidentally hit an existing one (unlikely but possible with random)
                db_id = await self._save_topic_to_db(temp_topic_id_reservation, temp_label_reservation)
                
                # 2. Determine final label
                final_label = label if label else f"Person {db_id}"
                
                # 3. Create topic with FINAL name directly
                # Get the hub group entity
                hub = await client.get_input_entity(self.hub_group_id)
                
                result = await client(CreateForumTopicRequest(
                    channel=hub,
                    title=final_label,
                    icon_color=icon_color,
                    random_id=random.randint(1, 2**31 - 1)
                ))
                
                # Extract topic ID from result
                telegram_topic_id = None
                if hasattr(result, 'updates'):
                    for update in result.updates:
                        if hasattr(update, 'id'):
                            telegram_topic_id = update.id
                            break
                
                if not telegram_topic_id:
                     # Fallback: get from the message
                    if hasattr(result, 'updates'):
                        for update in result.updates:
                            if hasattr(update, 'message') and hasattr(update.message, 'reply_to'):
                                if hasattr(update.message.reply_to, 'reply_to_top_id'):
                                    telegram_topic_id = update.message.reply_to.reply_to_top_id
                                    break
                
                if not telegram_topic_id:
                    # rollback DB entry? or just leave as unused gap?
                    # Gap is safer than deleting potentially wrong thing
                    logger.error(f"Failed to get topic ID for DB ID {db_id}")
                    raise RuntimeError("Could not extract topic ID")
                
                # 4. Update the DB entry with the real Telegram Topic ID and Label
                async with get_db_connection() as conn:
                    async with conn.cursor() as cur:
                        await cur.execute("""
                            UPDATE telegram_topics 
                            SET topic_id = %s, label = %s
                            WHERE id = %s
                        """, (telegram_topic_id, final_label, db_id))

                logger.info(f"Created topic '{final_label}' (DB ID: {db_id}, Telegram ID: {telegram_topic_id})")
                
                return {
                    'db_id': db_id,
                    'telegram_topic_id': telegram_topic_id,
                    'label': final_label
                }
                
            except FloodWaitError as e:
                wait_time = e.seconds + 5
                logger.warning(f"⏳ FloodWait creating topic. Waiting {wait_time}s (attempt {attempt + 1}/{max_retries})...")
                await asyncio.sleep(wait_time)
                # Continue to next attempt
                
            except Exception as e:
                logger.error(f"Failed to create topic: {e}")
                # If we reserved an ID but failed, it stays as "Reserved_Topic" with ID 0
                # This is a minor junk data issue, acceptable trade-off for speed
                raise
        
        raise RuntimeError(f"Failed to create topic after {max_retries} FloodWait retries")
    
    async def rename_topic(self, db_topic_id: int, new_label: str) -> bool:
        """
        Renames an existing forum topic.
        
        Args:
            db_topic_id: The database ID of the topic
            new_label: The new name for the topic
            
        Returns:
            True if successful
        """
        client = await self._get_client()
        from telethon.errors import FloodWaitError
        
        try:
            # Get the Telegram topic ID from database
            topic_info = await self.get_topic_info(db_topic_id)
            if not topic_info:
                logger.error(f"Topic {db_topic_id} not found in database")
                return False
            
            telegram_topic_id = topic_info['telegram_topic_id']
            
            # Get hub entity
            hub = await client.get_input_entity(self.hub_group_id)
            
            # Rename in Telegram (with FloodWait handling)
            try:
                await client(EditForumTopicRequest(
                    channel=hub,
                    topic_id=telegram_topic_id,
                    title=new_label
                ))
            except FloodWaitError as e:
                logger.warning(f"⏳ FloodWait on rename. Waiting {e.seconds}s...")
                await asyncio.sleep(e.seconds)
                await client(EditForumTopicRequest(
                    channel=hub,
                    topic_id=telegram_topic_id,
                    title=new_label
                ))
            
            # Update database
            async with get_db_connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("""
                        UPDATE telegram_topics 
                        SET label = %s, updated_at = NOW()
                        WHERE id = %s
                    """, (new_label, db_topic_id))
            
            logger.info(f"Renamed topic {db_topic_id} to '{new_label}'")
            return True
            
        except Exception as e:
            logger.error(f"Failed to rename topic {db_topic_id}: {e}")
            return False
    
    async def get_topic_info(self, db_topic_id: int) -> dict:
        """
        Retrieves topic details from the database.
        
        Args:
            db_topic_id: The database ID of the topic
            
        Returns:
            Dict with topic info or None if not found
        """
        async with get_db_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("""
                    SELECT id, topic_id, label, face_count, message_count, created_at
                    FROM telegram_topics
                    WHERE id = %s
                """, (db_topic_id,))
                row = await cur.fetchone()
                
                if row:
                    return {
                        'db_id': row[0],
                        'telegram_topic_id': row[1],
                        'label': row[2],
                        'face_count': row[3],
                        'message_count': row[4],
                        'created_at': row[5]
                    }
                return None
    
    async def get_topic_by_telegram_id(self, telegram_topic_id: int) -> dict:
        """
        Retrieves topic by Telegram topic ID.
        """
        async with get_db_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("""
                    SELECT id, topic_id, label, face_count, message_count
                    FROM telegram_topics
                    WHERE topic_id = %s
                """, (telegram_topic_id,))
                row = await cur.fetchone()
                
                if row:
                    return {
                        'db_id': row[0],
                        'telegram_topic_id': row[1],
                        'label': row[2],
                        'face_count': row[3],
                        'message_count': row[4]
                    }
                return None
    
    async def _save_topic_to_db(self, telegram_topic_id: int, label: str) -> int:
        """
        Saves a new topic to the database.
        
        Returns:
            The database ID of the new topic
        """
        async with get_db_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("""
                    INSERT INTO telegram_topics (topic_id, label)
                    VALUES (%s, %s)
                    RETURNING id
                """, (telegram_topic_id, label))
                result = await cur.fetchone()
                return result[0] if result else 0
    
    async def increment_message_count(self, db_topic_id: int):
        """Increments the message count for a topic."""
        async with get_db_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("""
                    UPDATE telegram_topics 
                    SET message_count = message_count + 1
                    WHERE id = %s
                """, (db_topic_id,))
    
    async def increment_face_count(self, db_topic_id: int):
        """Increments the face count for a topic."""
        async with get_db_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("""
                    UPDATE telegram_topics 
                    SET face_count = face_count + 1
                    WHERE id = %s
                """, (db_topic_id,))
