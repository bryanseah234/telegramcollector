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
import uuid
from datetime import datetime, timedelta, timezone
from database import get_db_connection
from resilience import CircuitOpenError

try:
    from telethon.tl.functions.channels import CreateForumTopicRequest, EditForumTopicRequest
except ImportError:
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
        temp_label = f"Person_{uuid.uuid4().hex[:8]}"
        
        from telethon.errors import FloodWaitError
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                # 1. First, reserve a DB ID to know the "Person X" number in advance
                # Use a random negative number to avoid collision on the unique constraint for topic_id
                # (Postgres constraint is on topic_id, which must be unique)
                temp_topic_id_reservation = -1 * random.randint(1, 2**31 - 1)
                temp_label_reservation = "Reserved_Topic" 
                
                # Verify we don't accidentally hit an existing one (unlikely but possible with random)
                db_id = await self._save_topic_to_db(temp_topic_id_reservation, temp_label_reservation)
                # 2. Determine final label (Backwards compatibility: if label provided, use it, else wait for ID)
                # But per user request: "make it simple as possible... label same string as thread id"
                # So we will update it AFTER we get the telegram_topic_id
                
                # 3. Create topic with FINAL name directly
                # Get the hub group entity
                try:
                    hub = await client.get_input_entity(self.hub_group_id)
                except (ValueError, Exception) as e:
                    logger.warning(f"Failed to get input entity for Hub {self.hub_group_id}, attempting full resolution: {e}")
                    # Force full resolution if input entity fails
                    hub = await client.get_entity(self.hub_group_id)
                
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
                # Per user request: Label = Thread ID
                final_label = label if label else str(telegram_topic_id)
                
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
            
            except CircuitOpenError as e:
                wait_time = 10 * (attempt + 1)
                logger.warning(f"🔌 Database Circuit Open. Waiting {wait_time}s (attempt {attempt + 1}/{max_retries})...")
                await asyncio.sleep(wait_time)
                
            except Exception as e:
                logger.error(f"Failed to create topic: {e}")
                
                # Cleanup: If we failed to create the Telegram topic, 
                # delete the "Reserved_Topic" entry from DB to prevent junk buildup
                try:
                    async with get_db_connection() as conn:
                        async with conn.cursor() as cur:
                            await cur.execute("DELETE FROM telegram_topics WHERE id = %s", (db_id,))
                    logger.info(f"Cleaned up reserved DB ID {db_id} after failure")
                except Exception as cleanup_e:
                    logger.debug(f"Failed to cleanup DB ID {db_id}: {cleanup_e}")
                
                raise
        
        raise RuntimeError(f"Failed to create topic after {max_retries} retries")
    
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
            try:
                hub = await client.get_input_entity(self.hub_group_id)
            except (ValueError, Exception) as e:
                logger.warning(f"Failed to get input entity for Hub on rename, attempting full resolution: {e}")
                hub = await client.get_entity(self.hub_group_id)
            
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
            
        except CircuitOpenError:
            logger.warning(f"🔌 Database Circuit Open. Skipping rename of topic {db_topic_id}.")
            return False
            
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
    
    async def cleanup_topic(self, telegram_topic_id: int, retention_hours: int = 12, client=None):
        """
        Deletes messages in a topic older than retention_hours.
        
        Args:
            telegram_topic_id: The Telegram ID of the topic (thread_id)
            retention_hours: Delete messages older than this
            client: Optional client to use (defaults to bot client)
        """
        if client is None:
            client = await self._get_client()
            
        cutoff = datetime.now(timezone.utc) - timedelta(hours=retention_hours)
        
        logger.info(f"🧹 Periodic Cleanup: Checking topic {telegram_topic_id} for messages older than {retention_hours}h (cutoff: {cutoff})")
        
        try:
            hub_entity = await client.get_input_entity(self.hub_group_id)
            
            # Fetch messages from the topic
            # Note: reply_to is the thread_id in Telegram API
            messages_to_delete = []
            async for message in client.iter_messages(hub_entity, reply_to=telegram_topic_id):
                if message.date < cutoff:
                    # Don't delete the service message that created the topic if possible
                    # (Though in "General" there isn't one specific creation message like other topics)
                    messages_to_delete.append(message.id)
                
                # Batch deletion to avoid flood
                if len(messages_to_delete) >= 100:
                    await client.delete_messages(hub_entity, messages_to_delete)
                    logger.info(f"  🗑️ Deleted {len(messages_to_delete)} old messages from topic {telegram_topic_id}")
                    messages_to_delete = []
                    await asyncio.sleep(1) # Be gentle
            
            # Delete remaining
            if messages_to_delete:
                await client.delete_messages(hub_entity, messages_to_delete)
                logger.info(f"  🗑️ Deleted {len(messages_to_delete)} old messages from topic {telegram_topic_id}")
            
            return True
        except Exception as e:
            logger.error(f"Failed to cleanup topic {telegram_topic_id}: {e}")
            return False

    async def ensure_all_topics_exist(self, client=None):
        """
        Scans all topics in DB and ensures they exist in Telegram.
        If a topic is missing (deleted in Telegram), it recreates it.
        """
        if client is None:
            client = await self._get_client()
            
        logger.info("🔬 Topic Repair: Checking Hub for missing topics...")
        
        try:
            hub_entity = await client.get_input_entity(self.hub_group_id)
            
            # Get all topics from DB
            async with get_db_connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("SELECT id, topic_id, label FROM telegram_topics WHERE topic_id > 1")
                    db_topics = await cur.fetchall()
            
            if not db_topics:
                return
                
            repair_count = 0
            for db_id, telegram_id, label in db_topics:
                try:
                    # Check if topic exists by trying to get messages from it
                    # (Cheapest way to verify existence and bot access)
                    await client.get_messages(hub_entity, limit=1, reply_to=telegram_id)
                except Exception as e:
                    # If error is about missing topic or restricted access
                    if "not found" in str(e).lower() or "reference" in str(e).lower():
                        logger.warning(f"  Topic {telegram_id} ('{label}') missing or unreachable. Recreating...")
                        
                        # Recreate
                        icon_color = random.choice(TOPIC_ICON_COLORS)
                        result = await client(CreateForumTopicRequest(
                            channel=hub_entity,
                            title=label, # Keep existing label (which might be the ID)
                            icon_color=icon_color,
                            random_id=random.randint(1, 2**31 - 1)
                        ))
                        
                        # Extract new ID
                        new_id = None
                        if hasattr(result, 'updates'):
                            for update in result.updates:
                                if hasattr(update, 'id'):
                                    new_id = update.id
                                    break
                                    
                        if new_id:
                            # Update DB
                            async with get_db_connection() as conn:
                                async with conn.cursor() as cur:
                                    # Update mapping and set label to new ID if it was numeric-looking
                                    new_label = str(new_id) if label.isdigit() else label
                                    await cur.execute("""
                                        UPDATE telegram_topics 
                                        SET topic_id = %s, label = %s, updated_at = NOW()
                                        WHERE id = %s
                                    """, (new_id, new_label, db_id))
                                    
                                    # Update embeddings that point to this topic_id (DB ID remains same)
                                    # No action needed for embeddings as they use FK to telegram_topics.id
                                    
                            logger.info(f"  ✅ Recreated topic {telegram_id} → {new_id}")
                            repair_count += 1
                        
            if repair_count > 0:
                logger.info(f"🔧 Topic Repair: Recreated {repair_count} missing topics")
            else:
                logger.info("✓ Topic Repair: All topics verified")
                
        except Exception as e:
            logger.error(f"Topic repair failed: {e}")

    async def migrate_labels_to_ids(self):
        """
        One-time migration to set all topic labels to their Telegram Thread IDs.
        Makes identifying topics in Telegram easier.
        """
        logger.info("🚀 Migrating topic labels to match Thread IDs...")
        try:
            async with get_db_connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("""
                        UPDATE telegram_topics 
                        SET label = topic_id::text
                        WHERE label LIKE 'Person %' OR label = 'Unknown Person'
                    """)
                    count = cur.rowcount
            logger.info(f"✅ Migrated {count} topic labels")
        except Exception as e:
            logger.error(f"Migration failed: {e}")
