"""
Identity Matcher - Face embedding similarity search and identity management.

Uses pgvector for efficient cosine similarity search.
Integrates with Phase 1 database and Phase 2 topic manager.
"""
import logging
import os
from typing import List, Dict, Tuple, Optional
from database import get_db_connection
from hub_notifier import increment_stat

logger = logging.getLogger(__name__)


class IdentityMatcher:
    """
    Handles face embedding matching and identity management.
    Uses PostgreSQL pgvector for efficient similarity search.
    """
    
    def __init__(self, topic_manager=None):
        """
        Args:
            topic_manager: TopicManager instance from Phase 2 for creating new topics
        """
        self.topic_manager = topic_manager
        
        # Similarity threshold from environment (default 0.55)
        self.similarity_threshold = float(os.getenv('SIMILARITY_THRESHOLD', 0.55))
        
        # Minimum quality for storage
        self.min_quality = float(os.getenv('MIN_QUALITY_THRESHOLD', 0.5))
        
        logger.info(f"IdentityMatcher initialized. Similarity threshold: {self.similarity_threshold}")
    
    async def find_or_create_identity(
        self,
        embedding: List[float],
        quality_score: float,
        source_chat_id: int,
        source_message_id: int,
        frame_index: int = 0
    ) -> Tuple[int, bool]:
        """
        Finds an existing identity matching the embedding, or creates a new one.
        
        Args:
            embedding: 512-dimensional face embedding vector
            quality_score: Detection confidence score (0-1)
            source_chat_id: Chat where face was found
            source_message_id: Message ID where face was found
            frame_index: Frame index for video sources
            
        Returns:
            Tuple of (topic_id, is_new) where is_new indicates if a new identity was created
        """
        # Quality filtering
        if quality_score < self.min_quality:
            logger.debug(f"Skipping low quality embedding: {quality_score:.3f}")
            return (0, False)
        
        # Search for matching identity
        match = await self._find_similar_embedding(embedding)
        
        if match:
            topic_id = match['topic_id']
            similarity = match['similarity']
            logger.debug(f"Found matching identity (topic {topic_id}) with similarity {similarity:.3f}")
            
            # Store this embedding as additional sample
            await self._store_embedding(
                embedding=embedding,
                topic_id=topic_id,
                quality_score=quality_score,
                source_chat_id=source_chat_id,
                source_message_id=source_message_id,
                frame_index=frame_index
            )
            
            return (topic_id, False)
        
        else:
            # Create new identity
            topic_id = await self._create_new_identity(
                embedding=embedding,
                quality_score=quality_score,
                source_chat_id=source_chat_id,
                source_message_id=source_message_id,
                frame_index=frame_index
            )
            
            logger.info(f"Created new identity (topic {topic_id})")
            return (topic_id, True)
    
    async def _find_similar_embedding(self, embedding: List[float]) -> Optional[Dict]:
        """
        Searches for a similar embedding in the database using pgvector.
        
        Returns:
            Dict with 'topic_id' and 'similarity' if found, None otherwise
        """
        try:
            async with get_db_connection() as conn:
                async with conn.cursor() as cur:
                    # Convert embedding to pgvector format
                    embedding_str = f"[{','.join(map(str, embedding))}]"
                    
                    # Cosine similarity search using pgvector
                    # 1 - cosine_distance = cosine_similarity
                    # We cast to ::vector to ensure PG understands it, though usually explicit cast isn't strictly needed if context is clear
                    await cur.execute("""
                        SELECT 
                            topic_id,
                            1 - (embedding <=> %s::vector) AS similarity
                        FROM face_embeddings
                        WHERE embedding IS NOT NULL
                        ORDER BY embedding <=> %s::vector
                        LIMIT 1
                    """, (embedding_str, embedding_str))
                    
                    row = await cur.fetchone()
                    
                    if row and row[1] >= self.similarity_threshold:
                        return {
                            'topic_id': row[0],
                            'similarity': row[1]
                        }
                    
                    return None
                
        except Exception as e:
            logger.error(f"Similarity search failed: {e}")
            return None
    
    async def _store_embedding(
        self,
        embedding: List[float],
        topic_id: int,
        quality_score: float,
        source_chat_id: int,
        source_message_id: int,
        frame_index: int = 0
    ) -> int:
        """Stores a face embedding in the database."""
        try:
            async with get_db_connection() as conn:
                async with conn.cursor() as cur:
                    embedding_str = f"[{','.join(map(str, embedding))}]"
                    
                    await cur.execute("""
                        INSERT INTO face_embeddings 
                            (topic_id, embedding, quality_score, source_chat_id, source_message_id, frame_index)
                        VALUES (%s, %s::vector, %s, %s, %s, %s)
                        RETURNING id
                    """, (topic_id, embedding_str, quality_score, source_chat_id, source_message_id, frame_index))
                    
                    result = await cur.fetchone()
                    
                    # Track embedding storage for Hub stats
                    if result:
                        await increment_stat('faces_detected', 1)
                    
                    return result[0] if result else 0
                
        except Exception as e:
            logger.error(f"Failed to store embedding: {e}")
            return 0
    
    async def _create_new_identity(
        self,
        embedding: List[float],
        quality_score: float,
        source_chat_id: int,
        source_message_id: int,
        frame_index: int = 0
    ) -> int:
        """Creates a new identity with a topic and stores the initial embedding."""
        try:
            # Create topic using Phase 2 TopicManager
            if self.topic_manager:
                topic_result = await self.topic_manager.create_topic()
                db_topic_id = topic_result['db_id']
            else:
                # Fallback: create topic in database only
                db_topic_id = await self._create_db_only_topic()
            
            # Store the initial embedding
            await self._store_embedding(
                embedding=embedding,
                topic_id=db_topic_id,
                quality_score=quality_score,
                source_chat_id=source_chat_id,
                source_message_id=source_message_id,
                frame_index=frame_index
            )
            
            # Update topic face count
            if self.topic_manager:
                await self.topic_manager.increment_face_count(db_topic_id)
            
            return db_topic_id
            
        except Exception as e:
            logger.error(f"Failed to create identity: {e}")
            return 0
    
    async def _create_db_only_topic(self) -> int:
        """Creates a topic record in database without Telegram (fallback)."""
        async with get_db_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("""
                    INSERT INTO telegram_topics (topic_id, label)
                    VALUES (0, 'Person ' || (SELECT COALESCE(MAX(id), 0) + 1 FROM telegram_topics))
                    RETURNING id
                """)
                result = await cur.fetchone()
                return result[0] if result else 0
    
    async def get_identity_embeddings(self, topic_id: int) -> List[Dict]:
        """Retrieves all embeddings for an identity."""
        async with get_db_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("""
                    SELECT id, quality_score, source_chat_id, source_message_id, detection_timestamp
                    FROM face_embeddings
                    WHERE topic_id = %s
                    ORDER BY quality_score DESC
                """, (topic_id,))
                
                results = []
                for row in await cur.fetchall():
                    results.append({
                        'id': row[0],
                        'quality_score': row[1],
                        'source_chat_id': row[2],
                        'source_message_id': row[3],
                        'detection_timestamp': row[4]
                    })
                
                return results
    
    async def update_representative_embedding(self, topic_id: int) -> bool:
        """
        Sets the highest quality embedding as the representative for an identity.
        This can improve matching accuracy.
        """
        try:
            async with get_db_connection() as conn:
                async with conn.cursor() as cur:
                    # Mark highest quality embedding as representative
                    await cur.execute("""
                        UPDATE face_embeddings
                        SET is_representative = (
                            id = (
                                SELECT id FROM face_embeddings
                                WHERE topic_id = %s
                                ORDER BY quality_score DESC
                                LIMIT 1
                            )
                        )
                        WHERE topic_id = %s
                    """, (topic_id, topic_id))
                    
                    return True
                
        except Exception as e:
            logger.error(f"Failed to update representative embedding: {e}")
            return False
