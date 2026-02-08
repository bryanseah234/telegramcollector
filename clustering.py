import logging
from database import get_db_connection
import numpy as np

logger = logging.getLogger(__name__)

class FaceClusterer:
    """
    Handles identity matching and topic creation.
    """
    def __init__(self, similarity_threshold=0.55):
        self.threshold = similarity_threshold

    async def find_match(self, embedding):
        """
        Finds the closest matching topic for a given embedding using pgvector.
        Returns topic_id or None.
        """
        embedding_list = str(embedding) # pgvector expects string representation for queries? 
        # Actually psycopg2 checks adapters. But for vector type, typically passing list is fine if adpater registered.
        # or string format '[0.1, 0.2, ...]'
        
        with get_db_connection() as conn:
            cursor = conn.cursor()
            # Perform similarity search
            # <-> is L2 distance, <=> is Cosine distance. 
            # We want Cosine Similarity, so we want smallest Cosine Distance.
            cursor.execute("""
                SELECT topic_id, (embedding <=> %s) as distance
                FROM face_embeddings
                ORDER BY distance ASC
                LIMIT 1
            """, (str(embedding),))
            
            row = cursor.fetchone()
            if row:
                topic_id, distance = row
                # Cosine distance = 1 - Cosine Similarity
                # So similarity = 1 - distance
                similarity = 1 - distance
                
                if similarity >= self.threshold:
                    return topic_id
            
            return None

    async def create_topic(self, label="Unknown"):
        """
        Creates a new topic (identity).
        """
        with get_db_connection() as conn:
            cursor = conn.cursor()
            # We need a unique topic_id for Telegram. 
            # In real app, we might create a Forum Topic via API and get ID.
            # Here we might need to generate one or let DB handle it?
            # Table schema says topic_id BIGINT UNIQUE NOT NULL.
            # This implies the Telegram Topic ID. 
            # If we haven't created it on Telegram yet, we might need a temporary one or create it now.
            # For this logic, we assume we defer topic creation to 'main' or 'worker' calls 
            # OR we generate a placeholder. 
            # Let's assume the Worker calls Telegram to create topic, then inserts here.
            pass
            
    async def save_embedding(self, topic_id, embedding, chat_id, message_id, quality):
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO face_embeddings 
                (topic_id, embedding, source_chat_id, source_message_id, quality_score)
                VALUES (%s, %s, %s, %s, %s)
            """, (topic_id, str(embedding), chat_id, message_id, quality))
            conn.commit()
