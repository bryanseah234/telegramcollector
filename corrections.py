"""
Correction Handlers - Implements merge, split, and rename operations.

Used by the dashboard to manually correct clustering mistakes.
"""
import logging
from database import get_db_connection

logger = logging.getLogger(__name__)

class CorrectionHandler:
    """
    Handles manual correction operations for identities.
    """
    
    def __init__(self, topic_manager=None):
        self.topic_manager = topic_manager
    
    def merge_identities(self, source_topic_id: int, target_topic_id: int) -> bool:
        """
        Merges two identities by moving all embeddings from source to target.
        
        Args:
            source_topic_id: The topic to merge FROM (will be deleted)
            target_topic_id: The topic to merge INTO (will remain)
            
        Returns:
            True if successful
        """
        logger.info(f"Merging topic {source_topic_id} into {target_topic_id}")
        
        try:
            with get_db_connection() as conn:
                cursor = conn.cursor()
                
                # Move all embeddings to target
                cursor.execute("""
                    UPDATE face_embeddings 
                    SET topic_id = %s 
                    WHERE topic_id = %s
                """, (target_topic_id, source_topic_id))
                
                # Move uploaded media references
                cursor.execute("""
                    UPDATE uploaded_media 
                    SET topic_id = %s 
                    WHERE topic_id = %s
                """, (target_topic_id, source_topic_id))
                
                # Delete the source topic record
                cursor.execute("""
                    DELETE FROM telegram_topics WHERE id = %s
                """, (source_topic_id,))
                
                conn.commit()
                
            # TODO: Optionally archive/delete the Telegram forum topic
            
            logger.info(f"Merge complete: {source_topic_id} -> {target_topic_id}")
            return True
            
        except Exception as e:
            logger.error(f"Merge failed: {e}")
            return False
    
    def split_embedding(self, embedding_id: int, new_label: str = "Split Identity") -> int:
        """
        Splits a single embedding into a new identity.
        
        Args:
            embedding_id: The embedding to split out
            new_label: Label for the new identity
            
        Returns:
            The new topic ID, or 0 on failure
        """
        logger.info(f"Splitting embedding {embedding_id} into new identity: {new_label}")
        
        try:
            with get_db_connection() as conn:
                cursor = conn.cursor()
                
                # Create new topic
                cursor.execute("""
                    INSERT INTO telegram_topics (topic_id, label)
                    VALUES (0, %s)
                    RETURNING id
                """, (new_label,))
                
                new_topic_id = cursor.fetchone()[0]
                
                # Move the embedding
                cursor.execute("""
                    UPDATE face_embeddings 
                    SET topic_id = %s 
                    WHERE id = %s
                """, (new_topic_id, embedding_id))
                
                conn.commit()
                
            # TODO: Create actual Telegram forum topic
            
            logger.info(f"Split complete. New topic ID: {new_topic_id}")
            return new_topic_id
            
        except Exception as e:
            logger.error(f"Split failed: {e}")
            return 0
    
    def rename_identity(self, topic_id: int, new_label: str) -> bool:
        """
        Renames an identity.
        
        Args:
            topic_id: The topic to rename
            new_label: The new label
            
        Returns:
            True if successful
        """
        logger.info(f"Renaming topic {topic_id} to: {new_label}")
        
        try:
            with get_db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    UPDATE telegram_topics 
                    SET label = %s 
                    WHERE id = %s
                """, (new_label, topic_id))
                conn.commit()
            
            # TODO: Rename actual Telegram forum topic
            
            return True
            
        except Exception as e:
            logger.error(f"Rename failed: {e}")
            return False
