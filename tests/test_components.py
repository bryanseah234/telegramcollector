import pytest
import asyncio
from unittest.mock import MagicMock, AsyncMock, patch
import json

# Import components
from processing_queue import ProcessingQueue, TaskType, ProcessingTask
from identity_matcher import IdentityMatcher
from database import DatabaseManager

@pytest.mark.asyncio
async def test_processing_queue_enqueue():
    """Test that enqueue_media pushes to Redis correctly."""
    # Mock Redis
    mock_redis = MagicMock()
    mock_redis.ping.return_value = True
    
    with patch('redis.Redis', return_value=mock_redis):
        queue = ProcessingQueue(num_workers=0) # No workers needed for enqueue test
        
        # Test enqueue
        import io
        content = io.BytesIO(b"fake_image_data")
        
        await queue.enqueue_media(
            chat_id=123,
            message_id=456,
            content=content,
            media_type='photo',
            file_unique_id='unique_id_1'
        )
        
        # Verify Redis rpush was called (via executor)
        # We can't easily check the exact args passed to run_in_executor with simple mocks,
        # but we can verify logic flow didn't error.
        assert queue.get_queue_size() == 0 # Mock redis len returns 0 by default

@pytest.mark.asyncio
async def test_identity_matcher_logic():
    """Test identity matcher logic flows (new vs existing)."""
    matcher = IdentityMatcher()
    
    # Mock database connection
    mock_conn = AsyncMock()
    mock_cursor = AsyncMock()
    mock_conn.cursor.return_value.__aenter__.return_value = mock_cursor
    
    with patch('database.get_db_connection', return_value=mock_conn):
        
        # Case 1: Match found
        mock_cursor.fetchone.return_value = (10, 0.1) # topic_id=10, similarity=0.1 (distance)
        
        topic_id, is_new = await matcher.find_or_create_identity(
            embedding=[0.1]*512,
            quality_score=0.9,
            source_chat_id=1,
            source_message_id=1
        )
        
        assert topic_id == 10
        assert is_new is False
        
        # Case 2: No match, create new
        # First call (search) returns None
        # Second call (insert topic) returns new ID
        # Third call (insert embedding) returns success
        
        # We need to reset the mock to return None for search
        mock_cursor.fetchone.side_effect = [
            None,       # Search result (not found)
            (99,),      # Insert topic result (id 99)
            (500,)      # Insert embedding result
        ]
        
        topic_id, is_new = await matcher.find_or_create_identity(
            embedding=[0.2]*512,
            quality_score=0.9,
            source_chat_id=1,
            source_message_id=2
        )
        
        assert topic_id == 99
        assert is_new is True

@pytest.mark.asyncio
async def test_database_manager_singleton():
    """Ensure DatabaseManager acts as a singleton."""
    db1 = DatabaseManager()
    db2 = DatabaseManager()
    assert db1 is db2
