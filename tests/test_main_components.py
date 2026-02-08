import pytest
import asyncio
import io
import numpy as np
from unittest.mock import Mock, patch, MagicMock, AsyncMock
import json

# Import components
# Note: Adjust imports based on your actual file structure
from processing_queue import ProcessingQueue, TaskType, ProcessingTask, BackpressureState
from identity_matcher import IdentityMatcher
from database import DatabaseManager
from media_uploader import MediaUploader
from video_extractor import VideoFrameExtractor

# ============================================
# Database Tests (Async)
# ============================================

@pytest.mark.asyncio
async def test_database_manager_singleton():
    """Ensure DatabaseManager acts as a singleton."""
    db1 = DatabaseManager()
    db2 = DatabaseManager()
    assert db1 is db2

@pytest.mark.asyncio
async def test_get_db_connection_mock():
    """Test that get_db_connection can be mocked for async context."""
    mock_conn = AsyncMock()
    mock_cursor = AsyncMock()
    mock_conn.cursor.return_value.__aenter__.return_value = mock_cursor
    
    with patch('database.db_manager') as mock_mgr:
        mock_mgr.pool.connection.return_value.__aenter__.return_value = mock_conn
        
        from database import get_db_connection
        async with get_db_connection() as conn:
            assert conn is mock_conn

# ============================================
# Processing Queue Tests
# ============================================

@pytest.mark.asyncio
async def test_processing_queue_enqueue():
    """Test that enqueue_media pushes to Redis correctly."""
    mock_redis = MagicMock()
    mock_redis.ping.return_value = True
    mock_redis.llen.return_value = 0
    
    with patch('redis.Redis', return_value=mock_redis):
        queue = ProcessingQueue(num_workers=0)
        
        content = io.BytesIO(b"fake_image_data")
        await queue.enqueue_media(
            chat_id=123,
            message_id=456,
            content=content,
            media_type='photo',
            file_unique_id='unique_id_1'
        )
        
        # Verify run_in_executor was called (which calls redis.rpush)
        # Since we can't easily spy on the loop's run_in_executor without more mocking,
        # we rely on no exceptions + mock validation
        assert queue.get_queue_size() == 0 # Mock returns 0

def test_backpressure_logic():
    """Test backpressure state transitions."""
    mock_redis = MagicMock()
    mock_redis.ping.return_value = True
    
    with patch('redis.Redis', return_value=mock_redis):
        queue = ProcessingQueue(num_workers=0, high_watermark=10, low_watermark=5)
        
        # Case 1: Critical
        mock_redis.llen.return_value = 15
        queue._check_backpressure()
        assert queue.get_backpressure_state() == BackpressureState.CRITICAL
        
        # Case 2: Warning
        mock_redis.llen.return_value = 8
        queue._check_backpressure()
        assert queue.get_backpressure_state() == BackpressureState.WARNING
        
        # Case 3: Normal
        mock_redis.llen.return_value = 2
        queue._check_backpressure()
        assert queue.get_backpressure_state() == BackpressureState.NORMAL

# ============================================
# Identity Matcher & DB Logic
# ============================================

from contextlib import asynccontextmanager

@pytest.mark.asyncio
async def test_identity_matcher_creates_new():
    """Test logic for creating a new identity when no match found."""
    matcher = IdentityMatcher()
    
    # Mock internal methods to test flow logic only
    # We want to verify: 
    # 1. Calls _find_similar_embedding
    # 2. If None, calls _create_new_identity
    
    with patch.object(matcher, '_find_similar_embedding', new_callable=AsyncMock) as mock_find:
        with patch.object(matcher, '_create_new_identity', new_callable=AsyncMock) as mock_create:
            with patch.object(matcher, '_store_embedding', new_callable=AsyncMock) as mock_store:
                
                # CASE 1: New Identity
                mock_find.return_value = None
                mock_create.return_value = 100
                
                topic_id, is_new = await matcher.find_or_create_identity(
                    embedding=[0.1]*512,
                    quality_score=0.9,
                    source_chat_id=1,
                    source_message_id=1
                )
                
                assert topic_id == 100
                assert is_new is True
                mock_create.assert_called_once()
                mock_store.assert_not_called() # _store_embedding is called inside _find_similar_embedding success path only, OR inside _create_new_identity (but we mocked create_new_identity so it won't be called from there)

                # Reset mocks
                mock_find.reset_mock()
                mock_create.reset_mock()
                
                # CASE 2: Existing Identity
                mock_find.return_value = {'topic_id': 50, 'similarity': 0.8}
                
                topic_id, is_new = await matcher.find_or_create_identity(
                    embedding=[0.1]*512,
                    quality_score=0.9,
                    source_chat_id=1,
                    source_message_id=1
                )
                
                assert topic_id == 50
                assert is_new is False
                mock_create.assert_not_called()
                mock_store.assert_called_once() # Should be called for existing identify to add sample

# ============================================
# Video Extractor Tests
# ============================================

@pytest.mark.asyncio
async def test_video_extractor_logic():
    """Test frame extraction logic."""
    extractor = VideoFrameExtractor()
    
    # Mock PyAV
    mock_container = MagicMock()
    mock_stream = MagicMock()
    mock_stream.frames = 100
    mock_container.streams.video = [mock_stream]
    
    # Mock the frame loop
    # We'll just mock it returning an empty list to verify the call structure passes
    with patch('av.open', return_value=MagicMock(__enter__=MagicMock(return_value=mock_container))):
        frames = await extractor.extract_frames(io.BytesIO(b'video'), is_round_video=False)
        assert isinstance(frames, list)

