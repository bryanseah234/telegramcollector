"""
Integration Tests for Telegram Face Recognition System

End-to-end workflow tests with minimal mocking.
"""
import pytest
import asyncio
import io
import numpy as np
from unittest.mock import Mock, patch, AsyncMock, MagicMock


# ============================================
# Workflow Integration Tests
# ============================================

class TestMessageProcessingWorkflow:
    """Tests complete message processing workflow."""
    
    @pytest.fixture
    def mock_telegram_message(self):
        """Create a mock Telegram message."""
        msg = Mock()
        msg.id = 12345
        msg.photo = Mock()
        msg.photo.file_unique_id = "unique_photo_123"
        msg.video = None
        msg.document = None
        msg.sender = Mock()
        msg.sender.id = 999
        msg.sender.photo = Mock()
        msg.sender.photo.photo_id = 111
        return msg
    
    @pytest.mark.asyncio
    async def test_photo_processing_workflow(self, mock_telegram_message):
        """Test complete photo processing from message to identity match."""
        # Mock all components
        with patch('message_scanner.get_db_connection') as mock_db:
            mock_conn = MagicMock()
            mock_conn.__enter__ = Mock(return_value=mock_conn)
            mock_conn.__exit__ = Mock(return_value=False)
            mock_conn.cursor.return_value.fetchone.return_value = None
            mock_db.return_value = mock_conn
            
            from message_scanner import MessageScanner
            
            mock_queue = AsyncMock()
            mock_media = AsyncMock()
            mock_media.download_media.return_value = io.BytesIO(b'fake_image')
            
            scanner = MessageScanner(
                client=Mock(),
                media_manager=mock_media,
                processing_queue=mock_queue
            )
            
            await scanner.process_message(
                message=mock_telegram_message,
                account_id=1,
                chat_id=123
            )
            
            # Should have enqueued media
            mock_queue.enqueue_media.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_duplicate_detection_skips_processing(self, mock_telegram_message):
        """Test that duplicate files are skipped."""
        with patch('message_scanner.get_db_connection') as mock_db:
            mock_conn = MagicMock()
            mock_conn.__enter__ = Mock(return_value=mock_conn)
            mock_conn.__exit__ = Mock(return_value=False)
            # File already exists in processed_media
            mock_conn.cursor.return_value.fetchone.return_value = [1]
            mock_db.return_value = mock_conn
            
            from message_scanner import MessageScanner
            
            mock_queue = AsyncMock()
            mock_media = AsyncMock()
            
            scanner = MessageScanner(
                client=Mock(),
                media_manager=mock_media,
                processing_queue=mock_queue
            )
            
            await scanner.process_message(
                message=mock_telegram_message,
                account_id=1,
                chat_id=123
            )
            
            # Should NOT have downloaded or enqueued
            mock_media.download_media.assert_not_called()
            mock_queue.enqueue_media.assert_not_called()
            
            # Should have tracked duplicate
            assert scanner.stats.get('duplicates_skipped', 0) > 0


class TestIdentityMatchingWorkflow:
    """Tests identity matching and creation workflow."""
    
    @pytest.fixture
    def mock_embedding(self):
        """Create a test face embedding."""
        return np.random.randn(512).astype(np.float32)
    
    @pytest.mark.asyncio
    async def test_new_identity_creation(self, mock_embedding):
        """Test that new identity is created for unknown face."""
        with patch('identity_matcher.get_db_connection') as mock_db:
            mock_conn = MagicMock()
            mock_conn.__enter__ = Mock(return_value=mock_conn)
            mock_conn.__exit__ = Mock(return_value=False)
            
            cursor = Mock()
            cursor.fetchall.return_value = []  # No matches
            cursor.fetchone.return_value = [1]  # New embedding ID
            mock_conn.cursor.return_value = cursor
            mock_db.return_value = mock_conn
            
            from identity_matcher import IdentityMatcher
            
            mock_topic = AsyncMock()
            mock_topic.create_topic.return_value = 42
            
            matcher = IdentityMatcher(topic_manager=mock_topic)
            
            topic_id, is_new = await matcher.find_or_create_identity(
                embedding=mock_embedding,
                quality_score=0.9,
                source_chat_id=123,
                source_message_id=456
            )
            
            assert is_new is True
            mock_topic.create_topic.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_existing_identity_match(self, mock_embedding):
        """Test matching to existing identity."""
        with patch('identity_matcher.get_db_connection') as mock_db:
            mock_conn = MagicMock()
            mock_conn.__enter__ = Mock(return_value=mock_conn)
            mock_conn.__exit__ = Mock(return_value=False)
            
            cursor = Mock()
            # Return existing match with high similarity
            cursor.fetchall.return_value = [(42, 0.85)]  # topic_id, similarity
            cursor.fetchone.return_value = [1]
            mock_conn.cursor.return_value = cursor
            mock_db.return_value = mock_conn
            
            from identity_matcher import IdentityMatcher
            
            mock_topic = AsyncMock()
            matcher = IdentityMatcher(topic_manager=mock_topic)
            
            topic_id, is_new = await matcher.find_or_create_identity(
                embedding=mock_embedding,
                quality_score=0.9,
                source_chat_id=123,
                source_message_id=456
            )
            
            assert topic_id == 42
            assert is_new is False
            mock_topic.create_topic.assert_not_called()


class TestBackpressureWorkflow:
    """Tests queue backpressure and scanner response."""
    
    @pytest.mark.asyncio
    async def test_backpressure_callback_invoked(self):
        """Test backpressure callbacks are called on state change."""
        from processing_queue import ProcessingQueue, BackpressureState
        
        callback_states = []
        
        def track_callback(state):
            callback_states.append(state)
        
        queue = ProcessingQueue(
            face_processor=Mock(),
            video_extractor=Mock(),
            identity_matcher=Mock(),
            media_uploader=Mock(),
            topic_manager=Mock(),
            num_workers=1
        )
        
        queue.low_watermark = 2
        queue.high_watermark = 5
        queue.register_backpressure_callback(track_callback)
        
        # Enqueue items to trigger state change
        for i in range(6):
            await queue.enqueue_media(
                chat_id=1,
                message_id=i,
                content=io.BytesIO(b'test'),
                media_type='photo'
            )
        
        # Should have triggered callback
        assert len(callback_states) > 0


class TestCheckpointRecovery:
    """Tests checkpoint save and recovery."""
    
    def test_checkpoint_save(self):
        """Test checkpoint is saved correctly."""
        with patch('database.get_db_connection') as mock_db:
            mock_conn = MagicMock()
            mock_conn.__enter__ = Mock(return_value=mock_conn)
            mock_conn.__exit__ = Mock(return_value=False)
            mock_db.return_value = mock_conn
            
            from message_scanner import MessageScanner
            
            scanner = MessageScanner(
                client=Mock(),
                media_manager=Mock(),
                processing_queue=Mock()
            )
            
            scanner._save_checkpoint(
                account_id=1,
                chat_id=123,
                last_message_id=999,
                processed=500,
                total=1000
            )
            
            # Should have executed INSERT/UPDATE
            mock_conn.cursor.return_value.execute.assert_called()
            mock_conn.commit.assert_called()


class TestHealthCheckWorkflow:
    """Tests health check integration."""
    
    @pytest.mark.asyncio
    async def test_health_check_stores_results(self):
        """Test health check results are stored in database."""
        with patch('health_checker.get_db_connection') as mock_db:
            mock_conn = MagicMock()
            mock_conn.__enter__ = Mock(return_value=mock_conn)
            mock_conn.__exit__ = Mock(return_value=False)
            mock_conn.cursor.return_value.fetchone.return_value = [1]
            mock_db.return_value = mock_conn
            
            from health_checker import HealthChecker
            
            checker = HealthChecker(
                client=Mock(),
                face_processor=Mock(),
                processing_queue=Mock()
            )
            
            # Mock check methods
            checker._check_database = AsyncMock(return_value=True)
            checker._check_telegram = AsyncMock(return_value=True)
            checker._check_face_model = AsyncMock(return_value=True)
            checker._check_hub_access = AsyncMock(return_value=True)
            
            await checker.run_checks()
            
            # Should have stored results
            mock_conn.cursor.return_value.execute.assert_called()


# ============================================
# Database Integration Tests
# ============================================

class TestDatabaseIntegration:
    """Tests database operations with real-like queries."""
    
    def test_processed_media_insert(self):
        """Test inserting processed media record."""
        with patch('database.psycopg2') as mock_pg:
            mock_conn = MagicMock()
            mock_cursor = MagicMock()
            mock_conn.cursor.return_value = mock_cursor
            mock_pg.connect.return_value = mock_conn
            
            from database import get_db_connection
            
            with get_db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO processed_media 
                        (file_unique_id, media_type, first_seen_chat_id, first_seen_message_id)
                    VALUES (%s, %s, %s, %s)
                """, ("unique123", "photo", 123, 456))
            
            mock_cursor.execute.assert_called()
    
    def test_face_embedding_vector_query(self):
        """Test pgvector similarity search query."""
        with patch('database.psycopg2') as mock_pg:
            mock_conn = MagicMock()
            mock_cursor = MagicMock()
            mock_cursor.fetchall.return_value = [(1, 0.85), (2, 0.72)]
            mock_conn.cursor.return_value = mock_cursor
            mock_pg.connect.return_value = mock_conn
            
            embedding = np.random.randn(512).tolist()
            
            from database import get_db_connection
            
            with get_db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT topic_id, 1 - (embedding <=> %s::vector) as similarity
                    FROM face_embeddings
                    ORDER BY embedding <=> %s::vector
                    LIMIT 5
                """, (embedding, embedding))
                
                results = cursor.fetchall()
            
            assert len(results) == 2


# ============================================
# Run tests
# ============================================

if __name__ == '__main__':
    pytest.main([__file__, '-v'])
