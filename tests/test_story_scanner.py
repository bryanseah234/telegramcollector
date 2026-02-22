"""
Tests for StoryScanner and story-related processing queue functionality.
"""
import pytest
import asyncio
import io
from unittest.mock import AsyncMock, MagicMock, patch


class TestStoryTaskType:
    """Tests for the STORY task type in ProcessingQueue."""
    
    def test_story_task_type_exists(self):
        """TaskType.STORY should exist."""
        from processing_queue import TaskType
        assert hasattr(TaskType, 'STORY')
        assert TaskType.STORY.value == 'story'
    
    def test_all_task_types(self):
        """All expected task types should be present."""
        from processing_queue import TaskType
        expected = {'media', 'profile_photo', 'story'}
        actual = {t.value for t in TaskType}
        assert expected == actual


class TestStoryConfig:
    """Tests for story-related configuration settings."""
    
    def test_story_settings_defaults(self):
        """Story settings should have correct defaults."""
        from config import Settings
        # Test field defaults exist (without loading .env)
        fields = Settings.model_fields
        assert 'STORY_SCAN_INTERVAL' in fields
        assert 'STORY_SCAN_ENABLED' in fields
        assert 'STORY_PRIORITY_BOOST' in fields
        
        assert fields['STORY_SCAN_INTERVAL'].default == 300
        assert fields['STORY_SCAN_ENABLED'].default is True
        assert fields['STORY_PRIORITY_BOOST'].default == 10
    
    def test_bot_token_optional(self):
        """BOT_TOKEN should have a default empty string."""
        from config import Settings
        assert Settings.model_fields['BOT_TOKEN'].default == ""


class TestStoryScannerUnit:
    """Unit tests for StoryScanner with mocked dependencies."""
    
    def test_scanner_init(self):
        """StoryScanner should initialize with correct defaults."""
        from story_scanner import StoryScanner
        
        mock_client = MagicMock()
        mock_queue = MagicMock()
        mock_media = MagicMock()
        
        scanner = StoryScanner(mock_client, mock_queue, mock_media)
        
        assert scanner._running is False
        assert scanner._poll_task is None
        assert scanner.stats['stories_checked'] == 0
        assert scanner.stats['stories_downloaded'] == 0
        assert scanner.stats['last_scan'] is None
    
    def test_get_status(self):
        """get_status should return a complete status dict."""
        from story_scanner import StoryScanner
        
        scanner = StoryScanner(MagicMock(), MagicMock(), MagicMock())
        status = scanner.get_status()
        
        assert 'running' in status
        assert 'stories_checked' in status
        assert 'stories_downloaded' in status
        assert 'errors' in status
        assert 'last_scan' in status
        assert 'cache_size' in status
        assert status['running'] is False
    
    @pytest.mark.asyncio
    async def test_is_story_processed_returns_false_for_new(self):
        """_is_story_processed should return False for brand new stories."""
        from story_scanner import StoryScanner
        
        scanner = StoryScanner(MagicMock(), MagicMock(), MagicMock())
        
        # Mock DB to return None (not processed)
        mock_cursor = AsyncMock()
        mock_cursor.fetchone = AsyncMock(return_value=None)
        mock_cursor.__aenter__ = AsyncMock(return_value=mock_cursor)
        mock_cursor.__aexit__ = AsyncMock(return_value=False)
        
        mock_conn = AsyncMock()
        mock_conn.cursor = MagicMock(return_value=mock_cursor)
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=False)
        
        with patch('story_scanner.get_db_connection', return_value=mock_conn):
            result = await scanner._is_story_processed(12345, 67890, 1)
            assert result is False

    @pytest.mark.asyncio
    async def test_stop_when_not_started(self):
        """Stopping a scanner that was never started should not crash."""
        from story_scanner import StoryScanner
        
        scanner = StoryScanner(MagicMock(), MagicMock(), MagicMock())
        await scanner.stop()  # Should not raise
        assert scanner._running is False


class TestProcessingQueueEnqueueStory:
    """Tests for the enqueue_story method."""
    
    def test_enqueue_story_method_exists(self):
        """ProcessingQueue should have an enqueue_story method."""
        from processing_queue import ProcessingQueue
        assert hasattr(ProcessingQueue, 'enqueue_story')
