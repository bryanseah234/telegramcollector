"""
Tests for the HubNotifier module.

Tests rate limiting, batching, and message formatting.
"""
import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
import sys


class TestHubNotifier:
    """Tests for HubNotifier class."""
    
    @pytest.fixture
    def notifier(self):
        """Creates a HubNotifier instance for testing."""
        from hub_notifier import HubNotifier
        
        # Reset singleton
        HubNotifier._instance = None
        
        # Create instance with explicit params to avoid reading settings in __init__
        notifier = HubNotifier(
            batch_interval=60,
            rate_limit_per_minute=10,
            enable_notifications=True
        )
        # Mock supervisor loop to prevent actual background tasks during tests
        notifier._supervisor_loop = AsyncMock()
        return notifier
    
    @pytest.mark.asyncio
    async def test_queue_event_adds_to_queue(self, notifier):
        """Test that events are queued correctly."""
        await notifier.queue_event('scan', 'Test message')
        
        from hub_notifier import EventCategory
        assert len(notifier._event_queue[EventCategory.SCAN]) == 1
        assert notifier._event_queue[EventCategory.SCAN][0].message == 'Test message'
    
    @pytest.mark.asyncio
    async def test_queue_event_string_category(self, notifier):
        """Test that string categories are converted to enum."""
        await notifier.queue_event('error', 'Error message')
        
        from hub_notifier import EventCategory
        assert len(notifier._event_queue[EventCategory.ERROR]) == 1
    
    @pytest.mark.asyncio
    async def test_increment_stat(self, notifier):
        """Test stat increment functionality."""
        await notifier.increment_stat('faces_detected', 5)
        assert notifier._stats['faces_detected'] == 5
        
        await notifier.increment_stat('faces_detected', 3)
        assert notifier._stats['faces_detected'] == 8
    
    @pytest.mark.asyncio
    async def test_flush_clears_queue_and_stats(self, notifier):
        """Test that flush clears the queue and resets stats."""
        # Queue some events and stats
        await notifier.queue_event('scan', 'Test 1')
        await notifier.queue_event('face', 'Test 2')
        await notifier.increment_stat('faces_detected', 10)
        
        # Mock the send_message to avoid actual sending
        with patch('bot_client.bot_client_manager') as mock_bot:
            mock_bot.client = AsyncMock()
            mock_bot.client.send_message = AsyncMock()
            
            with patch('config.settings') as mock_settings:
                mock_settings.HUB_GROUP_ID = 123456
                await notifier.flush()
        
        # Verify queue and stats are cleared
        from hub_notifier import EventCategory
        assert len(notifier._event_queue[EventCategory.SCAN]) == 0
        assert len(notifier._event_queue[EventCategory.FACE]) == 0
        assert notifier._stats['faces_detected'] == 0
    
    @pytest.mark.asyncio
    async def test_disabled_notifications(self):
        """Test that disabled notifier doesn't queue events."""
        from hub_notifier import HubNotifier
        HubNotifier._instance = None
        
        notifier = HubNotifier(enable_notifications=False)
        await notifier.queue_event('scan', 'Should not be queued')
        
        from hub_notifier import EventCategory
        assert len(notifier._event_queue[EventCategory.SCAN]) == 0
    
    @pytest.mark.asyncio
    async def test_high_priority_immediate_send(self, notifier):
        """Test that high priority events are sent immediately."""
        with patch('bot_client.bot_client_manager') as mock_bot:
            mock_client = AsyncMock()
            mock_bot.client = mock_client
            
            with patch('config.settings') as mock_settings:
                mock_settings.HUB_GROUP_ID = 123456
                
                await notifier.queue_event('error', 'Urgent error', priority=2)
                
                # Should have called send_message immediately
                mock_client.send_message.assert_called_once()


class TestEventCategory:
    """Tests for EventCategory enum."""
    
    def test_category_values(self):
        """Test that category values are correct."""
        from hub_notifier import EventCategory
        
        assert EventCategory.SCAN.value == 'scan'
        assert EventCategory.STORE.value == 'store'
        assert EventCategory.FACE.value == 'face'
        assert EventCategory.ERROR.value == 'error'
        assert EventCategory.SYSTEM.value == 'system'
    
    def test_emoji_mapping(self):
        """Test that emoji mapping is complete."""
        from hub_notifier import EventCategory, CATEGORY_EMOJI
        
        for category in EventCategory:
            assert category in CATEGORY_EMOJI
