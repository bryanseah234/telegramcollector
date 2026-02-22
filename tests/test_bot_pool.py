"""
Tests for the BotPool module.

Tests rotation logic, locking, auto-unlock, recommendations, and config parsing.
All tests use mocked TelegramClient — no real Telegram connections.
"""
import pytest
import asyncio
import time
from unittest.mock import AsyncMock, MagicMock, patch


class TestBotTokenParsing:
    """Tests for bot token parsing in config."""
    
    def test_parse_semicolon_separated(self):
        """Test parsing multiple semicolon-separated tokens."""
        from config import Settings
        
        with patch.dict('os.environ', {
            'TG_API_ID': '123',
            'TG_API_HASH': 'hash',
            'BOT_TOKEN': '111:tok1',
            'BOT_TOKENS': 'Bot1:111:tok1;Bot2:222:tok2;Bot3:333:tok3',
            'HUB_GROUP_ID': '-100123',
            'DB_PASSWORD': 'test',
        }, clear=False):
            # Create a fresh settings instance that reads from env
            s = Settings(
                TG_API_ID=123,
                TG_API_HASH='hash',
                BOT_TOKEN='111:tok1',
                BOT_TOKENS='Bot1:111:tok1;Bot2:222:tok2;Bot3:333:tok3',
                HUB_GROUP_ID=-100123,
                DB_PASSWORD='test'
            )
            tokens = s.parsed_bot_tokens
            
            assert len(tokens) == 3
            assert tokens[0]['name'] == 'Bot1'
            assert tokens[0]['token'] == '111:tok1'
            assert tokens[1]['name'] == 'Bot2'
            assert tokens[1]['token'] == '222:tok2'
            assert tokens[2]['name'] == 'Bot3'
            assert tokens[2]['token'] == '333:tok3'
    
    def test_fallback_to_single_token(self):
        """Test fallback to BOT_TOKEN when BOT_TOKENS is empty."""
        from config import Settings
        
        s = Settings(
            TG_API_ID=123,
            TG_API_HASH='hash',
            BOT_TOKEN='999:fallback',
            BOT_TOKENS='',
            HUB_GROUP_ID=-100123,
            DB_PASSWORD='test'
        )
        tokens = s.parsed_bot_tokens
        
        assert len(tokens) == 1
        assert tokens[0]['name'] == 'default'
        assert tokens[0]['token'] == '999:fallback'
    
    def test_parse_with_whitespace(self):
        """Test parsing handles extra whitespace."""
        from config import Settings
        
        s = Settings(
            TG_API_ID=123,
            TG_API_HASH='hash',
            BOT_TOKEN='111:tok1',
            BOT_TOKENS=' Bot1 : 111:tok1 ; Bot2 : 222:tok2 ; ',
            HUB_GROUP_ID=-100123,
            DB_PASSWORD='test'
        )
        tokens = s.parsed_bot_tokens
        
        assert len(tokens) == 2
        assert tokens[0]['name'] == 'Bot1'
        assert tokens[0]['token'] == '111:tok1'


class TestBotPool:
    """Tests for BotPool rotation and health tracking."""
    
    @pytest.fixture(autouse=True)
    def reset_pool(self):
        """Reset singleton before each test."""
        from bot_pool import BotPool
        BotPool.reset_instance()
        yield
        BotPool.reset_instance()
    
    def _make_mock_client(self, username="TestBot", connected=True):
        """Creates a mock TelegramClient."""
        client = MagicMock()
        client.is_connected.return_value = connected
        client.start = AsyncMock()
        client.connect = AsyncMock()
        client.disconnect = AsyncMock()
        
        me = MagicMock()
        me.username = username
        client.get_me = AsyncMock(return_value=me)
        
        return client

    @pytest.mark.asyncio
    async def test_rotation_round_robin(self):
        """Test that get_bot cycles through bots in order."""
        from bot_pool import BotPool, BotEntry, BotStatus
        
        pool = BotPool()
        pool._bots = [
            BotEntry(name='Bot1', token='t1', client=self._make_mock_client('Bot1'), 
                     username='Bot1', status=BotStatus.HEALTHY),
            BotEntry(name='Bot2', token='t2', client=self._make_mock_client('Bot2'),
                     username='Bot2', status=BotStatus.HEALTHY),
            BotEntry(name='Bot3', token='t3', client=self._make_mock_client('Bot3'),
                     username='Bot3', status=BotStatus.HEALTHY),
        ]
        
        # Round-robin: should cycle through Bot1, Bot2, Bot3, Bot1...
        bot1 = pool.get_bot()
        assert bot1.name == 'Bot1'
        
        bot2 = pool.get_bot()
        assert bot2.name == 'Bot2'
        
        bot3 = pool.get_bot()
        assert bot3.name == 'Bot3'
        
        # Full cycle back
        bot1_again = pool.get_bot()
        assert bot1_again.name == 'Bot1'
    
    @pytest.mark.asyncio
    async def test_skip_locked_bot(self):
        """Test that locked bots are skipped in rotation."""
        from bot_pool import BotPool, BotEntry, BotStatus
        
        pool = BotPool()
        pool._bots = [
            BotEntry(name='Bot1', token='t1', client=self._make_mock_client('Bot1'),
                     username='Bot1', status=BotStatus.LOCKED, 
                     locked_until=time.time() + 3600),  # Locked for 1 hour
            BotEntry(name='Bot2', token='t2', client=self._make_mock_client('Bot2'),
                     username='Bot2', status=BotStatus.HEALTHY),
            BotEntry(name='Bot3', token='t3', client=self._make_mock_client('Bot3'),
                     username='Bot3', status=BotStatus.HEALTHY),
        ]
        
        # Bot1 is locked, should get Bot2
        bot = pool.get_bot()
        assert bot.name == 'Bot2'
        
        # Next should be Bot3
        bot = pool.get_bot()
        assert bot.name == 'Bot3'
        
        # Should skip Bot1 again and go to Bot2
        bot = pool.get_bot()
        assert bot.name == 'Bot2'
    
    @pytest.mark.asyncio
    async def test_auto_unlock_after_expiry(self):
        """Test that locked bots auto-unlock after their lockout expires."""
        from bot_pool import BotPool, BotEntry, BotStatus
        
        pool = BotPool()
        pool._bots = [
            BotEntry(name='Bot1', token='t1', client=self._make_mock_client('Bot1'),
                     username='Bot1', status=BotStatus.LOCKED,
                     locked_until=time.time() - 1),  # Already expired
            BotEntry(name='Bot2', token='t2', client=self._make_mock_client('Bot2'),
                     username='Bot2', status=BotStatus.HEALTHY),
        ]
        
        # Bot1 lockout has expired, should be available
        bot = pool.get_bot()
        assert bot.name == 'Bot1'
        assert bot.status == BotStatus.HEALTHY
    
    @pytest.mark.asyncio
    async def test_all_bots_locked_raises_error(self):
        """Test that RuntimeError is raised when all bots are locked."""
        from bot_pool import BotPool, BotEntry, BotStatus
        
        pool = BotPool()
        pool._bots = [
            BotEntry(name='Bot1', token='t1', client=self._make_mock_client('Bot1'),
                     username='Bot1', status=BotStatus.LOCKED,
                     locked_until=time.time() + 3600),
            BotEntry(name='Bot2', token='t2', client=self._make_mock_client('Bot2'),
                     username='Bot2', status=BotStatus.LOCKED,
                     locked_until=time.time() + 3600),
        ]
        
        with pytest.raises(RuntimeError, match="All bots are locked"):
            pool.get_bot()
    
    @pytest.mark.asyncio
    async def test_mark_locked_and_healthy(self):
        """Test marking bots as locked and then healthy."""
        from bot_pool import BotPool, BotEntry, BotStatus
        
        pool = BotPool()
        pool._bots = [
            BotEntry(name='Bot1', token='t1', client=self._make_mock_client('Bot1'),
                     username='Bot1', status=BotStatus.HEALTHY),
        ]
        
        # Mark locked
        pool.mark_locked('Bot1', duration=60, reason='FloodWait')
        assert pool._bots[0].status == BotStatus.LOCKED
        assert pool._bots[0].lock_reason == 'FloodWait'
        assert pool._bots[0].error_count == 1
        
        # Mark healthy
        pool.mark_healthy('Bot1')
        assert pool._bots[0].status == BotStatus.HEALTHY
        assert pool._bots[0].lock_reason == ''
    
    @pytest.mark.asyncio
    async def test_get_recommendation(self):
        """Test getting bot recommendations for fallback."""
        from bot_pool import BotPool, BotEntry, BotStatus
        
        pool = BotPool()
        pool._bots = [
            BotEntry(name='Bot1', token='t1', client=self._make_mock_client('Bot1'),
                     username='Bot1', status=BotStatus.LOCKED,
                     locked_until=time.time() + 3600),
            BotEntry(name='Bot2', token='t2', client=self._make_mock_client('Bot2'),
                     username='Bot2', status=BotStatus.HEALTHY),
            BotEntry(name='Bot3', token='t3', client=self._make_mock_client('Bot3'),
                     username='Bot3', status=BotStatus.HEALTHY),
        ]
        
        # Excluding Bot1, should recommend Bot2 (first healthy)
        rec = pool.get_recommendation(exclude_name='Bot1')
        assert rec == '@Bot2'
        
        # Excluding Bot2, should recommend Bot3 (Bot1 is locked)
        rec = pool.get_recommendation(exclude_name='Bot2')
        assert rec == '@Bot3'
    
    @pytest.mark.asyncio
    async def test_get_recommendation_none_available(self):
        """Test recommendation returns None when no alternatives."""
        from bot_pool import BotPool, BotEntry, BotStatus
        
        pool = BotPool()
        pool._bots = [
            BotEntry(name='Bot1', token='t1', client=self._make_mock_client('Bot1'),
                     username='Bot1', status=BotStatus.HEALTHY),
        ]
        
        rec = pool.get_recommendation(exclude_name='Bot1')
        assert rec is None
    
    @pytest.mark.asyncio
    async def test_get_bot_by_name(self):
        """Test getting a bot by name."""
        from bot_pool import BotPool, BotEntry, BotStatus
        
        pool = BotPool()
        pool._bots = [
            BotEntry(name='Bot1', token='t1', username='Bot1', status=BotStatus.HEALTHY),
            BotEntry(name='Bot2', token='t2', username='Bot2', status=BotStatus.HEALTHY),
        ]
        
        bot = pool.get_bot_by_name('Bot2')
        assert bot is not None
        assert bot.name == 'Bot2'
        
        bot = pool.get_bot_by_name('NonExistent')
        assert bot is None
    
    @pytest.mark.asyncio
    async def test_get_bot_by_username(self):
        """Test getting a bot by @username."""
        from bot_pool import BotPool, BotEntry, BotStatus
        
        pool = BotPool()
        pool._bots = [
            BotEntry(name='Bot1', token='t1', username='TestBot1', status=BotStatus.HEALTHY),
        ]
        
        # With and without @
        bot = pool.get_bot_by_username('@TestBot1')
        assert bot is not None
        assert bot.name == 'Bot1'
        
        bot = pool.get_bot_by_username('TestBot1')
        assert bot is not None
    
    @pytest.mark.asyncio
    async def test_get_healthy_bots(self):
        """Test listing healthy bots."""
        from bot_pool import BotPool, BotEntry, BotStatus
        
        pool = BotPool()
        pool._bots = [
            BotEntry(name='Bot1', token='t1', username='Bot1', status=BotStatus.HEALTHY),
            BotEntry(name='Bot2', token='t2', username='Bot2', status=BotStatus.LOCKED,
                     locked_until=time.time() + 3600),
            BotEntry(name='Bot3', token='t3', username='Bot3', status=BotStatus.HEALTHY),
        ]
        
        healthy = pool.get_healthy_bots()
        assert len(healthy) == 2
        assert healthy[0].name == 'Bot1'
        assert healthy[1].name == 'Bot3'
    
    @pytest.mark.asyncio
    async def test_bot_count(self):
        """Test bot count property."""
        from bot_pool import BotPool, BotEntry, BotStatus
        
        pool = BotPool()
        assert pool.bot_count == 0
        
        pool._bots = [
            BotEntry(name='Bot1', token='t1', status=BotStatus.HEALTHY),
            BotEntry(name='Bot2', token='t2', status=BotStatus.HEALTHY),
        ]
        assert pool.bot_count == 2
    
    @pytest.mark.asyncio
    async def test_status_report(self):
        """Test formatted status report generation."""
        from bot_pool import BotPool, BotEntry, BotStatus
        
        pool = BotPool()
        pool._bots = [
            BotEntry(name='Bot1', token='t1', username='Bot1', status=BotStatus.HEALTHY),
            BotEntry(name='Bot2', token='t2', username='Bot2', status=BotStatus.LOCKED,
                     locked_until=time.time() + 60, lock_reason='FloodWait'),
        ]
        
        report = pool.get_status_report()
        assert 'Bot Pool' in report
        assert '2 bots' in report
        assert '✅' in report
        assert '🔒' in report
        assert 'FloodWait' in report
    
    @pytest.mark.asyncio
    async def test_shutdown(self):
        """Test pool shutdown disconnects all bots."""
        from bot_pool import BotPool, BotEntry, BotStatus
        
        mock_client1 = self._make_mock_client('Bot1')
        mock_client2 = self._make_mock_client('Bot2')
        
        pool = BotPool()
        pool._bots = [
            BotEntry(name='Bot1', token='t1', client=mock_client1, 
                     username='Bot1', status=BotStatus.HEALTHY),
            BotEntry(name='Bot2', token='t2', client=mock_client2,
                     username='Bot2', status=BotStatus.HEALTHY),
        ]
        pool._running = True
        pool._health_task = None
        
        await pool.shutdown()
        
        mock_client1.disconnect.assert_called_once()
        mock_client2.disconnect.assert_called_once()
        assert len(pool._bots) == 0
    
    @pytest.mark.asyncio
    async def test_empty_pool_raises_error(self):
        """Test that get_bot raises error on empty pool."""
        from bot_pool import BotPool
        
        pool = BotPool()
        
        with pytest.raises(RuntimeError, match="not initialized"):
            pool.get_bot()


class TestLoginBotParsing:
    """Tests for login bot token parsing."""
    
    def test_parse_bot_tokens_multi(self):
        """Test parsing multiple tokens from env."""
        with patch.dict('os.environ', {
            'BOT_TOKENS': 'Bot1:111:tok1;Bot2:222:tok2;Bot3:333:tok3',
            'BOT_TOKEN': '111:tok1'
        }):
            from login_bot import parse_bot_tokens
            tokens = parse_bot_tokens()
            
            assert len(tokens) == 3
            assert tokens[0]['name'] == 'Bot1'
            assert tokens[1]['name'] == 'Bot2'
            assert tokens[2]['name'] == 'Bot3'
    
    def test_parse_bot_tokens_fallback(self):
        """Test fallback to single BOT_TOKEN."""
        with patch.dict('os.environ', {
            'BOT_TOKENS': '',
            'BOT_TOKEN': '999:fallback_token'
        }):
            from login_bot import parse_bot_tokens
            tokens = parse_bot_tokens()
            
            assert len(tokens) == 1
            assert tokens[0]['name'] == 'default'
            assert tokens[0]['token'] == '999:fallback_token'

    def test_parse_bot_tokens_empty(self):
        """Test empty tokens returns empty list."""
        with patch.dict('os.environ', {
            'BOT_TOKENS': '',
            'BOT_TOKEN': ''
        }, clear=False):
            from login_bot import parse_bot_tokens
            tokens = parse_bot_tokens()
            
            assert len(tokens) == 0


class TestLoginBotRecommendation:
    """Tests for bot recommendation logic."""
    
    def test_get_bot_recommendation(self):
        """Test bot recommendation excludes the specified bot."""
        from login_bot import get_bot_recommendation, active_login_bots
        
        # Setup mock bots
        active_login_bots.clear()
        active_login_bots['BotA'] = {'client': MagicMock(), 'locked': False}
        active_login_bots['BotB'] = {'client': MagicMock(), 'locked': False}
        active_login_bots['BotC'] = {'client': MagicMock(), 'locked': True}
        
        rec = get_bot_recommendation('BotA')
        assert rec is not None
        assert rec != '@BotA'  # Should not recommend excluded bot
        
        # Cleanup
        active_login_bots.clear()
    
    def test_get_bot_recommendation_no_alternatives(self):
        """Test recommendation when only one bot exists."""
        from login_bot import get_bot_recommendation, active_login_bots
        
        active_login_bots.clear()
        active_login_bots['OnlyBot'] = {'client': MagicMock(), 'locked': False}
        
        rec = get_bot_recommendation('OnlyBot')
        assert rec is None
        
        active_login_bots.clear()
