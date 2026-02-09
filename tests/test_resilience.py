"""
Tests for the resilience module.

Tests circuit breaker, retry with jitter, and rate limiter.
"""
import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
import time


class TestCircuitBreaker:
    """Tests for CircuitBreaker class."""
    
    @pytest.fixture
    def circuit_breaker(self):
        """Creates a CircuitBreaker for testing."""
        from resilience import CircuitBreaker
        return CircuitBreaker(
            name='test_circuit',
            failure_threshold=3,
            reset_timeout=5
        )
    
    @pytest.mark.asyncio
    async def test_initial_state_closed(self, circuit_breaker):
        """Test that circuit starts in CLOSED state."""
        from resilience import CircuitState
        assert circuit_breaker.state == CircuitState.CLOSED
    
    @pytest.mark.asyncio
    async def test_successful_call(self, circuit_breaker):
        """Test that successful calls pass through."""
        async def success_func():
            return 'success'
        
        result = await circuit_breaker.call(success_func)
        assert result == 'success'
    
    @pytest.mark.asyncio
    async def test_opens_after_threshold(self, circuit_breaker):
        """Test that circuit opens after failure threshold."""
        from resilience import CircuitState, CircuitOpenError
        
        async def fail_func():
            raise Exception('Test error')
        
        # Fail threshold times
        for _ in range(3):
            with pytest.raises(Exception):
                await circuit_breaker.call(fail_func)
        
        assert circuit_breaker.state == CircuitState.OPEN
        
        # Next call should raise CircuitOpenError
        with pytest.raises(CircuitOpenError):
            await circuit_breaker.call(fail_func)
    
    @pytest.mark.asyncio
    async def test_half_open_after_timeout(self, circuit_breaker):
        """Test transition to HALF_OPEN after reset timeout."""
        from resilience import CircuitState
        
        async def fail_func():
            raise Exception('Test error')
        
        # Open the circuit
        for _ in range(3):
            with pytest.raises(Exception):
                await circuit_breaker.call(fail_func)
        
        assert circuit_breaker.state == CircuitState.OPEN
        
        # Simulate timeout passing (mock the last failure time)
        from datetime import datetime, timezone, timedelta
        circuit_breaker._stats.last_failure_time = (
            datetime.now(timezone.utc) - timedelta(seconds=10)
        )
        
        # Should transition to HALF_OPEN
        assert circuit_breaker.state == CircuitState.HALF_OPEN
    
    @pytest.mark.asyncio
    async def test_closes_on_half_open_success(self, circuit_breaker):
        """Test that circuit closes on successful call in HALF_OPEN."""
        from resilience import CircuitState
        from datetime import datetime, timezone, timedelta
        
        async def fail_func():
            raise Exception('Test error')
        
        async def success_func():
            return 'success'
        
        # Open the circuit
        for _ in range(3):
            with pytest.raises(Exception):
                await circuit_breaker.call(fail_func)
        
        # Simulate timeout
        circuit_breaker._stats.last_failure_time = (
            datetime.now(timezone.utc) - timedelta(seconds=10)
        )
        
        # Successful call in HALF_OPEN should close circuit
        result = await circuit_breaker.call(success_func)
        assert result == 'success'
        assert circuit_breaker.state == CircuitState.CLOSED
    
    def test_reset(self, circuit_breaker):
        """Test manual reset."""
        from resilience import CircuitState
        
        circuit_breaker._stats.failures = 5
        circuit_breaker._stats.state = CircuitState.OPEN
        
        circuit_breaker.reset()
        
        assert circuit_breaker._stats.failures == 0
        assert circuit_breaker.state == CircuitState.CLOSED


class TestRetryWithJitter:
    """Tests for retry_with_jitter decorator."""
    
    @pytest.mark.asyncio
    async def test_returns_on_success(self):
        """Test that successful function returns immediately."""
        from resilience import retry_with_jitter
        
        call_count = 0
        
        @retry_with_jitter(max_retries=3)
        async def success_func():
            nonlocal call_count
            call_count += 1
            return 'success'
        
        result = await success_func()
        assert result == 'success'
        assert call_count == 1
    
    @pytest.mark.asyncio
    async def test_retries_on_failure(self):
        """Test that function is retried on failure."""
        from resilience import retry_with_jitter
        
        call_count = 0
        
        @retry_with_jitter(max_retries=3, base_delay=0.01)
        async def fail_then_succeed():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ValueError('Temporary error')
            return 'success'
        
        result = await fail_then_succeed()
        assert result == 'success'
        assert call_count == 3
    
    @pytest.mark.asyncio
    async def test_raises_after_max_retries(self):
        """Test that exception is raised after max retries."""
        from resilience import retry_with_jitter
        
        @retry_with_jitter(max_retries=2, base_delay=0.01)
        async def always_fail():
            raise ValueError('Persistent error')
        
        with pytest.raises(ValueError):
            await always_fail()


class TestRateLimiter:
    """Tests for RateLimiter class."""
    
    @pytest.mark.asyncio
    async def test_allows_burst(self):
        """Test that burst up to bucket size is allowed."""
        from resilience import RateLimiter
        
        limiter = RateLimiter(rate_per_second=1, bucket_size=5)
        
        # Should allow 5 immediate acquisitions
        start = time.monotonic()
        for _ in range(5):
            await limiter.acquire()
        elapsed = time.monotonic() - start
        
        # Should be nearly instantaneous
        assert elapsed < 0.5
    
    @pytest.mark.asyncio
    async def test_rate_limits_after_burst(self):
        """Test that rate limiting kicks in after burst."""
        from resilience import RateLimiter
        
        limiter = RateLimiter(rate_per_second=10, bucket_size=2)
        
        # Exhaust burst
        await limiter.acquire(2)
        
        # Next acquire should wait
        start = time.monotonic()
        await limiter.acquire()
        elapsed = time.monotonic() - start
        
        # Should have waited approximately 0.1s (1/10 second)
        assert elapsed >= 0.05


class TestHealthAggregator:
    """Tests for HealthAggregator class."""
    
    @pytest.mark.asyncio
    async def test_register_and_check(self):
        """Test registering and running health checks."""
        from resilience import HealthAggregator
        
        aggregator = HealthAggregator()
        
        # Register a healthy check
        async def healthy_check():
            return True, 'All good'
        
        aggregator.register('test_component', healthy_check)
        
        results = await aggregator.check_all()
        
        assert 'test_component' in results
        assert results['test_component'] == (True, 'All good')
    
    @pytest.mark.asyncio
    async def test_is_healthy_property(self):
        """Test is_healthy property."""
        from resilience import HealthAggregator
        
        aggregator = HealthAggregator()
        
        async def healthy():
            return True, 'OK'
        
        async def unhealthy():
            return False, 'Failed'
        
        aggregator.register('healthy', healthy)
        await aggregator.check_all()
        assert aggregator.is_healthy == True
        
        aggregator.register('unhealthy', unhealthy)
        await aggregator.check_all()
        assert aggregator.is_healthy == False
