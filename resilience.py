"""
Resilience Utilities - Circuit breaker, retry, and rate limiting patterns.

Provides decorator-based utilities for building resilient systems:
- Circuit breaker to prevent cascade failures
- Retry with jitter to avoid thundering herd
- Timeout wrapper for stuck operations
- Rate limiter for external API calls
"""
import logging
import asyncio
import random
import time
from typing import Callable, Optional, Any
from functools import wraps
from enum import Enum
from dataclasses import dataclass
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = 'closed'      # Normal operation
    OPEN = 'open'          # Blocking calls
    HALF_OPEN = 'half_open'  # Testing if service recovered


@dataclass
class CircuitStats:
    """Statistics for a circuit breaker."""
    failures: int = 0
    successes: int = 0
    last_failure_time: Optional[datetime] = None
    state: CircuitState = CircuitState.CLOSED


class CircuitBreaker:
    """
    Circuit breaker pattern implementation.
    
    Prevents cascade failures by stopping calls to a failing service
    after a threshold of consecutive failures.
    
    States:
    - CLOSED: Normal operation, calls pass through
    - OPEN: Calls fail immediately without trying
    - HALF_OPEN: Single test call allowed to check recovery
    """
    
    def __init__(
        self,
        name: str,
        failure_threshold: int = 5,
        reset_timeout: int = 60,
        half_open_max_calls: int = 1
    ):
        """
        Args:
            name: Identifier for this circuit breaker
            failure_threshold: Consecutive failures before opening
            reset_timeout: Seconds before trying again (OPEN -> HALF_OPEN)
            half_open_max_calls: Max calls allowed in HALF_OPEN state
        """
        self.name = name
        self.failure_threshold = failure_threshold
        self.reset_timeout = reset_timeout
        self.half_open_max_calls = half_open_max_calls
        self._stats = CircuitStats()
        self._half_open_calls = 0
        self._lock = asyncio.Lock()
    
    @property
    def state(self) -> CircuitState:
        """Returns current state, transitioning OPEN->HALF_OPEN if timeout passed."""
        if self._stats.state == CircuitState.OPEN:
            if self._stats.last_failure_time:
                elapsed = (datetime.now(timezone.utc) - self._stats.last_failure_time).total_seconds()
                if elapsed >= self.reset_timeout:
                    self._stats.state = CircuitState.HALF_OPEN
                    self._half_open_calls = 0
                    logger.info(f"Circuit '{self.name}' transitioning OPEN -> HALF_OPEN")
        return self._stats.state
    
    async def call(self, func: Callable, *args, **kwargs) -> Any:
        """
        Executes function through the circuit breaker.
        
        Raises:
            CircuitOpenError: If circuit is OPEN
        """
        async with self._lock:
            current_state = self.state
            
            if current_state == CircuitState.OPEN:
                raise CircuitOpenError(f"Circuit '{self.name}' is OPEN")
            
            if current_state == CircuitState.HALF_OPEN:
                if self._half_open_calls >= self.half_open_max_calls:
                    raise CircuitOpenError(f"Circuit '{self.name}' is HALF_OPEN (max calls reached)")
                self._half_open_calls += 1
        
        try:
            if asyncio.iscoroutinefunction(func):
                result = await func(*args, **kwargs)
            else:
                result = func(*args, **kwargs)
            
            await self._on_success()
            return result
            
        except Exception as e:
            await self._on_failure()
            raise
    
    async def _on_success(self):
        """Records a successful call."""
        async with self._lock:
            self._stats.successes += 1
            
            if self._stats.state == CircuitState.HALF_OPEN:
                # Success in HALF_OPEN closes the circuit
                self._stats.state = CircuitState.CLOSED
                self._stats.failures = 0
                logger.info(f"Circuit '{self.name}' CLOSED (recovered)")
    
    async def _on_failure(self):
        """Records a failed call."""
        async with self._lock:
            self._stats.failures += 1
            self._stats.last_failure_time = datetime.now(timezone.utc)
            
            if self._stats.state == CircuitState.HALF_OPEN:
                # Failure in HALF_OPEN reopens the circuit
                self._stats.state = CircuitState.OPEN
                logger.warning(f"Circuit '{self.name}' OPEN (failed in HALF_OPEN)")
            
            elif self._stats.failures >= self.failure_threshold:
                self._stats.state = CircuitState.OPEN
                logger.warning(f"Circuit '{self.name}' OPEN (threshold reached: {self._stats.failures})")
    
    def reset(self):
        """Manually resets the circuit breaker."""
        self._stats = CircuitStats()
        self._half_open_calls = 0
        logger.info(f"Circuit '{self.name}' manually reset")


class CircuitOpenError(Exception):
    """Raised when circuit breaker is open."""
    pass


def retry_with_jitter(
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    jitter_factor: float = 0.5,
    retryable_exceptions: tuple = (Exception,)
):
    """
    Decorator for async functions that retries with exponential backoff and jitter.
    
    Args:
        max_retries: Maximum number of retry attempts
        base_delay: Initial delay in seconds
        max_delay: Maximum delay cap
        jitter_factor: Random jitter as fraction of delay (0-1)
        retryable_exceptions: Tuple of exception types to retry
    """
    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            last_exception = None
            
            for attempt in range(max_retries + 1):
                try:
                    return await func(*args, **kwargs)
                except retryable_exceptions as e:
                    last_exception = e
                    
                    if attempt == max_retries:
                        logger.error(f"{func.__name__} failed after {max_retries + 1} attempts: {e}")
                        raise
                    
                    # Calculate delay with exponential backoff
                    delay = min(base_delay * (2 ** attempt), max_delay)
                    
                    # Add jitter
                    jitter = delay * jitter_factor * random.random()
                    actual_delay = delay + jitter
                    
                    logger.warning(
                        f"{func.__name__} failed (attempt {attempt + 1}/{max_retries + 1}): {e}. "
                        f"Retrying in {actual_delay:.2f}s..."
                    )
                    
                    await asyncio.sleep(actual_delay)
            
            # Should not reach here, but just in case
            if last_exception:
                raise last_exception
        
        return wrapper
    return decorator


def with_timeout(timeout_seconds: float):
    """
    Decorator that adds a timeout to an async function.
    
    Args:
        timeout_seconds: Maximum execution time
        
    Raises:
        asyncio.TimeoutError: If function doesn't complete in time
    """
    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            return await asyncio.wait_for(
                func(*args, **kwargs),
                timeout=timeout_seconds
            )
        return wrapper
    return decorator


class RateLimiter:
    """
    Token bucket rate limiter for API calls.
    
    Allows burst traffic up to bucket_size, then limits to rate_per_second.
    """
    
    def __init__(
        self,
        rate_per_second: float = 1.0,
        bucket_size: int = 10
    ):
        """
        Args:
            rate_per_second: Sustained rate limit
            bucket_size: Maximum burst size
        """
        self.rate = rate_per_second
        self.bucket_size = bucket_size
        self._tokens = bucket_size
        self._last_update = time.monotonic()
        self._lock = asyncio.Lock()
    
    async def acquire(self, tokens: int = 1):
        """
        Acquires tokens, waiting if necessary.
        
        Args:
            tokens: Number of tokens to acquire
        """
        async with self._lock:
            # Refill tokens based on elapsed time
            now = time.monotonic()
            elapsed = now - self._last_update
            self._tokens = min(
                self.bucket_size,
                self._tokens + elapsed * self.rate
            )
            self._last_update = now
            
            # Wait if not enough tokens
            if self._tokens < tokens:
                wait_time = (tokens - self._tokens) / self.rate
                await asyncio.sleep(wait_time)
                self._tokens = tokens  # Will become 0 after consumption
            
            self._tokens -= tokens


class HealthAggregator:
    """
    Aggregates health status from multiple components.
    
    Components register their health check functions, and the
    aggregator provides a unified view of system health.
    """
    
    def __init__(self):
        self._checks: dict[str, Callable] = {}
        self._last_results: dict[str, tuple[bool, str]] = {}
    
    def register(self, name: str, check_func: Callable):
        """Registers a health check function."""
        self._checks[name] = check_func
    
    async def check_all(self) -> dict[str, tuple[bool, str]]:
        """
        Runs all health checks and returns results.
        
        Returns:
            Dict mapping component name to (healthy, message) tuple
        """
        results = {}
        
        for name, check_func in self._checks.items():
            try:
                if asyncio.iscoroutinefunction(check_func):
                    healthy, message = await check_func()
                else:
                    healthy, message = check_func()
                results[name] = (healthy, message)
            except Exception as e:
                results[name] = (False, f"Check failed: {e}")
        
        self._last_results = results
        return results
    
    @property
    def is_healthy(self) -> bool:
        """Returns True if all components are healthy."""
        return all(healthy for healthy, _ in self._last_results.values())
    
    def get_summary(self) -> str:
        """Returns a formatted summary of health status."""
        if not self._last_results:
            return "No health checks performed yet"
        
        lines = []
        for name, (healthy, message) in self._last_results.items():
            status = "✅" if healthy else "❌"
            lines.append(f"{status} {name}: {message}")
        
        return "\n".join(lines)


# Pre-configured circuit breakers for common services
_circuit_breakers: dict[str, CircuitBreaker] = {}


def get_circuit_breaker(name: str) -> CircuitBreaker:
    """
    Gets or creates a circuit breaker by name.
    
    Pre-configured breakers:
    - 'database': For DB connections
    - 'telegram': For Telegram API
    - 'face_processor': For face detection
    """
    if name not in _circuit_breakers:
        from config import settings
        
        threshold = getattr(settings, 'CIRCUIT_BREAKER_THRESHOLD', 5)
        timeout = getattr(settings, 'CIRCUIT_BREAKER_TIMEOUT', 60)
        
        _circuit_breakers[name] = CircuitBreaker(
            name=name,
            failure_threshold=threshold,
            reset_timeout=timeout
        )
    
    return _circuit_breakers[name]
