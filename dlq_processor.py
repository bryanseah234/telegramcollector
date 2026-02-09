"""
Dead Letter Queue Processor - Handles retry of failed tasks.

Implements smart retry logic with error classification to distinguish
transient failures (retry) from permanent ones (discard).
"""
import logging
import asyncio
import json
from enum import Enum
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)


class ErrorType(Enum):
    """Classification of error types for retry decisions."""
    TRANSIENT = 'transient'      # Network, timeout - retry with backoff
    PERMANENT = 'permanent'      # Bad data, missing resource - no retry
    RESOURCE = 'resource'        # Memory, disk - retry with longer backoff


@dataclass
class DLQEntry:
    """A dead letter queue entry with metadata."""
    task_data: dict
    error_reason: str
    error_type: ErrorType
    failed_at: datetime
    retry_count: int
    next_retry_at: Optional[datetime] = None


class DLQProcessor:
    """
    Processes dead letter queue with smart retry logic.
    
    Features:
    - Error classification (transient vs permanent)
    - Exponential backoff for retries
    - Maximum retry limits
    - Metrics and monitoring
    """
    
    # Retry intervals: 1m, 5m, 15m, 1h, 2h
    RETRY_INTERVALS = [60, 300, 900, 3600, 7200]
    MAX_RETRIES = 5
    
    # Error patterns for classification
    TRANSIENT_PATTERNS = [
        'timeout', 'connection', 'flood', 'temporary', 'rate limit',
        'network', 'reset', 'unavailable', 'retry'
    ]
    PERMANENT_PATTERNS = [
        'not found', 'invalid', 'corrupt', 'permission', 'denied',
        'deleted', 'expired', 'malformed', 'unsupported'
    ]
    RESOURCE_PATTERNS = [
        'memory', 'disk', 'space', 'quota', 'limit exceeded',
        'out of', 'too large'
    ]
    
    def __init__(self, redis_client, processing_queue=None):
        """
        Args:
            redis_client: Redis connection
            processing_queue: Optional ProcessingQueue for re-enqueuing
        """
        self.redis_client = redis_client
        self.processing_queue = processing_queue
        self.dlq_key = "processing_queue:dead_letter"
        self.processed_dlq_key = "processing_queue:dlq_processed"
        self._running = False
        self._task = None
        
        # Statistics
        self.stats = {
            'retried': 0,
            'succeeded': 0,
            'permanently_failed': 0,
            'pending': 0
        }
        
        logger.info("DLQProcessor initialized")
    
    async def start(self):
        """Starts the DLQ processing loop."""
        if self._running:
            return
        
        self._running = True
        self._task = asyncio.create_task(self._process_loop())
        logger.info("DLQ processor started")
    
    async def stop(self):
        """Stops the DLQ processor."""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("DLQ processor stopped")
    
    async def _process_loop(self):
        """Main processing loop - runs every 60 seconds."""
        # Initial check immediately on start
        try:
             await self._process_eligible_tasks()
        except Exception:
             pass

        while self._running:
            try:
                await asyncio.sleep(60)  # Check every minute (was 300)
                await self._process_eligible_tasks()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"DLQ processing error: {e}")
    
    async def _process_eligible_tasks(self):
        """Processes tasks that are eligible for retry."""
        entries = await self.get_all_entries()
        now = datetime.now(timezone.utc)
        
        retried = 0
        for entry in entries:
            # Skip if not ready for retry
            if entry.next_retry_at and entry.next_retry_at > now:
                continue
            
            # Skip permanent failures
            if entry.error_type == ErrorType.PERMANENT:
                continue
            
            # Skip if max retries exceeded
            if entry.retry_count >= self.MAX_RETRIES:
                await self._mark_permanently_failed(entry)
                continue
            
            # Attempt retry
            success = await self._retry_task(entry)
            if success:
                retried += 1
        
        if retried > 0:
            logger.info(f"DLQ: Retried {retried} tasks")
            self.stats['retried'] += retried
    
    async def _retry_task(self, entry: DLQEntry) -> bool:
        """Attempts to re-enqueue a task for processing."""
        if not self.processing_queue:
            logger.warning("No processing queue available for retry")
            return False
        
        try:
            # Re-enqueue task
            task_data = entry.task_data.copy()
            task_data['_retry_count'] = entry.retry_count + 1
            task_data['_retried_at'] = datetime.now(timezone.utc).isoformat()
            
            # Remove from DLQ
            await self._remove_from_dlq(entry)
            
            # Add back to main queue
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                None,
                self.redis_client.lpush,
                "processing_queue:tasks",
                json.dumps(task_data)
            )
            
            logger.info(f"Retried task (attempt {entry.retry_count + 1})")
            return True
            
        except Exception as e:
            logger.error(f"Failed to retry task: {e}")
            return False
    
    async def _remove_from_dlq(self, entry: DLQEntry):
        """Removes an entry from the DLQ."""
        try:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                None,
                self.redis_client.lrem,
                self.dlq_key,
                1,
                json.dumps(entry.task_data)
            )
        except Exception as e:
            logger.error(f"Failed to remove from DLQ: {e}")
    
    async def _mark_permanently_failed(self, entry: DLQEntry):
        """Moves a task to permanent failure storage."""
        try:
            entry.task_data['_permanently_failed'] = True
            entry.task_data['_final_error'] = entry.error_reason
            
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                None,
                self.redis_client.rpush,
                self.processed_dlq_key,
                json.dumps(entry.task_data)
            )
            
            await self._remove_from_dlq(entry)
            self.stats['permanently_failed'] += 1
            
            logger.warning(f"Task permanently failed after {entry.retry_count} retries")
        except Exception as e:
            logger.error(f"Failed to mark permanent failure: {e}")
    
    def classify_error(self, error: str) -> ErrorType:
        """
        Classifies an error string to determine retry strategy.
        
        Returns:
            ErrorType indicating how to handle the error
        """
        error_lower = error.lower()
        
        # Check permanent patterns first (most specific)
        for pattern in self.PERMANENT_PATTERNS:
            if pattern in error_lower:
                return ErrorType.PERMANENT
        
        # Check resource patterns
        for pattern in self.RESOURCE_PATTERNS:
            if pattern in error_lower:
                return ErrorType.RESOURCE
        
        # Check transient patterns
        for pattern in self.TRANSIENT_PATTERNS:
            if pattern in error_lower:
                return ErrorType.TRANSIENT
        
        # Default to transient (gives task another chance)
        return ErrorType.TRANSIENT
    
    def get_retry_delay(self, retry_count: int, error_type: ErrorType) -> int:
        """
        Calculates delay before next retry attempt.
        
        Args:
            retry_count: Number of retries so far
            error_type: Type of error
            
        Returns:
            Delay in seconds
        """
        if retry_count >= len(self.RETRY_INTERVALS):
            base_delay = self.RETRY_INTERVALS[-1]
        else:
            base_delay = self.RETRY_INTERVALS[retry_count]
        
        # Resource errors get double delay
        if error_type == ErrorType.RESOURCE:
            return base_delay * 2
        
        return base_delay
    
    async def get_all_entries(self) -> List[DLQEntry]:
        """Retrieves all DLQ entries with parsed metadata."""
        entries = []
        
        try:
            loop = asyncio.get_event_loop()
            raw_entries = await loop.run_in_executor(
                None,
                self.redis_client.lrange,
                self.dlq_key,
                0,
                -1
            )
            
            for raw in raw_entries:
                try:
                    data = json.loads(raw)
                    
                    # Parse metadata
                    error_reason = data.get('_failure_reason', 'Unknown error')
                    error_type = self.classify_error(error_reason)
                    retry_count = data.get('_retry_count', 0)
                    
                    failed_at_str = data.get('_failed_at')
                    if failed_at_str:
                        failed_at = datetime.fromisoformat(failed_at_str.replace('Z', '+00:00'))
                    else:
                        failed_at = datetime.now(timezone.utc)
                    
                    # Calculate next retry time
                    delay = self.get_retry_delay(retry_count, error_type)
                    next_retry = failed_at + timedelta(seconds=delay)
                    
                    entries.append(DLQEntry(
                        task_data=data,
                        error_reason=error_reason,
                        error_type=error_type,
                        failed_at=failed_at,
                        retry_count=retry_count,
                        next_retry_at=next_retry
                    ))
                except json.JSONDecodeError:
                    logger.warning("Invalid JSON in DLQ entry")
                    
        except Exception as e:
            logger.error(f"Failed to get DLQ entries: {e}")
        
        self.stats['pending'] = len(entries)
        return entries
    
    async def get_summary(self) -> Dict:
        """Returns a summary of DLQ status."""
        entries = await self.get_all_entries()
        
        by_type = {
            ErrorType.TRANSIENT.value: 0,
            ErrorType.PERMANENT.value: 0,
            ErrorType.RESOURCE.value: 0
        }
        
        for entry in entries:
            by_type[entry.error_type.value] += 1
        
        return {
            'total': len(entries),
            'by_error_type': by_type,
            'stats': self.stats.copy()
        }
    
    async def retry_all_transient(self) -> int:
        """Immediately retries all transient errors. Returns count."""
        entries = await self.get_all_entries()
        count = 0
        
        for entry in entries:
            if entry.error_type == ErrorType.TRANSIENT:
                if await self._retry_task(entry):
                    count += 1
        
        return count
    
    async def clear_permanent_failures(self) -> int:
        """Clears all permanently failed tasks. Returns count."""
        try:
            loop = asyncio.get_event_loop()
            count = await loop.run_in_executor(
                None,
                self.redis_client.delete,
                self.processed_dlq_key
            )
            logger.info(f"Cleared {count} permanent failures")
            return count
        except Exception as e:
            logger.error(f"Failed to clear permanent failures: {e}")
            return 0
