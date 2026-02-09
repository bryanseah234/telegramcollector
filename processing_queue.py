"""
Processing Queue - Coordinates face processing pipeline.

Orchestrates the complete workflow:
Download → Extract Frames → Detect Faces → Match Identities → Upload

Integrates all Phase 1-3 components.
Includes queue backpressure for adaptive scan rate control.
"""
import logging
import asyncio
import io
import json
import base64
import time
import gc
from typing import Dict, Any, Optional, Callable
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime, timezone
import os

try:
    import redis
except ImportError:
    redis = None

from database import get_db_connection
from hub_notifier import notify, increment_stat
from resilience import get_circuit_breaker, CircuitOpenError
from config import settings, get_dynamic_setting

logger = logging.getLogger(__name__)


class TaskType(Enum):
    """Types of processing tasks."""
    MEDIA = 'media'
    PROFILE_PHOTO = 'profile_photo'


class BackpressureState(Enum):
    """Queue backpressure states."""
    NORMAL = 'normal'      # Queue size < low watermark
    WARNING = 'warning'    # Queue size between watermarks
    CRITICAL = 'critical'  # Queue size > high watermark


@dataclass
class ProcessingTask:
    """A task to be processed by the queue."""
    task_type: TaskType
    chat_id: int = 0
    message_id: int = 0
    user_id: int = 0
    content: io.BytesIO = None
    media_type: str = 'photo'  # 'photo', 'video', 'video_note'
    file_unique_id: str = None  # For deduplication tracking
    metadata: Dict[str, Any] = field(default_factory=dict)


class ProcessingQueue:
    """
    Coordinates the face processing pipeline with a worker pool.
    
    Integrates:
    - Phase 1: Database (storing embeddings, topics)
    - Phase 2: Telegram (topic manager, media uploader, scanner)
    - Phase 3: Face processing (detection, matching, video extraction)
    
    Features queue backpressure to prevent memory overload and
    provide signals for adaptive scan rate control.
    """
    
    def __init__(
        self,
        face_processor=None,
        video_extractor=None,
        identity_matcher=None,
        media_uploader=None,
        topic_manager=None,
        num_workers: int = 3,
        high_watermark: int = 100,
        low_watermark: int = 20
    ):
        """
        Args:
            face_processor: FaceProcessor instance
            video_extractor: VideoFrameExtractor instance
            identity_matcher: IdentityMatcher instance
            media_uploader: MediaUploader instance
            topic_manager: TopicManager instance
            num_workers: Number of concurrent workers
            high_watermark: Queue size above which to signal slowdown
            low_watermark: Queue size below which to resume normal speed
        """
        self.face_processor = face_processor
        self.video_extractor = video_extractor
        self.identity_matcher = identity_matcher
        self.media_uploader = media_uploader
        self.topic_manager = topic_manager
        
        self.num_workers = num_workers
        self._workers = []
        self._running = False
        
        # Redis Connection with Fallback
        self.redis_available = False
        self.fallback_queue = asyncio.Queue()  # In-memory fallback
        
        try:
            if redis is None:
                raise ImportError("redis package not installed")

            self.redis_client = redis.Redis(
                host=settings.REDIS_HOST,
                port=settings.REDIS_PORT,
                db=settings.REDIS_DB,
                password=settings.REDIS_PASSWORD,
                decode_responses=False  # Keep as bytes for binary content
            )
            self.redis_client.ping()
            self.redis_available = True
            logger.info(f"ProcessingQueue initialized with {num_workers} workers [Redis Connected]")
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}. Using in-memory fallback.")
            self.redis_client = None
            self.redis_available = False
            
        self.queue_key = "processing_queue:tasks"
        
        # Backpressure configuration
        self.high_watermark = high_watermark
        self.low_watermark = low_watermark
        self._backpressure_state = BackpressureState.NORMAL
        self._backpressure_callbacks: list = []
        
        # Adaptive backpressure tracking
        self._per_chat_times: Dict[int, float] = {}  # chat_id -> avg processing time
        self._processed_last_minute = 0
        self._processing_times: list = []  # Recent processing times for ETA
        
        # Statistics
        self.stats = {
            'processed': 0,
            'faces_found': 0,
            'new_identities': 0,
            'errors': 0
        }
        
        # Dead letter queue for failed tasks
        self.dead_letter_key = "processing_queue:dead_letter"
        self.max_task_retries = 3
        
        # Task execution timeout (prevent stuck workers)
        self.task_timeout_seconds = 300  # 5 minutes max per task
        
        # Worker memory limit (MB)
        # Worker memory limit (MB)
        self.worker_memory_limit_mb = 2048

    def _get_trace_id(self):
        """Helper to get current trace ID or generate new one."""
        try:
            from observability import get_trace_id
            return get_trace_id()
        except ImportError:
            import uuid
            return str(uuid.uuid4())
            
    async def start(self):
        """Starts the worker pool."""
        self._running = True
        
        for i in range(self.num_workers):
            worker = asyncio.create_task(self._worker(i))
            self._workers.append(worker)
        
        # Start heartbeat monitor
        self._heartbeat_monitor_task = asyncio.create_task(self._monitor_heartbeats())
        
        logger.info(f"Started {self.num_workers} processing workers")
    
    async def stop(self):
        """Stops the worker pool gracefully."""
        self._running = False
        
        # Cancel all workers
        for worker in self._workers:
            worker.cancel()
        
        if hasattr(self, '_heartbeat_monitor_task'):
            self._heartbeat_monitor_task.cancel()
        
        await asyncio.gather(*self._workers, return_exceptions=True)
        self._workers = []
        
        logger.info("Processing workers stopped")
    
    async def enqueue_media(
        self,
        chat_id: int,
        message_id: int,
        content: io.BytesIO,
        media_type: str = 'photo',
        file_unique_id: str = None
    ):
        """Enqueues media for face processing."""
        
        content.seek(0)
        content_b64 = base64.b64encode(content.read()).decode('ascii')
        
        task_data = {
            'task_type': TaskType.MEDIA.value,
            'chat_id': chat_id,
            'message_id': message_id,
            'content_b64': content_b64,
            'media_type': media_type,
            'file_unique_id': file_unique_id,
            'metadata': {
                'trace_id': self._get_trace_id()
            }
        }
        
        if self.redis_available:
            # Push to Redis list (Right Push)
            # Run in executor to avoid blocking asyncio loop with sync Redis call
            loop = asyncio.get_event_loop()
            try:
                await loop.run_in_executor(
                    None, 
                    self.redis_client.rpush, 
                    self.queue_key, 
                    json.dumps(task_data)
                )
            except Exception as e:
                logger.error(f"Redis enqueue failed: {e}. Switching to fallback.")
                self.redis_available = False
                await self.fallback_queue.put(json.dumps(task_data))
        else:
            # Use in-memory fallback
            await self.fallback_queue.put(json.dumps(task_data))
        
        self._check_backpressure()
        logger.debug(f"Enqueued {media_type} from chat {chat_id}, msg {message_id}")
    
    async def enqueue_profile_photo(
        self,
        user_id: int,
        content: io.BytesIO
    ):
        """Enqueues a profile photo for face processing."""
        
        content.seek(0)
        content_b64 = base64.b64encode(content.read()).decode('ascii')
        
        task_data = {
            'task_type': TaskType.PROFILE_PHOTO.value,
            'user_id': user_id,
            'content_b64': content_b64,
            'media_type': 'photo',
            'metadata': {
                'trace_id': self._get_trace_id()
            }
        }
        
        if self.redis_available:
            loop = asyncio.get_event_loop()
            try:
                await loop.run_in_executor(
                    None, 
                    self.redis_client.rpush, 
                    self.queue_key, 
                    json.dumps(task_data)
                )
            except Exception as e:
                logger.error(f"Redis enqueue failed: {e}. Switching to fallback.")
                self.redis_available = False
                await self.fallback_queue.put(json.dumps(task_data))
        else:
            await self.fallback_queue.put(json.dumps(task_data))
        
        self._check_backpressure()
        logger.debug(f"Enqueued profile photo for user {user_id}")
    
    def _check_backpressure(self):
        """Checks queue size and updates backpressure state."""
        if self.redis_available:
            try:
                queue_size = self.redis_client.llen(self.queue_key)
            except Exception:
                self.redis_available = False
                queue_size = self.fallback_queue.qsize()
        else:
            queue_size = self.fallback_queue.qsize()
            
        old_state = self._backpressure_state
        
        # Get dynamic high watermark
        dynamic_high = get_dynamic_setting("QUEUE_MAX_SIZE", self.high_watermark)
        dynamic_low = int(dynamic_high * 0.2)  # Maintain ratio
        
        if queue_size >= dynamic_high:
            self._backpressure_state = BackpressureState.CRITICAL
        elif queue_size >= dynamic_low:
            self._backpressure_state = BackpressureState.WARNING
        else:
            self._backpressure_state = BackpressureState.NORMAL
        
        # Notify callbacks if state changed
        if old_state != self._backpressure_state:
            logger.info(f"Backpressure state: {old_state.value} → {self._backpressure_state.value} (queue: {queue_size})")
            self._notify_backpressure_change()
            
    def _notify_backpressure_change(self):
        """Notifies registered callbacks of backpressure state change."""
        for callback in self._backpressure_callbacks:
            try:
                callback(self._backpressure_state)
            except Exception as e:
                logger.error(f"Backpressure callback error: {e}")
    
    def register_backpressure_callback(self, callback: Callable):
        """Registers a callback to be notified of backpressure state changes."""
        self._backpressure_callbacks.append(callback)
    
    def get_backpressure_state(self) -> BackpressureState:
        """Returns current backpressure state."""
        return self._backpressure_state
    
    def should_slow_down(self) -> bool:
        """Returns True if scanners should reduce their rate."""
        return self._backpressure_state != BackpressureState.NORMAL
    
    def should_pause(self) -> bool:
        """Returns True if scanners should pause completely."""
        return self._backpressure_state == BackpressureState.CRITICAL
    
    def get_adaptive_delay(self) -> float:
        """
        Returns recommended delay in seconds based on queue pressure.
        Provides gradual slowdown instead of binary pause.
        """
        if self.redis_available:
            try:
                queue_size = self.redis_client.llen(self.queue_key)
            except Exception:
                self.redis_available = False
                queue_size = self.fallback_queue.qsize()
        else:
            queue_size = self.fallback_queue.qsize()
            
        # Get dynamic high watermark
        high_water = get_dynamic_setting("QUEUE_MAX_SIZE", self.high_watermark)
        
        if queue_size < high_water * 0.5:
            return 0.0  # Full speed
        elif queue_size < high_water * 0.75:
            return 1.0  # Light slowdown
        elif queue_size < high_water:
            return 3.0  # Moderate slowdown
        else:
            return 5.0  # Heavy slowdown (but not paused)
    
    def get_chat_delay(self, chat_id: int) -> float:
        """
        Returns additional delay for a specific chat based on its processing history.
        Slow chats get extra delay to prevent blocking others.
        """
        if chat_id not in self._per_chat_times:
            return 0.0
        
        avg_time = self._per_chat_times[chat_id]
        if avg_time > 10.0:  # Very slow chat (>10s per item)
            return 2.0
        elif avg_time > 5.0:  # Slow chat
            return 1.0
        return 0.0
    
    def record_processing_time(self, chat_id: int, duration: float):
        """Records processing time for a chat for adaptive throttling."""
        # Update per-chat average (exponential moving average)
        if chat_id in self._per_chat_times:
            self._per_chat_times[chat_id] = 0.8 * self._per_chat_times[chat_id] + 0.2 * duration
        else:
            self._per_chat_times[chat_id] = duration
        
        # Track for ETA calculation
        self._processing_times.append(duration)
        if len(self._processing_times) > 100:
            self._processing_times.pop(0)
    
    def get_queue_eta(self) -> Optional[float]:
        """
        Estimates time to process current queue in minutes.
        Returns None if not enough data.
        """
        if len(self._processing_times) < 5:
            return None
        
        queue_size = self.redis_client.llen(self.queue_key)
        if queue_size == 0:
            return 0.0
        
        avg_time = sum(self._processing_times) / len(self._processing_times)
        eta_seconds = queue_size * avg_time / self.num_workers
        return eta_seconds / 60  # Convert to minutes
    
    async def _worker(self, worker_id: int):
        """Worker coroutine that processes tasks from the Redis queue."""
        logger.info(f"Worker {worker_id} started")
        
        loop = asyncio.get_event_loop()
        consecutive_errors = 0
        
        while self._running:
            try:
                # Check memory limit
                if self._check_worker_memory():
                    logger.warning(f"Worker {worker_id} exceeded memory limit ({self.worker_memory_limit_mb}MB)")
                    # Instead of crashing, clear caches and run GC
                    gc.collect()
                    # If still over limit, log warning
                    if self._check_worker_memory():
                        logger.error(f"Worker {worker_id} still over memory after GC, consider restart")
                
                # Update heartbeat
                await self._update_heartbeat(worker_id)
                
                # Fetch task
                task_json = None
                
                if self.redis_available:
                    try:
                        # Fetch task from Redis (Left Pop with blocking)
                        result = await loop.run_in_executor(
                            None,
                            self.redis_client.blpop,
                            self.queue_key,
                            1  # 1 second timeout
                        )
                        
                        if result:
                            _, task_json = result
                            
                    except Exception as e:
                        logger.error(f"Redis fetch failed: {e}. Switching to fallback.")
                        self.redis_available = False
                        # Try fallback immediately
                        if not self.fallback_queue.empty():
                            task_json = await self.fallback_queue.get()
                else:
                    # Redis unavailable - try fallback queue
                    try:
                        # Check if Redis is back (every 30s roughly)
                        if worker_id == 0 and int(time.time()) % 30 == 0:
                            self._try_reconnect_redis()
                            
                        # Non-blocking get from fallback
                        task_json = self.fallback_queue.get_nowait()
                    except asyncio.QueueEmpty:
                        await asyncio.sleep(1)
                        continue
                
                if not task_json:
                    continue
                
                if not result:
                    continue
                    
                # Decode if bytes (Redis) or str (Fallback)
                if isinstance(task_json, bytes):
                    task_json = task_json.decode('utf-8')
                
                task_data = json.loads(task_json)
                
                # Reconstruct ProcessingTask
                content_bytes = base64.b64decode(task_data['content_b64'])
                content_io = io.BytesIO(content_bytes)
                
                task = ProcessingTask(
                    task_type=TaskType(task_data['task_type']),
                    chat_id=task_data.get('chat_id', 0),
                    message_id=task_data.get('message_id', 0),
                    user_id=task_data.get('user_id', 0),
                    content=content_io,
                    media_type=task_data.get('media_type', 'photo'),
                    file_unique_id=task_data.get('file_unique_id')
                )
                
                try:
                    # Update queue depth metric
                    from observability import update_queue_gauge, TraceContext, set_trace_id, get_trace_id
                    
                    if self.redis_available:
                        try:
                            q_len = self.redis_client.llen(self.queue_key)
                            update_queue_gauge(q_len)
                        except:
                            pass
                    else:
                        update_queue_gauge(self.fallback_queue.qsize())
                        
                    # Generate/propagate trace ID
                    # (In a real scenario, extract from task_data metadata if present)
                    trace_id = task_data.get('metadata', {}).get('trace_id')
                    if trace_id:
                        set_trace_id(trace_id)
                    else:
                        trace_id = get_trace_id()
                        
                    # Track processing time for adaptive throttling AND Prometheus
                    with TraceContext("ProcessTask", task_type=task_data.get('task_type'), chat_id=task_data.get('chat_id')):
                        start_time = time.time()
                        
                        # Add timeout protection to prevent stuck workers
                        await asyncio.wait_for(
                            self._process_task(task, worker_id),
                            timeout=self.task_timeout_seconds
                        )
                        
                        # Record processing duration for per-chat throttling
                        duration = time.time() - start_time
                        self.record_processing_time(task.chat_id, duration)
                    
                    self.stats['processed'] += 1
                except asyncio.TimeoutError:
                    from observability import record_error
                    record_error("TimeoutError")
                    logger.error(f"Worker {worker_id}: Task timed out after {self.task_timeout_seconds}s")
                    self.stats['errors'] += 1
                    await self._move_to_dead_letter(task_data, "timeout")
                except Exception as e:
                    logger.error(f"Worker {worker_id} error processing task: {e}")
                    self.stats['errors'] += 1
                    await self._move_to_dead_letter(task_data, str(e))
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                consecutive_errors += 1
                delay = min(30, 1.5 ** consecutive_errors)  # Exponential backoff up to 30s
                logger.error(f"Worker {worker_id} loop error (consecutive={consecutive_errors}): {e}")
                logger.info(f"Worker {worker_id} sleeping {delay:.1f}s for panic recovery...")
                await asyncio.sleep(delay)
            else:
                # Reset error count on successful loop iteration (if we get here)
                consecutive_errors = 0
        
        logger.info(f"Worker {worker_id} stopped")

    async def _move_to_dead_letter(self, task_data: dict, error_reason: str):
        """
        Moves a failed task to the dead letter queue for later analysis.
        
        Args:
            task_data: Original task data dict
            error_reason: Description of why the task failed
        """
        try:
            # Add failure metadata
            task_data['_failure_reason'] = error_reason
            task_data['_failed_at'] = datetime.now(timezone.utc).isoformat()
            task_data['_retry_count'] = task_data.get('_retry_count', 0) + 1
            
            # Remove large content to save space (just keep ID/metadata)
            if 'content_b64' in task_data:
                task_data['_had_content'] = True
                del task_data['content_b64']
            
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                None,
                self.redis_client.rpush,
                self.dead_letter_key,
                json.dumps(task_data)
            )
            
            # Also log to database for dashboard visibility
            from database import log_processing_error
            await log_processing_error(
                error_type='TaskFailed',
                error_message=error_reason,
                error_context={
                    'chat_id': task_data.get('chat_id'),
                    'message_id': task_data.get('message_id'),
                    'task_type': task_data.get('task_type')
                }
            )
            
            logger.warning(f"Task moved to dead letter queue: {error_reason}")
            
            # Notify Hub about task failure (immediate alert)
            await notify('error', f"Task failed: {error_reason[:80]}", priority=2)
            await increment_stat('errors_count', 1)
        except Exception as e:
            logger.error(f"Failed to move task to dead letter queue: {e}")

    async def _update_heartbeat(self, worker_id: int):
        """Updates the heartbeat timestamp for a worker in Redis."""
        try:
            timestamp = int(datetime.now(timezone.utc).timestamp())
            key = f"worker_heartbeat:{worker_id}"
            # Set with 5 minute TTL (if worker dies hard, key expires)
            self.redis_client.setex(key, 300, timestamp)
        except Exception as e:
            logger.warning(f"Failed to update heartbeat for worker {worker_id}: {e}")
    
    def _check_worker_memory(self) -> bool:
        """
        Checks if worker process exceeds memory limit.
        
        Returns:
            True if memory exceeds limit, False otherwise
        """
        try:
            import psutil
            process = psutil.Process()
            mem_mb = process.memory_info().rss / 1024 / 1024
            return mem_mb > self.worker_memory_limit_mb
        except ImportError:
            # psutil not available, skip check
            return False
        except Exception as e:
            logger.debug(f"Memory check failed: {e}")
            return False

    async def _monitor_heartbeats(self):
        """Monitors worker heartbeats and alerts on stalls."""
        from bot_client import get_bot_client
        from config import settings
        
        while self._running:
            await asyncio.sleep(60)  # Check every minute
            
            try:
                now = int(datetime.now(timezone.utc).timestamp())
                stuck_workers = []
                
                for i in range(self.num_workers):
                    key = f"worker_heartbeat:{i}"
                    last_beat = self.redis_client.get(key)
                    
                    if last_beat:
                        last_beat = int(last_beat)
                        if now - last_beat > 300:  # 5 minutes
                            stuck_workers.append(i)
                
                if stuck_workers:
                    msg = f"⚠️ **Worker Alert**: Workers {stuck_workers} haven't heartbeat in 5 mins!"
                    logger.warning(msg)
                    
                    # Alert via Bot
                    try:
                        bot = await get_bot_client()
                        await bot.send_message(settings.HUB_GROUP_ID, msg)
                    except Exception as e:
                        logger.error(f"Failed to send alert: {e}")
                        
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Heartbeat monitor error: {e}")
                await asyncio.sleep(60)

    def _try_reconnect_redis(self):
        """Attempts to reconnect to Redis if connection was lost."""
        if self.redis_available:
            return
            
        logger.info("Attempting to reconnect to Redis...")
        try:
            # Recreate client
            if redis is None:
                return

            self.redis_client = redis.Redis(
                host=settings.REDIS_HOST,
                port=settings.REDIS_PORT,
                db=settings.REDIS_DB,
                password=settings.REDIS_PASSWORD,
                decode_responses=False
            )
            self.redis_client.ping()
            self.redis_available = True
            logger.info("✅ Redis reconnected! Draining fallback queue...")
            
            # Drain fallback queue to Redis
            count = 0
            while not self.fallback_queue.empty():
                try:
                    task_json = self.fallback_queue.get_nowait()
                    self.redis_client.rpush(self.queue_key, task_json)
                    count += 1
                except asyncio.QueueEmpty:
                    break
                except Exception as e:
                    logger.error(f"Error draining fallback queue: {e}")
                    # Put back if possible, or log as lost
                    break
            
            if count > 0:
                logger.info(f"Drained {count} tasks from in-memory fallback to Redis")
                
        except Exception as e:
            logger.warning(f"Redis reconnection failed: {e}")


    async def _process_task(self, task: ProcessingTask, worker_id: int):
        """Processes a single task through the complete pipeline."""
        
        if task.task_type == TaskType.PROFILE_PHOTO:
            await self._process_profile_photo(task)
        else:
            await self._process_media(task)
    
    async def _process_media(self, task: ProcessingTask):
        """
        Complete media processing pipeline:
        1. Validate media (check for corruption)
        2. Extract frames (for video) or use image directly
        3. Detect faces in each frame
        4. Match each face to identity
        5. Upload media to matched topics
        """
        logger.debug(f"Processing {task.media_type} from chat {task.chat_id}")
        
        # Step 0: Validate media before processing
        if not self._validate_media(task.content, task.media_type):
            logger.warning(f"⚠️ Skipping invalid/corrupted {task.media_type} from chat {task.chat_id}")
            self.stats['errors'] += 1
            return
        
        # Step 1: Get frames
        if task.media_type in ('video', 'video_note'):
            is_round = task.media_type == 'video_note'
            frames = await self.video_extractor.extract_frames(task.content, is_round)
        else:
            # Single image - wrap in list
            frames = [task.content]
        
        if not frames:
            logger.warning(f"No frames extracted from {task.media_type}")
            return
        
        # Step 2 & 3: Detect faces and match identities
        matched_topics = set()
        total_faces_detected = 0
        
        for frame_idx, frame in enumerate(frames):
            faces = await self.face_processor.process_image(frame)
            total_faces_detected += len(faces)
            
            if faces:
                logger.info(f"🔍 Frame {frame_idx}: detected {len(faces)} face(s)")
            
            for face in faces:
                self.stats['faces_found'] += 1
                
                topic_id, is_new = await self.identity_matcher.find_or_create_identity(
                    embedding=face['embedding'],
                    quality_score=face['quality'],
                    source_chat_id=task.chat_id,
                    source_message_id=task.message_id,
                    frame_index=frame_idx
                )
                
                if topic_id:
                    logger.debug(f"  → Face matched to topic {topic_id} (new={is_new}, quality={face['quality']:.2f})")
                    matched_topics.add(topic_id)
                    if is_new:
                        self.stats['new_identities'] += 1
                        # Notify Hub about new identity (high priority)
                        await notify('face', f"🆕 New identity **Person {topic_id}** created!", priority=2)
                else:
                    logger.debug(f"  → Face skipped (quality={face['quality']:.2f}, below threshold?)")
        
        logger.info(f"👥 Total: {total_faces_detected} faces detected → {len(matched_topics)} unique topic(s)")
        
        # Update stats for batched Hub notification
        if total_faces_detected > 0:
            await increment_stat('faces_detected', total_faces_detected)
        if matched_topics:
            await increment_stat('uploads_completed', len(matched_topics))
        
        # Step 4: Upload media to all matched topics
        if matched_topics and self.media_uploader:
            logger.info(f"📤 Uploading to {len(matched_topics)} topic(s) for msg {task.message_id}")
            task.content.seek(0)
            
            for topic_id in matched_topics:
                result = await self.media_uploader.upload_to_topic(
                    db_topic_id=topic_id,
                    media_buffer=task.content,
                    source_message_id=task.message_id,
                    source_chat_id=task.chat_id,
                    media_type=task.media_type
                )
                if result == 0:
                    logger.warning(f"⚠️ Upload to topic {topic_id} returned 0 (failed or duplicate)")
                task.content.seek(0)  # Reset for next upload
        elif not matched_topics:
            logger.debug(f"No faces matched for {task.media_type} msg {task.message_id}")
        
        # Step 5: Mark file as processed for deduplication
        if task.file_unique_id:
            await self._mark_file_processed(
                file_unique_id=task.file_unique_id,
                media_type=task.media_type,
                chat_id=task.chat_id,
                message_id=task.message_id,
                faces_found=len([t for t in matched_topics]),
                topics_matched=list(matched_topics)
            )
        
        logger.info(f"Processed {task.media_type}: {len(frames)} frames, matched to {len(matched_topics)} topics")
    
    async def _process_profile_photo(self, task: ProcessingTask):
        """Processes a user's profile photo."""
        logger.debug(f"Processing profile photo for user {task.user_id}")
        
        faces = await self.face_processor.process_image(task.content)
        
        for face in faces:
            self.stats['faces_found'] += 1
            
            topic_id, is_new = await self.identity_matcher.find_or_create_identity(
                embedding=face['embedding'],
                quality_score=face['quality'],
                source_chat_id=0,  # Profile photos don't have chat context
                source_message_id=task.user_id,  # Use user_id as identifier
                frame_index=0
            )
            
            if is_new:
                self.stats['new_identities'] += 1
                
                # Optionally upload profile photo to the new topic
                if self.media_uploader and topic_id:
                    task.content.seek(0)
                    await self.media_uploader.upload_to_topic(
                        db_topic_id=topic_id,
                        media_buffer=task.content,
                        source_message_id=task.user_id,
                        source_chat_id=0,
                        caption=f"👤 Profile photo (User ID: {task.user_id})"
                    )
        
        logger.debug(f"Profile photo processed: {len(faces)} faces found")
    
    async def _mark_file_processed(
        self,
        file_unique_id: str,
        media_type: str,
        chat_id: int,
        message_id: int,
        faces_found: int,
        topics_matched: list
    ):
        """
        Records that a media file has been processed.
        Used for deduplication of forwarded/duplicate content.
        """
        try:
            async with get_db_connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("""
                        INSERT INTO processed_media 
                            (file_unique_id, media_type, first_seen_chat_id, 
                             first_seen_message_id, faces_found, topics_matched)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (file_unique_id) DO NOTHING
                    """, (
                        file_unique_id,
                        media_type,
                        chat_id,
                        message_id,
                        faces_found,
                        topics_matched if topics_matched else None
                    ))
        except Exception as e:
            logger.warning(f"Failed to mark file as processed: {e}")
    
    def _validate_media(self, content: io.BytesIO, media_type: str) -> bool:
        """
        Validates that media content is not empty or corrupted.
        
        Args:
            content: BytesIO buffer with media data
            media_type: 'photo', 'video', or 'video_note'
            
        Returns:
            True if valid, False if corrupted/empty
        """
        try:
            content.seek(0)
            data = content.read(1024)  # Read first 1KB for header check
            content.seek(0)  # Reset for later use
            
            if not data or len(data) < 10:
                logger.warning(f"Empty or too small {media_type} content ({len(data)} bytes)")
                return False
            
            # Check for common image magic bytes
            if media_type == 'photo':
                # JPEG: FF D8 FF
                # PNG: 89 50 4E 47
                # GIF: 47 49 46 38
                # WebP: 52 49 46 46 ... 57 45 42 50
                jpeg_magic = data[:3] == b'\xff\xd8\xff'
                png_magic = data[:4] == b'\x89PNG'
                gif_magic = data[:4] == b'GIF8'
                webp_magic = data[:4] == b'RIFF' and data[8:12] == b'WEBP'
                
                if not (jpeg_magic or png_magic or gif_magic or webp_magic):
                    logger.warning(f"Invalid image format (magic bytes: {data[:4].hex()})")
                    return False
                    
            elif media_type in ('video', 'video_note'):
                # MP4/MOV: 00 00 00 XX 66 74 79 70 (ftyp at offset 4)
                # WebM/MKV: 1A 45 DF A3
                # AVI: 52 49 46 46 ... 41 56 49
                mp4_ftyp = b'ftyp' in data[:32]
                webm_magic = data[:4] == b'\x1aE\xdf\xa3'
                avi_magic = data[:4] == b'RIFF' and data[8:11] == b'AVI'
                
                if not (mp4_ftyp or webm_magic or avi_magic):
                    logger.warning(f"Invalid video format (magic bytes: {data[:8].hex()})")
                    return False
            
            return True
            
        except Exception as e:
            logger.error(f"Media validation error: {e}")
            return False
    
    def get_stats(self) -> Dict[str, int]:
        """Returns processing statistics."""
        return self.stats.copy()
    
    def get_queue_size(self) -> int:
        """Returns current queue size."""
        try:
            return self.redis_client.llen(self.queue_key)
        except Exception:
            return 0
