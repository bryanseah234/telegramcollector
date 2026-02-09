"""
Observability Module - Handles Prometheus metrics and structured logging.

Provides:
- Prometheus metrics registry and HTTP server
- Structured JSON logger with trace ID support
- Context manager for tracing execution time
"""
import logging
import json
import time
import uuid
import contextvars
from typing import Dict, Any, Optional
from prometheus_client import Counter, Gauge, Histogram, start_http_server
from pythonjsonlogger import jsonlogger
from datetime import datetime

# Trace Context
_trace_id_ctx = contextvars.ContextVar('trace_id', default=None)

# Prometheus Metrics
QUEUE_DEPTH = Gauge('telegram_queue_depth', 'Current items in processing queue')
PROCESSING_LATENCY = Histogram('telegram_processing_seconds', 'Time spent processing tasks', buckets=[1, 5, 10, 30, 60, 120, 300])
ERROR_RATE = Counter('telegram_errors_total', 'Total errors by type', ['type'])
WORKER_STATUS = Gauge('telegram_worker_status', 'Worker status (1=up, 0=down)', ['worker_id'])
FACES_DETECTED = Counter('telegram_faces_detected_total', 'Total faces detected')
MEDIA_PROCESSED = Counter('telegram_media_processed_total', 'Total media items processed', ['type'])

def start_metrics_server(port: int = 8000):
    """Starts Prometheus metrics server."""
    try:
        start_http_server(port)
        logging.info(f"Prometheus metrics server started on port {port}")
    except Exception as e:
        logging.error(f"Failed to start metrics server: {e}")

class CustomJsonFormatter(jsonlogger.JsonFormatter):
    """Custom JSON formatter that includes trace ID and timestamp."""
    def add_fields(self, log_record, record, message_dict):
        super(CustomJsonFormatter, self).add_fields(log_record, record, message_dict)
        
        # Add timestamp
        if not log_record.get('timestamp'):
            now = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
            log_record['timestamp'] = now
            
        # Add trace ID if available
        trace_id = _trace_id_ctx.get()
        if trace_id:
            log_record['trace_id'] = trace_id
            
        # Add log level
        if log_record.get('level'):
            log_record['level'] = log_record['level'].upper()
        else:
            log_record['level'] = record.levelname

def setup_logging(log_level: str = "INFO"):
    """Configures structured JSON logging."""
    logger = logging.getLogger()
    logger.setLevel(log_level)
    
    handler = logging.StreamHandler()
    formatter = CustomJsonFormatter('%(timestamp)s %(level)s %(name)s %(message)s')
    handler.setFormatter(formatter)
    
    # Remove existing handlers to avoid duplicates
    logger.handlers = []
    logger.addHandler(handler)

def get_trace_id() -> str:
    """Gets current trace ID or generates new one."""
    tid = _trace_id_ctx.get()
    if not tid:
        tid = str(uuid.uuid4())
        _trace_id_ctx.set(tid)
    return tid

def set_trace_id(trace_id: str):
    """Sets the current trace ID context."""
    _trace_id_ctx.set(trace_id)

class TraceContext:
    """Context manager for tracing execution blocks."""
    def __init__(self, operation_name: str, **labels):
        self.operation_name = operation_name
        self.labels = labels
        self.start_time = None
        
    def __enter__(self):
        self.start_time = time.time()
        # Ensure trace ID exists
        get_trace_id()
        logging.info(f"Started {self.operation_name}", extra=self.labels)
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        duration = time.time() - self.start_time
        PROCESSING_LATENCY.observe(duration)
        
        if exc_type:
            ERROR_RATE.labels(type=exc_type.__name__).inc()
            logging.error(f"Failed {self.operation_name}: {exc_val}", extra={'duration': duration})
        else:
            logging.info(f"Completed {self.operation_name}", extra={'duration': duration, **self.labels})

# Metric updaters
def update_queue_gauge(size: int):
    QUEUE_DEPTH.set(size)

def record_error(error_type: str):
    ERROR_RATE.labels(type=error_type).inc()
