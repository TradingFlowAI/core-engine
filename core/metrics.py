"""
Prometheus Metrics for Weather Station

Responsibilities:
- Define and manage Prometheus metrics
- Provide key metrics for log publishing, node execution, etc.
- Support Prometheus scrape endpoint
"""

import logging
from typing import Optional

logger = logging.getLogger(__name__)

# Try to import prometheus_client, disable metrics if not available
try:
    from prometheus_client import Counter, Histogram, Gauge, Info
    PROMETHEUS_AVAILABLE = True
except ImportError:
    logger.warning(
        "prometheus_client not installed. Metrics will be disabled. "
        "Install with: pip install prometheus-client"
    )
    PROMETHEUS_AVAILABLE = False
    
    # Define empty placeholder classes to avoid errors
    class Counter:
        def __init__(self, *args, **kwargs):
            pass
        def inc(self, *args, **kwargs):
            pass
        def labels(self, *args, **kwargs):
            return self
    
    class Histogram:
        def __init__(self, *args, **kwargs):
            pass
        def observe(self, *args, **kwargs):
            pass
        def time(self):
            return _DummyContextManager()
        def labels(self, *args, **kwargs):
            return self
    
    class Gauge:
        def __init__(self, *args, **kwargs):
            pass
        def set(self, *args, **kwargs):
            pass
        def inc(self, *args, **kwargs):
            pass
        def dec(self, *args, **kwargs):
            pass
        def labels(self, *args, **kwargs):
            return self
    
    class Info:
        def __init__(self, *args, **kwargs):
            pass
        def info(self, *args, **kwargs):
            pass
    
    class _DummyContextManager:
        def __enter__(self):
            return self
        def __exit__(self, *args):
            pass


# ==================== Redis Log Publisher Metrics ====================

# Log publish success counter
log_publish_success_total = Counter(
    'redis_log_publish_success_total',
    'Total number of successful log publishes to Redis',
    ['flow_id', 'cycle']
)

# Log publish failure counter
log_publish_failure_total = Counter(
    'redis_log_publish_failure_total',
    'Total number of failed log publishes to Redis',
    ['flow_id', 'cycle', 'error_type']
)

# Log publish latency (seconds)
log_publish_duration_seconds = Histogram(
    'redis_log_publish_duration_seconds',
    'Time spent publishing logs to Redis in seconds',
    ['flow_id', 'cycle'],
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0]
)

# Redis connection status
redis_log_publisher_connected = Gauge(
    'redis_log_publisher_connected',
    'Whether the Redis log publisher is connected (1=connected, 0=disconnected)'
)

# Log publish retry counter
log_publish_retry_total = Counter(
    'redis_log_publish_retry_total',
    'Total number of log publish retries',
    ['flow_id', 'cycle', 'attempt']
)


# ==================== Node Execution Metrics ====================

# Node execution counter
node_execution_total = Counter(
    'node_execution_total',
    'Total number of node executions',
    ['node_type', 'status']
)

# Node execution duration (seconds)
node_execution_duration_seconds = Histogram(
    'node_execution_duration_seconds',
    'Time spent executing nodes in seconds',
    ['node_type'],
    buckets=[0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0]
)

# Current number of executing nodes
nodes_executing_current = Gauge(
    'nodes_executing_current',
    'Current number of nodes being executed',
    ['flow_id']
)


# ==================== Flow Execution Metrics ====================

# Flow execution counter
flow_execution_total = Counter(
    'flow_execution_total',
    'Total number of flow executions',
    ['flow_id', 'status']
)

# Flow cycle counter
flow_cycle_total = Counter(
    'flow_cycle_total',
    'Total number of flow cycles executed',
    ['flow_id']
)

# Flow current cycle
flow_current_cycle = Gauge(
    'flow_current_cycle',
    'Current cycle number for each flow',
    ['flow_id']
)


# ==================== Helper Functions ====================

def is_metrics_enabled() -> bool:
    """Check if Prometheus metrics is enabled."""
    return PROMETHEUS_AVAILABLE


def record_log_publish_success(flow_id: str, cycle: int, duration: float):
    """Record successful log publish."""
    if not PROMETHEUS_AVAILABLE:
        return
    
    try:
        log_publish_success_total.labels(flow_id=flow_id, cycle=str(cycle)).inc()
        log_publish_duration_seconds.labels(flow_id=flow_id, cycle=str(cycle)).observe(duration)
    except Exception as e:
        logger.debug("Failed to record log publish success metric: %s", str(e))


def record_log_publish_failure(flow_id: str, cycle: int, error_type: str):
    """Record failed log publish."""
    if not PROMETHEUS_AVAILABLE:
        return
    
    try:
        log_publish_failure_total.labels(
            flow_id=flow_id,
            cycle=str(cycle),
            error_type=error_type
        ).inc()
    except Exception as e:
        logger.debug("Failed to record log publish failure metric: %s", str(e))


def record_log_publish_retry(flow_id: str, cycle: int, attempt: int):
    """Record log publish retry."""
    if not PROMETHEUS_AVAILABLE:
        return
    
    try:
        log_publish_retry_total.labels(
            flow_id=flow_id,
            cycle=str(cycle),
            attempt=str(attempt)
        ).inc()
    except Exception as e:
        logger.debug("Failed to record log publish retry metric: %s", str(e))


def set_redis_connection_status(connected: bool):
    """Set Redis connection status."""
    if not PROMETHEUS_AVAILABLE:
        return
    
    try:
        redis_log_publisher_connected.set(1 if connected else 0)
    except Exception as e:
        logger.debug("Failed to set Redis connection status metric: %s", str(e))


def record_node_execution(node_type: str, status: str, duration: Optional[float] = None):
    """Record node execution."""
    if not PROMETHEUS_AVAILABLE:
        return
    
    try:
        node_execution_total.labels(node_type=node_type, status=status).inc()
        if duration is not None:
            node_execution_duration_seconds.labels(node_type=node_type).observe(duration)
    except Exception as e:
        logger.debug("Failed to record node execution metric: %s", str(e))


def set_nodes_executing(flow_id: str, count: int):
    """Set current number of executing nodes."""
    if not PROMETHEUS_AVAILABLE:
        return
    
    try:
        nodes_executing_current.labels(flow_id=flow_id).set(count)
    except Exception as e:
        logger.debug("Failed to set nodes executing metric: %s", str(e))


def record_flow_execution(flow_id: str, status: str):
    """Record flow execution."""
    if not PROMETHEUS_AVAILABLE:
        return
    
    try:
        flow_execution_total.labels(flow_id=flow_id, status=status).inc()
    except Exception as e:
        logger.debug("Failed to record flow execution metric: %s", str(e))


def record_flow_cycle(flow_id: str, cycle: int):
    """Record flow cycle."""
    if not PROMETHEUS_AVAILABLE:
        return
    
    try:
        flow_cycle_total.labels(flow_id=flow_id).inc()
        flow_current_cycle.labels(flow_id=flow_id).set(cycle)
    except Exception as e:
        logger.debug("Failed to record flow cycle metric: %s", str(e))


# ==================== Prometheus Server ====================

def start_metrics_server(port: int = 9090):
    """
    Start Prometheus metrics HTTP server.
    
    Args:
        port: HTTP server port (default 9090)
    """
    if not PROMETHEUS_AVAILABLE:
        logger.warning("Prometheus client not available, metrics server will not start")
        return
    
    try:
        from prometheus_client import start_http_server
        start_http_server(port)
        logger.info("Prometheus metrics server started on port %s", port)
    except Exception as e:
        logger.error("Failed to start Prometheus metrics server: %s", str(e))
