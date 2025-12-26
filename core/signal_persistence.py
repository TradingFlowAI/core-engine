"""
Signal Persistence
Async persistence of Signals to database
"""

import logging
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

# Service instance cache
_signal_service = None


def _get_signal_service():
    """Get or create Signal service instance."""
    global _signal_service
    if _signal_service is None:
        try:
            from infra.db.services.flow_execution_signal_service import (
                FlowExecutionSignalService,
            )
            _signal_service = FlowExecutionSignalService()
        except Exception as e:
            logger.warning(f"Failed to initialize signal service: {e}")
            return None
    return _signal_service


async def persist_signal(
    flow_id: str,
    cycle: int,
    direction: str,
    from_node_id: Optional[str] = None,
    to_node_id: Optional[str] = None,
    source_handle: Optional[str] = None,
    target_handle: Optional[str] = None,
    signal_type: str = 'ANY',
    data_type: str = 'unknown',
    payload: Optional[Dict[str, Any]] = None,
) -> bool:
    """
    Persist Signal to database.
    
    Args:
        flow_id: Flow ID
        cycle: Execution cycle
        direction: Signal direction ('input' or 'output')
        from_node_id: Source node ID
        to_node_id: Target node ID
        source_handle: Source Handle
        target_handle: Target Handle
        signal_type: Signal type
        data_type: Data type
        payload: Signal data
        
    Returns:
        Whether save was successful
    """
    try:
        service = _get_signal_service()
        if service is None:
            logger.debug("Signal service not available, skipping persistence")
            return False
        
        # Serialize payload
        serialized_payload = _serialize_payload(payload)
        
        result = await service.save_signal(
            flow_id=flow_id,
            cycle=cycle,
            direction=direction,
            from_node_id=from_node_id,
            to_node_id=to_node_id,
            source_handle=source_handle,
            target_handle=target_handle,
            signal_type=signal_type,
            data_type=data_type,
            payload=serialized_payload,
        )
        
        if result:
            logger.debug(
                f"Signal persisted: {from_node_id}:{source_handle} -> {to_node_id}:{target_handle}"
            )
            return True
        return False
        
    except Exception as e:
        logger.warning(f"Failed to persist signal: {e}")
        return False


def _serialize_payload(payload: Any) -> Any:
    """Serialize payload, handle types that cannot be directly JSON serialized."""
    if payload is None:
        return None
    
    if isinstance(payload, (str, int, float, bool)):
        return payload
    
    if isinstance(payload, (list, tuple)):
        return [_serialize_payload(item) for item in payload]
    
    if isinstance(payload, dict):
        return {k: _serialize_payload(v) for k, v in payload.items()}
    
    # For complex objects, convert to string representation
    try:
        return str(payload)
    except Exception:
        return "<unserializable>"
