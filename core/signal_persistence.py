"""
Signal Persistence
异步持久化 Signal 到数据库
"""

import logging
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

# 服务实例缓存
_signal_service = None


def _get_signal_service():
    """获取或创建 Signal 服务实例"""
    global _signal_service
    if _signal_service is None:
        try:
            from weather_depot.db.services.flow_execution_signal_service import (
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
    持久化 Signal 到数据库
    
    Args:
        flow_id: Flow ID
        cycle: 执行周期
        direction: 信号方向 ('input' 或 'output')
        from_node_id: 源节点 ID
        to_node_id: 目标节点 ID
        source_handle: 源 Handle
        target_handle: 目标 Handle
        signal_type: 信号类型
        data_type: 数据类型
        payload: 信号数据
        
    Returns:
        是否保存成功
    """
    try:
        service = _get_signal_service()
        if service is None:
            logger.debug("Signal service not available, skipping persistence")
            return False
        
        # 序列化 payload
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
    """序列化 payload，处理不可直接 JSON 序列化的类型"""
    if payload is None:
        return None
    
    if isinstance(payload, (str, int, float, bool)):
        return payload
    
    if isinstance(payload, (list, tuple)):
        return [_serialize_payload(item) for item in payload]
    
    if isinstance(payload, dict):
        return {k: _serialize_payload(v) for k, v in payload.items()}
    
    # 对于复杂对象，转换为字符串表示
    try:
        return str(payload)
    except Exception:
        return "<unserializable>"
