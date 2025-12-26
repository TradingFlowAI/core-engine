"""Flow Execution Signal Model

Stores node-to-node signal transmission records.
"""

from datetime import datetime
from typing import Optional, Dict, Any
from sqlalchemy import Column, Integer, String, Text, DateTime, JSON, Index
from sqlalchemy.dialects.postgresql import JSONB
from infra.db.base import Base


class FlowExecutionSignal(Base):
    """Node-to-node signal record table"""
    
    __tablename__ = 'flow_execution_signals'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    flow_id = Column(String(255), nullable=False, index=True)
    cycle = Column(Integer, nullable=False, index=True)
    
    # Signal direction and node info
    direction = Column(String(10), nullable=False)  # 'input' or 'output'
    from_node_id = Column(String(255), index=True)
    to_node_id = Column(String(255), index=True)
    
    # Handle info
    source_handle = Column(String(255))
    target_handle = Column(String(255))
    
    # Signal content
    signal_type = Column(String(50), nullable=False, default='ANY')
    data_type = Column(String(50), nullable=False, default='unknown')
    payload = Column(JSONB)
    
    # Timestamp
    created_at = Column(DateTime, default=datetime.utcnow, index=True)
    
    # Composite indexes
    __table_args__ = (
        Index('idx_flow_execution_signals_flow_cycle', 'flow_id', 'cycle'),
        Index('idx_flow_execution_signals_flow_cycle_from', 'flow_id', 'cycle', 'from_node_id'),
        Index('idx_flow_execution_signals_flow_cycle_to', 'flow_id', 'cycle', 'to_node_id'),
    )
    
    def __init__(
        self,
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
        created_at: Optional[datetime] = None,
    ):
        self.flow_id = flow_id
        self.cycle = cycle
        self.direction = direction
        self.from_node_id = from_node_id
        self.to_node_id = to_node_id
        self.source_handle = source_handle
        self.target_handle = target_handle
        self.signal_type = signal_type
        self.data_type = data_type
        self.payload = payload
        self.created_at = created_at or datetime.utcnow()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary format"""
        return {
            'id': self.id,
            'flow_id': self.flow_id,
            'cycle': self.cycle,
            'direction': self.direction,
            'from_node_id': self.from_node_id,
            'to_node_id': self.to_node_id,
            'source_handle': self.source_handle,
            'target_handle': self.target_handle,
            'signal_type': self.signal_type,
            'data_type': self.data_type,
            'payload': self.payload,
            'created_at': self.created_at.isoformat() if self.created_at else None,
        }
    
    def __repr__(self):
        return (
            f"<FlowExecutionSignal(id={self.id}, flow_id='{self.flow_id}', "
            f"cycle={self.cycle}, direction='{self.direction}', "
            f"from={self.from_node_id}, to={self.to_node_id})>"
        )
