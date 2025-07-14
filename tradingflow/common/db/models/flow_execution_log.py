"""Flow execution log model"""

from datetime import datetime
from typing import Dict, Optional

from sqlalchemy import Column, DateTime, Integer, String, Text, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class FlowExecutionLog(Base):
    """Flow execution log model"""

    __tablename__ = "flow_execution_logs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    flow_id = Column(String(255), nullable=False, index=True)
    cycle = Column(Integer, nullable=False, index=True)
    node_id = Column(String(255), nullable=True, index=True)  # NULL for system/user logs
    log_level = Column(String(20), nullable=False, default="INFO", index=True)
    log_source = Column(String(50), nullable=False, default="node", index=True)
    message = Column(Text, nullable=False)
    log_metadata = Column(JSONB, nullable=True)
    created_at = Column(
        DateTime(timezone=True), nullable=False, default=func.now(), index=True
    )

    def __repr__(self):
        return (
            f"<FlowExecutionLog(id={self.id}, flow_id='{self.flow_id}', "
            f"cycle={self.cycle}, node_id='{self.node_id}', "
            f"log_level='{self.log_level}', log_source='{self.log_source}')>"
        )

    def to_dict(self) -> Dict:
        """Convert to dictionary"""
        return {
            "id": self.id,
            "flow_id": self.flow_id,
            "cycle": self.cycle,
            "node_id": self.node_id,
            "log_level": self.log_level,
            "log_source": self.log_source,
            "message": self.message,
            "metadata": self.log_metadata,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }

    @classmethod
    def from_dict(cls, data: Dict) -> "FlowExecutionLog":
        """Create instance from dictionary"""
        return cls(
            flow_id=data["flow_id"],
            cycle=data["cycle"],
            node_id=data.get("node_id"),
            log_level=data.get("log_level", "INFO"),
            log_source=data.get("log_source", "node"),
            message=data["message"],
            metadata=data.get("metadata"),
        )
