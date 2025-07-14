import json
import logging

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    DateTime,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import func

from tradingflow.common.config import CONFIG
from tradingflow.common.db.base import Base

logger = logging.getLogger(__name__)

DATABASE_URL = CONFIG.get("DATABASE_URL")
logger.debug(f"Using database URL: {DATABASE_URL}")


class ContractEvent(Base):
    __tablename__ = "contract_events"
    id = Column(String, primary_key=True, index=True)  # 修改为String类型，匹配数据库schema
    transaction_hash = Column(String, index=True, nullable=False)
    log_index = Column(Integer, nullable=False)
    block_number = Column(BigInteger, index=True, nullable=False)
    block_timestamp = Column(DateTime(timezone=True), index=True, nullable=False)
    network = Column(String, index=True, nullable=False)  # 添加缺失的network字段
    chain_id = Column(Integer, index=True, nullable=False)
    contract_address = Column(String, index=True, nullable=False)
    event_name = Column(String, index=True, nullable=False)
    parameters = Column(
        JSONB if "postgresql" in DATABASE_URL else Text, nullable=False
    )  # Use JSONB for PG, Text otherwise
    # Optional indexed fields (example)
    user_address = Column(String, index=True, nullable=True)
    processed = Column(Boolean, nullable=False, default=False)  # 添加缺失的processed字段
    created_at = Column(DateTime(timezone=True), nullable=False, default=func.now())  # 添加缺失的created_at字段

    # Ensure uniqueness for each event log
    __table_args__ = (
        UniqueConstraint("transaction_hash", "log_index", name="_tx_hash_log_index_uc"),
        Index("ix_contract_events_user_address", "user_address"),  # Example index
    )

    # Helper to serialize parameters if stored as Text
    def set_parameters(self, params_dict):
        if "postgresql" in DATABASE_URL:
            self.parameters = params_dict
        else:
            self.parameters = json.dumps(params_dict)

    def get_parameters(self):
        if "postgresql" in DATABASE_URL:
            return self.parameters
        else:
            try:
                return json.loads(self.parameters)
            except (json.JSONDecodeError, TypeError):
                logger.error(f"Failed to parse parameters: {self.parameters}")
                return {}  # Or handle error appropriately


class ListenerState(Base):
    __tablename__ = "listener_state"
    chain_id = Column(Integer, primary_key=True)
    last_processed_block = Column(BigInteger, nullable=False)
    updated_at = Column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )


class EventProcessorState(Base):
    """记录事件处理器的状态，包括最后处理的事件ID"""

    __tablename__ = "event_processor_state"
    processor_id = Column(
        String, primary_key=True
    )  # 处理器标识，可以用于区分不同类型的处理器
    last_processed_event_id = Column(Integer, nullable=False, default=0)
    updated_at = Column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    # 其他可能有用的信息
    processed_count = Column(Integer, nullable=False, default=0)  # 总处理数量
    last_status = Column(String, nullable=True)  # 最后一次处理的状态

    def __repr__(self):
        return (
            f"EventProcessorState(processor_id={self.processor_id}, "
            f"last_processed_event_id={self.last_processed_event_id}, "
            f"updated_at={self.updated_at})"
        )
