from sqlalchemy import Boolean, Column, DateTime, Integer, String, UniqueConstraint
from sqlalchemy.sql import func

from tradingflow.depot.db.base import Base


class MonitoredContract(Base):
    __tablename__ = "monitored_contracts"
    id = Column(String, primary_key=True, index=True)  # 使用String ID匹配companion服务
    contract_address = Column(String, index=True, nullable=False)

    # 网络信息 - 支持多链
    network = Column(
        String,
        index=True,
        nullable=False,
        comment="网络标识符，如 'ethereum'、'aptos'、'sui'、'flow-evm'等",
    )
    chain_id = Column(Integer, index=True, nullable=False)

    contract_type = Column(String, index=True)
    abi_name = Column(String, nullable=False)  # e.g., "Vault", "PriceOracle"

    # 添加缺失的is_active字段
    is_active = Column(Boolean, nullable=False, default=True)

    # transaction_hash = Column(String, index=True)  # 部署合约的交易哈希
    added_at = Column(DateTime(timezone=True), server_default=func.now())

    # 确保每个网络和链上地址只有一条记录
    __table_args__ = (
        UniqueConstraint("network", "chain_id", "contract_address", name="uix_contract_network_chain_address"),
    )

    def __repr__(self):
        return f"<MonitoredContract {self.contract_type} at {self.contract_address} ({self.network})>"
