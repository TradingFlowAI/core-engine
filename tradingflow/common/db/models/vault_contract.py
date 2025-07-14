from sqlalchemy import Column, DateTime, Integer, Numeric, String, UniqueConstraint
from sqlalchemy.sql import func

from tradingflow.depot.db.base import Base


class VaultContract(Base):
    __tablename__ = "vault_contracts"

    # 主键和合约信息
    id = Column(Integer, primary_key=True, index=True)
    contract_address = Column(String, index=True, nullable=False)

    # 网络信息 - 支持多链
    network = Column(
        String,
        index=True,
        nullable=False,
        comment="网络标识符，如 'ethereum'、'aptos'、'sui'、'flow-evm'等",
    )
    chain_id = Column(Integer, index=True, nullable=False)

    transaction_hash = Column(String, index=True)  # 部署交易哈希

    # Vault 特定的信息
    vault_name = Column(String)
    vault_symbol = Column(String)
    asset_address = Column(String, index=True)  # 底层资产地址

    deployer_address = Column(String, index=True)
    investor_address = Column(String, index=True)

    # 时间信息
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    last_updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    # 可能的额外信息
    deployment_block = Column(Integer)
    deployment_cost_eth = Column(Numeric(precision=36, scale=18))

    # 确保每个网络和链上的合约地址唯一
    __table_args__ = (
        UniqueConstraint(
            "network", "chain_id", "contract_address",
            name="uix_vault_contract_network_chain_address"
        ),
    )

    def __repr__(self):
        return f"<VaultContract {self.vault_symbol} at {self.contract_address} ({self.network})>"
