from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    Index,
    Integer,
    Numeric,
    String,
    UniqueConstraint,
)
from sqlalchemy.sql import func

from tradingflow.common.db.base import Base


class VaultValueHistory(Base):
    """Vault价值历史记录表"""

    __tablename__ = "vault_value_history"

    id = Column(BigInteger, primary_key=True, autoincrement=True)

    # 关联到vault_contract表
    vault_contract_id = Column(
        Integer,
        nullable=False,
        comment="关联的Vault合约ID",
    )

    # 基本信息
    chain_id = Column(Integer, nullable=False, comment="区块链ID")
    vault_address = Column(String(255), nullable=False, comment="Vault合约地址")  # 修改为255以支持多链地址

    # 价值信息
    total_value_usd = Column(
        Numeric(precision=36, scale=18), nullable=False, comment="Vault总价值(USD)"
    )

    created_at = Column(
        DateTime(timezone=True), nullable=False, default=func.now(), comment="创建时间"
    )

    updated_at = Column(
        DateTime(timezone=True),
        onupdate=func.now(),
        default=func.now(),
        comment="更新时间",
    )

    # 索引
    __table_args__ = (
        # 确保同一时间点同一Vault只有一条记录
        UniqueConstraint(
            "vault_contract_id", "updated_at", name="uq_vault_value_history_vault_time"
        ),
        # 单独的vault_contract_id索引（用于按合约查询所有历史）
        Index("idx_vault_value_history_vault_id", "vault_contract_id"),
        # 按Vault和时间查询的复合索引（用于时间范围查询）
        Index("idx_vault_value_history_vault_time", "vault_contract_id", "updated_at"),
        # 按链ID和时间查询的索引
        Index("idx_vault_value_history_chain_time", "chain_id", "updated_at"),
        # 按地址和时间查询的索引
        Index("idx_vault_value_history_address_time", "vault_address", "updated_at"),
        {"comment": "Vault价值历史记录表"},
    )

    def __repr__(self):
        return (
            f"<VaultValueHistory(id={self.id}, "
            f"vault_address='{self.vault_address}', "
            f"total_value_usd={self.total_value_usd}, "
            f"updated_at='{self.updated_at}')>"
        )
