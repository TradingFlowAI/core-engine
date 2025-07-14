from enum import Enum as PyEnum

from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    Enum,
    Index,
    Integer,
    Numeric,
    String,
)
from sqlalchemy.sql import func

from tradingflow.common.db.base import Base


class OperationType(PyEnum):
    """操作类型枚举"""

    DEPOSIT = "deposit"  # 用户质押
    WITHDRAW = "withdraw"  # 用户赎回
    SWAP = "swap"  # 预言机调用合约进行交易（通用）
    SWAP_BUY = "swap_buy"  # 预言机调用合约进行买入
    SWAP_SELL = "swap_sell"  # 预言机调用合约进行卖出


class VaultOperationHistory(Base):
    """Vault操作历史记录表，记录所有影响Vault价值的操作"""

    __tablename__ = "vault_operation_history"

    id = Column(BigInteger, primary_key=True, autoincrement=True)

    # 关联到vault_contract表
    vault_contract_id = Column(
        Integer,
        nullable=False,
        comment="关联的Vault合约ID",
    )

    # 网络信息 - 支持多链
    network = Column(
        String(50),
        index=True,
        nullable=False,
        comment="网络标识符，如 'ethereum'、'sui'、'aptos'",
    )

    chain_id = Column(
        Integer, index=True, nullable=True, comment="EVM网络的链ID，非EVM网络为空"
    )

    network_type = Column(
        String(50),
        nullable=False,
        default="evm",
        comment="网络类型: evm, sui, aptos, solana等",
    )

    # Vault地址 - 根据不同链调整长度
    vault_address = Column(
        String(255),  # 增加长度以支持非EVM地址
        nullable=False,
        comment="Vault合约地址（EVM: 42字符，Aptos/Sui: 更长）",
    )

    # 交易信息 - 支持不同链的交易哈希格式
    transaction_hash = Column(
        String(255),  # 增加长度以支持不同链的交易哈希
        nullable=True,
        comment="交易哈希（不同链格式不同）",
    )

    # 操作信息
    operation_type = Column(Enum(OperationType), nullable=False, comment="操作类型")

    # 资产信息 - 代币地址也需要支持不同链
    input_token_address = Column(
        String(255), nullable=True, comment="输入代币地址（支持不同链格式）"
    )
    input_token_amount = Column(
        Numeric(precision=36, scale=18), nullable=True, comment="输入代币数量"
    )
    input_token_usd_value = Column(
        Numeric(precision=36, scale=18), nullable=True, comment="输入代币USD价值"
    )

    output_token_address = Column(
        String(255), nullable=True, comment="输出代币地址（支持不同链格式）"
    )
    output_token_amount = Column(
        Numeric(precision=36, scale=18), nullable=True, comment="输出代币数量"
    )
    output_token_usd_value = Column(
        Numeric(precision=36, scale=18), nullable=True, comment="输出代币USD价值"
    )

    # 成本计算 - Gas概念在不同链上可能不同
    gas_used = Column(
        BigInteger, nullable=True, comment="使用的Gas数量（EVM链）或等效计算单位"
    )

    gas_price = Column(
        Numeric(precision=36, scale=18),
        nullable=True,
        comment="Gas价格（EVM: Wei, 其他链: 相应单位）",
    )
    total_gas_cost_usd = Column(
        Numeric(precision=36, scale=18), nullable=True, comment="总交易成本(USD)"
    )

    # 记录时间
    created_at = Column(
        DateTime(timezone=True), nullable=False, default=func.now(), comment="创建时间"
    )

    updated_at = Column(
        DateTime(timezone=True),
        onupdate=func.now(),
        default=func.now(),
        comment="更新时间",
    )

    # 索引 - 更新索引以支持多链查询
    __table_args__ = (
        # 按Vault ID索引
        Index("idx_vault_operation_vault_id", "vault_contract_id"),
        # 按网络和Vault地址索引
        Index("idx_vault_operation_network_address", "network", "vault_address"),
        # 按操作类型和Vault索引
        Index("idx_vault_operation_type_vault", "operation_type", "vault_contract_id"),
        # 按Vault和时间索引（用于时间范围查询）
        Index("idx_vault_operation_vault_time", "vault_contract_id", "created_at"),
        # 按网络类型索引
        Index("idx_vault_operation_network_type", "network_type"),
        {"comment": "Vault操作历史记录表"},
    )

    def __repr__(self):
        return (
            f"<VaultOperationHistory(id={self.id}, "
            f"network='{self.network}', "
            f"vault_address='{self.vault_address}', "
            f"operation='{self.operation_type}', "
            f"transaction_hash={self.transaction_hash})>"
        )
