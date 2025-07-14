from sqlalchemy import Boolean, Column, DateTime, Integer, String, UniqueConstraint
from sqlalchemy.sql import func

from tradingflow.common.db.base import Base


class MonitoredToken(Base):
    """需要监控实时价格的代币"""

    __tablename__ = "monitored_tokens"

    id = Column(Integer, primary_key=True, index=True)
    # 使用字符串型网络标识符替代整数型chain_id
    network = Column(
        String(50),
        index=True,
        nullable=False,
        comment="网络标识符，如 'ethereum'、'sui'、'aptos'",
    )

    chain_id = Column(
        Integer, index=True, nullable=True, comment="EVM网络的链ID，非EVM网络为空"
    )

    token_address = Column(String, index=True, nullable=False)

    name = Column(String, nullable=True)  # 代币名称，例如 "Ethereum"
    symbol = Column(String, nullable=True)  # 代币符号，例如 "ETH"
    decimals = Column(Integer, default=18)  # 代币精度

    # 价格来源配置
    primary_source = Column(String, default="geckoterminal")  # 主要价格来源

    # 网络相关信息
    network_type = Column(
        String(50),
        nullable=False,
        default="evm",
        comment="网络类型: evm, sui, aptos, solana等",
    )

    # 监控配置
    is_active = Column(Boolean, default=True)  # 是否主动监控

    # 元数据
    logo_url = Column(String, nullable=True)  # 代币Logo URL
    description = Column(String, nullable=True)  # 代币描述

    # 时间戳
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    # last_price_update = Column(DateTime(timezone=True), nullable=True)  # 最后一次价格更新时间

    # 最新价格（可选，取决于是否要在数据库中存储最新价格）
    # last_price_usd = Column(Float, nullable=True)

    # 确保每个网络上的代币地址只有一条记录
    __table_args__ = (
        UniqueConstraint("network", "token_address", name="uix_network_token"),
    )

    def __repr__(self):
        return f"<MonitoredToken {self.symbol} ({self.chain_id}:{self.token_address})>"
