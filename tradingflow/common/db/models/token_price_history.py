from sqlalchemy import Column, DateTime, Float, Index, Integer, String
from sqlalchemy.sql import func

from tradingflow.depot.db.base import Base


class TokenPriceHistory(Base):
    """代币价格历史记录表"""

    __tablename__ = "token_price_history"

    id = Column(Integer, primary_key=True, index=True)

    network = Column(
        String(50),
        nullable=False,
        index=True,
        comment="网络标识符，如 'ethereum'、'aptos'、'sui'",
    )

    chain_id = Column(
        Integer, nullable=True, index=True, comment="EVM网络的链ID，非EVM网络为空"
    )

    token_address = Column(String, nullable=False, index=True, comment="代币合约地址")

    # 价格数据
    price_usd = Column(Float, nullable=False, comment="USD价格")

    # 元数据
    source = Column(
        String, nullable=False, default="geckoterminal", comment="价格数据来源"
    )

    # 时间戳
    timestamp = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        comment="价格记录时间",
    )

    created_at = Column(
        DateTime(timezone=True), server_default=func.now(), comment="记录创建时间"
    )

    # 创建复合索引以优化查询性能
    __table_args__ = (
        # 按网络、地址和时间查询的索引
        Index("idx_network_address_timestamp", "network", "token_address", "timestamp"),
        # 按链和时间查询的索引（EVM网络）
        Index("idx_chain_timestamp", "chain_id", "timestamp"),
        # 按时间查询的索引（用于清理旧数据）
        Index("idx_timestamp", "timestamp"),
    )

    def __repr__(self):
        return f"<TokenPriceHistory {self.network}:{self.token_address} ${self.price_usd} at {self.timestamp}>"
