"""Token Price History Model"""

from sqlalchemy import Column, DateTime, Float, Index, Integer, String
from sqlalchemy.sql import func

from infra.db.base import Base


class TokenPriceHistory(Base):
    """Token price history records"""

    __tablename__ = "token_price_history"

    id = Column(Integer, primary_key=True, index=True)

    network = Column(
        String(50),
        nullable=False,
        index=True,
        comment="Network identifier, e.g., 'ethereum', 'aptos', 'sui'",
    )

    chain_id = Column(
        Integer, nullable=True, index=True, comment="Chain ID for EVM networks, null for non-EVM"
    )

    token_address = Column(String, nullable=False, index=True, comment="Token contract address")

    # Price data
    price_usd = Column(Float, nullable=False, comment="USD price")

    # Metadata
    source = Column(
        String, nullable=False, default="geckoterminal", comment="Price data source"
    )

    # Timestamps
    timestamp = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        comment="Price record time",
    )

    created_at = Column(
        DateTime(timezone=True), server_default=func.now(), comment="Record creation time"
    )

    # Create composite indexes for query performance
    __table_args__ = (
        # Index for network, address and time queries
        Index("idx_network_address_timestamp", "network", "token_address", "timestamp"),
        # Index for chain and time queries (EVM networks)
        Index("idx_chain_timestamp", "chain_id", "timestamp"),
        # Index for time queries (for cleaning old data)
        Index("idx_timestamp", "timestamp"),
    )

    def __repr__(self):
        return f"<TokenPriceHistory {self.network}:{self.token_address} ${self.price_usd} at {self.timestamp}>"
