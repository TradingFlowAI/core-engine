"""Monitored Token Model"""

from sqlalchemy import Boolean, Column, DateTime, Integer, String, UniqueConstraint
from sqlalchemy.sql import func

from infra.db.base import Base


class MonitoredToken(Base):
    """Token to monitor for real-time price updates"""

    __tablename__ = "monitored_tokens"

    id = Column(Integer, primary_key=True, index=True)
    # Use string network identifier instead of integer chain_id
    network = Column(
        String(50),
        index=True,
        nullable=False,
        comment="Network identifier, e.g., 'ethereum', 'sui', 'aptos'",
    )

    chain_id = Column(
        Integer, index=True, nullable=True, comment="Chain ID for EVM networks, null for non-EVM"
    )

    token_address = Column(String, index=True, nullable=False)

    name = Column(String, nullable=True)  # Token name, e.g., "Ethereum"
    symbol = Column(String, nullable=True)  # Token symbol, e.g., "ETH"
    decimals = Column(Integer, default=18)  # Token decimals

    # Price source configuration
    primary_source = Column(String, default="geckoterminal")  # Primary price source

    # Network related info
    network_type = Column(
        String(50),
        nullable=False,
        default="evm",
        comment="Network type: evm, sui, aptos, solana, etc.",
    )

    # Monitoring configuration
    is_active = Column(Boolean, default=True)  # Whether actively monitored

    # Metadata
    logo_url = Column(String, nullable=True)  # Token logo URL
    description = Column(String, nullable=True)  # Token description

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    # Ensure only one record per token address per network
    __table_args__ = (
        UniqueConstraint("network", "token_address", name="uix_network_token"),
    )

    def __repr__(self):
        return f"<MonitoredToken {self.symbol} ({self.chain_id}:{self.token_address})>"
