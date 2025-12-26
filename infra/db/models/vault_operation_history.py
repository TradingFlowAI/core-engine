"""Vault Operation History Model"""

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

from infra.db.base import Base


class OperationType(PyEnum):
    """Operation type enum"""

    DEPOSIT = "deposit"      # User stake
    WITHDRAW = "withdraw"    # User redeem
    SWAP = "swap"           # Oracle calls contract for trading (generic)
    SWAP_BUY = "swap_buy"   # Oracle calls contract for buy
    SWAP_SELL = "swap_sell" # Oracle calls contract for sell


class VaultOperationHistory(Base):
    """Vault operation history table, records all operations affecting Vault value"""

    __tablename__ = "vault_operation_history"

    id = Column(BigInteger, primary_key=True, autoincrement=True)

    # Reference to vault_contract table
    vault_contract_id = Column(
        Integer,
        nullable=False,
        comment="Associated Vault contract ID",
    )

    # Network info - multi-chain support
    network = Column(
        String(50),
        index=True,
        nullable=False,
        comment="Network identifier, e.g., 'ethereum', 'sui', 'aptos'",
    )

    chain_id = Column(
        Integer, index=True, nullable=True, comment="Chain ID for EVM networks, null for non-EVM"
    )

    network_type = Column(
        String(50),
        nullable=False,
        default="evm",
        comment="Network type: evm, sui, aptos, solana, etc.",
    )

    # Vault address - adjusted length for different chains
    vault_address = Column(
        String(255),  # Increased length to support non-EVM addresses
        nullable=False,
        comment="Vault contract address (EVM: 42 chars, Aptos/Sui: longer)",
    )

    # Transaction info - supports different chain hash formats
    transaction_hash = Column(
        String(255),  # Increased length to support different chain hash formats
        nullable=True,
        comment="Transaction hash (format varies by chain)",
    )

    # Aptos specific: transaction version (for deduplication and ordering)
    transaction_version = Column(
        BigInteger,
        nullable=True,
        comment="Transaction version for Aptos (used for deduplication and ordering)",
    )

    # Operation info
    operation_type = Column(Enum(OperationType), nullable=False, comment="Operation type")

    # Asset info - token addresses also need to support different chains
    input_token_address = Column(
        String(255), nullable=True, comment="Input token address (supports different chain formats)"
    )
    input_token_amount = Column(
        Numeric(precision=36, scale=18), nullable=True, comment="Input token amount"
    )
    input_token_usd_value = Column(
        Numeric(precision=36, scale=18), nullable=True, comment="Input token USD value"
    )

    output_token_address = Column(
        String(255), nullable=True, comment="Output token address (supports different chain formats)"
    )
    output_token_amount = Column(
        Numeric(precision=36, scale=18), nullable=True, comment="Output token amount"
    )
    output_token_usd_value = Column(
        Numeric(precision=36, scale=18), nullable=True, comment="Output token USD value"
    )

    # Cost calculation - Gas concept differs across chains
    gas_used = Column(
        BigInteger, nullable=True, comment="Gas used (EVM) or equivalent compute units"
    )

    gas_price = Column(
        Numeric(precision=36, scale=18),
        nullable=True,
        comment="Gas price (EVM: Wei, others: corresponding units)",
    )
    total_gas_cost_usd = Column(
        Numeric(precision=36, scale=18), nullable=True, comment="Total transaction cost (USD)"
    )

    # On-chain event timestamp
    block_timestamp = Column(
        DateTime(timezone=True),
        nullable=True,
        comment="Block timestamp from on-chain event",
    )

    # Record time
    created_at = Column(
        DateTime(timezone=True), nullable=False, default=func.now(), comment="Creation time"
    )

    updated_at = Column(
        DateTime(timezone=True),
        onupdate=func.now(),
        default=func.now(),
        comment="Update time",
    )

    # Indexes - updated for multi-chain queries
    __table_args__ = (
        # Index by Vault ID
        Index("idx_vault_operation_vault_id", "vault_contract_id"),
        # Index by network and Vault address
        Index("idx_vault_operation_network_address", "network", "vault_address"),
        # Index by operation type and Vault
        Index("idx_vault_operation_type_vault", "operation_type", "vault_contract_id"),
        # Index by Vault and time (for time range queries)
        Index("idx_vault_operation_vault_time", "vault_contract_id", "created_at"),
        # Index by network type
        Index("idx_vault_operation_network_type", "network_type"),
        # Index by transaction version (Aptos)
        Index("idx_vault_operation_tx_version", "transaction_version"),
        # Index by block timestamp
        Index("idx_vault_operation_block_timestamp", "block_timestamp"),
        {"comment": "Vault operation history table"},
    )

    def __repr__(self):
        return (
            f"<VaultOperationHistory(id={self.id}, "
            f"network='{self.network}', "
            f"vault_address='{self.vault_address}', "
            f"operation='{self.operation_type}', "
            f"transaction_hash={self.transaction_hash})>"
        )
