"""Vault Contract Model"""

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    DateTime,
    Index,
    Integer,
    Numeric,
    String,
)
from sqlalchemy.sql import func

from infra.db.base import Base


class VaultContract(Base):
    """Vault contract table, stores deployed vault information"""

    __tablename__ = "vault_contracts"

    id = Column(Integer, primary_key=True, autoincrement=True)

    # Contract address - supports different chain formats
    contract_address = Column(
        String(255),
        nullable=False,
        comment="Vault contract address",
    )

    # Network info
    network = Column(
        String(50),
        index=True,
        nullable=False,
        comment="Network identifier, e.g., 'ethereum', 'bsc', 'aptos'",
    )

    network_type = Column(
        String(50),
        nullable=False,
        default="evm",
        comment="Network type: evm, aptos, sui, solana, etc.",
    )

    chain_id = Column(
        Integer,
        index=True,
        nullable=True,
        comment="Chain ID for EVM networks, null for non-EVM",
    )

    # Transaction hash
    transaction_hash = Column(
        String(255),
        nullable=True,
        comment="Deployment transaction hash",
    )

    # Vault info
    vault_name = Column(String(255), nullable=True, comment="Vault name")
    vault_symbol = Column(String(50), nullable=True, comment="Vault token symbol")
    asset_address = Column(String(255), nullable=True, comment="Base asset address")

    # Addresses
    deployer_address = Column(
        String(255),
        nullable=True,
        comment="Deployer wallet address",
    )
    investor_address = Column(
        String(255),
        index=True,
        nullable=True,
        comment="Investor wallet address",
    )

    # Status
    is_active = Column(Boolean, default=True, comment="Whether vault is active")

    # Timestamps
    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=func.now(),
        comment="Creation time",
    )
    last_updated_at = Column(
        DateTime(timezone=True),
        onupdate=func.now(),
        default=func.now(),
        comment="Last update time",
    )

    # Deployment info
    deployment_block = Column(BigInteger, nullable=True, comment="Deployment block number")
    deployment_cost_eth = Column(
        Numeric(precision=36, scale=18),
        nullable=True,
        comment="Deployment cost in native token",
    )

    # Aptos specific
    last_scanned_version = Column(
        BigInteger,
        nullable=True,
        default=0,
        comment="Last scanned transaction version (Aptos)",
    )
    last_scanned_at = Column(
        DateTime(timezone=True),
        nullable=True,
        comment="Last scan timestamp",
    )

    # Indexes
    __table_args__ = (
        Index("idx_vault_contract_address_chain", "contract_address", "chain_id", unique=True),
        Index("idx_vault_contract_network", "network"),
        Index("idx_vault_contract_investor", "investor_address"),
        Index("idx_vault_contract_deployer", "deployer_address"),
        {"comment": "Vault contracts table"},
    )

    def __repr__(self):
        return (
            f"<VaultContract(id={self.id}, "
            f"address='{self.contract_address}', "
            f"network='{self.network}', "
            f"chain_id={self.chain_id})>"
        )
