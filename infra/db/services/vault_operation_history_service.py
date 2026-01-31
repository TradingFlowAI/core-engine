"""Vault Operation History Service"""

import logging
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional

from sqlalchemy import and_, desc
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from infra.db.models.vault_operation_history import (
    OperationType,
    VaultOperationHistory,
)
from infra.exceptions import (
    DuplicateResourceException,
    ResourceNotFoundException,
)

logger = logging.getLogger(__name__)


class VaultOperationHistoryService:
    """Vault operation history service"""

    @staticmethod
    def _normalize_address(address: str) -> Optional[str]:
        """Convert address to lowercase format"""
        if address is None:
            return None
        return address.lower() if isinstance(address, str) else str(address).lower()

    @staticmethod
    def create_operation_record(
        db: Session,
        vault_contract_id: int,
        vault_address: str,
        operation_type: OperationType,
        network: str,
        network_type: str = "evm",
        chain_id: Optional[int] = None,
        transaction_hash: Optional[str] = None,
        input_token_address: Optional[str] = None,
        input_token_amount: Optional[Decimal] = None,
        input_token_usd_value: Optional[Decimal] = None,
        output_token_address: Optional[str] = None,
        output_token_amount: Optional[Decimal] = None,
        output_token_usd_value: Optional[Decimal] = None,
        gas_used: Optional[int] = None,
        gas_price: Optional[Decimal] = None,
        total_gas_cost_usd: Optional[Decimal] = None,
    ) -> VaultOperationHistory:
        """
        Create Vault operation record.

        Args:
            db: Database session
            vault_contract_id: Vault contract ID
            vault_address: Vault contract address
            operation_type: Operation type
            network: Network identifier (e.g., 'ethereum', 'aptos', 'sui')
            network_type: Network type (e.g., 'evm', 'aptos', 'sui')
            chain_id: Chain ID (required for EVM chains, None for non-EVM)
            transaction_hash: Transaction hash
            input_token_address: Input token address
            input_token_amount: Input token amount
            input_token_usd_value: Input token USD value
            output_token_address: Output token address
            output_token_amount: Output token amount
            output_token_usd_value: Output token USD value
            gas_used: Gas used or equivalent compute units
            gas_price: Gas price or equivalent fee
            total_gas_cost_usd: Total gas cost in USD

        Returns:
            Created operation record
        """
        # Normalize addresses
        vault_address = VaultOperationHistoryService._normalize_address(vault_address)
        input_token_address = VaultOperationHistoryService._normalize_address(
            input_token_address
        )
        output_token_address = VaultOperationHistoryService._normalize_address(
            output_token_address
        )
        transaction_hash = VaultOperationHistoryService._normalize_address(
            transaction_hash
        )

        operation_record = VaultOperationHistory(
            vault_contract_id=vault_contract_id,
            vault_address=vault_address,
            operation_type=operation_type,
            network=network,
            network_type=network_type,
            chain_id=chain_id,
            transaction_hash=transaction_hash,
            input_token_address=input_token_address,
            input_token_amount=input_token_amount,
            input_token_usd_value=input_token_usd_value,
            output_token_address=output_token_address,
            output_token_amount=output_token_amount,
            output_token_usd_value=output_token_usd_value,
            gas_used=gas_used,
            gas_price=gas_price,
            total_gas_cost_usd=total_gas_cost_usd,
        )

        try:
            db.add(operation_record)
            db.commit()
            db.refresh(operation_record)
            logger.info(
                "Successfully created operation record - Network: %s, Vault: %s, Operation: %s",
                network,
                vault_address,
                operation_type.value,
            )
            return operation_record
        except IntegrityError as e:
            db.rollback()
            logger.error("Failed to create operation record: %s", e)
            if "uq_vault_operation_txhash" in str(e):
                raise DuplicateResourceException(
                    f"Operation record already exists for transaction hash {transaction_hash}"
                )
            raise

    @staticmethod
    def get_record_by_id(db: Session, record_id: int) -> VaultOperationHistory:
        """Get operation record by ID"""
        record = (
            db.query(VaultOperationHistory)
            .filter(VaultOperationHistory.id == record_id)
            .first()
        )

        if record is None:
            raise ResourceNotFoundException(f"Operation record with ID {record_id} not found")

        return record

    @staticmethod
    def get_vault_operations(
        db: Session,
        vault_contract_id: int,
        operation_types: Optional[List[OperationType]] = None,
        network: Optional[str] = None,
        network_type: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        skip: int = 0,
        limit: int = 100,
    ) -> List[VaultOperationHistory]:
        """Get Vault operation records"""
        query = db.query(VaultOperationHistory).filter(
            VaultOperationHistory.vault_contract_id == vault_contract_id
        )

        if operation_types:
            query = query.filter(
                VaultOperationHistory.operation_type.in_(operation_types)
            )

        if network:
            query = query.filter(VaultOperationHistory.network == network)

        if network_type:
            query = query.filter(VaultOperationHistory.network_type == network_type)

        if start_date:
            query = query.filter(VaultOperationHistory.created_at >= start_date)

        if end_date:
            query = query.filter(VaultOperationHistory.created_at <= end_date)

        return (
            query.order_by(desc(VaultOperationHistory.created_at))
            .offset(skip)
            .limit(limit)
            .all()
        )

    @staticmethod
    def get_vault_operations_count(
        db: Session,
        vault_contract_id: int,
        operation_types: Optional[List[OperationType]] = None,
        network: Optional[str] = None,
        network_type: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> int:
        """
        Get Vault operation record count for pagination.

        Args:
            db: Database session
            vault_contract_id: Vault contract ID
            operation_types: Operation type filter
            network: Network filter
            network_type: Network type filter
            start_date: Start date
            end_date: End date

        Returns:
            Count of matching records
        """
        query = db.query(VaultOperationHistory).filter(
            VaultOperationHistory.vault_contract_id == vault_contract_id
        )

        if operation_types:
            query = query.filter(
                VaultOperationHistory.operation_type.in_(operation_types)
            )

        if network:
            query = query.filter(VaultOperationHistory.network == network)

        if network_type:
            query = query.filter(VaultOperationHistory.network_type == network_type)

        if start_date:
            query = query.filter(VaultOperationHistory.created_at >= start_date)

        if end_date:
            query = query.filter(VaultOperationHistory.created_at <= end_date)

        return query.count()

    @staticmethod
    def get_operations_by_transaction_hash(
        db: Session, chain_id: int, transaction_hash: str
    ) -> List[VaultOperationHistory]:
        """Get operation records by transaction hash"""
        normalized_hash = VaultOperationHistoryService._normalize_address(
            transaction_hash
        )

        return (
            db.query(VaultOperationHistory)
            .filter(
                and_(
                    VaultOperationHistory.chain_id == chain_id,
                    VaultOperationHistory.transaction_hash == normalized_hash,
                )
            )
            .all()
        )

    @staticmethod
    def get_operations_by_investor(
        db: Session,
        investor_address: str,
        network: str,
        operation_types: Optional[List[OperationType]] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        skip: int = 0,
        limit: int = 100,
    ) -> List[VaultOperationHistory]:
        """
        Get operation records by investor address

        Args:
            db: Database session
            investor_address: Investor address
            network: Network name (e.g., 'aptos', 'flow-evm', 'bsc')
            operation_types: Operation type filter
            start_date: Start date
            end_date: End date
            skip: Skip records
            limit: Limit records

        Returns:
            List of operation records
        """
        from infra.db.services.vault_contract_service import VaultContractService

        # 网络名称到 chain_id 的映射
        NETWORK_CHAIN_ID_MAP = {
            'aptos': 1,  # Aptos mainnet
            'flow-evm': 747,  # Flow EVM mainnet
            'flow-evm-testnet': 545,  # Flow EVM testnet
            'bsc': 56,  # BSC mainnet
            'bsc-testnet': 97,  # BSC testnet
            'eth': 1,  # Ethereum mainnet
            'ethereum': 1,
            'polygon': 137,
            'arbitrum': 42161,
            'optimism': 10,
            'avalanche': 43114,
        }

        # 获取 chain_id
        chain_id = NETWORK_CHAIN_ID_MAP.get(network.lower())
        if chain_id is None:
            # 尝试从网络名称中提取 chain_id（如 "evm-56"）
            if network.startswith('evm-'):
                try:
                    chain_id = int(network.split('-')[1])
                except (ValueError, IndexError):
                    raise ValueError(f"Unsupported network: {network}")
            else:
                raise ValueError(f"Unsupported network: {network}")

        # 首先获取该投资者的所有 vault
        vaults = VaultContractService.get_vaults_by_investor(
            db, investor_address, chain_id
        )

        if not vaults:
            return []

        # 获取所有 vault 的 ID
        vault_ids = [vault.id for vault in vaults]

        # 构建查询
        query = db.query(VaultOperationHistory).filter(
            VaultOperationHistory.vault_contract_id.in_(vault_ids)
        ).filter(
            VaultOperationHistory.network == network
        )

        if operation_types:
            query = query.filter(
                VaultOperationHistory.operation_type.in_(operation_types)
            )

        if start_date:
            query = query.filter(VaultOperationHistory.created_at >= start_date)

        if end_date:
            query = query.filter(VaultOperationHistory.created_at <= end_date)

        return (
            query.order_by(desc(VaultOperationHistory.created_at))
            .offset(skip)
            .limit(limit)
            .all()
        )

    @staticmethod
    def delete_record(db: Session, record_id: int) -> bool:
        """Delete operation record"""
        record = VaultOperationHistoryService.get_record_by_id(db, record_id)
        db.delete(record)
        db.commit()
        logger.info("Successfully deleted operation record ID: %s", record_id)
        return True
