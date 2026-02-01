"""Vault Contract Service"""

import logging
from typing import Any, Dict, List, Optional

from sqlalchemy import and_, or_
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from infra.db.models.vault_contract import VaultContract
from infra.exceptions import DuplicateResourceException, ResourceNotFoundException

logger = logging.getLogger(__name__)


class VaultContractService:
    """Vault contract service - basic CRUD operations"""

    @staticmethod
    def _normalize_address(address: str) -> Optional[str]:
        """Convert address to lowercase format"""
        if address is None:
            return None
        return address.lower() if isinstance(address, str) else str(address).lower()

    @staticmethod
    def _normalize_addresses_in_data(data: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize all address fields in data dictionary"""
        address_fields = [
            "contract_address",
            "deployer_address",
            "investor_address",
            "asset_address",
        ]

        normalized_data = data.copy()
        for field in address_fields:
            if field in normalized_data and normalized_data[field]:
                normalized_data[field] = VaultContractService._normalize_address(
                    normalized_data[field]
                )

        return normalized_data

    @staticmethod
    def create_vault(db: Session, vault_data: Dict[str, Any]) -> VaultContract:
        """
        Create new vault contract record

        Args:
            db: Database session
            vault_data: Vault contract data dictionary

        Returns:
            Created vault contract object

        Raises:
            DuplicateResourceException: If contract already exists on same chain
        """
        normalized_data = VaultContractService._normalize_addresses_in_data(vault_data)
        vault = VaultContract(**normalized_data)

        try:
            db.add(vault)
            db.commit()
            db.refresh(vault)
            return vault
        except IntegrityError:
            db.rollback()
            raise DuplicateResourceException(
                f"Contract already exists at {normalized_data.get('contract_address')}"
            )

    @staticmethod
    def get_vault_by_id(db: Session, vault_id: int) -> VaultContract:
        """Get vault contract by ID"""
        vault = db.query(VaultContract).filter(VaultContract.id == vault_id).first()
        if vault is None:
            raise ResourceNotFoundException(f"Vault not found with ID {vault_id}")
        return vault

    @staticmethod
    def get_vault_by_address(
        db: Session, contract_address: str, chain_id: int
    ) -> Optional[VaultContract]:
        """
        Get vault contract by address and chain ID

        Args:
            db: Database session
            contract_address: Contract address
            chain_id: Chain ID

        Returns:
            Vault contract object or None
        """
        normalized_address = VaultContractService._normalize_address(contract_address)
        return (
            db.query(VaultContract)
            .filter(
                and_(
                    VaultContract.contract_address == normalized_address,
                    VaultContract.chain_id == chain_id,
                )
            )
            .first()
        )

    @staticmethod
    def get_vaults_by_investor(
        db: Session,
        investor_address: str,
        chain_id: int,
        skip: int = 0,
        limit: int = 100,
    ) -> List[VaultContract]:
        """
        Get all vault contracts for an investor

        Args:
            db: Database session
            investor_address: Investor wallet address
            chain_id: Chain ID
            skip: Skip records
            limit: Limit records

        Returns:
            List of vault contracts
        """
        normalized_address = VaultContractService._normalize_address(investor_address)
        return (
            db.query(VaultContract)
            .filter(VaultContract.chain_id == chain_id)
            .filter(VaultContract.investor_address == normalized_address)
            .offset(skip)
            .limit(limit)
            .all()
        )

    @staticmethod
    def get_vaults_by_network(
        db: Session, network: str, skip: int = 0, limit: int = 100
    ) -> List[VaultContract]:
        """
        Get all vault contracts for a network

        Args:
            db: Database session
            network: Network name
            skip: Skip records
            limit: Limit records

        Returns:
            List of vault contracts
        """
        return (
            db.query(VaultContract)
            .filter(VaultContract.network == network)
            .offset(skip)
            .limit(limit)
            .all()
        )

    @staticmethod
    def list_vaults(
        db: Session, skip: int = 0, limit: int = 100
    ) -> List[VaultContract]:
        """List all vault contracts"""
        return db.query(VaultContract).offset(skip).limit(limit).all()

    @staticmethod
    def update_vault(
        db: Session, vault_id: int, update_data: Dict[str, Any]
    ) -> VaultContract:
        """Update vault contract info"""
        vault = VaultContractService.get_vault_by_id(db, vault_id)

        normalized_data = VaultContractService._normalize_addresses_in_data(update_data)

        for key, value in normalized_data.items():
            if hasattr(vault, key):
                setattr(vault, key, value)

        try:
            db.commit()
            db.refresh(vault)
            return vault
        except IntegrityError:
            db.rollback()
            raise DuplicateResourceException("Update causes duplicate contract")

    @staticmethod
    def delete_vault(db: Session, vault_id: int) -> bool:
        """Delete vault contract"""
        vault = VaultContractService.get_vault_by_id(db, vault_id)
        db.delete(vault)
        db.commit()
        return True

    @staticmethod
    def search_vaults(
        db: Session,
        search_term: Optional[str] = None,
        chain_id: Optional[int] = None,
        network: Optional[str] = None,
        skip: int = 0,
        limit: int = 100,
    ) -> List[VaultContract]:
        """Search vault contracts"""
        query = db.query(VaultContract)

        if search_term:
            search_pattern = f"%{search_term}%"
            query = query.filter(
                or_(
                    VaultContract.vault_name.ilike(search_pattern),
                    VaultContract.vault_symbol.ilike(search_pattern),
                    VaultContract.contract_address.ilike(search_pattern),
                )
            )

        if chain_id is not None:
            query = query.filter(VaultContract.chain_id == chain_id)

        if network:
            query = query.filter(VaultContract.network == network)

        return query.offset(skip).limit(limit).all()
