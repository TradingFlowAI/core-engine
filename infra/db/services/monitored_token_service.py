"""Monitored Token Service"""

import logging
from typing import Any, Dict, List, Optional, Union

from sqlalchemy import and_, or_
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from infra.exceptions import (
    DuplicateResourceException,
    ResourceNotFoundException,
)
from infra.utils.address_util import normalize_token_address
from infra.db.models.monitored_token import MonitoredToken

logger = logging.getLogger(__name__)


class MonitoredTokenService:
    """Service for monitored token CRUD operations, supporting EVM and non-EVM networks"""

    @staticmethod
    def create_token(db: Session, token_data: Dict[str, Any]) -> MonitoredToken:
        """
        Create new monitored token record.

        Args:
            db: Database session
            token_data: Token data dict, must contain network and token_address

        Returns:
            Newly created monitored token object

        Raises:
            DuplicateResourceException: If token with same address exists on network
            ValueError: If required parameters missing
        """
        if "network" not in token_data or "token_address" not in token_data:
            raise ValueError("Must provide network and token_address")

        # Normalize address
        token_data["token_address"] = normalize_token_address(
            token_data["token_address"],
            token_data.get("network_type", "evm"),
        )

        token = MonitoredToken(**token_data)
        try:
            db.add(token)
            db.commit()
            db.refresh(token)
            return token
        except IntegrityError:
            db.rollback()
            raise DuplicateResourceException(
                f"Token with address {token_data.get('token_address')} "
                f"already exists on network {token_data.get('network')}"
            )

    @staticmethod
    def get_token_by_id(db: Session, token_id: int) -> MonitoredToken:
        """
        Get monitored token by ID.

        Args:
            db: Database session
            token_id: Token ID

        Returns:
            Monitored token object

        Raises:
            ResourceNotFoundException: If token with ID not found
        """
        token = db.query(MonitoredToken).filter(MonitoredToken.id == token_id).first()
        if token is None:
            raise ResourceNotFoundException(f"Monitored token with ID {token_id} not found")
        return token

    @staticmethod
    def get_token_by_address(
        db: Session, token_address: str, network: str
    ) -> MonitoredToken:
        """
        Get monitored token by address and network.

        Args:
            db: Database session
            token_address: Token contract address
            network: Network identifier, e.g., 'ethereum', 'sui', 'aptos'

        Returns:
            Monitored token object

        Raises:
            ResourceNotFoundException: If token not found
        """
        # Correctly determine network type from network name
        if network in ["aptos"]:
            network_type = "aptos"
        elif network in ["sui", "sui-network"]:
            network_type = "sui"
        elif network in ["solana"]:
            network_type = "solana"
        else:
            network_type = "evm"

        token_address = normalize_token_address(token_address, network_type)

        token = (
            db.query(MonitoredToken)
            .filter(
                and_(
                    MonitoredToken.token_address == token_address,
                    MonitoredToken.network == network,
                )
            )
            .first()
        )

        if token is None:
            raise ResourceNotFoundException(
                f"Monitored token with address {token_address} on network {network} not found"
            )
        return token

    @staticmethod
    def list_tokens(
        db: Session,
        network: Optional[str] = None,
        network_type: Optional[str] = None,
        chain_id: Optional[int] = None,
        skip: int = 0,
        limit: int = 100,
    ) -> List[MonitoredToken]:
        """
        List monitored tokens.

        Args:
            db: Database session
            network: Optional network identifier filter
            network_type: Optional network type filter
            chain_id: Optional chain ID filter (EVM networks only)
            skip: Records to skip
            limit: Maximum records to return

        Returns:
            List of monitored token objects
        """
        query = db.query(MonitoredToken)

        if network is not None:
            query = query.filter(MonitoredToken.network == network)

        if network_type is not None:
            query = query.filter(MonitoredToken.network_type == network_type)

        if chain_id is not None:
            query = query.filter(MonitoredToken.chain_id == chain_id)

        return query.offset(skip).limit(limit).all()

    @staticmethod
    def get_all_active_tokens(
        db: Session,
        network: Optional[str] = None,
        network_type: Optional[str] = None,
        chain_id: Optional[int] = None,
    ) -> List[MonitoredToken]:
        """
        Get all active monitored tokens.

        Args:
            db: Database session
            network: Optional network identifier filter
            network_type: Optional network type filter
            chain_id: Optional chain ID filter (EVM networks only)

        Returns:
            List of active monitored token objects
        """
        query = db.query(MonitoredToken).filter(
            MonitoredToken.is_active == True  # noqa: E712
        )

        if network is not None:
            query = query.filter(MonitoredToken.network == network)

        if network_type is not None:
            query = query.filter(MonitoredToken.network_type == network_type)

        if chain_id is not None:
            query = query.filter(MonitoredToken.chain_id == chain_id)

        return query.all()
