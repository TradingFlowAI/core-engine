"""Token Price History Service"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

from sqlalchemy import and_, desc
from sqlalchemy.orm import Session

from infra.constants import EVM_CHAIN_ID_NETWORK_MAP
from infra.db.models.token_price_history import TokenPriceHistory

logger = logging.getLogger(__name__)


class TokenPriceHistoryService:
    """Token price history service"""

    @staticmethod
    def batch_insert_records(
        db: Session,
        price_data: Dict,
        timestamp: datetime,
    ) -> int:
        """
        Batch save price records.

        Args:
            db: Database session
            price_data: Price data, format:
                {
                    "evm": {network_id: {token_address: price_info}},
                    "non_evm": {network_name: {token_address: price_info}}
                }
            timestamp: Price timestamp

        Returns:
            Number of saved records
        """
        try:
            price_records = []

            # Process EVM networks
            for network_id, token_prices in price_data.get("evm", {}).items():
                if isinstance(token_prices, dict) and "error" not in token_prices:
                    try:
                        chain_id = int(network_id)

                        # Verify this is a known EVM chain ID
                        if chain_id not in EVM_CHAIN_ID_NETWORK_MAP:
                            logger.warning(f"Unknown EVM chain ID: {chain_id}, skipping")
                            continue

                        for token_address, price_info in token_prices.items():
                            token_address_lower = token_address.lower()

                            price_record = TokenPriceHistory(
                                network=EVM_CHAIN_ID_NETWORK_MAP.get(chain_id),
                                chain_id=chain_id,
                                token_address=token_address_lower,
                                price_usd=float(price_info.get("price_usd", 0)),
                                source=price_info.get("source", "unknown"),
                                timestamp=timestamp,
                            )
                            price_records.append(price_record)
                    except ValueError:
                        logger.warning(f"Invalid chain ID format: {network_id}, skipping")
                        continue

            # Process non-EVM networks
            for network_name, token_prices in price_data.get("non_evm", {}).items():
                if isinstance(token_prices, dict) and "error" not in token_prices:

                    for token_address, price_info in token_prices.items():
                        token_address_lower = token_address.lower()

                        price_record = TokenPriceHistory(
                            network=network_name,
                            chain_id=None,  # Non-EVM networks don't use chain_id
                            token_address=token_address_lower,
                            price_usd=float(price_info.get("price_usd", 0)),
                            source=price_info.get("source", "unknown"),
                            timestamp=timestamp,
                        )
                        price_records.append(price_record)

            # Batch insert
            if price_records:
                db.bulk_save_objects(price_records)
                db.commit()
                logger.info(f"Successfully saved {len(price_records)} price records")
                return len(price_records)
            else:
                logger.warning("No price records to save")
                return 0

        except Exception as e:
            logger.error(f"Failed to batch save price records: {e}")
            db.rollback()
            raise

    @staticmethod
    def get_latest_price(
        db: Session, network: str, token_address: str, chain_id: Optional[int] = None
    ) -> Optional[TokenPriceHistory]:
        """Get latest price record for specified token"""
        query = db.query(TokenPriceHistory).filter(
            and_(
                TokenPriceHistory.network == network,
                TokenPriceHistory.token_address == token_address.lower(),
            )
        )

        if chain_id is not None:
            query = query.filter(TokenPriceHistory.chain_id == chain_id)

        return query.order_by(desc(TokenPriceHistory.timestamp)).first()

    @staticmethod
    def get_price_history(
        db: Session,
        network: str,
        token_address: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        chain_id: Optional[int] = None,
        limit: int = 100,
    ) -> List[TokenPriceHistory]:
        """Get price history for specified token"""
        query = db.query(TokenPriceHistory).filter(
            and_(
                TokenPriceHistory.network == network,
                TokenPriceHistory.token_address == token_address.lower(),
            )
        )

        if chain_id is not None:
            query = query.filter(TokenPriceHistory.chain_id == chain_id)

        if start_time:
            query = query.filter(TokenPriceHistory.timestamp >= start_time)

        if end_time:
            query = query.filter(TokenPriceHistory.timestamp <= end_time)

        return query.order_by(desc(TokenPriceHistory.timestamp)).limit(limit).all()

    @staticmethod
    def get_price_closest_to_timestamp(
        db: Session,
        network: str,
        token_address: str,
        target_timestamp: datetime,
        chain_id: Optional[int] = None,
        max_time_diff_minutes: int = 60,
    ) -> Optional[TokenPriceHistory]:
        """
        Get token price record closest to specified timestamp.

        Args:
            db: Database session
            network: Network identifier
            token_address: Token address
            target_timestamp: Target timestamp
            chain_id: Chain ID (optional, for EVM networks)
            max_time_diff_minutes: Maximum time difference in minutes

        Returns:
            Price record closest to target timestamp, None if not found or time diff too large
        """
        try:
            # Ensure target_timestamp has timezone info
            if target_timestamp.tzinfo is None:
                target_timestamp = target_timestamp.replace(tzinfo=timezone.utc)

            # Build base query
            query = db.query(TokenPriceHistory).filter(
                and_(
                    TokenPriceHistory.network == network,
                    TokenPriceHistory.token_address == token_address.lower(),
                )
            )

            if chain_id is not None:
                query = query.filter(TokenPriceHistory.chain_id == chain_id)

            # Calculate time range
            time_range = timedelta(minutes=max_time_diff_minutes)
            start_time = target_timestamp - time_range
            end_time = target_timestamp + time_range

            # Find records within time range
            query = query.filter(
                and_(
                    TokenPriceHistory.timestamp >= start_time,
                    TokenPriceHistory.timestamp <= end_time,
                )
            )

            records = query.all()

            if not records:
                logger.debug(
                    f"No price record found for network {network} token {token_address} "
                    f"within {max_time_diff_minutes} minutes of {target_timestamp}"
                )
                return None

            # Find record with smallest time difference
            def get_time_diff(record):
                record_timestamp = record.timestamp
                if record_timestamp.tzinfo is None:
                    record_timestamp = record_timestamp.replace(tzinfo=timezone.utc)
                return abs((record_timestamp - target_timestamp).total_seconds())

            closest_record = min(records, key=get_time_diff)

            return closest_record

        except Exception as e:
            logger.error(f"Failed to query price record closest to timestamp: {e}")
            raise

    @staticmethod
    def get_price_at_timestamp(
        db: Session,
        network: str,
        token_address: str,
        target_timestamp: datetime,
        chain_id: Optional[int] = None,
        tolerance_minutes: int = 5,
    ) -> Optional[float]:
        """
        Get token price at specified time (simplified version, returns only price value).

        Args:
            db: Database session
            network: Network identifier
            token_address: Token address
            target_timestamp: Target timestamp
            chain_id: Chain ID (optional)
            tolerance_minutes: Tolerance time difference in minutes

        Returns:
            Price value, None if not found
        """
        record = TokenPriceHistoryService.get_price_closest_to_timestamp(
            db=db,
            network=network,
            token_address=token_address,
            target_timestamp=target_timestamp,
            chain_id=chain_id,
            max_time_diff_minutes=tolerance_minutes,
        )

        return record.price_usd if record else None
