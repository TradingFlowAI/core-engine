import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy import and_, desc, or_
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from tradingflow.common.exceptions import DuplicateResourceException, ResourceNotFoundException
from tradingflow.common.db.models.event import ContractEvent, EventProcessorState

logger = logging.getLogger(__name__)


class ContractEventService:
    """
    Service class for ContractEvent model operations

    Provides CRUD operations and specialized queries for contract events
    """

    @staticmethod
    def _normalize_address(address: str) -> str:
        """Normalize address to lowercase format"""
        if address is None:
            return None
        return address.lower() if isinstance(address, str) else str(address).lower()

    @staticmethod
    def _normalize_addresses_in_data(data: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize all address fields in the data dictionary"""
        address_fields = ["contract_address", "user_address"]

        normalized_data = data.copy()
        for field in address_fields:
            if field in normalized_data and normalized_data[field]:
                normalized_data[field] = ContractEventService._normalize_address(
                    normalized_data[field]
                )

        return normalized_data

    @staticmethod
    def create_event(db: Session, event_data: Dict[str, Any]) -> ContractEvent:
        """
        Create a new contract event record

        Args:
            db: Database session
            event_data: Event data dictionary

        Returns:
            Newly created ContractEvent object

        Raises:
            DuplicateResourceException: If the same event (tx hash + log index) already exists
        """
        normalized_data = ContractEventService._normalize_addresses_in_data(event_data)

        # Handle parameters field correctly based on database type
        if "parameters" in normalized_data and isinstance(
            normalized_data["parameters"], dict
        ):
            parameters = normalized_data.pop("parameters")
            event = ContractEvent(**normalized_data)
            event.set_parameters(parameters)
        else:
            event = ContractEvent(**normalized_data)

        try:
            db.add(event)
            db.commit()
            db.refresh(event)
            return event
        except IntegrityError:
            db.rollback()
            tx_hash = normalized_data.get("transaction_hash")
            log_index = normalized_data.get("log_index")
            raise DuplicateResourceException(
                f"Event with transaction hash {tx_hash} and log index {log_index} already exists"
            )

    @staticmethod
    def get_event_by_id(db: Session, event_id: str) -> ContractEvent:  # 改为str类型
        """
        Get contract event by ID

        Args:
            db: Database session
            event_id: Event ID (string format)

        Returns:
            ContractEvent object

        Raises:
            ResourceNotFoundException: If the event with the specified ID is not found
        """
        event = db.query(ContractEvent).filter(ContractEvent.id == event_id).first()
        if event is None:
            raise ResourceNotFoundException(f"Event with ID {event_id} not found")
        return event

    @staticmethod
    def get_event_by_tx_and_log_index(
        db: Session, tx_hash: str, log_index: int
    ) -> ContractEvent:
        """
        Get contract event by transaction hash and log index

        Args:
            db: Database session
            tx_hash: Transaction hash
            log_index: Log index

        Returns:
            ContractEvent object

        Raises:
            ResourceNotFoundException: If the event is not found
        """
        event = (
            db.query(ContractEvent)
            .filter(
                and_(
                    ContractEvent.transaction_hash == tx_hash,
                    ContractEvent.log_index == log_index,
                )
            )
            .first()
        )

        if event is None:
            raise ResourceNotFoundException(
                f"Event with transaction hash {tx_hash} and log index {log_index} not found"
            )
        return event

    @staticmethod
    def get_events_by_contract_address(
        db: Session,
        contract_address: str,
        chain_id: int,
        skip: int = 0,
        limit: int = 100,
    ) -> List[ContractEvent]:
        """
        Get events by contract address and chain ID

        Args:
            db: Database session
            contract_address: Contract address
            chain_id: Chain ID
            skip: Number of records to skip
            limit: Maximum number of records to return

        Returns:
            List of ContractEvent objects
        """
        normalized_address = ContractEventService._normalize_address(contract_address)
        return (
            db.query(ContractEvent)
            .filter(
                and_(
                    ContractEvent.contract_address == normalized_address,
                    ContractEvent.chain_id == chain_id,
                )
            )
            .order_by(desc(ContractEvent.block_number))
            .offset(skip)
            .limit(limit)
            .all()
        )

    @staticmethod
    def get_events_by_user_address(
        db: Session,
        user_address: str,
        chain_id: Optional[int] = None,
        skip: int = 0,
        limit: int = 100,
    ) -> List[ContractEvent]:
        """
        Get events by user address

        Args:
            db: Database session
            user_address: User wallet address
            chain_id: Optional chain ID filter
            skip: Number of records to skip
            limit: Maximum number of records to return

        Returns:
            List of ContractEvent objects
        """
        normalized_address = ContractEventService._normalize_address(user_address)
        query = db.query(ContractEvent).filter(
            ContractEvent.user_address == normalized_address
        )

        if chain_id is not None:
            query = query.filter(ContractEvent.chain_id == chain_id)

        return (
            query.order_by(desc(ContractEvent.block_number))
            .offset(skip)
            .limit(limit)
            .all()
        )

    @staticmethod
    def get_events_by_event_name(
        db: Session,
        event_name: str,
        contract_address: Optional[str] = None,
        chain_id: Optional[int] = None,
        skip: int = 0,
        limit: int = 100,
    ) -> List[ContractEvent]:
        """
        Get events by event name with optional contract and chain filters

        Args:
            db: Database session
            event_name: Event name
            contract_address: Optional contract address filter
            chain_id: Optional chain ID filter
            skip: Number of records to skip
            limit: Maximum number of records to return

        Returns:
            List of ContractEvent objects
        """
        query = db.query(ContractEvent).filter(ContractEvent.event_name == event_name)

        if contract_address:
            normalized_address = ContractEventService._normalize_address(
                contract_address
            )
            query = query.filter(ContractEvent.contract_address == normalized_address)

        if chain_id is not None:
            query = query.filter(ContractEvent.chain_id == chain_id)

        return (
            query.order_by(desc(ContractEvent.block_number))
            .offset(skip)
            .limit(limit)
            .all()
        )

    @staticmethod
    def get_events_by_block_range(
        db: Session,
        start_block: int,
        end_block: int,
        chain_id: int,
        contract_address: Optional[str] = None,
        skip: int = 0,
        limit: int = 100,
    ) -> List[ContractEvent]:
        """
        Get events within a block range

        Args:
            db: Database session
            start_block: Starting block number
            end_block: Ending block number
            chain_id: Chain ID
            contract_address: Optional contract address filter
            skip: Number of records to skip
            limit: Maximum number of records to return

        Returns:
            List of ContractEvent objects
        """
        query = db.query(ContractEvent).filter(
            and_(
                ContractEvent.block_number >= start_block,
                ContractEvent.block_number <= end_block,
                ContractEvent.chain_id == chain_id,
            )
        )

        if contract_address:
            normalized_address = ContractEventService._normalize_address(
                contract_address
            )
            query = query.filter(ContractEvent.contract_address == normalized_address)

        return (
            query.order_by(desc(ContractEvent.block_number))
            .offset(skip)
            .limit(limit)
            .all()
        )

    @staticmethod
    def get_events_by_time_range(
        db: Session,
        start_time: datetime,
        end_time: datetime,
        chain_id: Optional[int] = None,
        contract_address: Optional[str] = None,
        skip: int = 0,
        limit: int = 100,
    ) -> List[ContractEvent]:
        """
        Get events within a time range

        Args:
            db: Database session
            start_time: Starting datetime
            end_time: Ending datetime
            chain_id: Optional chain ID filter
            contract_address: Optional contract address filter
            skip: Number of records to skip
            limit: Maximum number of records to return

        Returns:
            List of ContractEvent objects
        """
        query = db.query(ContractEvent).filter(
            and_(
                ContractEvent.block_timestamp >= start_time,
                ContractEvent.block_timestamp <= end_time,
            )
        )

        if chain_id is not None:
            query = query.filter(ContractEvent.chain_id == chain_id)

        if contract_address:
            normalized_address = ContractEventService._normalize_address(
                contract_address
            )
            query = query.filter(ContractEvent.contract_address == normalized_address)

        return (
            query.order_by(desc(ContractEvent.block_number))
            .offset(skip)
            .limit(limit)
            .all()
        )

    @staticmethod
    def list_events(
        db: Session, chain_id: Optional[int] = None, skip: int = 0, limit: int = 100
    ) -> List[ContractEvent]:
        """
        List events with optional chain filter

        Args:
            db: Database session
            chain_id: Optional chain ID filter
            skip: Number of records to skip
            limit: Maximum number of records to return

        Returns:
            List of ContractEvent objects
        """
        query = db.query(ContractEvent)

        if chain_id is not None:
            query = query.filter(ContractEvent.chain_id == chain_id)

        return (
            query.order_by(desc(ContractEvent.block_number))
            .offset(skip)
            .limit(limit)
            .all()
        )

    @staticmethod
    def get_latest_block_by_chain(db: Session, chain_id: int) -> Optional[int]:
        """
        Get the latest processed block number for a specific chain

        Args:
            db: Database session
            chain_id: Chain ID

        Returns:
            Latest block number or None if no events for the chain
        """
        result = (
            db.query(ContractEvent.block_number)
            .filter(ContractEvent.chain_id == chain_id)
            .order_by(desc(ContractEvent.block_number))
            .first()
        )
        return result[0] if result else None

    @staticmethod
    def search_events(
        db: Session,
        search_term: Optional[str] = None,
        event_name: Optional[str] = None,
        contract_address: Optional[str] = None,
        user_address: Optional[str] = None,
        chain_id: Optional[int] = None,
        start_block: Optional[int] = None,
        end_block: Optional[int] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        skip: int = 0,
        limit: int = 100,
    ) -> List[ContractEvent]:
        """
        Advanced search for events with multiple optional filters

        Args:
            db: Database session
            search_term: Optional search term for transaction hash
            event_name: Optional event name filter
            contract_address: Optional contract address filter
            user_address: Optional user address filter
            chain_id: Optional chain ID filter
            start_block: Optional starting block number
            end_block: Optional ending block number
            start_time: Optional starting datetime
            end_time: Optional ending datetime
            skip: Number of records to skip
            limit: Maximum number of records to return

        Returns:
            List of ContractEvent objects matching the filters
        """
        query = db.query(ContractEvent)

        if search_term:
            search_pattern = f"%{search_term}%"
            query = query.filter(
                or_(
                    ContractEvent.transaction_hash.ilike(search_pattern),
                    ContractEvent.event_name.ilike(search_pattern),
                )
            )

        if event_name:
            query = query.filter(ContractEvent.event_name == event_name)

        if contract_address:
            normalized_address = ContractEventService._normalize_address(
                contract_address
            )
            query = query.filter(ContractEvent.contract_address == normalized_address)

        if user_address:
            normalized_user = ContractEventService._normalize_address(user_address)
            query = query.filter(ContractEvent.user_address == normalized_user)

        if chain_id is not None:
            query = query.filter(ContractEvent.chain_id == chain_id)

        if start_block is not None:
            query = query.filter(ContractEvent.block_number >= start_block)

        if end_block is not None:
            query = query.filter(ContractEvent.block_number <= end_block)

        if start_time is not None:
            query = query.filter(ContractEvent.block_timestamp >= start_time)

        if end_time is not None:
            query = query.filter(ContractEvent.block_timestamp <= end_time)

        return (
            query.order_by(desc(ContractEvent.block_number))
            .offset(skip)
            .limit(limit)
            .all()
        )

    @staticmethod
    def delete_event(db: Session, event_id: str) -> bool:  # 改为str类型
        """
        Delete a contract event

        Args:
            db: Database session
            event_id: Event ID to delete (string format)

        Returns:
            True if deleted successfully

        Raises:
            ResourceNotFoundException: If the event is not found
        """
        event = ContractEventService.get_event_by_id(db, event_id)
        db.delete(event)
        db.commit()
        return True

    @staticmethod
    def get_unprocessed_events(
        db: Session, processor_id: str = "vault_operation_processor", limit: int = 100
    ) -> List[ContractEvent]:
        """
        获取尚未处理的事件

        基于processed字段获取所有未处理的事件，
        而不是依赖ID比较（因为ID是字符串格式）

        Args:
            db: 数据库会话
            processor_id: 处理器ID，用于识别不同的处理器
            limit: 一次最多返回的记录数

        Returns:
            未处理的事件列表
        """

        # 直接查询processed=False的事件，按创建时间排序
        events = (
            db.query(ContractEvent)
            .filter(ContractEvent.processed == False)
            .order_by(ContractEvent.created_at)
            .limit(limit)
            .all()
        )

        return events

    @staticmethod
    def update_event_processed_status(
        db: Session,
        event_id: str,  # 改为str类型，因为ID现在是字符串
        is_success: bool,
        message: str = None,
        processor_id: str = "vault_operation_processor",
    ) -> bool:
        """
        更新事件处理状态

        直接更新事件的processed字段，而不是依赖处理器状态跟踪

        Args:
            db: 数据库会话
            event_id: 事件ID（字符串格式）
            is_success: 处理是否成功
            message: 处理消息
            processor_id: 处理器ID

        Returns:
            更新是否成功
        """

        try:
            # 获取事件
            event = db.query(ContractEvent).filter(ContractEvent.id == event_id).first()

            if not event:
                logger.warning("未找到ID为 %s 的事件", event_id)
                return False

            # 只有处理成功时才标记为已处理
            if is_success:
                event.processed = True

            # 可以选择性地记录处理状态到处理器状态表（用于统计）
            processor_state = (
                db.query(EventProcessorState)
                .filter(EventProcessorState.processor_id == processor_id)
                .first()
            )

            if not processor_state:
                processor_state = EventProcessorState(
                    processor_id=processor_id,
                    last_processed_event_id=0,  # 保留为兼容性，但不再使用
                    processed_count=0,
                )
                db.add(processor_state)

            # 更新统计信息
            if is_success:
                processor_state.processed_count += 1
                processor_state.last_status = message or "Success"
            else:
                processor_state.last_status = message or "Failed"

            db.commit()
            return True
        except Exception as e:
            db.rollback()
            logger.error("更新事件处理状态时出错: %s", e, exc_info=True)
            return False
