import logging
from typing import Any, Dict, List, Optional, Union

from sqlalchemy import and_, or_
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from tradingflow.common.exceptions import (
    DuplicateResourceException,
    ResourceNotFoundException,
)
from tradingflow.common.utils.address_util import normalize_token_address
from tradingflow.common.db.models.monitored_token import MonitoredToken

# 添加日志记录器
logger = logging.getLogger(__name__)


class MonitoredTokenService:
    """
    提供监控代币的基本增删改查服务，支持EVM和非EVM网络
    """

    @staticmethod
    def create_token(db: Session, token_data: Dict[str, Any]) -> MonitoredToken:
        """
        创建新的监控代币记录

        Args:
            db: 数据库会话
            token_data: 代币数据字典，必须包含network和token_address

        Returns:
            新创建的监控代币对象

        Raises:
            DuplicateResourceException: 如果相同网络上已存在相同地址的代币
            ValueError: 如果缺少必要参数
        """
        # 验证必要字段
        if "network" not in token_data or "token_address" not in token_data:
            raise ValueError("必须提供 network 和 token_address")

        # 规范化地址
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
                f"相同网络 {token_data.get('network')} 上已存在地址为 "
                f"{token_data.get('token_address')} 的监控代币"
            )

    @staticmethod
    def get_token_by_id(db: Session, token_id: int) -> MonitoredToken:
        """
        通过 ID 获取监控代币

        Args:
            db: 数据库会话
            token_id: 代币 ID

        Returns:
            监控代币对象

        Raises:
            ResourceNotFoundException: 如果未找到指定 ID 的代币
        """
        token = db.query(MonitoredToken).filter(MonitoredToken.id == token_id).first()
        if token is None:
            raise ResourceNotFoundException(f"未找到 ID 为 {token_id} 的监控代币")
        return token

    @staticmethod
    def get_token_by_address(
        db: Session, token_address: str, network: str
    ) -> MonitoredToken:
        """
        通过代币地址和网络获取监控代币

        Args:
            db: 数据库会话
            token_address: 代币合约地址
            network: 网络标识符，如 'ethereum', 'sui', 'aptos'

        Returns:
            监控代币对象

        Raises:
            ResourceNotFoundException: 如果未找到指定地址和网络的代币
        """
        # 根据网络名称正确判断网络类型
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
                f"未找到地址为 {token_address}，网络为 {network} 的监控代币"
            )
        return token

    @staticmethod
    def get_token_by_evm_address(
        db: Session, token_address: str, chain_id: int
    ) -> MonitoredToken:
        """
        通过代币地址和链 ID 获取监控代币（EVM 网络兼容性方法）

        Args:
            db: 数据库会话
            token_address: 代币合约地址
            chain_id: 链 ID

        Returns:
            监控代币对象

        Raises:
            ResourceNotFoundException: 如果未找到指定地址和链的代币
        """
        token_address = normalize_token_address(token_address, "evm")

        token = (
            db.query(MonitoredToken)
            .filter(
                and_(
                    MonitoredToken.token_address == token_address,
                    MonitoredToken.chain_id == chain_id,
                    MonitoredToken.network_type == "evm",
                )
            )
            .first()
        )

        if token is None:
            raise ResourceNotFoundException(
                f"未找到地址为 {token_address}，链 ID 为 {chain_id} 的监控代币"
            )
        return token

    @staticmethod
    def get_token_by_symbol(
        db: Session,
        symbol: str,
        network: Optional[str] = None,
        chain_id: Optional[int] = None,
    ) -> List[MonitoredToken]:
        """
        通过代币符号获取监控代币

        Args:
            db: 数据库会话
            symbol: 代币符号
            network: 可选的网络标识符
            chain_id: 可选的链 ID 筛选（仅适用于EVM网络）

        Returns:
            监控代币对象列表
        """
        query = db.query(MonitoredToken).filter(MonitoredToken.symbol == symbol)

        if network is not None:
            query = query.filter(MonitoredToken.network == network)

        if chain_id is not None:
            query = query.filter(MonitoredToken.chain_id == chain_id)

        return query.all()

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
        列出监控代币

        Args:
            db: 数据库会话
            network: 可选的网络标识符
            network_type: 可选的网络类型筛选
            chain_id: 可选的链 ID 筛选（仅适用于EVM网络）
            skip: 跳过记录数
            limit: 限制返回数量

        Returns:
            监控代币对象列表
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
    def update_token(
        db: Session, token_id: int, update_data: Dict[str, Any]
    ) -> MonitoredToken:
        """
        更新监控代币信息

        Args:
            db: 数据库会话
            token_id: 代币 ID
            update_data: 更新数据字典

        Returns:
            更新后的监控代币对象

        Raises:
            ResourceNotFoundException: 如果未找到指定 ID 的代币
        """
        token = MonitoredTokenService.get_token_by_id(db, token_id)

        # 规范化地址
        if "token_address" in update_data:
            update_data["token_address"] = normalize_token_address(
                update_data["token_address"],
                update_data.get("network_type", token.network_type),
            )

        for key, value in update_data.items():
            if hasattr(token, key):
                setattr(token, key, value)

        try:
            db.commit()
            db.refresh(token)
            return token
        except IntegrityError:
            db.rollback()
            raise DuplicateResourceException("更新导致代币地址和网络组合重复")

    @staticmethod
    def delete_token(db: Session, token_id: int) -> bool:
        """
        删除监控代币

        Args:
            db: 数据库会话
            token_id: 代币 ID

        Returns:
            删除是否成功

        Raises:
            ResourceNotFoundException: 如果未找到指定 ID 的代币
        """
        token = MonitoredTokenService.get_token_by_id(db, token_id)
        db.delete(token)
        db.commit()
        return True

    @staticmethod
    def search_tokens(
        db: Session,
        search_term: Optional[str] = None,
        network: Optional[str] = None,
        network_type: Optional[str] = None,
        chain_id: Optional[int] = None,
        skip: int = 0,
        limit: int = 100,
    ) -> List[MonitoredToken]:
        """
        搜索监控代币

        Args:
            db: 数据库会话
            search_term: 搜索关键词（匹配名称、符号或地址）
            network: 可选的网络标识符
            network_type: 可选的网络类型筛选
            chain_id: 可选的链 ID 筛选（仅适用于EVM网络）
            skip: 跳过记录数
            limit: 限制返回数量

        Returns:
            匹配条件的监控代币列表
        """
        query = db.query(MonitoredToken)

        if search_term:
            search_pattern = f"%{search_term}%"
            query = query.filter(
                or_(
                    MonitoredToken.name.ilike(search_pattern),
                    MonitoredToken.symbol.ilike(search_pattern),
                    MonitoredToken.token_address.ilike(search_pattern),
                    MonitoredToken.description.ilike(search_pattern),
                )
            )

        if network is not None:
            query = query.filter(MonitoredToken.network == network)

        if network_type is not None:
            query = query.filter(MonitoredToken.network_type == network_type)

        if chain_id is not None:
            query = query.filter(MonitoredToken.chain_id == chain_id)

        return query.offset(skip).limit(limit).all()

    @staticmethod
    def batch_create_or_update_tokens(
        db: Session, tokens_data: List[Dict[str, Any]]
    ) -> Dict[str, int]:
        """
        批量创建或更新监控代币

        如果代币已存在（根据network和token_address），则更新它；否则创建新代币

        Args:
            db: 数据库会话
            tokens_data: 包含代币数据的字典列表

        Returns:
            包含创建和更新数量的字典
        """
        created = 0
        updated = 0

        for token_data in tokens_data:
            network = token_data.get("network")
            network_type = token_data.get("network_type", "evm")
            token_address = normalize_token_address(
                token_data.get("token_address"), network_type
            )

            if not network or not token_address:
                logger.warning(f"跳过无效的代币数据: {token_data}")
                continue

            token_data["token_address"] = token_address  # 确保地址小写

            try:
                # 检查代币是否已存在
                token = (
                    db.query(MonitoredToken)
                    .filter(
                        and_(
                            MonitoredToken.network == network,
                            MonitoredToken.token_address == token_address,
                        )
                    )
                    .first()
                )

                if token:
                    # 更新现有代币
                    for key, value in token_data.items():
                        if hasattr(token, key):
                            setattr(token, key, value)
                    updated += 1
                else:
                    # 创建新代币
                    token = MonitoredToken(**token_data)
                    db.add(token)
                    created += 1

            except Exception as e:
                logger.error(f"处理代币数据时出错: {e}", exc_info=True)

        try:
            db.commit()
        except IntegrityError as e:
            db.rollback()
            logger.error(f"批量处理代币时发生完整性错误: {e}", exc_info=True)
            raise

        return {"created": created, "updated": updated}

    @staticmethod
    def bulk_delete_tokens(db: Session, token_ids: List[int]) -> int:
        """
        批量删除监控代币

        Args:
            db: 数据库会话
            token_ids: 要删除的代币ID列表

        Returns:
            成功删除的代币数量
        """
        result = (
            db.query(MonitoredToken)
            .filter(MonitoredToken.id.in_(token_ids))
            .delete(synchronize_session=False)
        )

        db.commit()
        return result

    @staticmethod
    def get_tokens_by_network(
        db: Session, network: str, skip: int = 0, limit: int = 100
    ) -> List[MonitoredToken]:
        """
        根据网络获取监控代币

        Args:
            db: 数据库会话
            network: 网络标识符
            skip: 跳过记录数
            limit: 限制返回数量

        Returns:
            监控代币对象列表
        """
        return (
            db.query(MonitoredToken)
            .filter(MonitoredToken.network == network)
            .offset(skip)
            .limit(limit)
            .all()
        )

    @staticmethod
    def get_tokens_by_chain(
        db: Session, chain_id: int, skip: int = 0, limit: int = 100
    ) -> List[MonitoredToken]:
        """
        根据链 ID 获取监控代币（EVM 网络兼容性方法）

        Args:
            db: 数据库会话
            chain_id: 链 ID
            skip: 跳过记录数
            limit: 限制返回数量

        Returns:
            监控代币对象列表
        """
        return (
            db.query(MonitoredToken)
            .filter(
                and_(
                    MonitoredToken.chain_id == chain_id,
                    MonitoredToken.network_type == "evm",
                )
            )
            .offset(skip)
            .limit(limit)
            .all()
        )

    @staticmethod
    def get_all_active_tokens(
        db: Session,
        network: Optional[str] = None,
        network_type: Optional[str] = None,
        chain_id: Optional[int] = None,
    ) -> List[MonitoredToken]:
        """
        获取所有活跃的监控代币

        Args:
            db: 数据库会话
            network: 可选的网络标识符
            network_type: 可选的网络类型筛选
            chain_id: 可选的链 ID 筛选（仅适用于EVM网络）

        Returns:
            活跃的监控代币对象列表
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

    @staticmethod
    def get_token_by_network_and_address(
        db: Session,
        network_identifier: Union[str, int],
        token_address: str,
        is_chain_id: bool = False,
    ) -> MonitoredToken:
        """
        统一方法，通过网络标识符和地址获取代币

        Args:
            db: 数据库会话
            network_identifier: 网络标识符（网络名称或链ID）
            token_address: 代币地址
            is_chain_id: network_identifier是否为链ID

        Returns:
            监控代币对象

        Raises:
            ResourceNotFoundException: 如果未找到指定代币
        """
        token_address = normalize_token_address(token_address)

        if is_chain_id:
            # 作为EVM链ID处理
            chain_id = int(network_identifier)
            token = (
                db.query(MonitoredToken)
                .filter(
                    and_(
                        MonitoredToken.token_address == token_address,
                        MonitoredToken.chain_id == chain_id,
                        MonitoredToken.network_type == "evm",
                    )
                )
                .first()
            )
        else:
            # 作为网络名称处理
            network = str(network_identifier)
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
            identifier_type = "链ID" if is_chain_id else "网络"
            raise ResourceNotFoundException(
                f"未找到地址为 {token_address}，{identifier_type} 为 {network_identifier} 的监控代币"
            )

        return token
