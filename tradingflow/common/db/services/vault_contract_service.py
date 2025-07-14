import logging
from typing import Any, Dict, List, Optional

from sqlalchemy import and_, or_
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from tradingflow.depot.exceptions import DuplicateResourceException, ResourceNotFoundException
from tradingflow.depot.db.models.vault_contract import VaultContract

# 添加日志记录器
logger = logging.getLogger(__name__)


class VaultContractService:
    """
    提供 Vault 合约的基本增删改查服务
    """

    @staticmethod
    def _normalize_address(address: str) -> str:
        """将地址转换为小写格式"""
        if address is None:
            return None
        return address.lower() if isinstance(address, str) else str(address).lower()

    @staticmethod
    def _normalize_addresses_in_data(data: Dict[str, Any]) -> Dict[str, Any]:
        """规范化数据字典中的所有地址字段"""
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
        创建新的 Vault 合约记录

        Args:
            db: 数据库会话
            vault_data: Vault 合约数据字典

        Returns:
            新创建的 Vault 合约对象

        Raises:
            DuplicateResourceException: 如果相同链上已存在相同地址的合约
        """
        # 规范化所有地址字段
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
                f"相同链上已存在地址为 {normalized_data.get('contract_address')} 的合约"
            )

    @staticmethod
    def get_vault_by_id(db: Session, vault_id: int) -> VaultContract:
        """
        通过 ID 获取 Vault 合约

        Args:
            db: 数据库会话
            vault_id: Vault 合约 ID

        Returns:
            Vault 合约对象

        Raises:
            ResourceNotFoundException: 如果未找到指定 ID 的 Vault
        """
        vault = db.query(VaultContract).filter(VaultContract.id == vault_id).first()
        if vault is None:
            raise ResourceNotFoundException(f"未找到 ID 为 {vault_id} 的 Vault 合约")
        return vault

    @staticmethod
    def get_vault_by_address(
        db: Session, contract_address: str, chain_id: int
    ) -> VaultContract:
        """
        通过合约地址和链 ID 获取 Vault 合约（兼容性方法）

        Args:
            db: 数据库会话
            contract_address: 合约地址
            chain_id: 链 ID

        Returns:
            Vault 合约对象

        Raises:
            ResourceNotFoundException: 如果未找到指定地址和链的 Vault
        """
        normalized_address = VaultContractService._normalize_address(contract_address)
        logger.debug(f"获取 Vault 合约，地址: {normalized_address}, 链 ID: {chain_id}")
        vault = (
            db.query(VaultContract)
            .filter(
                and_(
                    VaultContract.contract_address == normalized_address,
                    VaultContract.chain_id == chain_id,
                )
            )
            .first()
        )

        if vault is None:
            raise ResourceNotFoundException(
                f"未找到地址为 {contract_address}，链 ID 为 {chain_id} 的 Vault 合约"
            )
        return vault

    @staticmethod
    def get_vault_by_network_address(
        db: Session, contract_address: str, network: str, chain_id: int
    ) -> VaultContract:
        """
        通过合约地址、网络和链 ID 获取 Vault 合约

        Args:
            db: 数据库会话
            contract_address: 合约地址
            network: 网络名称（如：'ethereum', 'aptos', 'flow-evm'等）
            chain_id: 链 ID

        Returns:
            Vault 合约对象

        Raises:
            ResourceNotFoundException: 如果未找到指定条件的 Vault
        """
        normalized_address = VaultContractService._normalize_address(contract_address)
        logger.debug(f"获取 Vault 合约，地址: {normalized_address}, 网络: {network}, 链 ID: {chain_id}")
        vault = (
            db.query(VaultContract)
            .filter(
                and_(
                    VaultContract.contract_address == normalized_address,
                    VaultContract.network == network,
                    VaultContract.chain_id == chain_id,
                )
            )
            .first()
        )

        if vault is None:
            raise ResourceNotFoundException(
                f"未找到地址为 {contract_address}，网络为 {network}，链 ID 为 {chain_id} 的 Vault 合约"
            )
        return vault

    @staticmethod
    def get_vaults_by_network(
        db: Session, network: str, skip: int = 0, limit: int = 100
    ) -> List[VaultContract]:
        """
        获取指定网络的所有 Vault 合约

        Args:
            db: 数据库会话
            network: 网络名称
            skip: 跳过记录数
            limit: 限制返回数量

        Returns:
            Vault 合约对象列表
        """
        return (
            db.query(VaultContract)
            .filter(VaultContract.network == network)
            .offset(skip)
            .limit(limit)
            .all()
        )

    @staticmethod
    def get_vaults_by_deployer(
        db: Session, deployer_address: str, skip: int = 0, limit: int = 100
    ) -> List[VaultContract]:
        """
        获取部署者的所有 Vault 合约

        Args:
            db: 数据库会话
            deployer_address: 部署者钱包地址
            skip: 跳过记录数
            limit: 限制返回数量

        Returns:
            Vault 合约对象列表
        """
        normalized_address = VaultContractService._normalize_address(deployer_address)
        return (
            db.query(VaultContract)
            .filter(VaultContract.deployer_address == normalized_address)
            .offset(skip)
            .limit(limit)
            .all()
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
        获取投资者的所有 Vault 合约

        Args:
            db: 数据库会话
            investor_address: 投资者钱包地址
            chain_id: 链 ID
            skip: 跳过记录数
            limit: 限制返回数量

        Returns:
            Vault 合约对象列表
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
    def list_vaults(
        db: Session, skip: int = 0, limit: int = 100
    ) -> List[VaultContract]:
        """
        列出所有 Vault 合约

        Args:
            db: 数据库会话
            skip: 跳过记录数
            limit: 限制返回数量

        Returns:
            Vault 合约对象列表
        """
        return db.query(VaultContract).offset(skip).limit(limit).all()

    @staticmethod
    def update_vault(
        db: Session, vault_id: int, update_data: Dict[str, Any]
    ) -> VaultContract:
        """
        更新 Vault 合约信息

        Args:
            db: 数据库会话
            vault_id: Vault 合约 ID
            update_data: 更新数据字典

        Returns:
            更新后的 Vault 合约对象

        Raises:
            ResourceNotFoundException: 如果未找到指定 ID 的 Vault
        """
        vault = VaultContractService.get_vault_by_id(db, vault_id)

        # 规范化所有地址字段
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
            raise DuplicateResourceException("更新导致合约地址和链 ID 组合重复")

    @staticmethod
    def delete_vault(db: Session, vault_id: int) -> bool:
        """
        删除 Vault 合约

        Args:
            db: 数据库会话
            vault_id: Vault 合约 ID

        Returns:
            删除是否成功

        Raises:
            ResourceNotFoundException: 如果未找到指定 ID 的 Vault
        """
        vault = VaultContractService.get_vault_by_id(db, vault_id)
        db.delete(vault)
        db.commit()
        return True

    @staticmethod
    def search_vaults(
        db: Session,
        search_term: Optional[str] = None,
        chain_id: Optional[int] = None,
        asset_address: Optional[str] = None,
        skip: int = 0,
        limit: int = 100,
    ) -> List[VaultContract]:
        """
        搜索 Vault 合约

        Args:
            db: 数据库会话
            search_term: 搜索关键词（匹配名称或符号）
            chain_id: 链 ID 筛选
            asset_address: 底层资产地址筛选
            skip: 跳过记录数
            limit: 限制返回数量

        Returns:
            匹配条件的 Vault 合约列表
        """
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

        if asset_address:
            normalized_asset_address = VaultContractService._normalize_address(
                asset_address
            )
            query = query.filter(
                VaultContract.asset_address == normalized_asset_address
            )

        return query.offset(skip).limit(limit).all()
