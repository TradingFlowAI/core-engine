import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional

from sqlalchemy import and_, desc, func
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from tradingflow.common.exceptions import DuplicateResourceException, ResourceNotFoundException
from tradingflow.common.db.models.vault_vaule_history import VaultValueHistory

# 添加日志记录器
logger = logging.getLogger(__name__)


class VaultValueHistoryService:
    """
    提供 Vault 价值历史记录的基本增删改查服务
    """

    @staticmethod
    def _normalize_address(address: str) -> str:
        """将地址转换为小写格式"""
        if address is None:
            return None
        return address.lower() if isinstance(address, str) else str(address).lower()

    @staticmethod
    def create_value_record(
        db: Session,
        vault_contract_id: int,
        chain_id: int,
        vault_address: str,
        total_value_usd: Decimal,
        updated_at: Optional[datetime] = None,
    ) -> VaultValueHistory:
        """
        创建新的 Vault 价值记录

        Args:
            db: 数据库会话
            vault_contract_id: Vault 合约 ID
            chain_id: 区块链 ID
            vault_address: Vault 合约地址
            total_value_usd: 总价值 (USD)
            recorded_at: 记录时间，默认为当前时间

        Returns:
            新创建的价值记录对象

        Raises:
            DuplicateResourceException: 如果同一时间同一Vault已存在记录
        """
        if updated_at is None:
            updated_at = datetime.now(timezone.utc)

        normalized_address = VaultValueHistoryService._normalize_address(vault_address)

        value_record = VaultValueHistory(
            vault_contract_id=vault_contract_id,
            chain_id=chain_id,
            vault_address=normalized_address,
            total_value_usd=total_value_usd,
            updated_at=updated_at,
        )

        try:
            db.add(value_record)
            db.commit()
            db.refresh(value_record)
            logger.info(
                f"成功创建价值记录 - Vault ID: {vault_contract_id}, 价值: ${total_value_usd}"
            )
            return value_record
        except IntegrityError:
            db.rollback()
            raise DuplicateResourceException(
                f"Vault {vault_contract_id} 在时间 {updated_at} 已存在价值记录"
            )

    @staticmethod
    def get_record_by_id(db: Session, record_id: int) -> VaultValueHistory:
        """
        通过 ID 获取价值记录

        Args:
            db: 数据库会话
            record_id: 记录 ID

        Returns:
            价值记录对象

        Raises:
            ResourceNotFoundException: 如果未找到指定 ID 的记录
        """
        record = (
            db.query(VaultValueHistory)
            .filter(VaultValueHistory.id == record_id)
            .first()
        )
        if record is None:
            raise ResourceNotFoundException(f"未找到 ID 为 {record_id} 的价值记录")
        return record

    @staticmethod
    def get_latest_value(
        db: Session, vault_contract_id: int
    ) -> Optional[VaultValueHistory]:
        """
        获取指定 Vault 的最新价值记录

        Args:
            db: 数据库会话
            vault_contract_id: Vault 合约 ID

        Returns:
            最新的价值记录，如果不存在则返回 None
        """
        return (
            db.query(VaultValueHistory)
            .filter(VaultValueHistory.vault_contract_id == vault_contract_id)
            .order_by(desc(VaultValueHistory.updated_at))
            .first()
        )

    @staticmethod
    def get_vault_history(
        db: Session,
        vault_contract_id: int,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        skip: int = 0,
        limit: int = 100,
    ) -> List[VaultValueHistory]:
        """
        获取 Vault 价值历史记录

        Args:
            db: 数据库会话
            vault_contract_id: Vault 合约 ID
            start_date: 开始日期
            end_date: 结束日期
            skip: 跳过记录数
            limit: 限制返回数量

        Returns:
            价值历史记录列表
        """
        query = db.query(VaultValueHistory).filter(
            VaultValueHistory.vault_contract_id == vault_contract_id
        )

        if start_date:
            query = query.filter(VaultValueHistory.updated_at >= start_date)

        if end_date:
            query = query.filter(VaultValueHistory.updated_at <= end_date)

        return (
            query.order_by(desc(VaultValueHistory.updated_at))
            .offset(skip)
            .limit(limit)
            .all()
        )

    @staticmethod
    def get_history_by_address(
        db: Session,
        vault_address: str,
        chain_id: int,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        skip: int = 0,
        limit: int = 100,
    ) -> List[VaultValueHistory]:
        """
        通过 Vault 地址获取价值历史记录

        Args:
            db: 数据库会话
            vault_address: Vault 合约地址
            chain_id: 区块链 ID
            start_date: 开始日期
            end_date: 结束日期
            skip: 跳过记录数
            limit: 限制返回数量

        Returns:
            价值历史记录列表
        """
        normalized_address = VaultValueHistoryService._normalize_address(vault_address)

        query = db.query(VaultValueHistory).filter(
            and_(
                VaultValueHistory.vault_address == normalized_address,
                VaultValueHistory.chain_id == chain_id,
            )
        )

        if start_date:
            query = query.filter(VaultValueHistory.updated_at >= start_date)

        if end_date:
            query = query.filter(VaultValueHistory.updated_at <= end_date)

        return (
            query.order_by(desc(VaultValueHistory.updated_at))
            .offset(skip)
            .limit(limit)
            .all()
        )

    @staticmethod
    def get_history_by_chain(
        db: Session,
        chain_id: int,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        skip: int = 0,
        limit: int = 100,
    ) -> List[VaultValueHistory]:
        """
        获取指定链上所有 Vault 的价值历史记录

        Args:
            db: 数据库会话
            chain_id: 区块链 ID
            start_date: 开始日期
            end_date: 结束日期
            skip: 跳过记录数
            limit: 限制返回数量

        Returns:
            价值历史记录列表
        """
        query = db.query(VaultValueHistory).filter(
            VaultValueHistory.chain_id == chain_id
        )

        if start_date:
            query = query.filter(VaultValueHistory.updated_at >= start_date)

        if end_date:
            query = query.filter(VaultValueHistory.updated_at <= end_date)

        return (
            query.order_by(desc(VaultValueHistory.updated_at))
            .offset(skip)
            .limit(limit)
            .all()
        )

    @staticmethod
    def update_record(
        db: Session, record_id: int, update_data: Dict[str, Any]
    ) -> VaultValueHistory:
        """
        更新价值记录

        Args:
            db: 数据库会话
            record_id: 记录 ID
            update_data: 更新数据字典

        Returns:
            更新后的价值记录对象

        Raises:
            ResourceNotFoundException: 如果未找到指定 ID 的记录
        """
        record = VaultValueHistoryService.get_record_by_id(db, record_id)

        # 规范化地址字段
        if "vault_address" in update_data:
            update_data["vault_address"] = VaultValueHistoryService._normalize_address(
                update_data["vault_address"]
            )

        for key, value in update_data.items():
            if hasattr(record, key):
                setattr(record, key, value)

        try:
            db.commit()
            db.refresh(record)
            logger.info(f"成功更新价值记录 ID: {record_id}")
            return record
        except IntegrityError:
            db.rollback()
            raise DuplicateResourceException("更新导致唯一约束冲突")

    @staticmethod
    def delete_record(db: Session, record_id: int) -> bool:
        """
        删除价值记录

        Args:
            db: 数据库会话
            record_id: 记录 ID

        Returns:
            删除是否成功

        Raises:
            ResourceNotFoundException: 如果未找到指定 ID 的记录
        """
        record = VaultValueHistoryService.get_record_by_id(db, record_id)
        db.delete(record)
        db.commit()
        logger.info(f"成功删除价值记录 ID: {record_id}")
        return True

    @staticmethod
    def delete_vault_history(db: Session, vault_contract_id: int) -> int:
        """
        删除指定 Vault 的所有历史记录

        Args:
            db: 数据库会话
            vault_contract_id: Vault 合约 ID

        Returns:
            删除的记录数量
        """
        deleted_count = (
            db.query(VaultValueHistory)
            .filter(VaultValueHistory.vault_contract_id == vault_contract_id)
            .delete()
        )
        db.commit()
        logger.info(f"删除 Vault {vault_contract_id} 的 {deleted_count} 条历史记录")
        return deleted_count

    @staticmethod
    def get_value_at_time(
        db: Session, vault_contract_id: int, target_time: datetime
    ) -> Optional[VaultValueHistory]:
        """
        获取指定时间点最近的价值记录

        Args:
            db: 数据库会话
            vault_contract_id: Vault 合约 ID
            target_time: 目标时间

        Returns:
            最接近目标时间的价值记录，如果不存在则返回 None
        """
        return (
            db.query(VaultValueHistory)
            .filter(
                and_(
                    VaultValueHistory.vault_contract_id == vault_contract_id,
                    VaultValueHistory.updated_at <= target_time,
                )
            )
            .order_by(desc(VaultValueHistory.updated_at))
            .first()
        )

    @staticmethod
    def get_performance_metrics(
        db: Session, vault_contract_id: int, days: int = 30
    ) -> Dict[str, Any]:
        """
        获取 Vault 性能指标

        Args:
            db: 数据库会话
            vault_contract_id: Vault 合约 ID
            days: 计算天数

        Returns:
            包含各种性能指标的字典
        """
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=days)

        records = VaultValueHistoryService.get_vault_history(
            db, vault_contract_id, start_date, end_date, limit=1000
        )

        if not records:
            return {
                "vault_contract_id": vault_contract_id,
                "days": days,
                "records_count": 0,
                "error": "没有找到历史记录",
            }

        # 按时间顺序排序（最早到最新）
        records = sorted(records, key=lambda x: x.updated_at)
        values = [float(record.total_value_usd) for record in records]

        # 计算各种指标
        current_value = values[-1] if values else 0
        initial_value = values[0] if values else 0
        max_value = max(values) if values else 0
        min_value = min(values) if values else 0

        # 计算收益率
        total_return_pct = 0
        if initial_value > 0:
            total_return_pct = (current_value - initial_value) / initial_value * 100

        # 计算最大回撤
        max_drawdown_pct = 0
        peak = 0
        for value in values:
            if value > peak:
                peak = value
            drawdown = (peak - value) / peak * 100 if peak > 0 else 0
            if drawdown > max_drawdown_pct:
                max_drawdown_pct = drawdown

        # 计算日均收益率（如果有足够数据）
        daily_returns = []
        for i in range(1, len(values)):
            if values[i - 1] > 0:
                daily_return = (values[i] - values[i - 1]) / values[i - 1] * 100
                daily_returns.append(daily_return)

        avg_daily_return = (
            sum(daily_returns) / len(daily_returns) if daily_returns else 0
        )
        volatility = 0
        if len(daily_returns) > 1:
            variance = sum((r - avg_daily_return) ** 2 for r in daily_returns) / (
                len(daily_returns) - 1
            )
            volatility = variance**0.5

        return {
            "vault_contract_id": vault_contract_id,
            "period_days": days,
            "records_count": len(records),
            "current_value_usd": round(current_value, 2),
            "initial_value_usd": round(initial_value, 2),
            "max_value_usd": round(max_value, 2),
            "min_value_usd": round(min_value, 2),
            "total_return_pct": round(total_return_pct, 4),
            "max_drawdown_pct": round(max_drawdown_pct, 4),
            "avg_daily_return_pct": round(avg_daily_return, 4),
            "volatility_pct": round(volatility, 4),
            "first_record_date": records[0].updated_at.isoformat() if records else None,
            "last_record_date": records[-1].updated_at.isoformat() if records else None,
        }

    @staticmethod
    def get_records_count(
        db: Session,
        vault_contract_id: Optional[int] = None,
        chain_id: Optional[int] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> int:
        """
        获取满足条件的记录总数

        Args:
            db: 数据库会话
            vault_contract_id: Vault 合约 ID（可选）
            chain_id: 区块链 ID（可选）
            start_date: 开始日期（可选）
            end_date: 结束日期（可选）

        Returns:
            记录总数
        """
        query = db.query(func.count(VaultValueHistory.id))

        if vault_contract_id:
            query = query.filter(
                VaultValueHistory.vault_contract_id == vault_contract_id
            )

        if chain_id:
            query = query.filter(VaultValueHistory.chain_id == chain_id)

        if start_date:
            query = query.filter(VaultValueHistory.updated_at >= start_date)

        if end_date:
            query = query.filter(VaultValueHistory.updated_at <= end_date)

        return query.scalar()

    @staticmethod
    def batch_create_records(
        db: Session, records_data: List[Dict[str, Any]]
    ) -> List[VaultValueHistory]:
        """
        批量创建价值记录

        Args:
            db: 数据库会话
            records_data: 记录数据列表

        Returns:
            创建的价值记录列表

        Raises:
            DuplicateResourceException: 如果存在重复记录
        """
        records = []
        for data in records_data:
            # 规范化地址
            if "vault_address" in data:
                data["vault_address"] = VaultValueHistoryService._normalize_address(
                    data["vault_address"]
                )

            record = VaultValueHistory(**data)
            records.append(record)

        try:
            db.add_all(records)
            db.commit()
            for record in records:
                db.refresh(record)
            logger.info(f"成功批量创建 {len(records)} 条价值记录")
            return records
        except IntegrityError:
            db.rollback()
            raise DuplicateResourceException("批量创建时存在重复记录")
