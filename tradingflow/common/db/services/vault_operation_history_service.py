import logging
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any, Dict, List, Optional

from sqlalchemy import and_, desc
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from tradingflow.depot.db.models.vault_operation_history import (
    OperationType,
    VaultOperationHistory,
)
from tradingflow.depot.db.models.vault_vaule_history import VaultValueHistory
from tradingflow.depot.exceptions import (
    DuplicateResourceException,
    ResourceNotFoundException,
)

logger = logging.getLogger(__name__)


class VaultOperationHistoryService:
    """Vault操作历史记录服务"""

    @staticmethod
    def _normalize_address(address: str) -> Optional[str]:
        """将地址转换为小写格式"""
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
        创建Vault操作记录

        Args:
            db: 数据库会话
            vault_contract_id: Vault合约ID
            vault_address: Vault合约地址
            operation_type: 操作类型
            network: 网络标识符（如 'ethereum', 'aptos', 'sui'）
            network_type: 网络类型（如 'evm', 'aptos', 'sui'）
            chain_id: 链ID（仅EVM链需要，非EVM链为None）
            transaction_hash: 交易哈希
            input_token_address: 输入代币地址
            input_token_amount: 输入代币数量
            input_token_usd_value: 输入代币USD价值
            output_token_address: 输出代币地址
            output_token_amount: 输出代币数量
            output_token_usd_value: 输出代币USD价值
            gas_used: 使用的Gas数量或等效计算单位
            gas_price: Gas价格或等效费用
            total_gas_cost_usd: 总Gas成本(USD)

        Returns:
            创建的操作记录
        """
        # 规范化地址
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
                "成功创建操作记录 - Network: %s, Vault: %s, 操作: %s",
                network,
                vault_address,
                operation_type.value,
            )
            return operation_record
        except IntegrityError as e:
            db.rollback()
            logger.error("创建操作记录失败: %s", e)
            if "uq_vault_operation_txhash" in str(e):
                raise DuplicateResourceException(
                    f"交易哈希 {transaction_hash} 已存在操作记录"
                )
            raise

    @staticmethod
    def get_record_by_id(db: Session, record_id: int) -> VaultOperationHistory:
        """通过ID获取操作记录"""
        record = (
            db.query(VaultOperationHistory)
            .filter(VaultOperationHistory.id == record_id)
            .first()
        )

        if record is None:
            raise ResourceNotFoundException(f"未找到ID为 {record_id} 的操作记录")

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
        """获取Vault操作记录"""
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
        获取Vault操作记录总数，用于分页

        Args:
            db: 数据库会话
            vault_contract_id: Vault合约ID
            operation_types: 操作类型过滤
            network: 网络过滤
            network_type: 网络类型过滤
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            符合条件的记录总数
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
        """通过交易哈希获取操作记录"""
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
    def _get_vault_value_at_time(
        db: Session, vault_contract_id: int, timestamp: datetime
    ) -> Optional[Decimal]:
        """获取指定时间点最近的Vault价值"""
        value_record = (
            db.query(VaultValueHistory)
            .filter(
                and_(
                    VaultValueHistory.vault_contract_id == vault_contract_id,
                    VaultValueHistory.updated_at <= timestamp,
                )
            )
            .order_by(desc(VaultValueHistory.updated_at))
            .first()
        )

        return value_record.total_value_usd if value_record else None

    @staticmethod
    def delete_record(db: Session, record_id: int) -> bool:
        """删除操作记录"""
        record = VaultOperationHistoryService.get_record_by_id(db, record_id)
        db.delete(record)
        db.commit()
        logger.info("成功删除操作记录 ID: %s", record_id)
        return True

    @staticmethod
    def calculate_total_returns(
        db: Session,
        vault_contract_id: int,
        current_timestamp: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """
        计算金库的累计收益

        累计收益 = 当前价值 + 累计提现的价值 - 累计充值的价值

        Args:
            db: 数据库会话
            vault_contract_id: Vault合约ID
            current_timestamp: 计算时间点，默认为当前时间

        Returns:
            包含累计收益计算结果的字典
        """
        if current_timestamp is None:
            current_timestamp = datetime.utcnow()

        # 获取所有存款和取款操作
        deposit_withdraw_ops = (
            db.query(VaultOperationHistory)
            .filter(
                and_(
                    VaultOperationHistory.vault_contract_id == vault_contract_id,
                    VaultOperationHistory.operation_type.in_(
                        [OperationType.DEPOSIT, OperationType.WITHDRAW]
                    ),
                    VaultOperationHistory.created_at <= current_timestamp,
                )
            )
            .order_by(VaultOperationHistory.created_at)
            .all()
        )

        # 获取当前金库价值
        latest_value_record = (
            db.query(VaultValueHistory)
            .filter(
                and_(
                    VaultValueHistory.vault_contract_id == vault_contract_id,
                    VaultValueHistory.updated_at <= current_timestamp,
                )
            )
            .order_by(desc(VaultValueHistory.updated_at))
            .first()
        )

        if not latest_value_record:
            return {
                "vault_contract_id": vault_contract_id,
                "status": "error",
                "message": "找不到当前金库价值记录",
                "calculation_time": current_timestamp.isoformat(),
            }

        current_value = latest_value_record.total_value_usd

        # 计算累计存取款
        total_deposits = Decimal(0)
        total_withdrawals = Decimal(0)

        for op in deposit_withdraw_ops:
            if op.operation_type == OperationType.DEPOSIT and op.input_token_usd_value:
                total_deposits += op.input_token_usd_value
            elif (
                op.operation_type == OperationType.WITHDRAW
                and op.output_token_usd_value
            ):
                total_withdrawals += op.output_token_usd_value

        # 计算累计收益
        total_return = current_value + total_withdrawals - total_deposits

        # 计算收益率
        return_rate_pct = 0
        if total_deposits > 0:
            return_rate_pct = (total_return / total_deposits) * 100

        # 计算交易收益
        swap_operations = (
            db.query(VaultOperationHistory)
            .filter(
                and_(
                    VaultOperationHistory.vault_contract_id == vault_contract_id,
                    VaultOperationHistory.operation_type == OperationType.SWAP,
                    VaultOperationHistory.created_at <= current_timestamp,
                )
            )
            .all()
        )

        swap_profit_usd = Decimal(0)
        for op in swap_operations:
            input_value = op.input_token_usd_value or Decimal(0)
            output_value = op.output_token_usd_value or Decimal(0)
            swap_profit_usd += output_value - input_value

        # 计算总Gas成本
        total_gas_cost_usd = Decimal(0)
        for op in deposit_withdraw_ops + swap_operations:
            if op.total_gas_cost_usd:
                total_gas_cost_usd += op.total_gas_cost_usd

        return {
            "vault_contract_id": vault_contract_id,
            "current_value_usd": float(current_value),
            "total_deposits_usd": float(total_deposits),
            "total_withdrawals_usd": float(total_withdrawals),
            "total_return_usd": float(total_return),
            "return_rate_pct": float(return_rate_pct),
            "swap_profit_usd": float(swap_profit_usd),
            "total_gas_cost_usd": float(total_gas_cost_usd),
            "net_return_after_gas_usd": float(total_return - total_gas_cost_usd),
            "calculation_time": current_timestamp.isoformat(),
        }

    @staticmethod
    def calculate_daily_returns(
        db: Session,
        vault_contract_id: int,
        target_date: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """
        计算金库的指定日收益

        昨日收益 = 昨日最后时刻的价值 - 昨日最初时刻的价值 - 昨日充值的价值 + 昨日提现的价值

        Args:
            db: 数据库会话
            vault_contract_id: Vault合约ID
            target_date: 目标日期，默认为昨天

        Returns:
            包含日收益计算结果的字典
        """
        # 确定目标日期(默认为昨天)
        if target_date is None:
            target_date = datetime.utcnow().replace(
                hour=0, minute=0, second=0, microsecond=0
            ) - timedelta(days=1)

        day_start = target_date.replace(hour=0, minute=0, second=0, microsecond=0)
        day_end = target_date.replace(hour=23, minute=59, second=59, microsecond=999999)

        # 获取目标日开始和结束时的金库价值
        start_value_record = (
            db.query(VaultValueHistory)
            .filter(
                and_(
                    VaultValueHistory.vault_contract_id == vault_contract_id,
                    VaultValueHistory.updated_at <= day_start,
                )
            )
            .order_by(desc(VaultValueHistory.updated_at))
            .first()
        )

        end_value_record = (
            db.query(VaultValueHistory)
            .filter(
                and_(
                    VaultValueHistory.vault_contract_id == vault_contract_id,
                    VaultValueHistory.updated_at <= day_end,
                )
            )
            .order_by(desc(VaultValueHistory.updated_at))
            .first()
        )

        if not start_value_record or not end_value_record:
            return {
                "vault_contract_id": vault_contract_id,
                "date": day_start.date().isoformat(),
                "status": "error",
                "message": "缺少价值记录，无法计算日收益",
                "start_value_exists": bool(start_value_record),
                "end_value_exists": bool(end_value_record),
            }

        start_value = start_value_record.total_value_usd
        end_value = end_value_record.total_value_usd

        # 获取当天的存取款操作
        day_operations = (
            db.query(VaultOperationHistory)
            .filter(
                and_(
                    VaultOperationHistory.vault_contract_id == vault_contract_id,
                    VaultOperationHistory.operation_type.in_(
                        [OperationType.DEPOSIT, OperationType.WITHDRAW]
                    ),
                    VaultOperationHistory.created_at >= day_start,
                    VaultOperationHistory.created_at <= day_end,
                )
            )
            .order_by(VaultOperationHistory.created_at)
            .all()
        )

        # 计算当天的存取款
        day_deposits = Decimal(0)
        day_withdrawals = Decimal(0)

        for op in day_operations:
            if op.operation_type == OperationType.DEPOSIT and op.input_token_usd_value:
                day_deposits += op.input_token_usd_value
            elif (
                op.operation_type == OperationType.WITHDRAW
                and op.output_token_usd_value
            ):
                day_withdrawals += op.output_token_usd_value

        # 计算日收益
        daily_return = end_value - start_value - day_deposits + day_withdrawals

        # 计算日收益率
        daily_return_rate_pct = 0
        if start_value > 0:
            daily_return_rate_pct = (daily_return / start_value) * 100

        # 获取当日交易数据
        day_swaps = (
            db.query(VaultOperationHistory)
            .filter(
                and_(
                    VaultOperationHistory.vault_contract_id == vault_contract_id,
                    VaultOperationHistory.operation_type == OperationType.SWAP,
                    VaultOperationHistory.created_at >= day_start,
                    VaultOperationHistory.created_at <= day_end,
                )
            )
            .all()
        )

        # 计算当日交易收益
        day_swap_profit = Decimal(0)
        day_gas_cost = Decimal(0)

        for op in day_swaps + day_operations:
            if op.operation_type == OperationType.SWAP:
                input_value = op.input_token_usd_value or Decimal(0)
                output_value = op.output_token_usd_value or Decimal(0)
                day_swap_profit += output_value - input_value

            if op.total_gas_cost_usd:
                day_gas_cost += op.total_gas_cost_usd

        return {
            "vault_contract_id": vault_contract_id,
            "date": day_start.date().isoformat(),
            "start_value_usd": float(start_value),
            "end_value_usd": float(end_value),
            "day_deposits_usd": float(day_deposits),
            "day_withdrawals_usd": float(day_withdrawals),
            "daily_return_usd": float(daily_return),
            "daily_return_rate_pct": float(daily_return_rate_pct),
            "day_swap_profit_usd": float(day_swap_profit),
            "day_gas_cost_usd": float(day_gas_cost),
            "net_daily_return_usd": float(daily_return - day_gas_cost),
        }

    @staticmethod
    def get_returns_history(
        db: Session,
        vault_contract_id: int,
        days: int = 30,
        end_date: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """
        获取金库的收益历史

        Args:
            db: 数据库会话
            vault_contract_id: Vault合约ID
            days: 历史天数
            end_date: 结束日期，默认为当前时间

        Returns:
            包含收益历史数据的字典
        """
        if end_date is None:
            end_date = datetime.utcnow()

        start_date = end_date - timedelta(days=days)

        daily_returns = []
        current_date = start_date

        while current_date <= end_date:
            daily_return = VaultOperationHistoryService.calculate_daily_returns(
                db, vault_contract_id, current_date
            )

            if "status" not in daily_return or daily_return["status"] != "error":
                daily_returns.append(daily_return)

            current_date += timedelta(days=1)

        # 计算累计收益
        total_returns = VaultOperationHistoryService.calculate_total_returns(
            db, vault_contract_id, end_date
        )

        # 计算收益趋势
        daily_return_values = [day["daily_return_usd"] for day in daily_returns]
        daily_return_rates = [day["daily_return_rate_pct"] for day in daily_returns]

        avg_daily_return = (
            sum(daily_return_values) / len(daily_return_values)
            if daily_return_values
            else 0
        )
        avg_daily_return_rate = (
            sum(daily_return_rates) / len(daily_return_rates)
            if daily_return_rates
            else 0
        )

        return {
            "vault_contract_id": vault_contract_id,
            "period_days": days,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "daily_returns": daily_returns,
            "total_returns": total_returns,
            "avg_daily_return_usd": float(avg_daily_return),
            "avg_daily_return_rate_pct": float(avg_daily_return_rate),
            "daily_return_trend": daily_return_values,
        }


if __name__ == "__main__":
    from tradingflow.depot.db import db_session

    # 测试代码
    with db_session() as db:
        # 计算累计收益
        total_returns = VaultOperationHistoryService.calculate_total_returns(
            db=db, vault_contract_id=1
        )

        # 计算昨日收益
        yesterday_returns = VaultOperationHistoryService.calculate_daily_returns(
            db=db, vault_contract_id=1
        )

        # 获取30天收益历史
        returns_history = VaultOperationHistoryService.get_returns_history(
            db=db, vault_contract_id=1, days=30
        )
