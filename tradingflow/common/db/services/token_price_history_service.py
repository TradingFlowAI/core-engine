import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

from sqlalchemy import and_, desc
from sqlalchemy.orm import Session

from tradingflow.common.constants import EVM_CHAIN_ID_NETWORK_MAP
from tradingflow.common.config import CONFIG
from tradingflow.common.db.models.token_price_history import TokenPriceHistory
from tradingflow.common.logging_config import setup_logging

# Setup logging
setup_logging(CONFIG, "token_price_service")
logger = logging.getLogger(__name__)


class TokenPriceHistoryService:
    """代币价格历史服务"""

    @staticmethod
    def batch_insert_records(
        db: Session,
        price_data: Dict,
        timestamp: datetime,
    ) -> int:
        """
        批量保存价格记录

        Args:
            db: 数据库会话
            price_data: 价格数据，格式：
                {
                    "evm": {network_id: {token_address: price_info}},
                    "non_evm": {network_name: {token_address: price_info}}
                }
            timestamp: 价格时间戳

        Returns:
            保存的记录数量
        """
        try:
            price_records = []

            # 处理EVM网络
            for network_id, token_prices in price_data.get("evm", {}).items():
                if isinstance(token_prices, dict) and "error" not in token_prices:
                    try:
                        chain_id = int(network_id)

                        # 验证这确实是一个已知的EVM链ID
                        if chain_id not in EVM_CHAIN_ID_NETWORK_MAP:
                            logger.warning(f"未知的EVM链ID: {chain_id}，跳过处理")
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
                        logger.warning(f"无效的链ID格式: {network_id}，跳过处理")
                        continue

            # 处理非EVM网络
            for network_name, token_prices in price_data.get("non_evm", {}).items():
                if isinstance(token_prices, dict) and "error" not in token_prices:

                    for token_address, price_info in token_prices.items():
                        token_address_lower = token_address.lower()

                        price_record = TokenPriceHistory(
                            network=network_name,
                            chain_id=None,  # 非EVM网络不使用chain_id
                            token_address=token_address_lower,
                            price_usd=float(price_info.get("price_usd", 0)),
                            source=price_info.get("source", "unknown"),
                            timestamp=timestamp,
                        )
                        price_records.append(price_record)

            # 批量插入
            if price_records:
                db.bulk_save_objects(price_records)
                db.commit()
                logger.info(f"成功保存 {len(price_records)} 条价格记录")
                return len(price_records)
            else:
                logger.warning("没有价格记录需要保存")
                return 0

        except Exception as e:
            logger.error(f"批量保存价格记录失败: {e}")
            db.rollback()
            raise

    @staticmethod
    def get_latest_price(
        db: Session, network: str, token_address: str, chain_id: Optional[int] = None
    ) -> Optional[TokenPriceHistory]:
        """获取指定代币的最新价格记录"""
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
        """获取指定代币的价格历史"""
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
    def cleanup_old_records(db: Session, days_to_keep: int = 30) -> int:
        """清理旧的价格记录"""
        cutoff_date = datetime.now() - timedelta(days=days_to_keep)

        deleted_count = (
            db.query(TokenPriceHistory)
            .filter(TokenPriceHistory.timestamp < cutoff_date)
            .delete()
        )

        db.commit()
        logger.info(f"清理了 {deleted_count} 条 {days_to_keep} 天前的价格记录")
        return deleted_count

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
        获取距离指定时间戳最近的代币价格记录

        Args:
            db: 数据库会话
            network: 网络标识符
            token_address: 代币地址
            target_timestamp: 目标时间戳
            chain_id: 链ID（可选，用于EVM网络）
            max_time_diff_minutes: 最大时间差（分钟），超过此时间差的记录将被忽略

        Returns:
            距离目标时间戳最近的价格记录，如果没有找到或时间差过大则返回None
        """
        try:

            # 确保target_timestamp有时区信息
            if target_timestamp.tzinfo is None:
                # 如果没有时区信息，假设为UTC
                target_timestamp = target_timestamp.replace(tzinfo=timezone.utc)

            # 构建基础查询
            query = db.query(TokenPriceHistory).filter(
                and_(
                    TokenPriceHistory.network == network,
                    TokenPriceHistory.token_address == token_address.lower(),
                )
            )

            # 如果指定了chain_id，添加过滤条件
            if chain_id is not None:
                query = query.filter(TokenPriceHistory.chain_id == chain_id)

            # 计算时间范围（目标时间前后max_time_diff_minutes分钟）
            time_range = timedelta(minutes=max_time_diff_minutes)
            start_time = target_timestamp - time_range
            end_time = target_timestamp + time_range

            # 在时间范围内查找记录
            query = query.filter(
                and_(
                    TokenPriceHistory.timestamp >= start_time,
                    TokenPriceHistory.timestamp <= end_time,
                )
            )

            # 获取所有符合条件的记录
            records = query.all()

            if not records:
                logger.debug(
                    f"未找到网络 {network} 代币 {token_address} 在时间 {target_timestamp} "
                    f"附近 {max_time_diff_minutes} 分钟内的价格记录"
                )
                return None

            # 找到时间差最小的记录
            def get_time_diff(record):
                record_timestamp = record.timestamp
                # 确保两个时间戳都有时区信息
                if record_timestamp.tzinfo is None:
                    record_timestamp = record_timestamp.replace(tzinfo=timezone.utc)
                return abs((record_timestamp - target_timestamp).total_seconds())

            closest_record = min(records, key=get_time_diff)

            # 计算实际时间差
            record_timestamp = closest_record.timestamp
            if record_timestamp.tzinfo is None:
                record_timestamp = record_timestamp.replace(tzinfo=timezone.utc)

            time_diff = abs((record_timestamp - target_timestamp).total_seconds())
            time_diff_minutes = time_diff / 60

            logger.debug(
                f"找到网络 {network} 代币 {token_address} 距离目标时间 {target_timestamp} "
                f"最近的价格记录，时间差: {time_diff_minutes:.2f} 分钟，价格: ${closest_record.price_usd}"
            )

            return closest_record

        except Exception as e:
            logger.error(f"查询距离时间戳最近的价格记录失败: {e}")
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
        获取指定时间点的代币价格（简化版本，只返回价格数值）

        Args:
            db: 数据库会话
            network: 网络标识符
            token_address: 代币地址
            target_timestamp: 目标时间戳
            chain_id: 链ID（可选）
            tolerance_minutes: 容忍的时间差（分钟）

        Returns:
            价格数值，如果未找到则返回None
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


if __name__ == "__main__":
    # 示例用法
    from tradingflow.common.config import CONFIG
    from tradingflow.common.db.base import db_session
    from tradingflow.common.logging_config import setup_logging

    setup_logging(CONFIG)

    with db_session() as session:
        service = TokenPriceHistoryService()
        closet_price = service.get_price_at_timestamp(
            db=session,
            network="aptos",  # 示例网络
            token_address="0xa",
            target_timestamp=datetime(2025, 6, 3, 12, 0, 0, tzinfo=timezone.utc),
            tolerance_minutes=10000,
        )

        logger.info("Closest price: %s", closet_price)
