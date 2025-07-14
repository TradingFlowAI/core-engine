import logging
import traceback
from decimal import Decimal

from sqlalchemy.orm import Session

from tradingflow.depot.config import CONFIG
from tradingflow.depot.db.services.vault_contract_service import VaultContractService
from tradingflow.depot.db.services.vault_vaule_history_service import (
    VaultValueHistoryService,
)
from tradingflow.depot.logging_config import setup_logging
from tradingflow.depot.utils import eth_util

# Setup logging
setup_logging(CONFIG, "vault_value_update")
logger = logging.getLogger(__name__)


def get_vault_network_info(vault):
    """
    根据vault对象获取网络信息

    Args:
        vault: VaultContract对象

    Returns:
        tuple: (network, network_type)
    """
    from tradingflow.depot.constants import get_network_info_by_name, EVM_CHAIN_ID_NETWORK_MAP

    # 优先使用vault对象中的network字段（如果存在）
    if hasattr(vault, 'network') and vault.network:
        try:
            # 使用标准化网络信息函数
            network, network_type = get_network_info_by_name(vault.network)
            return network, network_type
        except Exception as e:
            logger.warning("使用vault.network字段获取网络信息失败: %s", e)

    # 回退到从chain_id推断网络
    if vault.chain_id and vault.chain_id in EVM_CHAIN_ID_NETWORK_MAP:
        network = EVM_CHAIN_ID_NETWORK_MAP[vault.chain_id]
        network_type = "evm"
        return network, network_type

    # 最后的回退方案
    logger.warning("无法确定vault %s (chain_id=%s)的网络类型，默认使用EVM",
                  vault.contract_address, vault.chain_id)
    network = f"chain_{vault.chain_id}" if vault.chain_id else "unknown"
    network_type = "evm"

    return network, network_type


async def update_vault_values_async(db: Session):
    """
    更新所有Vault的USD价值记录的异步实现

    Args:
        db: 数据库会话

    Returns:
        更新结果统计信息
    """
    try:
        logger.info("开始更新Vault价值记录")

        # 使用list_vaults函数获取所有Vault
        vaults = VaultContractService.list_vaults(db)

        success_count = 0
        error_count = 0

        for vault in vaults:
            try:
                # 使用延迟计算的方式记录日志中的合约地址
                vault_addr = vault.contract_address

                # 获取网络信息
                network, network_type = get_vault_network_info(vault)

                # 通过API获取Vault当前价值，传入网络信息
                vault_data = eth_util.fetch_vault_data_from_api(
                    vault.chain_id, vault_addr, network=network, network_type=network_type
                )

                if vault_data and "total_value_usd" in vault_data:
                    # 创建价值记录 - 只传递模型中存在的字段
                    VaultValueHistoryService.create_value_record(
                        db=db,
                        vault_contract_id=vault.id,
                        chain_id=vault.chain_id,
                        vault_address=vault_addr,
                        total_value_usd=Decimal(str(vault_data["total_value_usd"])),
                    )

                    success_count += 1
                    # 使用延迟计算模式，只有当日志级别为INFO时才构造消息字符串
                    logger.info("成功更新Vault %s 的价值记录 (网络: %s)", vault_addr, network)
                else:
                    error_count += 1
                    # 使用延迟计算模式
                    logger.warning("无法获取Vault %s 的价值数据 (网络: %s)", vault_addr, network)

            except Exception as e:
                error_count += 1
                # 使用延迟计算模式
                logger.error(traceback.format_exc())
                logger.error("更新Vault %s 价值记录时出错: %s", vault_addr, e)

        # 使用延迟计算模式记录汇总信息
        logger.info(
            "Vault价值更新完成 - 成功: %d, 失败: %d", success_count, error_count
        )

        return {
            "status": "completed",
            "success_count": success_count,
            "error_count": error_count,
        }

    except Exception as e:
        # 异常处理中也使用延迟计算模式
        logger.exception("更新Vault价值记录任务失败: %s", e)
        return {"status": "error", "error": str(e)}


def update_single_vault_value(db: Session, vault_contract_id: int):
    """
    更新单个Vault的USD价值记录

    Args:
        db: 数据库会话
        vault_contract_id: Vault合约ID

    Returns:
        更新结果
    """
    try:
        # 获取指定的Vault
        vault = VaultContractService.get_vault_by_id(db, vault_contract_id)

        if not vault:
            logger.warning("找不到ID为 %s 的Vault", vault_contract_id)
            return {"status": "error", "error": "找不到指定的Vault"}

        # 通过API获取Vault当前价值
        vault_addr = vault.contract_address

        # 获取网络信息
        network, network_type = get_vault_network_info(vault)

        vault_data = eth_util.fetch_vault_data_from_api(
            vault.chain_id, vault_addr, network=network, network_type=network_type
        )

        if not vault_data or "value_usd" not in vault_data:
            logger.warning("无法获取Vault %s 的价值数据 (网络: %s)", vault_addr, network)
            return {"status": "error", "error": "无法获取Vault价值数据"}

        # 创建价值记录
        value_record = VaultValueHistoryService.create_value_record(
            db=db,
            vault_contract_id=vault.id,
            chain_id=vault.chain_id,
            vault_address=vault_addr,
            total_value_usd=Decimal(str(vault_data["value_usd"])),
        )

        logger.info("成功更新Vault %s 的价值记录 (网络: %s)", vault_addr, network)

        return {
            "status": "success",
            "value_record_id": value_record.id,
            "vault_address": vault_addr,
            "value_usd": float(vault_data["value_usd"]),
            "network": network,
            "network_type": network_type,
        }

    except Exception as e:
        logger.exception("更新单个Vault价值记录失败: %s", e)
        return {"status": "error", "error": str(e)}


if __name__ == "__main__":
    # 测试代码
    import asyncio

    from tradingflow.depot.db.base import db_session

    with db_session() as db:
        asyncio.run(update_vault_values_async(db))
