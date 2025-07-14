import json
import logging
from datetime import datetime
from decimal import Decimal

from redis import Redis

from tradingflow.bank.services.vault_service import VaultService
from tradingflow.depot.db import db_session
from tradingflow.depot.db.models.monitored_contract import MonitoredContract
from tradingflow.depot.db.services.monitored_token_service import MonitoredTokenService
from tradingflow.depot.db.services.user_service import UserService
from tradingflow.depot.db.services.vault_contract_service import VaultContractService
from tradingflow.depot.exceptions.tf_exception import (
    ResourceNotFoundException,
    DuplicateResourceException,
)


logger = logging.getLogger(__name__)


def deploy_vault_async(
    task_id,
    chain_id,
    asset_address,
    investor_address,
    vault_name=None,
    vault_symbol=None,
    redis_config=None,
):
    """
    部署Vault合约的异步实现

    Args:
        task_id: 任务ID
        chain_id: 链ID
        asset_address: 资产代币地址
        investor_address: 投资者地址
        vault_name: Vault名称
        vault_symbol: Vault代币符号
        redis_config: Redis配置字典

    Returns:
        部署结果信息
    """
    try:
        logger.info("开始部署Vault: %s, 链ID: %s", task_id, chain_id)

        # 更新任务状态为处理中
        _update_task_status(task_id, "processing", redis_config)

        # 初始化Vault服务
        vault_service = VaultService.get_instance(chain_id)
        # 部署Vault合约
        deployment_info = vault_service.deploy_vault(
            asset_address=asset_address,
            investor_address=investor_address,
            vault_name=vault_name,
            vault_symbol=vault_symbol,
        )

        # 写入PostgreSQL数据库 (监控合约)
        _save_vault_to_monitored_contracts(
            chain_id=chain_id,
            vault_address=deployment_info.get("vault_address"),
            vault_name=deployment_info.get("vault_name", vault_name or "未命名Vault"),
            network=deployment_info.get("network"),
        )

        # 检查或创建用户记录
        _create_user_if_not_exists(investor_address)

        # 写入到vault_contract表
        _save_vault_to_vault_contracts(
            chain_id=chain_id,
            asset_address=asset_address,
            investor_address=investor_address,
            deployer_address=deployment_info.get("deployer_address"),
            vault_address=deployment_info.get("vault_address"),
            vault_name=deployment_info.get("vault_name", vault_name),
            vault_symbol=deployment_info.get("vault_symbol", vault_symbol),
            transaction_hash=deployment_info.get("transaction_hash"),
            deployment_block=deployment_info.get("block_number"),
            deployment_cost_eth=deployment_info.get("total_cost_eth"),
            network=deployment_info.get("network"),
        )

        # 添加资产代币到价格监控列表 - 传入网络信息
        # 根据chain_id推断网络信息
        from tradingflow.depot.constants import EVM_CHAIN_ID_NETWORK_MAP
        network = None
        if chain_id in EVM_CHAIN_ID_NETWORK_MAP:
            network = EVM_CHAIN_ID_NETWORK_MAP[chain_id]

        _add_asset_to_price_monitoring(chain_id, asset_address, network=network)

        # 更新任务状态为已完成
        _update_task_status(task_id, "completed", redis_config, result=deployment_info)

        logger.info(
            "Vault部署完成: %s, 地址: %s", task_id, deployment_info.get("vault_address")
        )

        return {"status": "success", "deployment_info": deployment_info}

    except Exception as e:
        logger.exception("Vault部署失败: %s", e)
        # 更新任务状态为失败
        _update_task_status(task_id, "failed", redis_config, error=str(e))
        return {"status": "error", "error": str(e)}


def _create_user_if_not_exists(wallet_address):
    """检查用户是否存在，如果不存在则创建新用户"""
    try:
        with db_session() as db:
            user_service = UserService(db)
            # 尝试创建用户，如果已存在则返回现有用户
            user = user_service.create_user(wallet_address=wallet_address)
            if user.created_at == user.updated_at:  # 简单判断是否是刚创建的
                logger.info(
                    "为投资者 %s 创建了新用户记录, ID: %s", wallet_address, user.id
                )
            else:
                logger.info("投资者 %s 已存在, 用户 ID: %s", wallet_address, user.id)
    except Exception as e:
        logger.error("检查或创建用户 %s 时出错: %s", wallet_address, e, exc_info=True)


def _save_vault_to_monitored_contracts(chain_id, vault_address, vault_name, network=None):
    """将部署的Vault保存到数据库中以便监控

    Args:
        chain_id: 链ID
        vault_address: Vault地址
        vault_name: Vault名称
        network: 网络名称（如：'ethereum', 'flow-evm'等）
    """
    try:
        # 使用重构后的db_session上下文管理器
        with db_session() as db:
            # 确保地址格式一致（小写）
            vault_address_lower = vault_address.lower() if vault_address else None
            if not vault_address_lower:
                logger.warning("Vault地址为空，无法保存到监控列表")
                return

            # 获取网络信息
            from tradingflow.depot.constants import EVM_CHAIN_ID_NETWORK_MAP

            # 如果没有提供network，尝试从chain_id推断
            if not network:
                if chain_id in EVM_CHAIN_ID_NETWORK_MAP:
                    network = EVM_CHAIN_ID_NETWORK_MAP[chain_id]
                else:
                    logger.warning("无法确定chain_id %s对应的网络，使用unknown", chain_id)
                    network = f"chain_{chain_id}"

            # 创建新的监控合约记录
            contract_id = f"{network}_{chain_id}_vault_{vault_address_lower}"
            new_contract = MonitoredContract(
                id=contract_id,
                contract_address=vault_address_lower,
                network=network,
                chain_id=chain_id,
                contract_type="Vault",  # 合约类型
                abi_name="Vault",  # ABI名称，对应common/config.py中的ABI_PATHS键
            )

            # 添加到数据库
            db.add(new_contract)

            # 会话上下文管理器会自动commit
            logger.info(
                "已将Vault %s (%s)添加到监控列表，网络：%s", vault_name, vault_address_lower, network
            )

    except Exception as e:
        logger.error("保存Vault到数据库失败: %s", e, exc_info=True)
        # 不抛出异常，避免影响主流程


def _update_task_status(task_id, status, redis_config=None, result=None, error=None):
    """更新Redis中的任务状态"""
    # 默认Redis配置
    if redis_config is None:
        import os

        redis_config = {
            "host": os.environ.get("REDIS_HOST", "localhost"),
            "port": int(os.environ.get("REDIS_PORT", 6379)),
            "db": int(os.environ.get("REDIS_DB", 0)),
            "password": os.environ.get("REDIS_PASSWORD", None),
        }

    redis_client = Redis(
        host=redis_config.get("host", "localhost"),
        port=redis_config.get("port", 6379),
        db=redis_config.get("db", 0),
        password=redis_config.get("password"),
        decode_responses=True,
    )

    # 获取现有任务信息
    task_key = f"vault_task:{task_id}"
    task_info_json = redis_client.get(task_key)
    task_info = json.loads(task_info_json) if task_info_json else {}

    # 更新状态
    task_info["status"] = status
    task_info["updated_at"] = datetime.now().isoformat()

    if result is not None:
        task_info["result"] = result

    if error is not None:
        task_info["error"] = error

    # 保存回Redis，设置24小时过期
    try:
        redis_client.setex(task_key, 86400, json.dumps(task_info))
    except TypeError as e:
        logger.error(
            "序列化任务状态到Redis时出错 (%s): %s. Task Info: %s",
            task_key,
            e,
            task_info,
        )
        # 可以尝试移除无法序列化的字段或进行转换
        if "result" in task_info:
            logger.warning("尝试移除 result 字段后重试 Redis setex")
            task_info.pop("result", None)  # 移除 result 字段
            try:
                redis_client.setex(task_key, 86400, json.dumps(task_info))
            except Exception as inner_e:
                logger.error("移除 result 后仍然无法保存到 Redis: %s", inner_e)


def _save_vault_to_vault_contracts(
    chain_id,
    asset_address,
    investor_address,
    deployer_address,
    vault_address,
    vault_name,
    vault_symbol,
    transaction_hash=None,
    deployment_block=None,
    deployment_cost_eth=None,
    network=None,
):
    """将部署的Vault保存到vault_contracts表中"""
    try:
        # 使用db_session上下文管理器
        with db_session() as db:
            # 确保地址格式一致（小写）
            vault_address_lower = vault_address.lower() if vault_address else None
            asset_address_lower = asset_address.lower() if asset_address else None
            investor_address_lower = (
                investor_address.lower() if investor_address else None
            )
            deployer_address_lower = (
                deployer_address.lower() if deployer_address else None
            )

            if not vault_address_lower:
                logger.warning("Vault地址为空，无法保存到vault_contracts表")
                return

            # 获取网络信息
            from tradingflow.depot.constants import EVM_CHAIN_ID_NETWORK_MAP

            # 如果没有提供network，尝试从chain_id推断
            if not network:
                if chain_id in EVM_CHAIN_ID_NETWORK_MAP:
                    network = EVM_CHAIN_ID_NETWORK_MAP[chain_id]
                else:
                    logger.warning("无法确定chain_id %s对应的网络，使用unknown", chain_id)
                    network = f"chain_{chain_id}"

            # 准备Vault合约数据
            vault_data = {
                "contract_address": vault_address_lower,
                "network": network,
                "chain_id": chain_id,
                "asset_address": asset_address_lower,
                "investor_address": investor_address_lower,
                "deployer_address": deployer_address_lower,
                "vault_name": vault_name,
                "vault_symbol": vault_symbol,
                "transaction_hash": transaction_hash,
                "deployment_block": deployment_block,
                "deployment_cost_eth": (
                    Decimal(str(deployment_cost_eth)) if deployment_cost_eth else None
                ),
            }

            logger.info(
                "准备保存Vault %s (%s) 到vault_contracts表，网络：%s",
                vault_name,
                vault_address_lower,
                network,
            )
            # log vault_data
            logger.debug("Vault数据: %s", vault_data)

            try:
                # 使用VaultService创建新的Vault合约记录
                vault = VaultContractService.create_vault(db, vault_data)
                logger.info(
                    "已将Vault %s (%s) 保存到vault_contracts表，ID: %s",
                    vault_name,
                    vault_address_lower,
                    vault.id,
                )
                return vault
            except Exception as e:
                logger.error("保存Vault到vault_contracts表失败: %s", e, exc_info=True)
                # 检查是否是重复记录的异常
                if isinstance(e, DuplicateResourceException):
                    logger.warning(
                        "Vault %s 在网络 %s 链 %s 上已存在记录", vault_address_lower, network, chain_id
                    )
                    # 尝试获取已存在的记录
                    try:
                        existing_vault = VaultContractService.get_vault_by_address(
                            db, vault_address_lower, chain_id
                        )
                        logger.info("找到已存在的Vault记录，ID: %s", existing_vault.id)
                        return existing_vault
                    except Exception as inner_e:
                        logger.error("尝试获取已存在的Vault记录时出错: %s", inner_e)
                return None

    except Exception as e:
        logger.error("保存Vault到vault_contracts表过程中出错: %s", e, exc_info=True)
        # 不抛出异常，避免影响主流程
        return None


def _add_asset_to_price_monitoring(chain_id, asset_address, network=None):
    """将Vault的资产代币添加到价格监控列表

    Args:
        chain_id: 链ID
        asset_address: 资产代币地址
        network: 网络名称（如：'aptos', 'ethereum', 'flow-evm'等）
    """
    if not asset_address:
        logger.warning("资产地址为空，无法添加至价格监控")
        return

    try:
        # 规范化地址（转为小写）
        asset_address = asset_address.lower()

        # 获取网络信息
        from tradingflow.depot.constants import get_network_info_by_name, EVM_CHAIN_ID_NETWORK_MAP

        # 如果没有提供network，尝试从chain_id推断
        if not network:
            if chain_id in EVM_CHAIN_ID_NETWORK_MAP:
                network = EVM_CHAIN_ID_NETWORK_MAP[chain_id]
                network_type = "evm"
            else:
                logger.warning("无法确定chain_id %s对应的网络，跳过添加代币监控", chain_id)
                return
        else:
            # 根据网络名称获取标准化的网络信息
            network, network_type = get_network_info_by_name(network)

        logger.info("添加资产 %s (网络: %s, 类型: %s) 至价格监控列表",
                   asset_address, network, network_type)

        # 准备添加代币的数据
        token_data = {
            "network": network,
            "network_type": network_type,
            "token_address": asset_address,
            "is_active": True,
        }

        # 对于EVM网络，也包含chain_id
        if network_type == "evm":
            token_data["chain_id"] = chain_id

        # 先检查是否已经存在该代币
        with db_session() as db:
            try:
                # 使用正确的方法查询代币
                existing_token = MonitoredTokenService.get_token_by_address(
                    db, asset_address, network
                )
                logger.info(
                    "资产 %s 已存在于监控列表中，ID: %s",
                    asset_address,
                    existing_token.id,
                )
                # 已经存在，无需再添加
                return
            except ResourceNotFoundException:
                # 不存在，需要添加
                pass
            except Exception as e:
                logger.error("查询代币时出错: %s", e)
                raise

        # 使用requests库发送同步HTTP请求
        try:
            import requests

            from tradingflow.depot.config import CONFIG

            # 获取API URL（从配置中）
            account_manager_host = CONFIG.get("ACCOUNT_MANAGER_HOST", "localhost")
            account_manager_port = CONFIG.get("ACCOUNT_MANAGER_PORT", 7001)
            api_base_url = f"http://{account_manager_host}:{account_manager_port}"

            # 调用API添加代币
            url = f"{api_base_url}/price/tokens"
            headers = {"Content-Type": "application/json"}

            logger.info(
                "发送请求: [URL] %s, [Headers] %s, [Data] %s",
                url,
                headers,
                json.dumps(token_data),
            )

            # 发送同步POST请求
            response = requests.post(url, json=token_data, headers=headers, timeout=10)

            # 处理响应
            if response.status_code == 200:
                result = response.json()
                if result.get("success"):
                    logger.info("通过API成功添加代币到监控列表: %s", asset_address)
                    return True
                else:
                    logger.warning(
                        "通过API添加代币到监控列表失败: %s",
                        result.get("error", "未知错误"),
                    )
            else:
                logger.warning(
                    "API请求失败，状态码: %d, 响应内容: %s",
                    response.status_code,
                    response.text,
                )
        except requests.exceptions.Timeout:
            logger.error("API请求超时")
        except requests.exceptions.RequestException as e:
            logger.error("API请求异常: %s", e)
        except Exception as e:
            logger.error("添加代币通过API时出错: %s", e)

    except Exception as e:
        logger.error("添加资产到价格监控列表时出错: %s", e, exc_info=True)
        # 不抛出异常，避免影响主流程
