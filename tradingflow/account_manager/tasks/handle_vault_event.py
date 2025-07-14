import logging
from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Optional, Tuple

from sqlalchemy.orm import Session

from tradingflow.bank.common.logging_config import setup_logging
from tradingflow.bank.utils.token_price_util import get_token_price_usd
from tradingflow.depot.config import CONFIG
from tradingflow.depot.constants import EVM_CHAIN_ID_NETWORK_MAP, ALL_CHAIN_NETWORK_MAP
from tradingflow.depot.db.models.event import ContractEvent
from tradingflow.depot.db.models.vault_operation_history import OperationType
from tradingflow.depot.db.services.contract_event_service import ContractEventService
from tradingflow.depot.db.services.vault_contract_service import VaultContractService
from tradingflow.depot.db.services.vault_operation_history_service import (
    VaultOperationHistoryService,
)
from tradingflow.depot.db.services.monitored_token_service import MonitoredTokenService
from tradingflow.depot.exceptions.tf_exception import ResourceNotFoundException
from tradingflow.depot.utils import eth_util

if TYPE_CHECKING:
    from tradingflow.depot.db.models.vault_operation_history import (
        VaultOperationHistory,
    )

# Setup logging
setup_logging(CONFIG)
logger = logging.getLogger(__name__)

# 全局缓存，减少数据库查询
token_price_cache = {}  # {token_address: {"price": price, "timestamp": timestamp}}
token_decimals_cache = {}  # {token_address: decimals}


def get_network_info(event: ContractEvent) -> tuple[str, str]:
    """
    获取事件的网络信息

    Returns:
        tuple: (network, network_type)
    """
    # 首先根据网络名称判断类型
    network_name = event.network.lower() if event.network else ""

    # 判断网络类型
    if network_name in ["aptos"]:
        # Aptos网络
        network_type = "aptos"
        network = "aptos"
    elif network_name in ["sui", "sui-network"]:
        # Sui网络
        network_type = "sui"
        network = "sui-network"
    elif network_name in ["solana"]:
        # Solana网络
        network_type = "solana"
        network = "solana"
    elif network_name in ["flow-evm", "flow_evm"]:
        # Flow EVM网络
        network_type = "evm"
        network = "flow-evm"
    else:
        # 默认为EVM网络，使用chain_id映射
        network_type = "evm"
        if event.chain_id and event.chain_id in EVM_CHAIN_ID_NETWORK_MAP:
            network = EVM_CHAIN_ID_NETWORK_MAP.get(event.chain_id)
        else:
            # 如果chain_id不在映射中，使用原始网络名称
            network = event.network or f"unknown_chain_{event.chain_id}"

    return network, network_type


async def auto_discover_tokens_from_event(db: Session, event: ContractEvent):
    """从合约事件中自动发现并添加代币到监控列表"""
    try:
        # 获取网络信息 - 改进的网络映射逻辑
        network, network_type = get_network_info(event)

        # 从事件参数中提取可能的代币地址
        token_addresses = set()

        if event.event_name == "UserDeposit":
            if "token" in event.parameters:
                token_addresses.add(event.parameters["token"])
            if "asset_metadata" in event.parameters:
                # Aptos资产元数据格式
                asset_metadata = event.parameters["asset_metadata"]
                if isinstance(asset_metadata, dict) and "inner" in asset_metadata:
                    token_addresses.add(asset_metadata["inner"])

        elif event.event_name == "TradeSignal":
            # 添加交易涉及的代币
            if "fromToken" in event.parameters:
                token_addresses.add(event.parameters["fromToken"])
            if "toToken" in event.parameters:
                token_addresses.add(event.parameters["toToken"])

            # Aptos格式
            if "from_asset_metadata" in event.parameters:
                asset_metadata = event.parameters["from_asset_metadata"]
                if isinstance(asset_metadata, dict) and "inner" in asset_metadata:
                    token_addresses.add(asset_metadata["inner"])
            if "to_asset_metadata" in event.parameters:
                asset_metadata = event.parameters["to_asset_metadata"]
                if isinstance(asset_metadata, dict) and "inner" in asset_metadata:
                    token_addresses.add(asset_metadata["inner"])

        # 为每个发现的代币地址检查是否需要添加到监控列表
        for token_address in token_addresses:
            if token_address and token_address != "0x0":
                try:
                    # 检查是否已经在监控列表中
                    existing_token = MonitoredTokenService.get_token_by_address(
                        db, token_address, network
                    )
                    # 如果已存在，跳过添加
                    logger.debug(
                        "代币 %s 已存在于监控列表中，跳过添加",
                        token_address
                    )
                except ResourceNotFoundException:
                    # 代币不存在，添加新的监控代币
                    try:
                        token_data = {
                            "network": network,
                            "network_type": network_type,
                            "chain_id": event.chain_id,
                            "token_address": token_address,
                            "is_active": True
                        }
                        MonitoredTokenService.create_token(db=db, token_data=token_data)
                        logger.info(
                            "自动添加新代币到监控列表: %s on %s (chain_id: %s, type: %s)",
                            token_address, network, event.chain_id, network_type
                        )
                    except Exception as add_error:
                        logger.warning(
                            "添加代币到监控列表失败: %s - %s",
                            token_address, str(add_error)
                        )
                except Exception as query_error:
                    logger.warning(
                        "查询代币监控状态失败: %s - %s",
                        token_address, str(query_error)
                    )

    except Exception as e:
        logger.error("自动发现代币时出错: %s", e, exc_info=True)


async def create_vault_from_event_data(
    db: Session,
    event: ContractEvent,
    vault_address: str = None,
    user_address: str = None,
    vault_type: str = "Vault"
) -> Optional[Any]:
    """
    从事件数据创建vault的通用函数

    Args:
        db: 数据库会话
        event: 合约事件
        vault_address: vault地址（如果为None则使用event.contract_address）
        user_address: 用户地址（如果为None则从event中提取）
        vault_type: vault类型（"Vault" 或 "BalanceManager"）

    Returns:
        创建的vault对象，失败返回None
    """
    try:
        # 获取网络信息
        network, network_type = get_network_info(event)

        # 确定vault地址
        if vault_address is None:
            vault_address = event.contract_address

        # 智能获取用户地址
        if user_address is None:
            if "user" in event.parameters:
                user_address = event.parameters["user"]
            elif event.user_address:
                user_address = event.user_address
            else:
                user_address = vault_address  # 使用vault地址作为默认

        # 提取资产地址（支持不同网络格式）
        asset_address = None
        if network_type == "aptos":
            # Aptos格式：asset_metadata.inner
            if "asset_metadata" in event.parameters:
                asset_metadata = event.parameters["asset_metadata"]
                if isinstance(asset_metadata, dict) and "inner" in asset_metadata:
                    raw_asset_address = asset_metadata["inner"]
                    # 验证地址格式：过滤掉以太坊地址
                    if not (raw_asset_address.startswith("0x") and len(raw_asset_address) == 42):
                        asset_address = raw_asset_address
        elif network_type in ["evm", "ethereum", "flow-evm"]:
            # EVM格式：fromToken和toToken
            # 优先使用toToken（目标代币），如果没有则使用fromToken
            if "toToken" in event.parameters:
                asset_address = event.parameters["toToken"]
            elif "fromToken" in event.parameters:
                asset_address = event.parameters["fromToken"]

            # 过滤掉ETH的特殊地址（0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE）
            if asset_address == "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE":
                # 如果toToken是ETH，尝试使用fromToken
                if "fromToken" in event.parameters and event.parameters["fromToken"] != asset_address:
                    asset_address = event.parameters["fromToken"]
                else:
                    asset_address = None  # 都是ETH地址，设为None
        else:
            # 其他网络，先尝试通用字段
            if "asset_metadata" in event.parameters:
                asset_metadata = event.parameters["asset_metadata"]
                if isinstance(asset_metadata, dict) and "inner" in asset_metadata:
                    asset_address = asset_metadata["inner"]

        # 创建vault数据
        vault_data = {
            "contract_address": vault_address.lower(),
            "network": network,
            "chain_id": event.chain_id,
            "asset_address": asset_address.lower() if asset_address else None,
            "investor_address": user_address.lower(),
            "deployer_address": user_address.lower(),
            "vault_name": f"Auto-Created {vault_type} {vault_address[:8]}",
            "vault_symbol": f"AC{vault_type[:1]}{vault_address[:4]}",
            "transaction_hash": event.transaction_hash,
            "deployment_block": event.block_number,
        }

        # 检查是否已存在
        try:
            existing_vault = VaultContractService.get_vault_by_network_address(
                db, vault_address.lower(), network, event.chain_id
            )
            logger.info("✅ Vault %s 已存在，使用现有记录", vault_address)
            return existing_vault
        except ResourceNotFoundException:
            # 不存在，创建新记录
            vault = VaultContractService.create_vault(db, vault_data)
            logger.info("✅ 成功创建%s合约记录: %s，ID: %s", vault_type, vault_address, vault.id)

        # 添加到监控列表
        try:
            from tradingflow.depot.db.models.monitored_contract import MonitoredContract
            from sqlalchemy.exc import IntegrityError

            contract_id = f"{network}_{event.chain_id}_{vault_type.lower()}_{vault_address.lower()}"
            new_contract = MonitoredContract(
                id=contract_id,
                contract_address=vault_address.lower(),
                network=network,
                chain_id=event.chain_id,
                contract_type=vault_type,
                abi_name=vault_type,
            )
            db.add(new_contract)
            db.commit()
            logger.info("✅ 已将%s %s 添加到监控列表", vault_type, vault_address)
        except IntegrityError:
            db.rollback()
            logger.info("✅ %s %s 已在监控列表中", vault_type, vault_address)
        except Exception as monitor_error:
            logger.warning("添加%s到监控列表失败: %s", vault_type, monitor_error)

        return vault

    except Exception as e:
        logger.error("创建%s失败: %s", vault_type, e, exc_info=True)
        return None


async def process_vault_created_event(
    db: Session, event: ContractEvent
) -> Optional["VaultOperationHistory"]:
    """处理VaultCreated事件"""
    try:
        event_params = event.parameters

        # 检查必要参数
        if "user" not in event_params or "vault" not in event_params:
            error_msg = f"VaultCreated事件缺少必要参数: {event_params}"
            logger.warning(error_msg)
            ContractEventService.update_event_processed_status(
                db, event.id, False, error_msg
            )
            return None

        user_address = event_params["user"]
        vault_address = event_params["vault"]

        logger.info(
            "开始处理VaultCreated事件: user=%s, vault=%s",
            user_address, vault_address
        )

        # 使用通用创建函数
        vault = await create_vault_from_event_data(
            db, event, vault_address, user_address, "Vault"
        )

        if vault:
            # 创建用户记录
            try:
                from tradingflow.depot.db.services.user_service import UserService
                user_service = UserService(db)
                user = user_service.create_user(wallet_address=user_address.lower())
                logger.info("✅ 用户记录创建成功: %s", user_address)
            except Exception as user_error:
                logger.warning("创建用户记录失败: %s", user_error)

            status_msg = f"VaultCreated事件处理成功: user={user_address}, vault={vault_address}"
            ContractEventService.update_event_processed_status(db, event.id, True, status_msg)
            logger.info("🎉 %s", status_msg)
        else:
            status_msg = f"VaultCreated事件处理失败: 无法创建vault {vault_address}"
            ContractEventService.update_event_processed_status(db, event.id, False, status_msg)
            logger.error("❌ %s", status_msg)

        return None

    except Exception as e:
        error_msg = f"处理VaultCreated事件时发生致命错误: {e}"
        logger.error(error_msg, exc_info=True)
        ContractEventService.update_event_processed_status(db, event.id, False, error_msg)
        return None


async def process_balance_manager_created_event(
    db: Session, event: ContractEvent
) -> Optional["VaultOperationHistory"]:
    """处理BalanceManagerCreated事件（Aptos特有）"""
    try:
        event_params = event.parameters

        # 检查必要参数
        if "user" not in event_params:
            error_msg = f"BalanceManagerCreated事件缺少必要参数: {event_params}"
            logger.warning(error_msg)
            ContractEventService.update_event_processed_status(
                db, event.id, False, error_msg
            )
            return None

        user_address = event_params["user"]
        vault_address = event.contract_address

        logger.info(
            "开始处理BalanceManagerCreated事件: user=%s, vault=%s",
            user_address, vault_address
        )

        # 使用通用创建函数
        vault = await create_vault_from_event_data(
            db, event, vault_address, user_address, "BalanceManager"
        )

        if vault:
            # 创建用户记录
            try:
                from tradingflow.depot.db.services.user_service import UserService
                user_service = UserService(db)
                user = user_service.create_user(wallet_address=user_address.lower())
                logger.info("✅ 用户记录创建成功: %s", user_address)
            except Exception as user_error:
                logger.warning("创建用户记录失败: %s", user_error)

            status_msg = f"BalanceManagerCreated事件处理成功: user={user_address}, vault={vault_address}"
            ContractEventService.update_event_processed_status(db, event.id, True, status_msg)
            logger.info("🎉 %s", status_msg)
        else:
            status_msg = f"BalanceManagerCreated事件处理失败: 无法创建vault {vault_address}"
            ContractEventService.update_event_processed_status(db, event.id, False, status_msg)
            logger.error("❌ %s", status_msg)

        return None

    except Exception as e:
        error_msg = f"处理BalanceManagerCreated事件时发生致命错误: {e}"
        logger.error(error_msg, exc_info=True)
        ContractEventService.update_event_processed_status(db, event.id, False, error_msg)
        return None


async def process_user_deposit_event(
    db: Session, event: ContractEvent, vault: Any
) -> Optional["VaultOperationHistory"]:
    """处理UserDeposit事件"""
    try:
        event_params = event.parameters

        # 检查必要参数
        if "amount" not in event_params or "user" not in event_params:
            logger.warning("UserDeposit事件缺少必要参数: %s", event_params)
            return None

        # 确定代币地址
        token_address = None
        if "token" in event_params:
            # EVM格式
            token_address = event_params["token"]
        elif "asset_metadata" in event.parameters:
            # Aptos格式
            asset_metadata = event.parameters["asset_metadata"]
            if isinstance(asset_metadata, dict) and "inner" in asset_metadata:
                token_address = asset_metadata["inner"]

        if not token_address:
            logger.warning("无法确定UserDeposit事件的代币地址: %s", event_params)
            return None

        # 获取网络信息
        network, network_type = get_network_info(event)

        # 转换代币数量为USD价值
        input_amount, input_usd_value = await convert_token_amount_to_usd(
            event.chain_id,
            token_address,
            event_params["amount"],
            event.block_timestamp,
            network,
            network_type,
        )

        gas_used, gas_price, gas_cost_usd = await calculate_gas_cost_usd(
            event.chain_id, event.transaction_hash, event.block_timestamp, network, network_type
        )

        # 创建操作记录
        operation = VaultOperationHistoryService.create_operation_record(
            db=db,
            vault_contract_id=vault.id if vault else None,
            network=get_network_info(event)[0],
            network_type=get_network_info(event)[1],
            chain_id=event.chain_id,
            vault_address=event.contract_address,
            operation_type=OperationType.DEPOSIT,
            transaction_hash=event.transaction_hash,
            input_token_address=token_address,
            input_token_amount=input_amount,
            input_token_usd_value=input_usd_value,
            gas_used=gas_used,
            gas_price=gas_price,
            total_gas_cost_usd=gas_cost_usd,
        )

        # 更新事件处理状态
        ContractEventService.update_event_processed_status(
            db, event.id, True, f"已转换为操作记录: {operation.id}"
        )

        logger.info(
            "处理UserDeposit事件成功: tx=%s, value=$%s, gas=$%s",
            event.transaction_hash,
            input_usd_value,
            gas_cost_usd,
        )

        return operation

    except Exception as e:
        logger.error("处理UserDeposit事件出错: %s", e, exc_info=True)
        ContractEventService.update_event_processed_status(
            db, event.id, False, f"处理出错: {str(e)}"
        )
        return None


async def process_trade_signal_event(
    db: Session, event: ContractEvent, vault: Any
) -> Optional["VaultOperationHistory"]:
    """处理TradeSignal事件"""
    try:
        event_params = event.parameters
        network, network_type = get_network_info(event)

        # 根据网络类型适配参数
        if network_type in ["evm", "ethereum", "flow-evm"]:
            # EVM格式参数转换
            event_params = {
                "user": event_params.get("user"),
                "amount_in": event_params.get("amountIn"),  # EVM格式使用amountIn
                "amount_out": event_params.get("amountOut"),  # EVM格式使用amountOut
                "from_asset_metadata": {"inner": event_params.get("fromToken")},
                "to_asset_metadata": {"inner": event_params.get("toToken")},
                "fee_amount": event_params.get("feeAmount", "0"),
                "timestamp": event_params.get("timestamp"),
                "fee_recipient": event_params.get("feeRecipient"),
            }

        # 检查必要参数
        required_params = ["user", "amount_in", "amount_out"]
        if not all(param in event_params for param in required_params):
            logger.warning("TradeSignal事件缺少必要参数: %s", event_params)
            return None

        # 确定输入和输出代币地址
        input_token = None
        output_token = None

        if network_type in ["evm", "ethereum", "flow-evm"]:
            # EVM格式
            input_token = event_params["from_asset_metadata"]["inner"]
            output_token = event_params["to_asset_metadata"]["inner"]
        else:
            # Aptos格式
            from_meta = event_params["from_asset_metadata"]
            to_meta = event_params["to_asset_metadata"]
            if isinstance(from_meta, dict) and "inner" in from_meta:
                input_token = from_meta["inner"]
            if isinstance(to_meta, dict) and "inner" in to_meta:
                output_token = to_meta["inner"]

        if not input_token or not output_token:
            logger.warning("无法确定TradeSignal事件的代币地址: %s", event_params)
            return None

        # 转换代币数量为USD价值
        input_amount, input_usd_value = await convert_token_amount_to_usd(
            event.chain_id,
            input_token,
            event_params["amount_in"],
            event.block_timestamp,
            network,
            network_type,
        )

        output_amount, output_usd_value = await convert_token_amount_to_usd(
            event.chain_id,
            output_token,
            event_params["amount_out"],
            event.block_timestamp,
            network,
            network_type,
        )

        gas_used, gas_price, gas_cost_usd = await calculate_gas_cost_usd(
            event.chain_id, event.transaction_hash, event.block_timestamp, network, network_type
        )

        # 创建操作记录
        operation = VaultOperationHistoryService.create_operation_record(
            db=db,
            network=network,
            network_type=network_type,
            vault_contract_id=vault.id if vault else None,
            chain_id=event.chain_id,
            vault_address=event.contract_address,
            operation_type=OperationType.SWAP,
            transaction_hash=event.transaction_hash,
            input_token_address=input_token,
            input_token_amount=input_amount,
            input_token_usd_value=input_usd_value,
            output_token_address=output_token,
            output_token_amount=output_amount,
            output_token_usd_value=output_usd_value,
            gas_used=gas_used,
            gas_price=gas_price,
            total_gas_cost_usd=gas_cost_usd,
        )

        # 更新事件处理状态
        ContractEventService.update_event_processed_status(
            db, event.id, True, f"已转换为操作记录: {operation.id}"
        )

        logger.info(
            "处理TradeSignal事件成功: tx=%s, input=$%s, output=$%s, gas=$%s",
            event.transaction_hash,
            input_usd_value,
            output_usd_value,
            gas_cost_usd,
        )

        return operation

    except Exception as e:
        logger.error("处理TradeSignal事件出错: %s", e, exc_info=True)
        ContractEventService.update_event_processed_status(
            db, event.id, False, f"处理出错: {str(e)}"
        )
        return None


async def get_token_price(
    chain_id: int, token_address: str, timestamp: datetime, network: str = None, network_type: str = "evm"
) -> Decimal:
    """获取代币在特定时间的价格"""
    # 使用已实现的token_price_util中的函数，传递网络参数
    if network_type == "evm":
        price = get_token_price_usd(token_address=token_address, chain_id=chain_id, network_type=network_type)
    else:
        price = get_token_price_usd(token_address=token_address, network=network, network_type=network_type)

    if price is not None:
        return Decimal(str(price))

    logger.warning("未能获取代币价格: network=%s, chain_id=%s, token=%s", network, chain_id, token_address)
    return Decimal("0")


async def get_token_decimals(chain_id: int, token_address: str, network: str = None, network_type: str = "evm") -> int:
    """获取代币小数位数，支持多链"""
    key = f"{network_type}_{chain_id}_{token_address.lower()}"

    # 检查缓存
    if key in token_decimals_cache:
        return token_decimals_cache[key]

    try:
        if network_type == "aptos":
            # 使用Aptos工具函数
            from tradingflow.depot.utils import aptos_util
            decimals = await aptos_util.get_token_decimals(token_address)
        elif network_type in ["evm", "ethereum", "flow-evm"]:
            # 使用EVM工具函数
            token_info = await eth_util.fetch_token_info(chain_id, token_address)
            if token_info and "decimals" in token_info:
                decimals = token_info["decimals"]
            else:
                logger.warning(
                    "未能获取EVM代币小数位数: chain_id=%s, token=%s", chain_id, token_address
                )
                decimals = 18  # EVM默认18位小数
        elif network_type == "sui":
            # TODO: 实现Sui代币信息获取
            logger.warning("暂不支持Sui网络代币信息获取，使用默认值")
            decimals = 9  # Sui默认9位小数
        elif network_type == "solana":
            # TODO: 实现Solana代币信息获取
            logger.warning("暂不支持Solana网络代币信息获取，使用默认值")
            decimals = 9  # Solana默认9位小数
        else:
            logger.warning("不支持的网络类型: %s，使用EVM默认值", network_type)
            token_info = await eth_util.fetch_token_info(chain_id, token_address)
            decimals = token_info.get("decimals", 18) if token_info else 18

        # 缓存结果
        token_decimals_cache[key] = decimals
        return decimals

    except Exception as e:
        logger.error("获取代币小数位数时出错: network_type=%s, chain_id=%s, token=%s, error=%s",
                    network_type, chain_id, token_address, e)
        # 根据网络类型返回合适的默认值
        if network_type == "aptos":
            return 8
        elif network_type in ["sui", "solana"]:
            return 9
        else:
            return 18


async def convert_token_amount_to_usd(
    chain_id: int, token_address: str, amount: str, timestamp: datetime, network: str = None, network_type: str = "evm"
) -> Tuple[Decimal, Decimal]:
    """
    将代币数量转换为美元价值

    Args:
        chain_id: 链ID
        token_address: 代币地址
        amount: 代币数量（原始字符串）
        timestamp: 时间戳
        network: 网络名称
        network_type: 网络类型

    Returns:
        (代币数量, USD价值)
    """
    # 获取代币小数位数
    decimals = await get_token_decimals(chain_id, token_address, network, network_type)

    # 转换为精确数量
    try:
        token_amount = Decimal(amount) / Decimal(10**decimals)
    except Exception as e:
        logger.error("转换代币数量出错: %s, amount=%s", e, amount, exc_info=True)
        return Decimal("0"), Decimal("0")

    # 获取代币价格，传递网络参数
    token_price = await get_token_price(chain_id, token_address, timestamp, network, network_type)

    # 计算USD价值
    usd_value = token_amount * token_price

    return token_amount, usd_value


async def calculate_gas_cost_usd(
    chain_id: int, tx_hash: str, timestamp: datetime, network: str = None, network_type: str = "evm"
) -> tuple:
    """计算交易的gas成本(USD)"""
    # 获取交易的Gas成本
    gas_info = await eth_util.get_transaction_gas_info_async(chain_id, tx_hash)

    gas_used = gas_info.get("gas_used", 0) if gas_info else 0
    gas_price = Decimal(str(gas_info.get("gas_price", 0))) if gas_info else Decimal("0")

    # 计算Gas成本(USD)，传递网络参数
    native_token_price = await get_token_price(
        chain_id,
        await eth_util.get_native_token_address(chain_id),
        timestamp,
        network,
        network_type,
    )

    gas_cost_eth = Decimal(gas_used) * gas_price / Decimal(10**18)
    gas_cost_usd = gas_cost_eth * native_token_price

    return gas_used, gas_price, gas_cost_usd


async def process_vault_events_async(db: Session):
    """处理Vault链上事件的异步实现"""
    try:
        # 获取未处理的事件 - 使用新方法获取未处理事件
        events = ContractEventService.get_unprocessed_events(
            db, processor_id="vault_operation_processor", limit=100
        )

        if not events:
            logger.info("没有未处理的事件")
            return {"status": "completed", "processed_count": 0}

        logger.info("发现 %d 个未处理的事件", len(events))

        processed_count = 0
        ignored_count = 0
        error_count = 0

        for i, event in enumerate(events):
            logger.debug("===== 事件 #%d 详细信息 =====", i+1)
            logger.debug("ID: %s", event.id)
            logger.debug("交易哈希: %s", event.transaction_hash)
            logger.debug("日志索引: %s", event.log_index)
            logger.debug("区块号: %s", event.block_number)
            logger.debug("区块时间: %s", event.block_timestamp)
            logger.debug("链ID: %s", event.chain_id)
            logger.debug("合约地址: %s", event.contract_address)
            logger.debug("事件名称: %s", event.event_name)
            logger.debug("参数: %s", event.parameters)

            try:
                # 自动发现并添加代币到监控列表
                await auto_discover_tokens_from_event(db, event)

                # 获取事件对应的金库合约（对于非金库创建事件）
                vault = None
                if event.event_name != "VaultCreated" and event.event_name != "BalanceManagerCreated":
                    try:
                        vault = VaultContractService.get_vault_by_address(
                            db,
                            event.contract_address,
                            event.chain_id,
                        )
                    except ResourceNotFoundException:
                        logger.warning("未找到对应的金库合约: %s，开始自动创建", event.contract_address)

                        # 使用通用创建函数自动创建vault
                        vault = await create_vault_from_event_data(db, event, event.contract_address)

                        if vault:
                            logger.info("✅ 成功自动创建vault: %s", event.contract_address)
                        else:
                            logger.error("❌ 自动创建vault失败: %s", event.contract_address)
                            # 创建失败时，标记为错误
                            ContractEventService.update_event_processed_status(
                                db, event.id, False, "未找到对应的金库合约且自动创建失败"
                            )
                            error_count += 1
                            continue

                operation = None
                initial_processed_status = event.processed  # 记录处理前的状态

                # 根据实际的事件名称进行处理
                try:
                    if event.event_name == "UserDeposit":
                        operation = await process_user_deposit_event(db, event, vault)
                    elif event.event_name == "TradeSignal":
                        operation = await process_trade_signal_event(db, event, vault)
                    elif event.event_name == "VaultCreated":
                        operation = await process_vault_created_event(db, event)
                    elif event.event_name == "BalanceManagerCreated":
                        operation = await process_balance_manager_created_event(db, event)
                    else:
                        # 不关心的事件类型，标记为已处理但忽略
                        ContractEventService.update_event_processed_status(
                            db, event.id, True, f"忽略不相关的事件类型: {event.event_name}"
                        )
                        ignored_count += 1
                        continue

                except Exception as process_error:
                    logger.error("处理事件 %s 时出错: %s", event.id, process_error, exc_info=True)
                    # 确保事件状态被标记为失败
                    try:
                        ContractEventService.update_event_processed_status(
                            db, event.id, False, f"处理出错: {str(process_error)}"
                        )
                    except Exception as status_error:
                        logger.error("更新事件状态时出错: %s", status_error, exc_info=True)
                    error_count += 1
                    continue

                # 检查事件是否被成功处理（通过检查数据库中的processed状态）
                db.refresh(event)  # 刷新事件对象以获取最新状态
                if event.processed:
                    processed_count += 1
                    if operation:
                        logger.debug("事件 %s 处理成功并创建了操作记录", event.id)
                    else:
                        logger.debug("事件 %s 处理成功，但无需创建操作记录（如VaultCreated/BalanceManagerCreated）", event.id)
                else:
                    error_count += 1
                    logger.debug("事件 %s 处理失败", event.id)

            except Exception as e:
                # 这里只处理上述流程之外的异常（如auto_discover_tokens_from_event等）
                error_count += 1
                logger.error(
                    "处理事件过程中发生意外错误: event_id=%s, error=%s", event.id, e, exc_info=True
                )
                # 只有在事件处理流程之外的异常才在这里更新状态
                try:
                    ContractEventService.update_event_processed_status(
                        db, event.id, False, f"意外错误: {str(e)}"
                    )
                except Exception as status_error:
                    logger.error("更新事件状态时出错: %s", status_error, exc_info=True)

        logger.info(
            "事件处理完成。成功: %d, 忽略: %d, 错误: %d",
            processed_count,
            ignored_count,
            error_count,
        )

        return {
            "status": "completed",
            "processed_count": processed_count,
            "ignored_count": ignored_count,
            "error_count": error_count,
        }

    except Exception as e:
        logger.error("处理事件过程中出错: %s", e, exc_info=True)
        return {"status": "error", "error": str(e)}


if __name__ == "__main__":
    # 测试代码
    import asyncio

    from tradingflow.depot.db.base import db_session

    with db_session() as db:
        asyncio.run(process_vault_events_async(db))
