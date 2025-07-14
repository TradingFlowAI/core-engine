import logging
from datetime import datetime

from sanic import Blueprint
from sanic.response import json

from tradingflow.account_manager.common.middleware import authenticate
from tradingflow.account_manager.services.flow_evm_vault_service import FlowEvmVaultService
from tradingflow.common.db.base import db_session
from tradingflow.common.db.models.vault_operation_history import OperationType
from tradingflow.common.db.services.vault_contract_service import VaultContractService
from tradingflow.common.db.services.vault_operation_history_service import VaultOperationHistoryService
from tradingflow.common.exceptions import ResourceNotFoundException

# 创建蓝图
evm_vault_bp = Blueprint("evm_vault", url_prefix="/evm/vaults")

logger = logging.getLogger(__name__)


@evm_vault_bp.route("/<chain_id>/<vault_address>", methods=["GET"])
@authenticate
async def get_vault(request, chain_id, vault_address):
    """获取特定Flow EVM Vault信息

    根据链ID和vault地址获取Vault的详细信息

    URL参数:
        chain_id: 链ID
        vault_address: vault合约地址
    """
    try:
        # 转换chain_id为整数
        chain_id = int(chain_id)

        # 获取FlowEvmVaultService实例
        vault_service = FlowEvmVaultService.get_instance(chain_id)

        # 使用Service层方法获取完整的Vault信息
        vault_data = await vault_service.get_vault_info_with_prices(vault_address)

        return json(vault_data)

    except ValueError:
        return json({"error": f"Invalid chain ID: {chain_id}"}, status=400)
    except Exception as e:
        logger.exception("Failed to retrieve vault information: %s", e)
        return json({"error": f"Internal server error: {str(e)}"}, status=500)


@evm_vault_bp.route("/investor/<chain_id>/<investor_address>", methods=["GET"])
@authenticate
async def get_vault_by_investor(request, chain_id, investor_address):
    """根据投资者地址获取Flow EVM Vault信息

    根据链ID和投资者地址获取Vault的详细信息

    URL参数:
        chain_id: 链ID
        investor_address: 投资者地址

    查询参数:
        factory_address: 工厂合约地址(必需)
    """
    try:
        # 转换chain_id为整数
        chain_id = int(chain_id)

        # 获取必要参数
        factory_address = request.args.get("factory_address")
        if not factory_address:
            return json({"error": "Missing factory_address parameter"}, status=400)

        # 获取FlowEvmVaultService实例
        vault_service = FlowEvmVaultService.get_instance(chain_id)

        # 使用Service层方法获取vault信息
        vault_data = await vault_service.get_vault_by_investor(investor_address, factory_address)

        return json(vault_data)

    except ValueError:
        return json({"error": f"Invalid chain ID: {chain_id}"}, status=400)
    except Exception as e:
        logger.exception("Failed to retrieve vault by investor: %s", e)
        return json({"error": f"Internal server error: {str(e)}"}, status=500)


@evm_vault_bp.route("/<chain_id>/<vault_address>/portfolio", methods=["GET"])
@authenticate
async def get_portfolio_composition(request, chain_id, vault_address):
    """获取投资组合构成

    根据链ID和vault地址获取投资组合的详细构成

    URL参数:
        chain_id: 链ID
        vault_address: vault合约地址
    """
    try:
        # 转换chain_id为整数
        chain_id = int(chain_id)

        # 获取FlowEvmVaultService实例
        vault_service = FlowEvmVaultService.get_instance(chain_id)

        # 使用Service层方法获取投资组合信息
        portfolio_data = await vault_service.get_vault_info_with_prices(vault_address)

        return json(portfolio_data)

    except ValueError:
        return json({"error": f"Invalid chain ID: {chain_id}"}, status=400)
    except Exception as e:
        logger.exception("Failed to retrieve portfolio composition: %s", e)
        return json({"error": f"Internal server error: {str(e)}"}, status=500)


@evm_vault_bp.route("/swap", methods=["POST"])
@authenticate
async def execute_swap(request):
    """执行swap交易

    通过companion服务执行swap交易

    参数:
        chain_id: 链ID
        vault_address: vault合约地址
        token_in: 输入代币地址
        token_out: 输出代币地址
        amount_in: 输入金额
        amount_out_min: (可选) 最小输出金额
        fee_recipient: (可选) 费用接收者地址
        fee_rate: (可选) 费用率
    """
    try:
        # 获取必要参数
        chain_id = request.json.get("chain_id")
        vault_address = request.json.get("vault_address")
        token_in = request.json.get("token_in")
        token_out = request.json.get("token_out")
        amount_in = request.json.get("amount_in")
        amount_out_min = request.json.get("amount_out_min", 0)
        fee_recipient = request.json.get("fee_recipient")
        fee_rate = request.json.get("fee_rate", 0)

        if not chain_id or not vault_address or not token_in or not token_out or not amount_in:
            return json({"error": "Missing required parameters: chain_id, vault_address, token_in, token_out, amount_in"}, status=400)

        # 获取FlowEvmVaultService实例
        vault_service = FlowEvmVaultService.get_instance(chain_id)

        # 使用Service层方法执行swap
        swap_result = await vault_service.execute_swap(
            vault_address=vault_address,
            token_in=token_in,
            token_out=token_out,
            amount_in=amount_in,
            amount_out_min=amount_out_min,
            fee_recipient=fee_recipient,
            fee_rate=fee_rate
        )

        return json(swap_result)

    except Exception as e:
        logger.exception("Failed to execute swap: %s", e)
        return json({"error": f"Internal server error: {str(e)}"}, status=500)


@evm_vault_bp.route("/tokens/<chain_id>/<token_address>", methods=["GET"])
@authenticate
async def get_token_info(request, chain_id, token_address):
    """获取代币信息

    根据链ID和代币地址获取代币的详细信息

    URL参数:
        chain_id: 链ID
        token_address: 代币地址
    """
    try:
        # 转换chain_id为整数
        chain_id = int(chain_id)

        # 获取FlowEvmVaultService实例
        vault_service = FlowEvmVaultService.get_instance(chain_id)

        # 使用Service层方法获取代币信息
        token_data = await vault_service.get_token_info(token_address)

        return json(token_data)

    except ValueError:
        return json({"error": f"Invalid chain ID: {chain_id}"}, status=400)
    except Exception as e:
        logger.exception("Failed to retrieve token information: %s", e)
        return json({"error": f"Internal server error: {str(e)}"}, status=500)


@evm_vault_bp.route("/tokens/<chain_id>/<token_address>/balance/<holder_address>", methods=["GET"])
@authenticate
async def get_token_balance(request, chain_id, token_address, holder_address):
    """获取代币余额

    根据链ID、代币地址和持有者地址获取代币余额

    URL参数:
        chain_id: 链ID
        token_address: 代币地址
        holder_address: 持有者地址
    """
    try:
        # 转换chain_id为整数
        chain_id = int(chain_id)

        # 获取FlowEvmVaultService实例
        vault_service = FlowEvmVaultService.get_instance(chain_id)

        # 使用Service层方法获取代币余额
        balance_data = await vault_service.get_token_balance(token_address, holder_address)

        return json(balance_data)

    except ValueError:
        return json({"error": f"Invalid chain ID: {chain_id}"}, status=400)
    except Exception as e:
        logger.exception("Failed to retrieve token balance: %s", e)
        return json({"error": f"Internal server error: {str(e)}"}, status=500)


@evm_vault_bp.route("/<chain_id>/<vault_address>/operations", methods=["GET"])
@authenticate
async def get_vault_operations(request, chain_id, vault_address):
    """获取金库的操作历史记录

    根据链ID和金库合约地址获取该金库的所有操作记录，如存款、提款和交易
    此API直接从数据库获取历史记录，不通过companion服务

    URL参数:
        chain_id: 区块链ID
        vault_address: 金库合约地址

    查询参数:
        operation_type: (可选) 过滤特定操作类型 (deposit, withdraw, swap, swap_buy, swap_sell, all)
        start_date: (可选) 起始日期，ISO格式 (YYYY-MM-DD)
        end_date: (可选) 结束日期，ISO格式 (YYYY-MM-DD)
        limit: (可选) 返回记录数量限制，默认100
        skip: (可选) 跳过记录数量，用于分页，默认0
    """
    try:
        # 转换chain_id为整数
        chain_id = int(chain_id)

        # 获取查询参数
        operation_type = request.args.get("operation_type")
        start_date = request.args.get("start_date")
        end_date = request.args.get("end_date")
        limit = int(request.args.get("limit", 100))
        skip = int(request.args.get("skip", 0))

        # 日期处理
        start_datetime = None
        end_datetime = None

        if start_date:
            start_datetime = datetime.fromisoformat(start_date)

        if end_date:
            # 设置为当天的结束时间
            end_datetime = datetime.fromisoformat(end_date)
            end_datetime = end_datetime.replace(
                hour=23, minute=59, second=59, microsecond=999999
            )

        # 使用数据库会话
        with db_session() as db:
            # 首先验证金库是否存在并获取其ID
            try:
                vault = VaultContractService.get_vault_by_address(
                    db, vault_address, chain_id
                )
            except ResourceNotFoundException:
                return json(
                    {"error": f"Vault {vault_address} not found on chain {chain_id}"},
                    status=404,
                )

            # 处理操作类型过滤
            operation_types = None

            if operation_type:
                if operation_type.lower() == "deposit":
                    operation_types = [OperationType.DEPOSIT]
                elif operation_type.lower() == "withdraw":
                    operation_types = [OperationType.WITHDRAW]
                elif operation_type.lower() == "swap_buy":
                    operation_types = [OperationType.SWAP_BUY]
                elif operation_type.lower() == "swap_sell":
                    operation_types = [OperationType.SWAP_SELL]
                elif operation_type.lower() == "swap":
                    operation_types = [OperationType.SWAP]
                elif operation_type.lower() == "all":
                    operation_types = [
                        OperationType.DEPOSIT,
                        OperationType.WITHDRAW,
                        OperationType.SWAP,
                        OperationType.SWAP_BUY,
                        OperationType.SWAP_SELL,
                    ]
                else:
                    return json(
                        {
                            "error": f"Invalid operation_type: {operation_type}. Must be one of: deposit, withdraw, swap, swap_buy, swap_sell, all."
                        },
                        status=400,
                    )

            operations = VaultOperationHistoryService.get_vault_operations(
                db,
                vault_contract_id=vault.id,
                operation_types=operation_types,
                start_date=start_datetime,
                end_date=end_datetime,
                skip=skip,
                limit=limit,
            )

            # 获取总记录数用于分页
            total_operations = VaultOperationHistoryService.get_vault_operations_count(
                db,
                vault_contract_id=vault.id,
                operation_types=operation_types,
                start_date=start_datetime,
                end_date=end_datetime,
            )

            # 转换操作记录为JSON格式
            operations_data = []
            for op in operations:
                # 构建操作数据
                op_data = {
                    "id": op.id,
                    "vault_contract_id": op.vault_contract_id,
                    "chain_id": op.chain_id,
                    "vault_address": op.vault_address,
                    "transaction_hash": op.transaction_hash,
                    "operation_type": op.operation_type.value,
                    "created_at": op.created_at.isoformat() if op.created_at else None,
                    "updated_at": op.updated_at.isoformat() if op.updated_at else None,
                    # 资产信息
                    "input_token": (
                        {
                            "address": op.input_token_address,
                            "amount": (
                                float(op.input_token_amount)
                                if op.input_token_amount
                                else None
                            ),
                            "usd_value": (
                                float(op.input_token_usd_value)
                                if op.input_token_usd_value
                                else None
                            ),
                        }
                        if op.input_token_address
                        else None
                    ),
                    "output_token": (
                        {
                            "address": op.output_token_address,
                            "amount": (
                                float(op.output_token_amount)
                                if op.output_token_amount
                                else None
                            ),
                            "usd_value": (
                                float(op.output_token_usd_value)
                                if op.output_token_usd_value
                                else None
                            ),
                        }
                        if op.output_token_address
                        else None
                    ),
                    # Gas成本
                    "gas": {
                        "used": op.gas_used,
                        "price_wei": str(op.gas_price) if op.gas_price else None,
                        "cost_usd": (
                            float(op.total_gas_cost_usd)
                            if op.total_gas_cost_usd
                            else None
                        ),
                    },
                }

                operations_data.append(op_data)

            # 返回操作记录数据
            return json(
                {
                    "success": True,
                    "vault_address": vault_address,
                    "chain_id": chain_id,
                    "operations": operations_data,
                    "pagination": {
                        "total": total_operations,
                        "skip": skip,
                        "limit": limit
                    },
                }
            )

    except ValueError as e:
        # 处理参数错误
        return json({"error": f"Invalid parameter: {str(e)}"}, status=400)
    except Exception as e:
        # 处理其他错误
        logger.exception(f"Failed to retrieve vault operations: {e}")
        return json({"error": f"Internal server error: {str(e)}"}, status=500)
