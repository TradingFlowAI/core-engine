import logging

from sanic import Blueprint
from sanic.response import json

from tradingflow.bank.common.middleware import authenticate
from tradingflow.bank.services.aptos_vault_service import AptosVaultService

# 创建蓝图
aptos_vault_bp = Blueprint("aptos_vault", url_prefix="/aptos/vaults")

logger = logging.getLogger(__name__)


@aptos_vault_bp.route("/<investor_address>/portfolio", methods=["GET"])
@authenticate
async def get_vault(request, investor_address):
    """获取特定Aptos Vault信息

    根据投资者地址获取Vault的详细信息，包括持仓和USD价值

    URL参数:
        investor_address: 投资者地址
    """
    try:
        # 获取AptosVaultService实例
        vault_service = AptosVaultService.get_instance()

        # 使用Service层方法获取完整的Vault信息
        vault_data = await vault_service.get_vault_info_with_prices(investor_address)

        return json({"success": True, "vault": vault_data})

    except Exception as e:
        logger.exception("Failed to retrieve vault information: %s", e)
        return json({"error": f"Internal server error: {str(e)}"}, status=500)


@aptos_vault_bp.route("/<investor_address>/operations", methods=["GET"])
@authenticate
async def get_vault_operations(request, investor_address):
    """获取特定Aptos Vault的操作记录

    根据投资者地址获取Vault的操作记录，包括存入、取出等操作，并计算USD价值

    URL参数:
        investor_address: 投资者地址
    """
    try:
        vault_service = AptosVaultService.get_instance()

        # 使用Service层方法获取包含价格信息的操作记录
        enhanced_operations = await vault_service.get_vault_operations_with_prices(investor_address)

        logger.debug("获取投资者操作记录: %s", enhanced_operations)

        if not enhanced_operations:
            return json(
                {
                    "success": False,
                    "error": "No operations found",
                    "message": f"投资者 {investor_address} 的操作记录未找到",
                },
                status=404,
            )

        return json({"success": True, "operations": enhanced_operations})

    except Exception as e:
        logger.exception("Failed to retrieve vault operations: %s", e)
        return json({"error": f"Internal server error: {str(e)}"}, status=500)


@aptos_vault_bp.route("/contract-address", methods=["GET"])
async def get_contract_address(request):
    """获取 TradingFlow Vault 合约地址

    作为代理服务，从companion服务获取Aptos Vault合约地址
    """
    try:
        vault_service = AptosVaultService.get_instance()

        # 使用Service层方法获取合约地址
        data = await vault_service.get_contract_address()

        return json(data)

    except Exception as e:
        logger.exception("Failed to retrieve contract address: %s", e)
        return json({"error": f"Internal server error: {str(e)}"}, status=500)
