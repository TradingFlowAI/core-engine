"""
Flow Vault API
提供Flow链上的vault相关接口
"""

import logging
from sanic import Blueprint
from sanic.response import json
from tradingflow.bank.services.flow_vault_service import FlowVaultService

logger = logging.getLogger(__name__)

# 创建Blueprint
flow_vault_bp = Blueprint("flow_vault", url_prefix="/flow/vaults")


@flow_vault_bp.route("/<address>", methods=["GET"])
async def get_vault_info(request, address):
    """获取特定Flow地址的Vault信息

    Args:
        request: Sanic请求对象
        address: Flow地址

    Returns:
        JSON响应包含vault信息
    """
    try:
        vault_service = FlowVaultService.get_instance()

        # 使用Service层方法获取vault信息
        data = await vault_service.get_vault_info(address)

        return json(data)

    except Exception as e:
        logger.exception("Failed to retrieve vault info for address %s: %s", address, e)
        return json({"error": f"Internal server error: {str(e)}"}, status=500)


@flow_vault_bp.route("/<address>/operations", methods=["GET"])
async def get_vault_operations(request, address):
    """获取特定Flow地址的Vault操作历史

    Args:
        request: Sanic请求对象
        address: Flow地址

    Returns:
        JSON响应包含操作历史
    """
    try:
        vault_service = FlowVaultService.get_instance()

        # 使用Service层方法获取操作历史
        data = await vault_service.get_vault_operations(address)

        return json(data)

    except Exception as e:
        logger.exception("Failed to retrieve vault operations for address %s: %s", address, e)
        return json({"error": f"Internal server error: {str(e)}"}, status=500)


@flow_vault_bp.route("/contract-address", methods=["GET"])
async def get_contract_address(request):
    """获取 TradingFlow Vault 合约地址

    作为代理服务，从companion服务获取Flow Vault合约地址
    """
    try:
        vault_service = FlowVaultService.get_instance()

        # 使用Service层方法获取合约地址
        data = await vault_service.get_contract_address()

        return json(data)

    except Exception as e:
        logger.exception("Failed to retrieve contract address: %s", e)
        return json({"error": f"Internal server error: {str(e)}"}, status=500)
