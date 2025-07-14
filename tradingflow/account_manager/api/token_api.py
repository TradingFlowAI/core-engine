import logging

from sanic import Blueprint
from sanic.response import json as sanic_json

from tradingflow.account_manager.common.middleware import authenticate
from tradingflow.common.db import db_session
from tradingflow.common.db.services.monitored_token_service import MonitoredTokenService
from tradingflow.common.exceptions import (
    DuplicateResourceException,
    ResourceNotFoundException,
)

logger = logging.getLogger(__name__)

# 创建蓝图
token_bp = Blueprint("token_api", url_prefix="/token")


@token_bp.route("/", methods=["POST"])
@authenticate
async def add_monitored_token(request):
    """添加新的监控代币

    请求体:
        JSON对象，包含以下字段:
        - chain_id: 链ID (必填)
        - token_address: 代币合约地址 (必填)
        - name: 代币名称
        - symbol: 代币符号
        - decimals: 代币精度，默认18
        - primary_source: 主要价格来源，默认"geckoterminal"
        - is_active: 是否主动监控，默认true
        - logo_url: 代币Logo URL
        - description: 代币描述

    返回:
        JSON对象，包含新增的代币信息或错误消息
    """
    try:
        # 验证请求体
        if not request.json:
            return sanic_json({"success": False, "error": "缺少请求体数据"}, status=400)

        # 检查必填字段
        required_fields = ["chain_id", "token_address"]
        missing_fields = [
            field for field in required_fields if field not in request.json
        ]
        if missing_fields:
            return sanic_json(
                {
                    "success": False,
                    "error": f"缺少必填字段: {', '.join(missing_fields)}",
                },
                status=400,
            )

        # 创建代币数据对象
        token_data = {
            "chain_id": request.json.get("chain_id"),
            "token_address": request.json.get("token_address").lower(),  # 规范化地址
            "name": request.json.get("name"),
            "symbol": request.json.get("symbol"),
            "decimals": request.json.get("decimals", 18),
            "primary_source": request.json.get("primary_source", "geckoterminal"),
            "is_active": request.json.get("is_active", True),
            "logo_url": request.json.get("logo_url"),
            "description": request.json.get("description"),
        }

        # 添加到数据库
        with db_session() as db:
            try:
                new_token = MonitoredTokenService.create_token(db, token_data)

                # 构造响应
                response = {
                    "success": True,
                    "message": "监控代币添加成功",
                    "token": {
                        "id": new_token.id,
                        "chain_id": new_token.chain_id,
                        "token_address": new_token.token_address,
                        "name": new_token.name,
                        "symbol": new_token.symbol,
                        "decimals": new_token.decimals,
                        "is_active": new_token.is_active,
                        "created_at": (
                            new_token.created_at.isoformat()
                            if new_token.created_at
                            else None
                        ),
                    },
                }

                return sanic_json(response, status=201)  # 201 Created

            except DuplicateResourceException as e:
                return sanic_json(
                    {"success": False, "error": str(e)}, status=409  # 409 Conflict
                )

    except ValueError as e:
        logger.warning("添加监控代币时出现参数错误: %s", str(e))
        return sanic_json(
            {"success": False, "error": f"参数无效: {str(e)}"},
            status=400,  # 400 Bad Request
        )

    except Exception as e:
        logger.exception("添加监控代币时出错: %s", str(e))
        return sanic_json(
            {"success": False, "error": f"服务器内部错误: {str(e)}"},
            status=500,  # 500 Internal Server Error
        )


@token_bp.route("/batch", methods=["POST"])
@authenticate
async def add_monitored_tokens_batch(request):
    """批量添加监控代币

    请求体:
        JSON对象，包含以下字段:
        - tokens: 代币数据对象数组，每个对象包含单个代币的所有字段

    返回:
        JSON对象，包含批量处理结果
    """
    try:
        # 验证请求体
        if not request.json or "tokens" not in request.json:
            return sanic_json({"success": False, "error": "缺少tokens字段"}, status=400)

        tokens_data = request.json.get("tokens")
        if not isinstance(tokens_data, list):
            return sanic_json(
                {"success": False, "error": "tokens必须是一个数组"}, status=400
            )

        # 处理每个代币数据
        processed_tokens = []
        for token in tokens_data:
            # 确保token_address是小写
            if "token_address" in token:
                token["token_address"] = token["token_address"].lower()
            processed_tokens.append(token)

        # 批量添加到数据库
        with db_session() as db:
            try:
                result = MonitoredTokenService.batch_create_or_update_tokens(
                    db, processed_tokens
                )

                # 构造响应
                response = {
                    "success": True,
                    "message": "批量处理完成",
                    "created": result["created"],
                    "updated": result["updated"],
                    "total_processed": result["created"] + result["updated"],
                }

                return sanic_json(response)

            except Exception as e:
                logger.exception("批量处理代币时出错")
                return sanic_json(
                    {"success": False, "error": f"批量处理失败: {str(e)}"}, status=500
                )

    except Exception as e:
        logger.exception("批量添加监控代币时出错: %s", str(e))
        return sanic_json(
            {"success": False, "error": f"服务器内部错误: {str(e)}"}, status=500
        )


@token_bp.route("/<token_id:int>", methods=["GET"])
@authenticate
async def get_monitored_token(request, token_id):
    """根据ID获取监控代币详情"""
    try:
        with db_session() as db:
            try:
                token = MonitoredTokenService.get_token_by_id(db, token_id)

                # 构造响应
                response = {
                    "success": True,
                    "token": {
                        "id": token.id,
                        "chain_id": token.chain_id,
                        "token_address": token.token_address,
                        "name": token.name,
                        "symbol": token.symbol,
                        "decimals": token.decimals,
                        "primary_source": token.primary_source,
                        "is_active": token.is_active,
                        "logo_url": token.logo_url,
                        "description": token.description,
                        "created_at": (
                            token.created_at.isoformat() if token.created_at else None
                        ),
                        "updated_at": (
                            token.updated_at.isoformat() if token.updated_at else None
                        ),
                    },
                }

                return sanic_json(response)

            except ResourceNotFoundException as e:
                return sanic_json(
                    {"success": False, "error": str(e)}, status=404  # 404 Not Found
                )

    except Exception as e:
        logger.exception("获取监控代币详情时出错: %s", str(e))
        return sanic_json(
            {"success": False, "error": f"服务器内部错误: {str(e)}"}, status=500
        )


@token_bp.route("/chain/<chain_id:int>", methods=["GET"])
@authenticate
async def list_tokens_by_chain(request, chain_id):
    """获取指定链上的监控代币列表"""
    try:
        # 解析查询参数
        skip = int(request.args.get("skip", 0))
        limit = min(int(request.args.get("limit", 100)), 1000)  # 最多1000条

        with db_session() as db:
            tokens = MonitoredTokenService.list_tokens(db, chain_id, skip, limit)

            # 构造响应
            token_list = []
            for token in tokens:
                token_list.append(
                    {
                        "id": token.id,
                        "chain_id": token.chain_id,
                        "token_address": token.token_address,
                        "name": token.name,
                        "symbol": token.symbol,
                        "is_active": token.is_active,
                    }
                )

            response = {
                "success": True,
                "chain_id": chain_id,
                "tokens": token_list,
                "total": len(token_list),
                "skip": skip,
                "limit": limit,
            }

            return sanic_json(response)

    except ValueError as e:
        return sanic_json(
            {"success": False, "error": f"参数无效: {str(e)}"}, status=400
        )

    except Exception as e:
        logger.exception("获取链上监控代币列表时出错: %s", str(e))
        return sanic_json(
            {"success": False, "error": f"服务器内部错误: {str(e)}"}, status=500
        )


@token_bp.route("/search", methods=["GET"])
@authenticate
async def search_tokens(request):
    """搜索监控代币"""
    try:
        # 解析查询参数
        search_term = request.args.get("q", "")
        chain_id = request.args.get("chain_id")
        if chain_id:
            chain_id = int(chain_id)

        skip = int(request.args.get("skip", 0))
        limit = min(int(request.args.get("limit", 100)), 1000)

        with db_session() as db:
            tokens = MonitoredTokenService.search_tokens(
                db, search_term, chain_id, skip, limit
            )

            # 构造响应
            token_list = []
            for token in tokens:
                token_list.append(
                    {
                        "id": token.id,
                        "chain_id": token.chain_id,
                        "token_address": token.token_address,
                        "name": token.name,
                        "symbol": token.symbol,
                        "is_active": token.is_active,
                    }
                )

            response = {
                "success": True,
                "search_term": search_term,
                "chain_id": chain_id,
                "tokens": token_list,
                "total": len(token_list),
                "skip": skip,
                "limit": limit,
            }

            return sanic_json(response)

    except ValueError as e:
        return sanic_json(
            {"success": False, "error": f"参数无效: {str(e)}"}, status=400
        )

    except Exception as e:
        logger.exception("搜索监控代币时出错: %s", str(e))
        return sanic_json(
            {"success": False, "error": f"服务器内部错误: {str(e)}"}, status=500
        )
