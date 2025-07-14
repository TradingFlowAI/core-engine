"""KMS 中间件"""

import functools
import logging

logger = logging.getLogger(__name__)


def authenticate(handler):
    """身份验证中间件"""

    @functools.wraps(handler)
    async def wrapper(request, *args, **kwargs):
        # 只允许特定 IP 访问
        # FIXME: 暂时不启用 IP 验证，后续可以根据需要启用
        # client_ip = request.ip
        # allowed_ips = CONFIG.get("ALLOWED_IPS", ["127.0.0.1"])

        # if client_ip not in allowed_ips:
        #     logger.warning(f"Unauthorized access attempt from {client_ip}")
        #     return json({"error": "Unauthorized access"}, status=403)

        # 可以添加其他身份验证逻辑，如 API 密钥验证等

        return await handler(request, *args, **kwargs)

    return wrapper
