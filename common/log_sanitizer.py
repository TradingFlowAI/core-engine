"""
Global Log Sanitizer - 全局日志脱敏器

在所有日志输出路径（persist_log, CodeNode stdout, Redis pub）之前，
自动检测并替换已知敏感值（环境变量中的密钥、密码、Token 等），
仅暴露最后 2 位字符用以确认。

Phase 0: 安全加固 — R&D Mode 前置必要条件
"""

import os
import re
import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger("LogSanitizer")

# ---------- 敏感环境变量名称模式 ----------
SENSITIVE_ENV_PATTERNS = [
    r".*_KEY$",           # OPENROUTER_API_KEY, BINANCE_API_KEY, TWITTER_API_KEY
    r".*_SECRET$",        # BINANCE_API_SECRET
    r".*_PASSWORD$",      # POSTGRES_PASSWORD, RABBITMQ_PASSWORD, REDIS_PASSWORD
    r".*_TOKEN$",         # CHAINCATCHER_API_TOKEN
    r".*PRIVATE_KEY.*",   # DEPLOYER_PRIVATE_KEY_*
    r".*CREDENTIALS.*",   # GOOGLE_CREDENTIALS_PATH (value is path but still sensitive)
]

# 编译正则以提升性能
_COMPILED_PATTERNS = [re.compile(p, re.IGNORECASE) for p in SENSITIVE_ENV_PATTERNS]

# ---------- 运行时敏感值缓存 ----------
_SENSITIVE_VALUES: List[str] = []


def init_sensitive_values() -> None:
    """
    启动时从环境变量中收集所有需要脱敏的值。

    只收集：
      - 匹配 SENSITIVE_ENV_PATTERNS 的环境变量
      - 值长度 >= 6（跳过空值和太短的值以免误伤常规日志）
    """
    global _SENSITIVE_VALUES
    _SENSITIVE_VALUES = []
    for key, value in os.environ.items():
        if not value or len(value) < 6:
            continue
        for pattern in _COMPILED_PATTERNS:
            if pattern.match(key):
                _SENSITIVE_VALUES.append(value)
                break
    logger.debug(
        "LogSanitizer initialized: collected %d sensitive values from environment",
        len(_SENSITIVE_VALUES),
    )


def register_sensitive_value(value: str) -> None:
    """
    运行时动态注册额外的敏感值（例如解密后的用户密钥）。

    在 R&D Mode Operator 解密密钥后应调用此函数，
    以确保该值如果意外出现在日志中也会被脱敏。
    """
    if value and len(value) >= 4 and value not in _SENSITIVE_VALUES:
        _SENSITIVE_VALUES.append(value)


def sanitize(message: str, extra_secrets: Optional[List[str]] = None) -> str:
    """
    脱敏消息字符串，将所有已知敏感值替换为 **** + 最后 2 位。

    Args:
        message: 待脱敏的消息
        extra_secrets: 可选的额外敏感值列表（一次性使用，不缓存）

    Returns:
        脱敏后的消息字符串
    """
    if not message:
        return message

    all_secrets = _SENSITIVE_VALUES + (extra_secrets or [])
    for secret in all_secrets:
        if secret and len(secret) > 4 and secret in message:
            # 保留最后 2 位，前面用 * 代替，最多 20 个 *
            masked = "*" * min(len(secret) - 2, 20) + secret[-2:]
            message = message.replace(secret, masked)

    return message


def sanitize_dict(data: Any) -> Any:
    """
    递归脱敏字典/列表中的所有字符串值。

    Args:
        data: 待脱敏的数据结构（dict / list / str / other）

    Returns:
        脱敏后的数据结构（原始结构不变，返回新对象）
    """
    if isinstance(data, str):
        return sanitize(data)
    elif isinstance(data, dict):
        return {k: sanitize_dict(v) for k, v in data.items()}
    elif isinstance(data, (list, tuple)):
        sanitized = [sanitize_dict(item) for item in data]
        return type(data)(sanitized)
    else:
        return data


# ---------- 模块加载时自动初始化 ----------
init_sensitive_values()
