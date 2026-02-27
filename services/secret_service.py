"""
SecretService - 密钥加密/解密服务

提供 AES-256-GCM 加密和解密功能，用于 R&D Mode 用户密钥的安全存储与执行时解密。

支持两种模式：
- local: 使用 MASTER_SECRET_KEY 环境变量 + HKDF 派生 per-user key
- kms: (预留) AWS KMS envelope encryption

Phase 1: 密钥管理基础设施
"""

import base64
import hashlib
import hmac
import json
import logging
import os
from dataclasses import dataclass
from typing import Optional

import httpx
from Crypto.Cipher import AES
from Crypto.Random import get_random_bytes

from infra.config import CONFIG
from common.log_sanitizer import register_sensitive_value

logger = logging.getLogger("SecretService")


@dataclass
class EncryptedSecret:
    """加密后的密钥数据结构"""
    encrypted_data: str   # base64 encoded ciphertext
    iv: str               # base64 encoded initialization vector
    auth_tag: str         # base64 encoded GCM authentication tag
    encryption_mode: str  # "local" or "kms"


class SecretService:
    """
    密钥管理服务

    加密模式由环境变量 SECRET_ENCRYPTION_MODE 控制：
    - "local" (默认): AES-256-GCM，master key 从 MASTER_SECRET_KEY 获取
    - "kms": AWS KMS envelope encryption (预留)
    """

    _instance = None

    def __init__(self):
        self._mode = CONFIG.get("SECRET_ENCRYPTION_MODE", "local")
        self._master_key = CONFIG.get("MASTER_SECRET_KEY", "")
        if self._mode == "local" and not self._master_key:
            logger.warning(
                "MASTER_SECRET_KEY not set! Using fallback key. "
                "This is INSECURE and should only be used in development."
            )
            # 开发环境回退密钥（仅供本地测试）
            self._master_key = "tf-dev-fallback-key-do-not-use-in-prod"

        self._weather_control_url = CONFIG.get(
            "WEATHER_CONTROL_URL", "http://localhost:8000"
        )

    @classmethod
    def get_instance(cls) -> "SecretService":
        """获取 SecretService 单例"""
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    # ==================== 加密/解密核心 ====================

    def _derive_user_key(self, user_id: str) -> bytes:
        """
        使用 HKDF 从 master key + user_id 派生 per-user 加密密钥。

        返回 32 bytes (256-bit) 密钥。
        """
        # HKDF-like key derivation using HMAC-SHA256
        salt = b"tradingflow-secret-v1"
        prk = hmac.new(salt, self._master_key.encode("utf-8"), hashlib.sha256).digest()
        info = f"user-secret:{user_id}".encode("utf-8")
        okm = hmac.new(prk, info + b"\x01", hashlib.sha256).digest()
        return okm  # 32 bytes

    def encrypt(self, plaintext: str, user_id: str) -> EncryptedSecret:
        """
        加密明文密钥。

        Args:
            plaintext: 待加密的密钥明文
            user_id: 用户 ID（用于密钥派生）

        Returns:
            EncryptedSecret: 加密后的数据
        """
        if self._mode == "kms":
            return self._encrypt_kms(plaintext, user_id)

        # Local mode: AES-256-GCM
        key = self._derive_user_key(user_id)
        iv = get_random_bytes(12)  # 96-bit IV for GCM
        cipher = AES.new(key, AES.MODE_GCM, nonce=iv)
        ciphertext, tag = cipher.encrypt_and_digest(plaintext.encode("utf-8"))

        return EncryptedSecret(
            encrypted_data=base64.b64encode(ciphertext).decode("ascii"),
            iv=base64.b64encode(iv).decode("ascii"),
            auth_tag=base64.b64encode(tag).decode("ascii"),
            encryption_mode="local",
        )

    def decrypt(self, encrypted: EncryptedSecret, user_id: str) -> str:
        """
        解密密钥。

        Args:
            encrypted: 加密后的数据
            user_id: 用户 ID（用于密钥派生）

        Returns:
            str: 解密后的明文

        Raises:
            ValueError: 解密失败（密钥错误或数据被篡改）
        """
        if encrypted.encryption_mode == "kms":
            return self._decrypt_kms(encrypted, user_id)

        # Local mode: AES-256-GCM
        key = self._derive_user_key(user_id)
        iv = base64.b64decode(encrypted.iv)
        ciphertext = base64.b64decode(encrypted.encrypted_data)
        tag = base64.b64decode(encrypted.auth_tag)

        cipher = AES.new(key, AES.MODE_GCM, nonce=iv)
        try:
            plaintext = cipher.decrypt_and_verify(ciphertext, tag)
            decrypted = plaintext.decode("utf-8")

            # 注册到日志脱敏器，防止意外泄露
            register_sensitive_value(decrypted)

            return decrypted
        except (ValueError, KeyError) as e:
            raise ValueError(
                f"Failed to decrypt secret: authentication failed. "
                f"The data may have been tampered with or the wrong key was used."
            ) from e

    def _encrypt_kms(self, plaintext: str, user_id: str) -> EncryptedSecret:
        """AWS KMS envelope encryption (预留)"""
        raise NotImplementedError(
            "KMS encryption is not yet implemented. "
            "Set SECRET_ENCRYPTION_MODE=local in your environment."
        )

    def _decrypt_kms(self, encrypted: EncryptedSecret, user_id: str) -> str:
        """AWS KMS envelope decryption (预留)"""
        raise NotImplementedError(
            "KMS decryption is not yet implemented. "
            "Set SECRET_ENCRYPTION_MODE=local in your environment."
        )

    # ==================== HTTP Client: 访问 02_control ====================

    async def create_secret(
        self,
        user_id: str,
        name: str,
        secret_type: str,
        plaintext: str,
        metadata: Optional[dict] = None,
    ) -> dict:
        """
        加密并存储密钥到 MongoDB（通过 02_control API）。

        Args:
            user_id: 用户 ID
            name: 密钥名称
            secret_type: "private_key" 或 "exchange_api"
            plaintext: 密钥明文
            metadata: 元数据（chain, exchange, addressPreview 等）

        Returns:
            dict: 创建的 Secret 文档（安全 JSON，不含明文）
        """
        # 1. 本地加密
        encrypted = self.encrypt(plaintext, user_id)

        # 2. 生成预览信息
        key_preview = "****" + plaintext[-2:] if len(plaintext) >= 2 else "****"
        if metadata is None:
            metadata = {}
        metadata["keyPreview"] = key_preview

        # 3. 发送到 02_control 存储
        payload = {
            "userId": user_id,
            "name": name,
            "secretType": secret_type,
            "encryptedData": encrypted.encrypted_data,
            "iv": encrypted.iv,
            "authTag": encrypted.auth_tag,
            "encryptionMode": encrypted.encryption_mode,
            "metadata": metadata,
        }

        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.post(
                f"{self._weather_control_url}/api/v1/secrets",
                json=payload,
            )
            response.raise_for_status()
            return response.json()

    async def get_secret_metadata(self, secret_id: str) -> dict:
        """
        获取密钥元信息（不含加密数据）。

        Args:
            secret_id: Secret 文档 ID

        Returns:
            dict: Secret 元信息
        """
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(
                f"{self._weather_control_url}/api/v1/secrets/{secret_id}",
            )
            response.raise_for_status()
            return response.json()

    async def get_and_decrypt(self, secret_id: str, user_id: str) -> str:
        """
        获取加密密钥并解密（仅供节点执行时内部使用）。

        Args:
            secret_id: Secret 文档 ID
            user_id: 用户 ID

        Returns:
            str: 解密后的密钥明文
        """
        # 请求完整的加密数据（内部端点）
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(
                f"{self._weather_control_url}/api/v1/secrets/{secret_id}/encrypted",
            )
            response.raise_for_status()
            data = response.json().get("secret", response.json())

        # 重建 EncryptedSecret 并解密
        encrypted = EncryptedSecret(
            encrypted_data=data["encryptedData"],
            iv=data["iv"],
            auth_tag=data["authTag"],
            encryption_mode=data.get("encryptionMode", "local"),
        )

        return self.decrypt(encrypted, user_id)

    async def get_user_secrets(self, user_id: str) -> list:
        """
        获取用户的所有密钥列表（元信息，不含明文）。

        Args:
            user_id: 用户 ID

        Returns:
            list: Secret 元信息列表
        """
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(
                f"{self._weather_control_url}/api/v1/secrets/user/{user_id}",
            )
            response.raise_for_status()
            return response.json().get("secrets", [])

    async def delete_secret(self, secret_id: str) -> dict:
        """
        删除密钥（软删除）。

        Args:
            secret_id: Secret 文档 ID

        Returns:
            dict: 操作结果
        """
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.delete(
                f"{self._weather_control_url}/api/v1/secrets/{secret_id}",
            )
            response.raise_for_status()
            return response.json()

    # ==================== 工具方法 ====================

    @staticmethod
    def mask_secret(value: str) -> str:
        """
        脱敏显示密钥：保留最后 2 位，前面用 * 代替。

        Args:
            value: 原始密钥字符串

        Returns:
            str: "****xx" 格式
        """
        if not value or len(value) < 2:
            return "****"
        return "*" * min(len(value) - 2, 20) + value[-2:]
