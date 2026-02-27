"""
CredentialStore — Thread-safe in-memory store for operator credentials.

Since signals go through RabbitMQ (JSON-only), we cannot pass live
Python objects or secrets in the signal payload. Instead:

1. PrivKeyNode / ExchangeAPINode stores credentials here during execution.
2. The signal contains a `credential_ref` (UUID) that references this store.
3. CodeNode's OperatorFactory retrieves credentials using the ref.
4. Credentials are automatically cleaned up after retrieval (one-time use).

Security:
- Credentials live only in the Python process memory.
- Each credential has a TTL (default 5 minutes) for automatic cleanup.
- Credentials are deleted after first retrieval (single-use by default).

Limitations:
- 本实现仅适用于 **单进程** 部署 (所有节点在同一 Sanic 进程中执行)。
- 如果 Station 水平扩展为多实例，PrivKeyNode 和 CodeNode 可能分布在
  不同进程，此时 credential_ref 无法跨进程解析。
- 多实例场景需要替换为 Redis-based 或 SecretService HTTP-based 方案。
"""

import logging
import threading
import time
import uuid
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

# Global TTL for stored credentials (seconds)
_DEFAULT_TTL = 300  # 5 minutes


class CredentialStore:
    """Thread-safe singleton credential store."""

    _instance: Optional["CredentialStore"] = None
    _lock = threading.Lock()

    def __new__(cls) -> "CredentialStore":
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._store: Dict[str, Dict[str, Any]] = {}
                cls._instance._store_lock = threading.Lock()
            return cls._instance

    @classmethod
    def get_instance(cls) -> "CredentialStore":
        """Get the singleton instance."""
        return cls()

    def store(self, credentials: dict, ttl: float = _DEFAULT_TTL) -> str:
        """
        Store credentials and return a unique reference ID.

        Args:
            credentials: Dict containing credential data (private keys, API keys, etc.)
            ttl: Time-to-live in seconds.

        Returns:
            A unique reference string (UUID) to retrieve the credentials.
        """
        ref_id = str(uuid.uuid4())
        with self._store_lock:
            self._store[ref_id] = {
                "credentials": credentials,
                "created_at": time.time(),
                "ttl": ttl,
            }
        logger.debug("Credential stored with ref=%s (ttl=%ss)", ref_id[:8], ttl)
        return ref_id

    def retrieve(self, ref_id: str, consume: bool = True) -> Optional[dict]:
        """
        Retrieve credentials by reference ID.

        Args:
            ref_id: The reference string returned by store().
            consume: If True, delete the entry after retrieval (single-use).

        Returns:
            The credentials dict, or None if not found or expired.
        """
        with self._store_lock:
            entry = self._store.get(ref_id)
            if entry is None:
                logger.warning("Credential ref=%s not found", ref_id[:8])
                return None

            # Check TTL
            elapsed = time.time() - entry["created_at"]
            if elapsed > entry["ttl"]:
                del self._store[ref_id]
                logger.warning(
                    "Credential ref=%s expired (elapsed=%.1fs, ttl=%.1fs)",
                    ref_id[:8], elapsed, entry["ttl"],
                )
                return None

            credentials = entry["credentials"]

            if consume:
                del self._store[ref_id]
                logger.debug("Credential ref=%s consumed and deleted", ref_id[:8])

            return credentials

    def cleanup_expired(self) -> int:
        """Remove all expired entries. Returns count of removed entries."""
        now = time.time()
        removed = 0
        with self._store_lock:
            expired_refs = [
                ref_id
                for ref_id, entry in self._store.items()
                if now - entry["created_at"] > entry["ttl"]
            ]
            for ref_id in expired_refs:
                del self._store[ref_id]
                removed += 1

        if removed:
            logger.info("Cleaned up %d expired credential entries", removed)
        return removed

    def size(self) -> int:
        """Current number of stored credentials."""
        with self._store_lock:
            return len(self._store)
