"""
BaseOperator — Abstract base class for all R&D Mode operators.

Design principles:
1. __repr__/__str__ always return sanitized strings (never leak secrets)
2. JSON serialization is blocked to prevent accidental log leaks
3. Operator 公开方法为同步 API，便于 CodeNode 用户代码直接调用
4. _run_async() 保留用于未来需要真正 async 操作的场景 (如 aiohttp)
   当前 web3.py HTTPProvider 和 ccxt 均为同步库，直接在 CodeNode 线程中调用
"""

import asyncio
import logging
from typing import Any, Optional

logger = logging.getLogger(__name__)


class BaseOperator:
    """
    Abstract base class for credential-based operators.

    Subclasses must implement actual trading/query methods.
    All operators expose synchronous public methods for CodeNode
    compatibility. Async work is done via _run_async().
    """

    # Subclasses should set these
    _operator_type: str = "base"
    _display_info: str = ""

    def __init__(self, loop: Optional[asyncio.AbstractEventLoop] = None):
        """
        Args:
            loop: The main asyncio event loop. If None, attempts to
                  get the running loop. Must be set before calling
                  _run_async() from a sync context.
        """
        self._loop = loop

    # ── Sanitized representations ────────────────────────────────────

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self._display_info})"

    def __str__(self) -> str:
        return self.__repr__()

    def __json__(self) -> None:
        """Block JSON serialization to prevent secret leakage."""
        raise TypeError(
            f"{self.__class__.__name__} cannot be serialized to JSON. "
            f"Operators contain sensitive credentials and must not be "
            f"logged, stored, or transmitted as JSON."
        )

    # Make json.dumps() also fail
    def __getstate__(self) -> None:
        raise TypeError(f"{self.__class__.__name__} cannot be pickled.")

    # ── Async bridge ─────────────────────────────────────────────────

    def _run_async(self, coro, timeout: float = 60.0) -> Any:
        """
        Run an async coroutine from a synchronous context (CodeNode thread).

        Uses asyncio.run_coroutine_threadsafe() to schedule the coroutine
        on the main event loop and blocks until the result is available.

        Args:
            coro: The coroutine to execute.
            timeout: Maximum seconds to wait for result. Default 60s.

        Returns:
            The coroutine's return value.

        Raises:
            RuntimeError: If no event loop is available.
            TimeoutError: If the coroutine doesn't complete within timeout.
        """
        if self._loop is None:
            raise RuntimeError(
                "No event loop set on this operator. "
                "This usually means the operator was not properly injected by CodeNode."
            )

        future = asyncio.run_coroutine_threadsafe(coro, self._loop)
        try:
            return future.result(timeout=timeout)
        except TimeoutError:
            future.cancel()
            raise TimeoutError(
                f"Operator call timed out after {timeout}s. "
                f"The operation may still be in progress."
            )

    # ── Info methods (safe to call) ──────────────────────────────────

    def info(self) -> dict:
        """Return a JSON-safe info dict about this operator (no secrets)."""
        return {
            "type": self._operator_type,
            "display": self._display_info,
            "class": self.__class__.__name__,
        }
