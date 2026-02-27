"""
Operators Package — R&D Mode Operator System

Provides credential-based operator objects for CodeNode to consume:
- BaseOperator: Abstract base with sanitized repr and async bridge
- EvmOperator: Direct wallet DEX operations (BSC, Flow EVM)
- BinanceOperator: Binance CEX operations (spot, futures, balance)
- OperatorFactory: Builds operator instances from signal descriptors

All operators expose **synchronous** public methods for CodeNode compatibility.
CodeNode 用户代码在独立线程中执行，web3/ccxt 同步调用直接在该线程中执行。
_run_async() 桥接保留用于未来真正的 async 操作 (如 aiohttp)。
"""

from operators.base import BaseOperator
from operators.evm_operator import EvmOperator
from operators.binance_operator import BinanceOperator
from operators.factory import OperatorFactory
from operators.credential_store import CredentialStore

__all__ = [
    "BaseOperator",
    "EvmOperator",
    "BinanceOperator",
    "OperatorFactory",
    "CredentialStore",
]
