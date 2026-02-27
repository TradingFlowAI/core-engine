"""
OperatorFactory — Builds live Operator instances from signal descriptors.

When CodeNode receives an input with `_tf_operator: True`, the factory:
1. Retrieves credentials from the CredentialStore (in-memory)
2. Creates the appropriate Operator subclass
3. Injects the event loop for async bridging
4. Returns the live operator for user code

Security:
- Credentials are consumed (deleted) from the store after retrieval
- The operator's private key / API key never appears in logs
"""

import asyncio
import logging
from typing import Optional

from operators.base import BaseOperator
from operators.credential_store import CredentialStore

logger = logging.getLogger(__name__)


class OperatorFactory:
    """Factory for creating Operator instances from signal descriptors."""

    @staticmethod
    async def from_descriptor(
        descriptor: dict,
        loop: Optional[asyncio.AbstractEventLoop] = None,
    ) -> BaseOperator:
        """
        Build an Operator from a signal descriptor dict.

        The descriptor is what PrivKeyNode / ExchangeAPINode emits,
        and must contain:
        - _tf_operator: True (marker)
        - _type: Operator class name (e.g. "EvmOperator", "BinanceOperator")
        - credential_ref: UUID reference to CredentialStore
        - Additional config fields (chain_id, exchange, etc.)

        Args:
            descriptor: The signal payload dict with _tf_operator marker.
            loop: The main asyncio event loop for sync bridging.

        Returns:
            A live BaseOperator subclass instance.
        """
        if not descriptor.get("_tf_operator"):
            raise ValueError("Not an operator descriptor (missing _tf_operator flag)")

        op_type = descriptor.get("_type", "")
        credential_ref = descriptor.get("credential_ref")

        if not credential_ref:
            raise ValueError(
                f"Operator descriptor missing credential_ref. "
                f"This means the credential node did not store credentials properly."
            )

        # Retrieve credentials from in-memory store
        store = CredentialStore.get_instance()
        credentials = store.retrieve(credential_ref, consume=True)

        if credentials is None:
            raise RuntimeError(
                f"Credentials for ref={credential_ref[:8]}... not found or expired. "
                f"The flow may have timed out or the credential was already consumed."
            )

        # Get event loop
        if loop is None:
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                loop = None

        # Build the appropriate operator
        if op_type in ("EvmOperator", "AptosOperator"):
            return _build_evm_operator(descriptor, credentials, loop)
        elif op_type in ("BinanceOperator", "OkxOperator", "BybitOperator", "BitgetOperator"):
            return _build_cex_operator(descriptor, credentials, loop)
        else:
            raise ValueError(f"Unknown operator type: {op_type}")

    @staticmethod
    def is_operator_descriptor(value) -> bool:
        """Check if a value is an operator descriptor dict."""
        return isinstance(value, dict) and value.get("_tf_operator") is True


def _build_evm_operator(
    descriptor: dict, credentials: dict, loop
) -> "BaseOperator":
    """Build an EvmOperator from descriptor + credentials."""
    from operators.evm_operator import EvmOperator

    private_key = credentials.get("private_key")
    if not private_key:
        raise ValueError("EVM operator credentials missing 'private_key'")

    chain_id = descriptor.get("chain_id")
    if chain_id is None:
        raise ValueError("EVM operator descriptor missing 'chain_id'")

    return EvmOperator(
        private_key=private_key,
        chain_id=chain_id,
        rpc_url=descriptor.get("rpc_url"),
        loop=loop,
    )


def _build_cex_operator(
    descriptor: dict, credentials: dict, loop
) -> "BaseOperator":
    """Build a CEX operator (Binance, OKX, etc.) from descriptor + credentials."""
    from operators.binance_operator import BinanceOperator

    api_key = credentials.get("api_key")
    api_secret = credentials.get("api_secret")
    passphrase = credentials.get("passphrase")

    if not api_key or not api_secret:
        raise ValueError("CEX operator credentials missing 'api_key' or 'api_secret'")

    exchange_id = descriptor.get("exchange", "binance")

    return BinanceOperator(
        api_key=api_key,
        api_secret=api_secret,
        passphrase=passphrase,
        exchange_id=exchange_id,
        loop=loop,
    )
