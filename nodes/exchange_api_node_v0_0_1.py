"""
Exchange API Node v0.0.1 (R&D Mode)

Accepts exchange API credentials (API Key, Secret, optional Passphrase)
and exchange selection. Outputs an Operator descriptor that CodeNode
can consume to perform CEX operations (spot/futures trading, balance
queries, market data).

Signal flow:
1. Node stores credentials in CredentialStore (in-memory, TTL-based)
2. Signal contains a `credential_ref` UUID + `_tf_operator: True` marker
3. CodeNode's OperatorFactory retrieves credentials and creates live Operator
"""

import traceback
from typing import Dict, List, Optional

from common.edge import Edge
from common.node_decorators import register_node_type
from common.log_sanitizer import sanitize, register_sensitive_value
from common.signal_types import SignalType
from nodes.node_base import NodeBase, NodeStatus
from operators.credential_store import CredentialStore

# ── Handle IDs (must match frontend ExchangeAPINode.tsx) ─────────────
EXCHANGE_HANDLE = "exchange"
API_KEY_HANDLE = "api_key"
API_SECRET_HANDLE = "api_secret"
PASSPHRASE_HANDLE = "passphrase"
OPERATOR_OUTPUT_HANDLE = "operator"

# Supported exchanges for R&D mode
SUPPORTED_EXCHANGES = {
    "binance": {
        "display_name": "Binance",
        "requires_passphrase": False,
        "operator_type": "BinanceOperator",
    },
    "okx": {
        "display_name": "OKX",
        "requires_passphrase": True,
        "operator_type": "OkxOperator",
    },
    "bybit": {
        "display_name": "Bybit",
        "requires_passphrase": False,
        "operator_type": "BybitOperator",
    },
    "bitget": {
        "display_name": "Bitget",
        "requires_passphrase": True,
        "operator_type": "BitgetOperator",
    },
}


@register_node_type(
    "exchange_api_node",
    default_params={
        "exchange": "binance",
        "api_key": "",
        "api_secret": "",
        "passphrase": "",
    },
)
class ExchangeAPINode(NodeBase):
    """
    Exchange API Node — R&D Mode credential provider for CEX.

    Reads API Key / Secret / Passphrase from the user, validates the
    exchange selection, stores credentials in the in-memory CredentialStore,
    and outputs a JSON-serializable operator descriptor with credential_ref.

    CodeNode's OperatorFactory will later retrieve the credentials
    and create a live BinanceOperator / OkxOperator instance.

    Security:
    - API Key, Secret, and Passphrase are registered with log_sanitizer
    - Credentials are stored in-memory only (never serialized to RabbitMQ)
    - The signal contains only a UUID reference, not the actual keys
    """

    def __init__(
        self,
        node_id: str,
        flow_id: str,
        input_edges: List[Edge],
        output_edges: List[Edge],
        params: Optional[Dict] = None,
        **kwargs,
    ):
        super().__init__(
            node_id=node_id,
            flow_id=flow_id,
            input_edges=input_edges,
            output_edges=output_edges,
            params=params,
            node_type="exchange_api_node",
            **kwargs,
        )
        self.exchange = (self.params.get("exchange") or "binance").lower()
        self.api_key = self.params.get("api_key") or ""
        self.api_secret = self.params.get("api_secret") or ""
        self.passphrase = self.params.get("passphrase") or ""

    # ── Helpers ──────────────────────────────────────────────────────
    @staticmethod
    def _mask(value: str, tail: int = 4) -> str:
        """Return masked preview: ****<last N chars>"""
        if not value:
            return ""
        return f"****{value[-tail:]}" if len(value) >= tail else "****"

    # ── Execution ────────────────────────────────────────────────────
    async def execute(self, input_data: Optional[Dict] = None) -> Dict:
        """Build an Operator descriptor from the user's exchange API credentials."""
        try:
            await self.update_status(NodeStatus.RUNNING)
            await self.persist_log("Exchange API Node starting (R&D Mode)", "INFO")

            # ── 1. Validate exchange ─────────────────────────────────
            if self.exchange not in SUPPORTED_EXCHANGES:
                raise ValueError(
                    f"Unsupported exchange '{self.exchange}'. "
                    f"Supported: {', '.join(SUPPORTED_EXCHANGES.keys())}"
                )
            exchange_cfg = SUPPORTED_EXCHANGES[self.exchange]

            # ── 2. Validate credentials ──────────────────────────────
            if not self.api_key:
                raise ValueError("API Key is required.")
            if not self.api_secret:
                raise ValueError("API Secret is required.")
            if exchange_cfg["requires_passphrase"] and not self.passphrase:
                raise ValueError(
                    f"{exchange_cfg['display_name']} requires a Passphrase. "
                    f"Please enter it in the node configuration."
                )

            # ── 3. Register secrets with log sanitizer ───────────────
            register_sensitive_value(self.api_key)
            register_sensitive_value(self.api_secret)
            if self.passphrase:
                register_sensitive_value(self.passphrase)

            # ── 4. Store credentials in CredentialStore ──────────────
            store = CredentialStore.get_instance()
            credentials = {
                "api_key": self.api_key,
                "api_secret": self.api_secret,
            }
            if self.passphrase:
                credentials["passphrase"] = self.passphrase

            credential_ref = store.store(credentials, ttl=300)  # 5 minute TTL

            # ── 5. Build operator descriptor (JSON-safe) ─────────────
            operator_descriptor = {
                "_tf_operator": True,  # Marker for CodeNode to detect
                "_type": exchange_cfg["operator_type"],
                "exchange": self.exchange,
                "display_name": exchange_cfg["display_name"],
                "api_key_preview": self._mask(self.api_key),
                "has_passphrase": bool(self.passphrase),
                "credential_ref": credential_ref,  # UUID to CredentialStore
            }

            await self.persist_log(
                f"Operator descriptor built for {exchange_cfg['display_name']} "
                f"(key: {self._mask(self.api_key)})",
                "INFO",
            )

            # ── 6. Send signal to downstream nodes ───────────────────
            await self.send_signal(
                source_handle=OPERATOR_OUTPUT_HANDLE,
                signal_type=SignalType.DATASET,
                payload={
                    OPERATOR_OUTPUT_HANDLE: operator_descriptor,
                },
            )

            await self.update_status(NodeStatus.COMPLETED)
            await self.persist_log("Exchange API Node completed", "INFO")

            return {
                "type": SignalType.DATASET.value,
                "data": {
                    OPERATOR_OUTPUT_HANDLE: {
                        **operator_descriptor,
                        "credential_ref": "[REDACTED]",  # Don't return ref in execute() return
                    },
                },
            }

        except Exception as e:
            error_msg = f"Exchange API Node error: {sanitize(str(e))}"
            await self.persist_log(error_msg, "ERROR")
            await self.update_status(NodeStatus.ERROR)
            tb = traceback.format_exc()
            return {
                "type": SignalType.ERROR.value,
                "data": {"error": sanitize(str(e)), "traceback": sanitize(tb)},
            }
