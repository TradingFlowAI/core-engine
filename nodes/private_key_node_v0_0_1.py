"""
Private Key Node v0.0.1 (R&D Mode)

Accepts a blockchain private key and chain selection.
Outputs an Operator descriptor that CodeNode can consume
to perform direct wallet signing, DEX swaps, and on-chain transactions.

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

# ── Handle IDs (must match frontend PrivateKeyNode.tsx) ──────────────
CHAIN_HANDLE = "chain"
PRIVATE_KEY_HANDLE = "private_key"
OPERATOR_OUTPUT_HANDLE = "operator"

# Supported chains for R&D mode direct-wallet operations
SUPPORTED_CHAINS = {
    "bsc": {
        "chain_id": 56,
        "network_type": "evm",
        "description": "BSC Mainnet",
    },
    "flow_evm": {
        "chain_id": 545,
        "network_type": "evm",
        "description": "Flow EVM Testnet",
    },
    "aptos": {
        "chain_id": None,
        "network_type": "aptos",
        "description": "Aptos Mainnet",
    },
}


@register_node_type(
    "private_key_node",
    default_params={
        "chain": "bsc",
        "private_key": "",
    },
)
class PrivateKeyNode(NodeBase):
    """
    Private Key Node — R&D Mode credential provider.

    Reads a raw private key from the user, validates the chain,
    stores credentials in the in-memory CredentialStore, and outputs
    an operator descriptor (JSON-serializable) with a credential_ref.

    CodeNode's OperatorFactory will later retrieve the credentials
    and create a live EvmOperator / AptosOperator instance.

    Security:
    - The private key is registered with log_sanitizer
    - Credentials are stored in-memory only (never serialized to RabbitMQ)
    - The signal contains only a UUID reference, not the actual key
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
            node_type="private_key_node",
            **kwargs,
        )
        self.chain = (self.params.get("chain") or "bsc").lower()
        self.private_key = self.params.get("private_key") or ""

    # ── Execution ────────────────────────────────────────────────────
    async def execute(self, input_data: Optional[Dict] = None) -> Dict:
        """Build an Operator descriptor from the user's private key."""
        try:
            await self.update_status(NodeStatus.RUNNING)
            await self.persist_log("Private Key Node starting (R&D Mode)", "INFO")

            # ── 1. Validate chain ────────────────────────────────────
            if self.chain not in SUPPORTED_CHAINS:
                raise ValueError(
                    f"Unsupported chain '{self.chain}'. "
                    f"Supported: {', '.join(SUPPORTED_CHAINS.keys())}"
                )
            chain_cfg = SUPPORTED_CHAINS[self.chain]

            # ── 2. Validate private key presence ─────────────────────
            if not self.private_key:
                raise ValueError(
                    "Private key is required. Please enter your private key "
                    "in the node configuration."
                )

            # ── 3. Register key with log sanitizer ───────────────────
            register_sensitive_value(self.private_key)

            # ── 4. Store credentials in CredentialStore ──────────────
            store = CredentialStore.get_instance()
            credential_ref = store.store(
                {"private_key": self.private_key},
                ttl=300,  # 5 minute TTL
            )

            # ── 5. Derive address for display (non-sensitive) ────────
            address_preview = "N/A"
            if chain_cfg["network_type"] == "evm":
                try:
                    from eth_account import Account
                    pk = self.private_key if self.private_key.startswith("0x") else f"0x{self.private_key}"
                    account = Account.from_key(pk)
                    address_preview = f"{account.address[:6]}...{account.address[-4:]}"
                except Exception:
                    address_preview = "invalid-key"

            # Masked preview: show only last 4 characters
            key_preview = f"****{self.private_key[-4:]}" if len(self.private_key) >= 4 else "****"

            # ── 6. Build operator descriptor (JSON-safe) ─────────────
            operator_descriptor = {
                "_tf_operator": True,  # Marker for CodeNode to detect
                "_type": "EvmOperator" if chain_cfg["network_type"] == "evm" else "AptosOperator",
                "chain": self.chain,
                "chain_id": chain_cfg["chain_id"],
                "network_type": chain_cfg["network_type"],
                "key_preview": key_preview,
                "address_preview": address_preview,
                "credential_ref": credential_ref,  # UUID to CredentialStore
            }

            await self.persist_log(
                f"Operator descriptor built for chain={self.chain} "
                f"(address: {address_preview}, key: {key_preview})",
                "INFO",
            )

            # ── 7. Send signal to downstream nodes ───────────────────
            await self.send_signal(
                source_handle=OPERATOR_OUTPUT_HANDLE,
                signal_type=SignalType.DATASET,
                payload={
                    OPERATOR_OUTPUT_HANDLE: operator_descriptor,
                },
            )

            await self.update_status(NodeStatus.COMPLETED)
            await self.persist_log("Private Key Node completed", "INFO")

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
            error_msg = f"Private Key Node error: {sanitize(str(e))}"
            await self.persist_log(error_msg, "ERROR")
            await self.update_status(NodeStatus.ERROR)
            tb = traceback.format_exc()
            return {
                "type": SignalType.ERROR.value,
                "data": {"error": sanitize(str(e)), "traceback": sanitize(tb)},
            }
