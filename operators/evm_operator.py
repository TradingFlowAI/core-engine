"""
EvmOperator — Direct wallet operations on EVM chains (BSC, Flow EVM).

Provides sync methods that CodeNode users can call without `await`:
- swap(): Execute DEX swap via PancakeSwap Smart Router
- get_balance(): Query token/native balance
- transfer(): Transfer tokens to another address
- get_portfolio(): Get full wallet asset list
- address: Wallet address property

Security:
- Private key is never in __repr__/__str__
- Private key is registered with log_sanitizer
"""

import json
import logging
from typing import Any, Dict, Optional

from web3 import Web3
from eth_account import Account

from operators.base import BaseOperator
from common.log_sanitizer import register_sensitive_value

logger = logging.getLogger(__name__)

# ── Default RPC URLs (can be overridden via constructor) ─────────────
DEFAULT_RPC_URLS = {
    56: "https://bsc-dataseed1.binance.org",          # BSC Mainnet
    97: "https://data-seed-prebsc-1-s1.binance.org:8545",  # BSC Testnet
    545: "https://testnet.evm.nodes.onflow.org",       # Flow EVM Testnet
    747: "https://mainnet.evm.nodes.onflow.org",       # Flow EVM Mainnet
}

# ── Common ERC20 ABI (minimal for balanceOf, transfer, approve, decimals) ──
ERC20_ABI = json.loads("""[
    {"constant":true,"inputs":[{"name":"_owner","type":"address"}],
     "name":"balanceOf","outputs":[{"name":"","type":"uint256"}],
     "type":"function"},
    {"constant":true,"inputs":[],"name":"decimals",
     "outputs":[{"name":"","type":"uint8"}],"type":"function"},
    {"constant":true,"inputs":[],"name":"symbol",
     "outputs":[{"name":"","type":"string"}],"type":"function"},
    {"constant":false,"inputs":[{"name":"_to","type":"address"},
     {"name":"_value","type":"uint256"}],"name":"transfer",
     "outputs":[{"name":"","type":"bool"}],"type":"function"},
    {"constant":false,"inputs":[{"name":"_spender","type":"address"},
     {"name":"_value","type":"uint256"}],"name":"approve",
     "outputs":[{"name":"","type":"bool"}],"type":"function"},
    {"constant":true,"inputs":[{"name":"_owner","type":"address"},
     {"name":"_spender","type":"address"}],"name":"allowance",
     "outputs":[{"name":"","type":"uint256"}],"type":"function"}
]""")


class EvmOperator(BaseOperator):
    """
    Direct wallet operator for EVM chains.

    Exposes synchronous public methods for CodeNode compatibility.
    web3.py HTTPProvider 为同步库，在 CodeNode 线程中直接执行。
    """

    _operator_type = "evm"

    def __init__(
        self,
        private_key: str,
        chain_id: int,
        rpc_url: Optional[str] = None,
        loop=None,
    ):
        super().__init__(loop=loop)

        # Normalize private key
        self._private_key = private_key if private_key.startswith("0x") else f"0x{private_key}"

        # Register with log sanitizer
        register_sensitive_value(self._private_key)
        register_sensitive_value(private_key)

        # Derive wallet address
        self._account = Account.from_key(self._private_key)
        self._address = self._account.address
        self._chain_id = chain_id

        # Connect to RPC
        rpc = rpc_url or DEFAULT_RPC_URLS.get(chain_id)
        if not rpc:
            raise ValueError(
                f"No RPC URL configured for chain_id={chain_id}. "
                f"Please provide rpc_url parameter."
            )
        self._w3 = Web3(Web3.HTTPProvider(rpc))

        # Display info (safe — address is public)
        chain_name = {56: "bsc", 97: "bsc-testnet", 545: "flow-evm-testnet", 747: "flow-evm"}.get(
            chain_id, f"chain-{chain_id}"
        )
        self._display_info = f"chain={chain_name}, address={self._address[:6]}...{self._address[-4:]}"

    # ── Properties ───────────────────────────────────────────────────

    @property
    def address(self) -> str:
        """Wallet address (public, non-sensitive)."""
        return self._address

    @property
    def chain_id(self) -> int:
        """Chain ID."""
        return self._chain_id

    # ── Public sync API ──────────────────────────────────────────────
    # 注意：web3.py HTTPProvider 是同步库。CodeNode 的用户代码已在独立线程
    # (threading.Thread) 中执行，所以这里直接用同步调用即可。
    # 不要用 _run_async() 桥接，否则会阻塞 Sanic 的主事件循环。

    def get_balance(self, token: Optional[str] = None) -> Dict[str, Any]:
        """
        Get token balance.

        Args:
            token: Token contract address. None for native token (BNB/FLOW).

        Returns:
            Dict with 'balance' (human-readable), 'raw' (wei), 'symbol', 'decimals'.
        """
        if token is None:
            # Native token balance
            raw_balance = self._w3.eth.get_balance(self._address)
            balance = Web3.from_wei(raw_balance, "ether")
            symbol = {56: "BNB", 97: "tBNB", 545: "FLOW", 747: "FLOW"}.get(
                self._chain_id, "ETH"
            )
            return {
                "balance": float(balance),
                "raw": raw_balance,
                "symbol": symbol,
                "decimals": 18,
                "token": "native",
            }
        else:
            # ERC20 balance
            contract = self._w3.eth.contract(
                address=Web3.to_checksum_address(token), abi=ERC20_ABI
            )
            raw_balance = contract.functions.balanceOf(self._address).call()
            decimals = contract.functions.decimals().call()
            try:
                symbol = contract.functions.symbol().call()
            except Exception:
                symbol = "UNKNOWN"
            balance = raw_balance / (10 ** decimals)
            return {
                "balance": float(balance),
                "raw": raw_balance,
                "symbol": symbol,
                "decimals": decimals,
                "token": token,
            }

    def transfer(self, to_address: str, amount: float, token: Optional[str] = None) -> Dict[str, Any]:
        """
        Transfer tokens to another address.

        Args:
            to_address: Recipient address.
            amount: Amount in human-readable units.
            token: Token contract address. None for native token.

        Returns:
            Dict with 'tx_hash', 'status', etc.
        """
        to_addr = Web3.to_checksum_address(to_address)

        if token is None:
            # Native token transfer
            tx = {
                "from": self._address,
                "to": to_addr,
                "value": Web3.to_wei(amount, "ether"),
                "gas": 21000,
                "gasPrice": self._w3.eth.gas_price,
                "nonce": self._w3.eth.get_transaction_count(self._address),
                "chainId": self._chain_id,
            }
        else:
            # ERC20 transfer
            contract = self._w3.eth.contract(
                address=Web3.to_checksum_address(token), abi=ERC20_ABI
            )
            decimals = contract.functions.decimals().call()
            raw_amount = int(amount * (10 ** decimals))
            tx = contract.functions.transfer(to_addr, raw_amount).build_transaction(
                {
                    "from": self._address,
                    "gas": 100000,
                    "gasPrice": self._w3.eth.gas_price,
                    "nonce": self._w3.eth.get_transaction_count(self._address),
                    "chainId": self._chain_id,
                }
            )

        signed = self._w3.eth.account.sign_transaction(tx, self._private_key)
        tx_hash = self._w3.eth.send_raw_transaction(signed.raw_transaction)
        receipt = self._w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)

        return {
            "tx_hash": receipt.transactionHash.hex(),
            "status": "success" if receipt.status == 1 else "failed",
            "block_number": receipt.blockNumber,
            "gas_used": receipt.gasUsed,
        }

    def get_portfolio(self) -> Dict[str, Any]:
        """
        Get wallet portfolio (native balance).

        Returns:
            Dict with native balance info.
        """
        native = self.get_balance(None)
        return {
            "address": self._address,
            "chain_id": self._chain_id,
            "native": native,
        }

    def swap(
        self,
        token_in: str,
        token_out: str,
        amount: float,
        slippage: float = 0.5,
        **kwargs,
    ) -> Dict[str, Any]:
        """
        Execute a DEX swap.

        Args:
            token_in: Input token address or symbol ('BNB', 'USDT', etc.)
            token_out: Output token address or symbol
            amount: Amount of input token (human-readable units)
            slippage: Slippage tolerance in percent (default 0.5%)

        Returns:
            Dict with 'tx_hash', 'amount_in', 'amount_out', etc.

        Note: Full DEX integration will be completed in a future iteration.
              Currently provides the transaction framework.
        """
        # TODO: Integrate PancakeSwap Smart Router for V2/V3/mixed paths
        raise NotImplementedError(
            "DEX swap via EvmOperator is under development. "
            "Full PancakeSwap Smart Router integration will be available soon. "
            "Currently supported operations: get_balance(), transfer(), send_transaction()."
        )

    def send_transaction(self, tx_params: dict) -> Dict[str, Any]:
        """
        Send a raw transaction (advanced use).

        Args:
            tx_params: Transaction parameters dict with 'to', 'value', 'data', etc.

        Returns:
            Dict with 'tx_hash', 'status'.
        """
        tx = {
            "from": self._address,
            "chainId": self._chain_id,
            "nonce": self._w3.eth.get_transaction_count(self._address),
            "gasPrice": self._w3.eth.gas_price,
            **tx_params,
        }

        # Estimate gas if not provided
        if "gas" not in tx:
            tx["gas"] = self._w3.eth.estimate_gas(tx)

        signed = self._w3.eth.account.sign_transaction(tx, self._private_key)
        tx_hash = self._w3.eth.send_raw_transaction(signed.raw_transaction)
        receipt = self._w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)

        return {
            "tx_hash": receipt.transactionHash.hex(),
            "status": "success" if receipt.status == 1 else "failed",
            "block_number": receipt.blockNumber,
            "gas_used": receipt.gasUsed,
        }
