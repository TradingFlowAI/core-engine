import json
import logging
from pathlib import Path
from typing import ClassVar, Dict, Optional

from tradingflow.account_manager.utils import vault_deployer
from tradingflow.common.config import TRADING_FLOW_ROOT
from tradingflow.common.constants import (
    UNISWAP_V3_DEPLOYMENT_MAPPING,
    VAULT_CONTRACT_FILE_PATH,
    erc20_abi,
    tf_price_oracle_mapping,
)

# 添加日志记录器
logger = logging.getLogger(__name__)


def load_abi(path):
    with open(path) as f:
        return json.load(f)["abi"]


def get_contract_bytecode(compiled_json_path):
    with open(compiled_json_path) as f:
        contract_json = json.load(f)
    return contract_json["bytecode"]


BASE_PATH = Path(__file__).parent.parent


class VaultService:
    """
    Service class for managing the vault.

    Provides complete lifecycle management for Vault contracts, including deployment,
    transaction execution and query functions. This class implements a singleton factory pattern
    to ensure the same instance is always returned for the same chain_id.
    """

    # 类变量用于存储已创建的实例
    _instances: ClassVar[Dict[int, "VaultService"]] = {}

    @classmethod
    def get_instance(cls, chain_id: int) -> "VaultService":
        """
        Returns a VaultService instance for the specified chain_id, creating one if it doesn't exist

        Args:
            chain_id: Blockchain network ID

        Returns:
            VaultService: Singleton instance for the corresponding chain_id
        """
        if chain_id not in cls._instances:
            cls._instances[chain_id] = cls(chain_id)
        return cls._instances[chain_id]

    def __init__(self, chain_id: int, private_key: Optional[str] = None):
        """
        Initialize VaultService

        Args:
            chain_id: Blockchain network ID
            private_key: Private key (optional, uses default account if not provided)

        Note: Please use get_instance method to obtain an instance instead of creating directly
        """
        self.chain_id = chain_id
        self.w3 = vault_deployer.get_w3(chain_id)
        self.deployer_account = vault_deployer.get_deployer_account(self.w3, chain_id)
        self.private_key = private_key

        # 交易结果记录
        self.last_tx_hash = None
        self.last_error = None

        # TODO: 暂时注释掉，因为需要使用新的合约 -- By CL @ July 4
        # 加载合约信息
        # Vault_bytecode = get_contract_bytecode(
        #     TRADING_FLOW_ROOT / VAULT_CONTRACT_FILE_PATH["EVM"]["path"]
        # )
        # Vault_abi = load_abi(
        #     TRADING_FLOW_ROOT / VAULT_CONTRACT_FILE_PATH["EVM"]["path"]
        # )
        # Vault = self.w3.eth.contract(abi=Vault_abi, bytecode=Vault_bytecode)
        # self.vault_contract = Vault
        # self.vault_abi = Vault_abi

        logger.info("VaultService initialized: chain_id=%s", self.chain_id)

    def deploy_vault(
        self,
        asset_address: str,
        investor_address: str,
        vault_name: str = None,
        vault_symbol: str = None,
    ) -> dict:
        """
        Deploy a Vault contract

        Args:
            asset_address: Base asset token address
            investor_address: Investor address
            vault_name: Vault name (optional, generated based on asset name if not provided)
            vault_symbol: Vault token symbol (optional, generated based on asset symbol if not provided)

        Returns:
            dict: Deployment details information

        Raises:
            ValueError: Raised when token information retrieval fails
        """
        # 获取代币信息以生成默认的vault名称和符号
        token_info = self.get_erc20_details(asset_address)
        # 检查代币信息是否有效
        if "error" in token_info:
            error_msg = f"Failed to get token information: {token_info['error']}"
            logger.error(error_msg)
            raise ValueError(error_msg)

        # 如果未提供名称和符号，则基于代币信息生成
        if not vault_name:
            vault_name = f"Vault{token_info['name']}"
        if not vault_symbol:
            vault_symbol = f"v{token_info['symbol']}"

        swap_router_address = UNISWAP_V3_DEPLOYMENT_MAPPING[self.chain_id][
            "swap_router"
        ]
        price_oracle_address = tf_price_oracle_mapping[self.chain_id]

        logger.info(
            "Deploying Vault - Underlying asset: %s",
            token_info.get("name", asset_address),
        )
        logger.info("Vault name: %s, symbol: %s", vault_name, vault_symbol)

        tx_hash = self.vault_contract.constructor(
            asset_address,  # _asset (ERC20地址)
            vault_name,  # _name
            vault_symbol,  # _symbol
            swap_router_address,  # _swapRouter
            price_oracle_address,  # _priceOracle
            investor_address,  # _initialInvestor
        ).transact({"from": self.deployer_account.address, "gas": 5000000})

        tx_receipt = self.w3.eth.wait_for_transaction_receipt(tx_hash)
        vault_address = tx_receipt.contractAddress

        # 构建更详细的返回信息
        deployment_info = {
            "status": "success" if tx_receipt.status == 1 else "failed",
            "vault_address": vault_address,
            "asset_address": asset_address,
            "asset_details": token_info,
            "vault_name": vault_name,
            "vault_symbol": vault_symbol,
            "transaction_hash": self.w3.to_hex(tx_receipt.transactionHash),
            "block_number": tx_receipt.blockNumber,
            "gas_used": tx_receipt.gasUsed,
            "effective_gas_price": tx_receipt.effectiveGasPrice,
            "total_cost_wei": tx_receipt.gasUsed * tx_receipt.effectiveGasPrice,
            "total_cost_eth": str(
                self.w3.from_wei(
                    tx_receipt.gasUsed * tx_receipt.effectiveGasPrice, "ether"
                )
            ),
            "deployer_address": self.deployer_account.address,
        }

        # 记录部署信息
        logger.info("Vault deployment successful: %s", vault_address)
        logger.debug("Deployment details: %s", deployment_info)
        return deployment_info

    def get_erc20_details(self, token_address: str) -> dict:
        """
        Get detailed information about an ERC20 token, including name and symbol

        Args:
            token_address: ERC20 token contract address

        Returns:
            Dictionary containing token details
        """
        # 首先检查地址格式
        try:
            # 确保地址格式正确并转换为校验后的地址
            token_address = self.w3.to_checksum_address(token_address)
        except ValueError as e:
            logger.warning("Invalid address format: %s", str(e))
            return {
                "address": token_address,
                "error": f"Invalid address format: {str(e)}",
            }

        # 检查合约是否存在
        try:
            code = self.w3.eth.get_code(token_address)
            if code == b"" or code == "0x":
                logger.warning(
                    "Address %s is not a contract or contract is not deployed",
                    token_address,
                )
                return {
                    "address": token_address,
                    "error": "This address is not a contract or contract is not deployed",
                }
        except Exception as e:
            logger.warning("Error checking contract code: %s", str(e))
            return {
                "address": token_address,
                "error": f"Error checking contract code: {str(e)}",
            }

        try:
            # 创建合约实例
            token_contract = self.w3.eth.contract(address=token_address, abi=erc20_abi)

            # 初始化结果字典
            result = {"address": token_address}

            # 分别尝试获取各个属性，而不是一次失败全部失败
            try:
                result["name"] = token_contract.functions.name().call()
            except Exception as e:
                logger.warning("Error getting token name: %s", str(e))
                result["name"] = "Unknown Token"

            try:
                result["symbol"] = token_contract.functions.symbol().call()
            except Exception as e:
                logger.warning("Error getting token symbol: %s", str(e))
                result["symbol"] = "???"

            try:
                result["decimals"] = token_contract.functions.decimals().call()
            except Exception as e:
                logger.warning("Error getting token decimals: %s", str(e))
                result["decimals"] = 18  # 默认设为18，这是最常见的精度

            try:
                total_supply = token_contract.functions.totalSupply().call()
                result["total_supply"] = total_supply
                result["total_supply_formatted"] = total_supply / (
                    10 ** result["decimals"]
                )
            except Exception as e:
                logger.warning("Error getting token total supply: %s", str(e))
                result["total_supply"] = None
                result["total_supply_formatted"] = None

            return result

        except Exception as e:
            error_msg = f"Error getting ERC20 details: {str(e)}"
            logger.error(error_msg)
            # 在出错时尝试提供更多诊断信息
            try:
                # 检查节点连接状态
                is_syncing = self.w3.eth.syncing
                if is_syncing:
                    error_msg += " (Blockchain node is syncing)"
                else:
                    block_num = self.w3.eth.block_number
                    error_msg += f" (Node synced to block {block_num})"
            except:
                pass

            return {"address": token_address, "error": error_msg}

    def get_portfolio_composition(self, vault_address: str) -> dict:
        """
        Get the portfolio composition of a Vault

        Args:
            vault_address: Vault contract address

        Returns:
            Dictionary containing portfolio information, including base assets and token balances
        """
        self.last_error = None

        try:
            # 确保地址格式正确并转换为校验后的地址
            vault_address = self.w3.to_checksum_address(vault_address)

            # 创建合约实例
            vault_contract = self.w3.eth.contract(
                address=vault_address, abi=self.vault_abi
            )

            # 调用合约方法获取持仓组成
            portfolio = vault_contract.functions.getPortfolioComposition().call()
            base_asset_amount, token_addresses, token_amounts = portfolio

            # 获取基础资产名称和符号
            try:
                base_asset_address = vault_contract.functions.asset().call()
                base_asset_contract = self.w3.eth.contract(
                    address=base_asset_address, abi=erc20_abi
                )
                base_asset_symbol = base_asset_contract.functions.symbol().call()
                base_asset_name = base_asset_contract.functions.name().call()
                decimals = vault_contract.functions.decimals().call()
            except Exception as e:
                logger.warning("Failed to get base asset information: %s", str(e))
                base_asset_symbol = "???"
                base_asset_name = "Unknown Asset"
                decimals = 18

            # 构建结果
            result = {
                "base_asset": {
                    "address": base_asset_address,
                    "symbol": base_asset_symbol,
                    "name": base_asset_name,
                    "amount": base_asset_amount,
                    "amount_human": self._format_amount(base_asset_amount, decimals),
                },
                "tokens": [],
            }

            # 添加所有持有的代币信息
            for i in range(len(token_addresses)):
                if token_addresses[i] == "0x0000000000000000000000000000000000000000":
                    continue

                token_info = self._get_token_info(token_addresses[i], token_amounts[i])
                result["tokens"].append(token_info)

            return result

        except Exception as e:
            error_msg = f"Error getting portfolio composition: {str(e)}"
            logger.error(error_msg)
            self.last_error = error_msg
            return {"error": error_msg}

    def _get_token_info(self, token_address: str, amount: int) -> dict:
        """
        Get token information

        Args:
            token_address: Token contract address
            amount: Token amount

        Returns:
            Dictionary containing token information
        """
        try:
            # 获取代币合约
            token_contract = self.w3.eth.contract(
                address=token_address,
                abi=[
                    {
                        "inputs": [],
                        "name": "symbol",
                        "outputs": [
                            {"internalType": "string", "name": "", "type": "string"}
                        ],
                        "stateMutability": "view",
                        "type": "function",
                    },
                    {
                        "inputs": [],
                        "name": "name",
                        "outputs": [
                            {"internalType": "string", "name": "", "type": "string"}
                        ],
                        "stateMutability": "view",
                        "type": "function",
                    },
                    {
                        "inputs": [],
                        "name": "decimals",
                        "outputs": [
                            {"internalType": "uint8", "name": "", "type": "uint8"}
                        ],
                        "stateMutability": "view",
                        "type": "function",
                    },
                ],
            )
            token_symbol = token_contract.functions.symbol().call()
            token_name = token_contract.functions.name().call()
            token_decimals = token_contract.functions.decimals().call()
        except Exception as e:
            logger.warning(
                "Failed to get token %s information: %s", token_address, str(e)
            )
            token_symbol = "Unknown Token"
            token_name = "Unknown Token"
            token_decimals = 18

        return {
            "address": token_address,
            "symbol": token_symbol,
            "name": token_name,
            "amount": amount,
            "amount_human": self._format_amount(amount, token_decimals),
        }

    def _format_amount(self, amount: int, decimals: int = 18) -> float:
        """
        Format token amount to human readable format

        Args:
            amount: Token amount (Wei)
            decimals: Token decimal places

        Returns:
            Formatted amount
        """
        return amount / (10**decimals)

    async def print_portfolio(self, vault_address: str) -> None:
        """
        Print vault portfolio information

        Args:
            vault_address: Vault contract address
        """
        portfolio = self.get_portfolio_composition(vault_address)
        if "error" in portfolio:
            logger.error(
                "Cannot get vault portfolio information: %s", portfolio["error"]
            )
            return

        logger.info("========== Vault Portfolio Information ==========")
        base_asset = portfolio["base_asset"]
        logger.info("Base Asset: %s (%s)", base_asset["symbol"], base_asset["name"])
        logger.info(
            "Balance: %s (%s wei)", base_asset["amount_human"], base_asset["amount"]
        )

        if portfolio["tokens"]:
            logger.info("\nHeld Tokens:")
            for token in portfolio["tokens"]:
                logger.info("- %s (%s)", token["symbol"], token["name"])
                logger.info("  Address: %s", token["address"])
                logger.info(
                    "  Balance: %s (%s wei)", token["amount_human"], token["amount"]
                )
        else:
            logger.info("\nNo other tokens held")

        logger.info("===================================")

    async def execute_buy_signal(
        self,
        vault_address: str,
        token_address: str,
        amount: int,
        min_output_amount: int = 0,
        max_allocation_percent: int = 3000,
    ) -> bool:
        """
        Execute buy signal

        Args:
            vault_address: Vault contract address
            token_address: Token contract address
            amount: Buy amount
            min_output_amount: Minimum output amount
            max_allocation_percent: Maximum allocation percentage (10000 = 100.00%)

        Returns:
            Whether the transaction was successful
        """
        self.last_tx_hash = None
        self.last_error = None

        try:
            # 确保地址格式正确
            vault_address = self.w3.to_checksum_address(vault_address)
            token_address = self.w3.to_checksum_address(token_address)

            # 创建合约实例
            vault_contract = self.w3.eth.contract(
                address=vault_address, abi=self.vault_abi
            )

            # 获取交易账户
            if self.private_key:
                account = self.w3.eth.account.from_key(self.private_key)
                from_address = account.address
            else:
                # 使用默认账户或部署者账户
                from_address = self.deployer_account.address

            # 构建交易
            logger.info(
                "Preparing to execute buy operation: vault=%s, token=%s, amount=%s, min_output_amount=%s, max_allocation_percent=%s",
                vault_address,
                token_address,
                amount,
                min_output_amount,
                max_allocation_percent,
            )

            tx = vault_contract.functions.executeBuySignal(
                token_address, amount, min_output_amount, max_allocation_percent
            ).build_transaction(
                {
                    "from": from_address,
                    "gas": 5000000,
                    "gasPrice": self.w3.eth.gas_price,
                    "nonce": self.w3.eth.get_transaction_count(from_address),
                }
            )

            # 签名并发送交易
            if self.private_key:
                signed_tx = self.w3.eth.account.sign_transaction(tx, self.private_key)
                tx_hash = self.w3.eth.send_raw_transaction(signed_tx.rawTransaction)
            else:
                tx_hash = self.w3.eth.send_transaction(tx)

            self.last_tx_hash = tx_hash
            logger.info("Buy transaction submitted: tx_hash=%s", tx_hash.hex())

            # 等待交易确认
            receipt = self.w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)

            if receipt.status == 1:
                logger.info("Buy transaction successful: tx_hash=%s", tx_hash.hex())
                return True
            else:
                self.last_error = f"Transaction failed, status code: {receipt.status}"
                logger.error(
                    "Buy transaction failed: tx_hash=%s, status code: %s",
                    tx_hash.hex(),
                    receipt.status,
                )
                return False

        except Exception as e:
            self.last_error = str(e)
            logger.exception("Failed to execute buy signal: %s", str(e))
            return False

    async def execute_sell_signal(
        self,
        vault_address: str,
        token_address: str,
        amount: int = 0,
        min_output_amount: int = 0,
    ) -> bool:
        """
        Execute sell signal

        Args:
            vault_address: Vault contract address
            token_address: Token contract address
            amount: Sell amount (0 means all)
            min_output_amount: Minimum output amount

        Returns:
            Whether the transaction was successful
        """
        self.last_tx_hash = None
        self.last_error = None

        try:
            # 确保地址格式正确
            vault_address = self.w3.to_checksum_address(vault_address)
            token_address = self.w3.to_checksum_address(token_address)

            # 创建合约实例
            vault_contract = self.w3.eth.contract(
                address=vault_address, abi=self.vault_abi
            )

            # 获取交易账户
            if self.private_key:
                account = self.w3.eth.account.from_key(self.private_key)
                from_address = account.address
            else:
                # 使用默认账户或部署者账户
                from_address = self.deployer_account.address

            # 构建交易
            amount_str = str(amount) if amount > 0 else "all"
            logger.info(
                "Preparing to execute sell operation: vault=%s, token=%s, amount=%s",
                vault_address,
                token_address,
                amount_str,
            )

            tx = vault_contract.functions.executeSellSignal(
                token_address, amount, min_output_amount
            ).build_transaction(
                {
                    "from": from_address,
                    "gas": 5000000,
                    "gasPrice": self.w3.eth.gas_price,
                    "nonce": self.w3.eth.get_transaction_count(from_address),
                }
            )

            # 签名并发送交易
            if self.private_key:
                signed_tx = self.w3.eth.account.sign_transaction(tx, self.private_key)
                tx_hash = self.w3.eth.send_raw_transaction(signed_tx.rawTransaction)
            else:
                tx_hash = self.w3.eth.send_transaction(tx)

            self.last_tx_hash = tx_hash
            logger.info("Sell transaction submitted: tx_hash=%s", tx_hash.hex())

            # 等待交易确认
            receipt = self.w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)

            if receipt.status == 1:
                logger.info("Sell transaction successful: tx_hash=%s", tx_hash.hex())
                return True
            else:
                self.last_error = f"Transaction failed, status code: {receipt.status}"
                logger.error(
                    "Sell transaction failed: tx_hash=%s, status code: %s",
                    tx_hash.hex(),
                    receipt.status,
                )
                return False

        except Exception as e:
            self.last_error = str(e)
            logger.exception("Failed to execute sell signal: %s", str(e))
            return False
