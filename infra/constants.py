"""TradingFlow Constants

Contains chain mappings, contract configurations, and other constants.
"""

from typing import Dict

# Chain RPC URL mapping
CHAIN_RPC_MAPPING: Dict[int, str] = {
    31337: "http://127.0.0.1:8545",  # Hardhat local network
    1337: "http://127.0.0.1:8545",   # Local network
}


# Uniswap V3 deployment mapping
UNISWAP_V3_DEPLOYMENT_MAPPING: Dict[int, Dict[str, str]] = {
    31337: {
        "factory": "0x1F98431c8aD98523631AE4a59f267346ea31F984",
        "non_fungible_position_manager": "0xC36442b4a4522E871399CD717aBDD847Ab11FE88",
        "swap_router": "0xE592427A0AEce92De3Edee1F18E0157C05861564",
    },
}

# TradingFlow price oracle mapping
tf_price_oracle_mapping: Dict[int, str] = {
    31337: "0xf4c5C29b14f0237131F7510A51684c8191f98E06",
}


# Vault contract file path
VAULT_CONTRACT_FILE_PATH = {
    "EVM": {
        "path": "contracts/uniswap/OracleGuidedVault.json",
        "abi": "abi",
        "bytecode": "bytecode",
    },
}

# ERC20 ABI for basic token operations
erc20_abi = [
    {
        "constant": True,
        "inputs": [],
        "name": "name",
        "outputs": [{"name": "", "type": "string"}],
        "payable": False,
        "stateMutability": "view",
        "type": "function",
    },
    {
        "constant": True,
        "inputs": [],
        "name": "symbol",
        "outputs": [{"name": "", "type": "string"}],
        "payable": False,
        "stateMutability": "view",
        "type": "function",
    },
    {
        "constant": True,
        "inputs": [],
        "name": "decimals",
        "outputs": [{"name": "", "type": "uint8"}],
        "payable": False,
        "stateMutability": "view",
        "type": "function",
    },
    {
        "constant": True,
        "inputs": [],
        "name": "totalSupply",
        "outputs": [{"name": "", "type": "uint256"}],
        "payable": False,
        "stateMutability": "view",
        "type": "function",
    },
]


# EVM chain ID to network name mapping - only for EVM networks
# Note: Non-EVM networks (like Aptos, Sui) should not use this mapping
# even if they have the same chain_id
EVM_CHAIN_ID_NETWORK_MAP = {
    1: "eth",           # Ethereum mainnet
    10: "optimism",
    56: "bsc",          # BSC mainnet
    97: "bsc-testnet",  # BSC testnet
    137: "polygon",
    31337: "eth",       # Hardhat local testnet
    42161: "arbitrum",
    43114: "avalanche",
    545: "flow-evm-testnet",  # Flow EVM testnet
    747: "flow-evm",    # Flow EVM mainnet
}

# BSC Chain Configuration
BSC_CONFIG = {
    "mainnet": {
        "chain_id": 56,
        "name": "bsc",
        "rpc_url": "https://bsc-dataseed.binance.org/",
        "native_symbol": "BNB",
        "wrapped_native": "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c",  # WBNB
        "swap_router": "0x13f4EA83D0bd40E75C8222255bc855a974568Dd4",  # PancakeSwap V3 Router
        "explorer": "https://bscscan.com",
    },
    "testnet": {
        "chain_id": 97,
        "name": "bsc-testnet",
        "rpc_url": "https://data-seed-prebsc-1-s1.binance.org:8545/",
        "native_symbol": "tBNB",
        "wrapped_native": "0xae13d989daC2f0dEbFf460aC112a837C89BAa7cd",  # WBNB Testnet
        "swap_router": "0x1b81D678ffb9C0263b24A97847620C99d213eB14",  # PancakeSwap V3 Router Testnet
        "explorer": "https://testnet.bscscan.com",
    },
}

# Flow EVM Chain Configuration
FLOW_EVM_CONFIG = {
    "mainnet": {
        "chain_id": 747,
        "name": "flow-evm",
        "rpc_url": "https://mainnet.evm.nodes.onflow.org",
        "native_symbol": "FLOW",
        "wrapped_native": "0xd3bF53DAC106A0290B0483EcBC89d40FcC961f3e",  # WFLOW
        "explorer": "https://evm.flowscan.io",
    },
    "testnet": {
        "chain_id": 545,
        "name": "flow-evm-testnet",
        "rpc_url": "https://testnet.evm.nodes.onflow.org",
        "native_symbol": "FLOW",
        "wrapped_native": "0xd3bF53DAC106A0290B0483EcBC89d40FcC961f3e",  # WFLOW Testnet
        "explorer": "https://evm-testnet.flowscan.io",
    },
}

# Extended network mapping, supporting non-EVM chains
NETWORK_TYPE_MAPPING = {
    "aptos": "aptos",
    "sui": "sui-network",
    "solana": "solana",
    "flow-evm": "flow-evm",
    "ethereum": "eth",
    "evm": "eth",  # Default EVM network
}

# Complete chain ID to network mapping (including non-EVM chains)
ALL_CHAIN_NETWORK_MAP = {
    # EVM chains
    **EVM_CHAIN_ID_NETWORK_MAP,
    # Non-EVM chains use special identifiers
    "aptos": "aptos",
    "sui": "sui-network",
    "solana": "solana",
}


def get_network_info_by_name(network_name: str) -> tuple[str, str]:
    """
    Safely get network info by network name.

    Args:
        network_name: Network name

    Returns:
        tuple: (standardized_network_name, network_type)
    """
    if not network_name:
        return "unknown", "unknown"

    network_lower = network_name.lower()

    # Non-EVM networks
    if network_lower in ["aptos"]:
        return "aptos", "aptos"
    elif network_lower in ["sui", "sui-network"]:
        return "sui-network", "sui"
    elif network_lower in ["solana"]:
        return "solana", "solana"
    # Flow EVM networks
    elif network_lower in ["flow-evm", "flow_evm"]:
        return "flow-evm", "evm"
    elif network_lower in ["flow-evm-testnet", "flow_evm_testnet"]:
        return "flow-evm-testnet", "evm"
    # BSC networks
    elif network_lower in ["bsc", "binance", "binance-smart-chain"]:
        return "bsc", "evm"
    elif network_lower in ["bsc-testnet", "bsc_testnet", "binance-testnet"]:
        return "bsc-testnet", "evm"
    else:
        # EVM network name variants
        evm_network_aliases = {
            "ethereum": "eth",
            "eth": "eth",
            "mainnet": "eth",
            "polygon": "polygon",
            "matic": "polygon",
            "optimism": "optimism",
            "arbitrum": "arbitrum",
            "avalanche": "avalanche",
            "hardhat": "eth",
            "localhost": "eth",
        }

        for alias, standard_name in evm_network_aliases.items():
            if alias in network_lower:
                return standard_name, "evm"

        # Default case: keep original name but mark as EVM type
        return network_name, "evm"


def get_chain_config(chain_id: int) -> dict:
    """
    Get chain configuration by chain ID.

    Args:
        chain_id: Chain ID

    Returns:
        dict: Chain configuration or empty dict if not found
    """
    # BSC
    if chain_id == 56:
        return BSC_CONFIG["mainnet"]
    if chain_id == 97:
        return BSC_CONFIG["testnet"]
    # Flow EVM
    if chain_id == 747:
        return FLOW_EVM_CONFIG["mainnet"]
    if chain_id == 545:
        return FLOW_EVM_CONFIG["testnet"]
    return {}


def is_evm_chain_id(chain_id: int) -> bool:
    """
    Check if the given chain_id belongs to an EVM network.

    Args:
        chain_id: Chain ID

    Returns:
        bool: True if it's a known EVM chain ID
    """
    return chain_id in EVM_CHAIN_ID_NETWORK_MAP
