from typing import Dict

# TODO: 需不需要检查chain_id是否正确
CHAIN_RPC_MAPPING: Dict[int, str] = {
    # "1": "https://mainnet.infura.io/v3/YOUR_INFURA_PROJECT_ID",
    # "3": "https://ropsten.infura.io/v3/YOUR_INFURA_PROJECT_ID",
    # "4": "https://rinkeby.infura.io/v3/YOUR_INFURA_PROJECT_ID",
    # "5": "https://goerli.infura.io/v3/YOUR_INFURA_PROJECT_ID",
    # "42": "https://kovan.infura.io/v3/YOUR_INFURA_PROJECT_ID",
    # Add more mappings as needed
    # local testing
    31337: "http://127.0.0.1:8545",
    1337: "http://127.0.0.1:8545",
}


UNISWAP_V3_DEPLOYMENT_MAPPING: Dict[int, Dict[str, str]] = {
    31337: {
        "factory": "0x1F98431c8aD98523631AE4a59f267346ea31F984",
        "non_fungible_position_manager": "0xC36442b4a4522E871399CD717aBDD847Ab11FE88",
        "swap_router": "0xE592427A0AEce92De3Edee1F18E0157C05861564",
    },
}

tf_price_oracle_mapping: Dict[int, str] = {
    31337: "0xf4c5C29b14f0237131F7510A51684c8191f98E06",
}


VAULT_CONTRACT_FILE_PATH = {
    "EVM": {
        "path": "contracts/uniswap/OracleGuidedVault.json",
        "abi": "abi",
        "bytecode": "bytecode",
    },
}

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


# EVM链ID到网络名称的映射 - 仅适用于EVM网络
# 注意：非EVM网络（如Aptos、Sui等）即使有相同的chain_id，也不应使用此映射
EVM_CHAIN_ID_NETWORK_MAP = {
    1: "eth",  # Ethereum主网
    10: "optimism",
    56: "bsc",
    137: "polygon",
    31337: "eth",  # Hardhat本地测试网
    42161: "arbitrum",
    43114: "avalanche",
    747: "flow-evm",  # Flow EVM主网
}

# 扩展网络映射，支持非EVM链
NETWORK_TYPE_MAPPING = {
    "aptos": "aptos",
    "sui": "sui-network",
    "solana": "solana",
    "flow-evm": "flow-evm",
    "ethereum": "eth",
    "evm": "eth",  # 默认EVM网络
}

# 完整的链ID到网络映射（包含非EVM链）
ALL_CHAIN_NETWORK_MAP = {
    # EVM链
    **EVM_CHAIN_ID_NETWORK_MAP,
    # 非EVM链使用特殊标识
    "aptos": "aptos",
    "sui": "sui-network",
    "solana": "solana",
}


def get_network_info_by_name(network_name: str) -> tuple[str, str]:
    """
    根据网络名称安全地获取网络信息

    Args:
        network_name: 网络名称

    Returns:
        tuple: (standardized_network_name, network_type)
    """
    if not network_name:
        return "unknown", "unknown"

    network_lower = network_name.lower()

    # 非EVM网络
    if network_lower in ["aptos"]:
        return "aptos", "aptos"
    elif network_lower in ["sui", "sui-network"]:
        return "sui-network", "sui"
    elif network_lower in ["solana"]:
        return "solana", "solana"
    elif network_lower in ["flow-evm", "flow_evm"]:
        return "flow-evm", "evm"
    else:
        # EVM网络的名称变体
        evm_network_aliases = {
            "ethereum": "eth",
            "eth": "eth",
            "mainnet": "eth",
            "polygon": "polygon",
            "matic": "polygon",
            "bsc": "bsc",
            "binance": "bsc",
            "optimism": "optimism",
            "arbitrum": "arbitrum",
            "avalanche": "avalanche",
            "hardhat": "eth",
            "localhost": "eth",
        }

        for alias, standard_name in evm_network_aliases.items():
            if alias in network_lower:
                return standard_name, "evm"

        # 默认情况，保持原名称但标记为EVM类型
        return network_name, "evm"


def is_evm_chain_id(chain_id: int) -> bool:
    """
    检查给定的chain_id是否属于EVM网络

    Args:
        chain_id: 链ID

    Returns:
        bool: 如果是已知的EVM链ID则返回True
    """
    return chain_id in EVM_CHAIN_ID_NETWORK_MAP
