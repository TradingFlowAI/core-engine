import os

from web3 import Web3

from tradingflow.depot.constants import CHAIN_RPC_MAPPING


# 根据chainId获取对应的私钥
def get_deployer_key_by_chain_id(chain_id):
    # 映射chainId到对应的环境变量名
    chain_key_mapping = {
        1: "DEPLOYER_PRIVATE_KEY_MAINNET",  # 以太坊主网
        5: "DEPLOYER_PRIVATE_KEY_GOERLI",  # Goerli测试网
        56: "DEPLOYER_PRIVATE_KEY_BSC",  # BSC
        137: "DEPLOYER_PRIVATE_KEY_POLYGON",  # Polygon
        31337: "DEPLOYER_PRIVATE_KEY_HARDHAT",  # Hardhat本地网络
        # 添加更多链
    }

    # 获取对应链的环境变量名
    env_var_name = chain_key_mapping.get(chain_id, "DEPLOYER_PRIVATE_KEY_DEFAULT")
    # 返回环境变量中存储的私钥
    return os.getenv(env_var_name)


def get_w3(chain_id=31337):
    """获取web3实例"""
    w3 = Web3(Web3.HTTPProvider(CHAIN_RPC_MAPPING[chain_id]))
    return w3


def get_deployer_account(w3, chain_id=31337):
    """获取部署者账户"""
    deployer_private_key = get_deployer_key_by_chain_id(chain_id)
    deployer_account = w3.eth.account.from_key(deployer_private_key)
    return deployer_account
