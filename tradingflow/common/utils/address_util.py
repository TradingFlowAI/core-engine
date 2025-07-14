def normalize_token_address(token_address: str, network_type: str = "evm") -> str:
    """
    根据不同的网络类型规范化代币地址

    Args:
        token_address: 代币地址
        network_type: 网络类型（evm, sui, aptos 等）

    Returns:
        规范化后的代币地址
    """
    if not token_address:
        return token_address

    if network_type.lower() == "evm":
        return token_address.lower()

    if network_type.lower() in ["sui", "aptos"]:
        # 检查是否包含 :: 分隔符
        if "::" in token_address:
            parts = token_address.split("::", 1)
            obj_id = parts[0].lower()  # 对象 ID 部分转小写
            module_type = parts[1]  # 保留模块和类型部分的大小写
            return f"{obj_id}::{module_type}"
        else:
            # 如果没有 :: 分隔符，视为对象 ID，整体转小写
            return token_address.lower()

    else:
        return token_address.lower()
