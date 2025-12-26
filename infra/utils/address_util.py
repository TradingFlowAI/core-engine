"""Address Utility Functions"""


def normalize_token_address(token_address: str, network_type: str = "evm") -> str:
    """
    Normalize token address based on different network types.

    Args:
        token_address: Token address
        network_type: Network type (evm, sui, aptos, etc.)

    Returns:
        Normalized token address
    """
    if not token_address:
        return token_address

    if network_type.lower() == "evm":
        return token_address.lower()

    if network_type.lower() in ["sui", "aptos"]:
        # Check if contains :: separator
        if "::" in token_address:
            parts = token_address.split("::", 1)
            obj_id = parts[0].lower()  # Object ID part to lowercase
            module_type = parts[1]  # Keep module and type part case
            return f"{obj_id}::{module_type}"
        else:
            # If no :: separator, treat as object ID, convert all to lowercase
            return token_address.lower()

    else:
        return token_address.lower()
