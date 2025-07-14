"""节点结果提取工具"""

from sanic.log import logger


def extract_node_result(node_instance):
    """从节点实例中提取结果数据，根据节点类型不同而不同"""
    try:
        # 对于PriceGenerator
        if hasattr(node_instance, "current_price") and hasattr(
            node_instance, "update_count"
        ):
            return {
                "symbol": getattr(node_instance, "symbol", "Unknown"),
                "final_price": getattr(node_instance, "current_price", 0),
                "updates_count": getattr(node_instance, "update_count", 0),
                "max_updates": getattr(node_instance, "max_updates", None),
            }
        # 可以添加其他类型节点的结果提取逻辑

        # 默认情况
        return {"status": str(node_instance.status)}
    except Exception as e:
        logger.error(f"Error extracting node result: {str(e)}")
        return {"error": "Failed to extract result data"}
