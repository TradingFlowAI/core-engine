class Edge:
    source_node: str
    source_node_handle: str
    target_node: str
    target_node_handle: str

    def __init__(
        self,
        source_node: str,
        source_node_handle: str,
        target_node: str,
        target_node_handle: str,
    ):
        self.source_node = source_node
        self.source_node_handle = source_node_handle
        self.target_node = target_node
        self.target_node_handle = target_node_handle

    @staticmethod
    def from_dict(data: dict) -> "Edge":
        """从字典创建边对象"""
        return Edge(
            source_node=data["source"],
            source_node_handle=data["source_handle"],
            target_node=data["target"],
            target_node_handle=data["target_handle"],
        )

    def to_dict(self) -> dict:
        """将边对象转换为字典"""
        return {
            "source": self.source_node,
            "source_handle": self.source_node_handle,
            "target": self.target_node,
            "target_handle": self.target_node_handle,
        }

    def __str__(self) -> str:
        return f"Edge(source_node={self.source_node}, source_node_handle={self.source_node_handle}, target_node={self.target_node}, target_node_handle={self.target_node_handle})"

    def __repr__(self) -> str:
        return self.__str__()
