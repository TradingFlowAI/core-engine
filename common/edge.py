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
        """Create Edge object from dictionary.
        
        Supports both camelCase (from frontend) and snake_case formats:
        - sourceHandle / source_handle
        - targetHandle / target_handle
        """
        # 支持 camelCase (sourceHandle) 和 snake_case (source_handle)
        source_handle = data.get("source_handle") or data.get("sourceHandle") or ""
        target_handle = data.get("target_handle") or data.get("targetHandle") or ""
        
        return Edge(
            source_node=data["source"],
            source_node_handle=source_handle,
            target_node=data["target"],
            target_node_handle=target_handle,
        )

    def to_dict(self) -> dict:
        """Convert Edge object to dictionary."""
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
