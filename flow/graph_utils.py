"""
Graph Utilities: 图结构计算工具

提供连通分量计算、上下游路径查找、拓扑排序等功能。
用于支持 partial-run 的节点选择逻辑。
"""

import logging
from collections import defaultdict, deque
from typing import Any, Dict, List, Set, Tuple

logger = logging.getLogger(__name__)


class FlowGraph:
    """
    Flow 图结构表示

    支持：
    - 连通分量计算
    - 上游/下游路径查找
    - 拓扑排序
    """

    def __init__(self, nodes: List[Dict], edges: List[Dict]):
        """
        初始化图结构

        Args:
            nodes: 节点列表 [{"id": "node_1", "type": "...", "data": {...}}, ...]
            edges: 边列表 [{"source": "node_1", "target": "node_2", ...}, ...]
        """
        self.nodes = {node["id"]: node for node in nodes}
        self.edges = edges

        # 构建邻接表
        self.adjacency: Dict[str, Set[str]] = defaultdict(set)  # 正向：source -> targets
        self.reverse_adjacency: Dict[str, Set[str]] = defaultdict(set)  # 反向：target -> sources

        for edge in edges:
            source = edge.get("source")
            target = edge.get("target")
            if source and target:
                self.adjacency[source].add(target)
                self.reverse_adjacency[target].add(source)

        # 确保所有节点都在邻接表中
        for node_id in self.nodes:
            if node_id not in self.adjacency:
                self.adjacency[node_id] = set()
            if node_id not in self.reverse_adjacency:
                self.reverse_adjacency[node_id] = set()

    def get_connected_component(self, node_id: str) -> List[str]:
        """
        获取包含指定节点的连通分量

        使用无向图的 BFS 遍历找到所有连通的节点。

        Args:
            node_id: 起始节点 ID

        Returns:
            连通分量中所有节点的 ID 列表
        """
        if node_id not in self.nodes:
            logger.warning(f"Node {node_id} not found in graph")
            return []

        visited = set()
        queue = deque([node_id])

        while queue:
            current = queue.popleft()
            if current in visited:
                continue
            visited.add(current)

            # 添加所有相邻节点（无向）
            for neighbor in self.adjacency.get(current, set()):
                if neighbor not in visited:
                    queue.append(neighbor)

            for neighbor in self.reverse_adjacency.get(current, set()):
                if neighbor not in visited:
                    queue.append(neighbor)

        return list(visited)

    def get_upstream_path(self, node_id: str) -> List[str]:
        """
        获取到达指定节点的上游路径（包含该节点）

        从指定节点反向遍历，找到所有上游节点。

        Args:
            node_id: 目标节点 ID

        Returns:
            上游路径中所有节点的 ID 列表（按拓扑排序）
        """
        if node_id not in self.nodes:
            logger.warning(f"Node {node_id} not found in graph")
            return []

        # 反向 BFS 找到所有上游节点
        visited = set()
        queue = deque([node_id])

        while queue:
            current = queue.popleft()
            if current in visited:
                continue
            visited.add(current)

            # 添加所有上游节点
            for upstream in self.reverse_adjacency.get(current, set()):
                if upstream not in visited:
                    queue.append(upstream)

        # 对结果进行拓扑排序
        return self.topological_sort(list(visited))

    def get_downstream_path(self, node_id: str) -> List[str]:
        """
        获取从指定节点出发的下游路径（包含该节点）

        从指定节点正向遍历，找到所有下游节点。

        Args:
            node_id: 起始节点 ID

        Returns:
            下游路径中所有节点的 ID 列表（按拓扑排序）
        """
        if node_id not in self.nodes:
            logger.warning(f"Node {node_id} not found in graph")
            return []

        # 正向 BFS 找到所有下游节点
        visited = set()
        queue = deque([node_id])

        while queue:
            current = queue.popleft()
            if current in visited:
                continue
            visited.add(current)

            # 添加所有下游节点
            for downstream in self.adjacency.get(current, set()):
                if downstream not in visited:
                    queue.append(downstream)

        # 对结果进行拓扑排序
        return self.topological_sort(list(visited))

    def topological_sort(self, node_ids: List[str]) -> List[str]:
        """
        对指定节点集合进行拓扑排序

        使用 Kahn 算法实现。

        Args:
            node_ids: 要排序的节点 ID 列表

        Returns:
            拓扑排序后的节点 ID 列表
        """
        if not node_ids:
            return []

        node_set = set(node_ids)

        # 计算子图的入度
        in_degree = {node_id: 0 for node_id in node_set}
        for node_id in node_set:
            for upstream in self.reverse_adjacency.get(node_id, set()):
                if upstream in node_set:
                    in_degree[node_id] += 1

        # Kahn 算法
        queue = deque([node_id for node_id, degree in in_degree.items() if degree == 0])
        result = []

        while queue:
            current = queue.popleft()
            result.append(current)

            for downstream in self.adjacency.get(current, set()):
                if downstream in node_set:
                    in_degree[downstream] -= 1
                    if in_degree[downstream] == 0:
                        queue.append(downstream)

        # 检查是否存在环
        if len(result) != len(node_set):
            logger.warning("Graph contains a cycle, returning partial result")

        return result

    def get_all_connected_components(self) -> List[List[str]]:
        """
        获取图中所有连通分量

        Returns:
            所有连通分量的列表
        """
        visited = set()
        components = []

        for node_id in self.nodes:
            if node_id not in visited:
                component = self.get_connected_component(node_id)
                visited.update(component)
                components.append(component)

        return components

    def get_source_nodes(self) -> List[str]:
        """
        获取所有源节点（没有入边的节点）

        Returns:
            源节点 ID 列表
        """
        return [
            node_id for node_id in self.nodes
            if not self.reverse_adjacency.get(node_id)
        ]

    def get_sink_nodes(self) -> List[str]:
        """
        获取所有汇节点（没有出边的节点）

        Returns:
            汇节点 ID 列表
        """
        return [
            node_id for node_id in self.nodes
            if not self.adjacency.get(node_id)
        ]

    def get_node_info(self, node_id: str) -> Dict[str, Any]:
        """
        获取节点信息

        Args:
            node_id: 节点 ID

        Returns:
            节点信息字典
        """
        return self.nodes.get(node_id, {})


def build_graph_from_flow_structure(flow_structure: Dict) -> FlowGraph:
    """
    从 Flow 结构构建图

    Args:
        flow_structure: Flow 结构数据
            {
                "nodes": [...],
                "edges": [...],
                "node_map": {...}  # 可选
            }

    Returns:
        FlowGraph 实例
    """
    nodes = flow_structure.get("nodes", [])
    edges = flow_structure.get("edges", [])

    # 如果 nodes 是空的但有 node_map，从 node_map 构建
    if not nodes and "node_map" in flow_structure:
        node_map = flow_structure["node_map"]
        nodes = [{"id": node_id, **node_data} for node_id, node_data in node_map.items()]

    return FlowGraph(nodes, edges)


def get_nodes_to_execute(
    graph: FlowGraph,
    trigger_node_id: str,
    mode: str
) -> List[str]:
    """
    根据模式获取需要执行的节点列表

    Args:
        graph: FlowGraph 实例
        trigger_node_id: 触发节点 ID
        mode: 执行模式 ("single", "upstream", "downstream", "component")

    Returns:
        需要执行的节点 ID 列表（已拓扑排序）
    """
    if mode == "single":
        return [trigger_node_id]
    elif mode == "upstream":
        return graph.get_upstream_path(trigger_node_id)
    elif mode == "downstream":
        return graph.get_downstream_path(trigger_node_id)
    elif mode == "component":
        nodes = graph.get_connected_component(trigger_node_id)
        return graph.topological_sort(nodes)
    else:
        logger.warning(f"Unknown mode: {mode}, defaulting to single")
        return [trigger_node_id]
