import json
from collections import defaultdict, deque
from typing import Any, Dict, List, Set


class FlowParser:
    """用于解析Flow JSON描述并识别其中的DAG的解析器"""

    def __init__(self, flow_json_path: str = None, flow_json: dict = None):
        """
        初始化解析器

        Args:
            flow_json_path: JSON文件路径
            flow_json: 直接传入的JSON对象
        """
        self.flow_json = None
        if flow_json_path:
            with open(flow_json_path, "r") as f:
                self.flow_json = json.load(f)
        elif flow_json:
            self.flow_json = flow_json

        self.nodes = []
        self.edges = []
        self.graph = defaultdict(list)
        self.node_map = {}  # 存储节点ID到索引的映射

        if self.flow_json:
            self._parse_flow()

    def _parse_flow(self):
        """解析Flow JSON，提取节点和边"""
        # 假设JSON格式为 {"nodes": [...], "edges": [...]}
        self.nodes = self.flow_json.get("nodes", [])
        self.edges = self.flow_json.get("edges", [])

        # 创建节点映射
        for i, node in enumerate(self.nodes):
            node_id = node.get("id")
            self.node_map[node_id] = i

        # 构建图的邻接表
        for edge in self.edges:
            source = edge.get("source")
            target = edge.get("target")
            if source in self.node_map and target in self.node_map:
                self.graph[source].append(target)

    def find_dags(self) -> List[Dict[str, Any]]:
        """
        识别Flow中的所有DAG

        Returns:
            包含所有DAG信息的列表，每个DAG包含其节点和边
        """
        if not self.flow_json:
            return []

        # 步骤1: 找出所有连通分量
        components = self._find_connected_components()

        # 步骤2: 检查每个连通分量是否为DAG (无环)
        dags = []
        for component in components:
            if self._is_dag(component):
                # 提取这个DAG的节点和边
                dag_nodes = [
                    self.nodes[self.node_map[node_id]] for node_id in component
                ]
                dag_edges = [
                    edge
                    for edge in self.edges
                    if edge.get("source") in component
                    and edge.get("target") in component
                ]

                dags.append({"nodes": dag_nodes, "edges": dag_edges})

        return dags

    def _find_connected_components(self) -> List[Set[str]]:
        """
        找出图中的所有连通分量

        Returns:
            连通分量列表，每个连通分量是节点ID的集合
        """
        # 构建无向图用于寻找连通分量
        undirected_graph = defaultdict(list)
        for source, targets in self.graph.items():
            for target in targets:
                undirected_graph[source].append(target)
                undirected_graph[target].append(source)

        # 使用BFS寻找连通分量
        visited = set()
        components = []

        for node_id in self.node_map:
            if node_id not in visited:
                # 找到一个新的连通分量
                component = set()
                queue = deque([node_id])
                visited.add(node_id)
                component.add(node_id)

                while queue:
                    current = queue.popleft()
                    for neighbor in undirected_graph[current]:
                        if neighbor not in visited:
                            visited.add(neighbor)
                            component.add(neighbor)
                            queue.append(neighbor)

                components.append(component)

        return components

    def _is_dag(self, component: Set[str]) -> bool:
        """
        检查一个连通分量是否为DAG（无环）

        Args:
            component: 节点ID的集合

        Returns:
            如果是DAG返回True，否则返回False
        """
        # 使用DFS检测环
        # 0: 未访问, 1: 正在访问, 2: 已访问完成
        status = {node_id: 0 for node_id in component}

        def dfs(node_id):
            status[node_id] = 1  # 正在访问

            for neighbor in self.graph[node_id]:
                if neighbor in component:  # 只考察当前连通分量内的邻居
                    if status[neighbor] == 0:  # 未访问
                        if not dfs(neighbor):
                            return False
                    elif status[neighbor] == 1:  # 有环
                        return False

            status[node_id] = 2  # 访问完成
            return True

        for node_id in component:
            if status[node_id] == 0:
                if not dfs(node_id):
                    return False

        return True

    def get_dag_count(self) -> int:
        """获取Flow中DAG的数量"""
        return len(self.find_dags())

    def analyze_flow(self) -> Dict[str, Any]:
        """
        分析Flow，返回详细信息

        Returns:
            包含分析结果的字典
        """
        dags = self.find_dags()

        result = {"total_dag_count": len(dags), "dags": []}

        for i, dag in enumerate(dags):
            result["dags"].append(
                {
                    "dag_id": i,
                    "node_count": len(dag["nodes"]),
                    "edge_count": len(dag["edges"]),
                    "nodes": dag["nodes"],
                    "edges": dag["edges"],
                }
            )

        return result


# 示例用法
if __name__ == "__main__":
    # 从文件加载
    # parser = FlowParser(flow_json_path="path/to/flow.json")

    # 或直接使用JSON对象
    example_json = {
        "nodes": [
            {"id": "A", "type": "task"},
            {"id": "B", "type": "task"},
            {"id": "C", "type": "task"},
            {"id": "D", "type": "task"},
            {"id": "E", "type": "task"},
            {"id": "F", "type": "task"},
        ],
        "edges": [
            {"source": "A", "target": "B"},
            {"source": "B", "target": "C"},
            {"source": "D", "target": "E"},
            {"source": "E", "target": "F"},
        ],
    }

    parser = FlowParser(flow_json=example_json)
    result = parser.analyze_flow()
    print(f"Flow 包含 {result['total_dag_count']} 个 DAG")

    for i, dag in enumerate(result["dags"]):
        print(f"\nDAG {i+1}:")
        print(f"  节点数量: {dag['node_count']}")
        print(f"  边数量: {dag['edge_count']}")
        print(f"  节点: {', '.join(node['id'] for node in dag['nodes'])}")
