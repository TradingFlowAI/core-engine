import asyncio
import json
import logging
import time
import traceback
from typing import Any, Dict, List

import aiohttp

from tradingflow.py_worker.common.edge import Edge
from tradingflow.py_worker.common.node_decorators import register_node_type
from tradingflow.py_worker.common.signal_types import SignalType
from tradingflow.py_worker.nodes.node_base import NodeBase, NodeStatus

# 定义输入输出处理器名称
DATA_OUTPUT_HANDLE = "data_output_handle"
ERROR_HANDLE = "error_handle"


@register_node_type(
    "rss_listener_node",
    default_params={
        "rsshub_url": "https://rsshub.app",
        "route": "",  # 例如: "/telegram/channel/awesomeRSSHub"
        "parameters": {},
        "keywords": "",
        "token": None,
        "timeout": 120,  # 请求超时时间（秒）
        "max_items": 20,  # 最大返回条目数
        "include_content": True,  # 是否包含内容详情
    },
)
class RSSHubNode(NodeBase):
    """
    RSSHub 节点 - 用于获取 RSSHub 提供的 RSS 内容

    输入参数:
    - rsshub_url: RSSHub 实例的 URL，默认为 'https://rsshub.app'
    - route: RSSHub 路由，例如 '/telegram/channel/awesomeRSSHub'
    - parameters: 有些路由的自定义参数
    - keywords: 过滤标题和描述
    - timeout: 请求超时时间（秒）
    - max_items: 最大返回条目数
    - include_content: 是否包含内容详情

    输出信号:
    - DATA_OUTPUT_HANDLE: 获取的 RSS 数据
    - ERROR_HANDLE: 错误信息
    """

    def __init__(
            self,
            flow_id: str,
            component_id: int,
            cycle: int,
            node_id: str,
            name: str,
            rsshub_url: str = "https://rsshub.app",
            route: str = "",
            parameters: Dict[str, Any] = None,
            keywords: str = None,
            token: str = None,
            timeout: int = 120,
            max_items: int = 20,
            include_content: bool = True,
            input_edges: List[Edge] = None,
            output_edges: List[Edge] = None,
            state_store=None,
            **kwargs,
    ):
        """
        初始化 RSSHub 节点

        Args:
            flow_id: 流程ID
            component_id: 组件ID
            cycle: 节点执行周期
            node_id: 节点唯一标识符
            name: 节点名称
            rsshub_url: RSSHub 实例的 URL
            route: RSSHub 路由
            timeout: 请求超时时间（秒）
            max_items: 最大返回条目数
            include_content: 是否包含内容详情
            input_edges: 输入边列表
            output_edges: 输出边列表
            state_store: 状态存储
            **kwargs: 传递给基类的其他参数
        """
        super().__init__(
            flow_id=flow_id,
            component_id=component_id,
            cycle=cycle,
            node_id=node_id,
            name=name,
            input_edges=input_edges,
            output_edges=output_edges,
            state_store=state_store,
            **kwargs,
        )

        # 保存参数
        self.rsshub_url = rsshub_url.rstrip("/")  # 移除尾部斜杠
        self.route = route.strip("/")  # 移除开头和结尾斜杠
        self.timeout = max(1, min(300, timeout))  # 限制在1-300秒之间
        self.max_items = max(1, min(100, max_items))  # 限制在1-100之间
        self.include_content = include_content
        self.parameters = parameters
        self.keywords = keywords
        self.token = token

        # 日志设置
        self.logger = logging.getLogger(f"RSSHubNode.{node_id}")

    async def fetch_rss(self) -> Dict[str, Any]:
        """
        从 RSSHub 获取 RSS 数据

        Returns:
            Dict[str, Any]: 包含 RSS 数据的字典
        """
        # 构建完整的 URL
        url = f"{self.rsshub_url}/{self.route}"

        # 添加自定义参数
        if self.parameters:
            url += '/' + '&'.join(f'{k}={v}' for k, v in self.parameters.items())

        # 添加查询参数
        if "?" not in url:
            url += "?"
        else:
            url += "&"

        # 添加限制条目数参数
        url += f"limit={self.max_items}"
        # 限制为 json 对象
        url += "&format=json"

        # 过滤标题和描述，支持正则
        if self.keywords:
            url += f"&filter={self.keywords}"

        # 添加全文输出参数（如果需要）
        if self.include_content:
            url += "&format=json"  # 使用 JSON 格式以获取完整内容

        self.logger.info(f"Fetching RSS from: {url}")

        try:
            # 创建异步 HTTP 会话
            async with aiohttp.ClientSession() as session:
                # 配置aiohttp代理
                # 发送 GET 请求
                async with session.get(url, timeout=self.timeout) as response:
                    # 检查响应状态
                    if response.status != 200:
                        error_msg = f"Failed to fetch RSS: HTTP {response.status}"
                        self.logger.error(error_msg)
                        return {"error": error_msg, "status_code": response.status}

                    # 尝试解析 JSON 响应
                    try:
                        data = await response.json()
                        self.logger.info(f"Successfully fetched RSS data with {len(data.get('items', []))} items")
                        return data
                    except json.JSONDecodeError:
                        # 如果不是 JSON 格式，尝试解析为 XML/RSS
                        content = await response.text()
                        self.logger.info("Response is not JSON, parsing as XML/RSS using feedparser")

                        # 使用 feedparser 解析 RSS 内容
                        # 注意：需要先安装 feedparser：pip install feedparser
                        try:
                            import feedparser
                            import io
                            from datetime import datetime

                            # 使用 feedparser 解析内容
                            feed = feedparser.parse(io.BytesIO(content.encode('utf-8')))

                            # 提取频道信息
                            title = feed.feed.get('title', '')
                            description = feed.feed.get('description', '')
                            link = feed.feed.get('link', '')

                            # 提取条目
                            items = []
                            for entry in feed.entries[:self.max_items]:
                                # 处理日期格式
                                published = ''
                                if hasattr(entry, 'published_parsed') and entry.published_parsed:
                                    try:
                                        published = datetime(*entry.published_parsed[:6]).isoformat()
                                    except Exception as e:
                                        self.logger.warning(f"Failed to parse date: {e}")

                                # 构建条目字典
                                item = {
                                    "title": entry.get('title', ''),
                                    "description": entry.get('description', ''),
                                    "content": entry.get('content', [{}])[0].get('value',
                                                                                 '') if 'content' in entry else '',
                                    "link": entry.get('link', ''),
                                    "published": published,
                                    "id": entry.get('id', ''),
                                    "author": entry.get('author', ''),
                                    "tags": [tag.get('term', '') for tag in entry.get('tags', [])]
                                }
                                items.append(item)

                            self.logger.info(f"Successfully parsed RSS feed with {len(items)} items")
                            return {
                                "title": title,
                                "description": description,
                                "link": link,
                                "items": items
                            }
                        except Exception as e:
                            error_msg = f"Failed to parse RSS content: {str(e)}"
                            self.logger.error(error_msg)
                            return {"error": error_msg, "raw_content": content[:1000]}  # 只返回部分内容以避免过大

        except aiohttp.ClientError as e:
            error_msg = f"HTTP request error: {str(e)}"
            self.logger.error(error_msg)
            return {"error": error_msg}
        except asyncio.TimeoutError:
            error_msg = f"Request timed out after {self.timeout} seconds"
            self.logger.error(error_msg)
            return {"error": error_msg}
        except Exception as e:
            error_msg = f"Unexpected error: {str(e)}"
            self.logger.error(error_msg)
            self.logger.debug(traceback.format_exc())
            return {"error": error_msg}

    async def execute(self) -> bool:
        """执行节点逻辑，获取 RSS 数据"""
        start_time = time.time()
        try:
            self.logger.info(f"Executing RSSHubNode for route: {self.route}")

            # 验证必要参数
            if not self.route:
                error_msg = "Route parameter is required"
                self.logger.error(error_msg)
                await self.set_status(NodeStatus.FAILED, error_msg)
                await self.send_signal(ERROR_HANDLE, SignalType.TEXT, payload=error_msg)
                return False

            await self.set_status(NodeStatus.RUNNING)

            # 获取 RSS 数据
            rss_data = await self.fetch_rss()

            # 检查是否有错误
            if "error" in rss_data:
                error_msg = f"Failed to fetch RSS data: {rss_data['error']}"
                self.logger.error(error_msg)
                await self.set_status(NodeStatus.FAILED, error_msg)
                await self.send_signal(ERROR_HANDLE, SignalType.TEXT, payload=error_msg)
                return False

            # 处理获取的数据
            processed_data = rss_data

            # 发送数据信号
            if await self.send_signal(DATA_OUTPUT_HANDLE, SignalType.DATASET, payload=processed_data):
                self.logger.info(f"Successfully sent RSS data with {processed_data['item_count']} items")
                await self.set_status(NodeStatus.COMPLETED)
                return True
            else:
                error_msg = "Failed to send RSS data signal"
                self.logger.error(error_msg)
                await self.set_status(NodeStatus.FAILED, error_msg)
                await self.send_signal(ERROR_HANDLE, SignalType.TEXT, payload=error_msg)
                return False

        except asyncio.CancelledError:
            # 任务被取消
            await self.set_status(NodeStatus.TERMINATED, "Task cancelled")
            return True
        except Exception as e:
            error_msg = f"Error executing RSSHubNode: {str(e)}"
            self.logger.error(error_msg)
            self.logger.debug(traceback.format_exc())
            await self.set_status(NodeStatus.FAILED, error_msg)
            await self.send_signal(ERROR_HANDLE, SignalType.TEXT, payload=error_msg)
            return False
