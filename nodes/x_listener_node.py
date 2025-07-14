import asyncio
import logging
import re
import traceback
import time
import os
from typing import Any, Dict, List, Optional, Tuple, Union

import requests
from tradingflow.station.common.edge import Edge
from tradingflow.depot.config import CONFIG

from tradingflow.station.common.node_decorators import register_node_type
from tradingflow.station.common.signal_types import SignalType
from tradingflow.station.nodes.node_base import NodeBase, NodeStatus

# 定义输入输出处理器名称
TWEETS_OUTPUT_HANDLE = "tweets_output_handle"  # 推文输出
USER_INFO_OUTPUT_HANDLE = "user_info_output_handle"  # 用户信息输出


def is_user_id(identifier: str) -> bool:
    """判断是否为 Twitter user ID（纯数字）"""
    return re.fullmatch(r"\d+", identifier) is not None


@register_node_type(
    "x_listener_node",
    default_params={
        "account": "",  # X账号，支持UserId和UserName
        "limit": 20,  # 获取的推文数量限制
        "keywords": "",
        "api_key": "",  # Twitter API密钥
    },
)
class XListenerNode(NodeBase):
    """
    Twitter监听器节点 - 用于获取指定用户的最近推文

    输入参数:
    - account: X 的用户名或id
    - tweet_limit: 获取的推文数量限制
    - include_user_info: 是否包含用户信息
    - api_key: Twitter API密钥

    输出信号:
    - TWEETS_OUTPUT_HANDLE: 获取的推文数据
    - USER_INFO_OUTPUT_HANDLE: 用户信息（如果include_user_info为True）
    """

    def __init__(
            self,
            flow_id: str,
            component_id: int,
            cycle: int,
            node_id: str,
            name: str,
            account: str = "",
            limit: int = 20,
            keywords: str = "",
            api_key: str = "",
            input_edges: List[Edge] = None,
            output_edges: List[Edge] = None,
            state_store=None,
            **kwargs,
    ):
        """
        初始化 X 监听器节点

        Args:
            flow_id: 流程ID
            component_id: 组件ID
            cycle: 节点执行周期
            node_id: 节点唯一标识符
            name: 节点名称
            account: X用户名
            tweet_limit: 获取的推文数量限制
            include_user_info: 是否包含用户信息
            api_key: Twitter API密钥
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
        self.account = account
        self.keywords = keywords
        self.limit = max(1, min(100, limit))  # 限制在1-100之间
        self.api_key = api_key or os.environ.get("TWITTER_API_KEY") or CONFIG.get("TWITTER_API_KEY", "")
        # API相关
        self.base_url = "https://api.twitterapi.io/twitter/user/last_tweets"
        self.headers = {"X-API-Key": self.api_key}

        # 日志设置
        self.logger = logging.getLogger(f"TwitterListenerNode.{node_id}")

    async def fetch_tweets(self, account:str = "", cursor="", max_pages=5) -> Dict[str, Any]:
        """
        获取用户的最近推文

        Args:
            account: Twitter用户名
            cursor: 分页游标
            max_pages: 最大页数

        Returns:
            Dict[str, Any]: 包含推文和用户信息的字典
        """
        if not self.account:
            error_msg = "必须提供account参数"
            self.logger.error(error_msg)
            return {"error": error_msg, "tweets": []}

        if is_user_id(self.account):
            user_id = account
            user_name = None
        else:
            user_name = self.account
            user_id = None

        # 准备结果容器
        all_tweets = []
        user_info = None
        current_page = 0
        current_cursor = cursor

        try:
            while current_page < max_pages:
                # 构建查询参数
                params = {}
                if user_id:
                    params["userId"] = user_id
                else:
                    params["userName"] = user_name

                if current_cursor:
                    params["cursor"] = current_cursor

                # 使用线程池执行同步API调用
                loop = asyncio.get_event_loop()
                response = await loop.run_in_executor(
                    None,
                    lambda: requests.get(self.base_url, headers=self.headers, params=params)
                )

                # 检查响应状态
                if response.status_code != 200:
                    error_msg = f"API请求失败: {response.status_code} - {response.text}"
                    self.logger.error(error_msg)
                    return {"error": error_msg, "tweets": all_tweets, "user_info": user_info}

                # 解析响应
                response_data = response.json()
                if response_data.get("status") != "success":
                    error_msg = f"API返回错误: {response_data.get('msg') or response_data.get('message')}"
                    self.logger.error(error_msg)
                    return {"error": error_msg, "tweets": all_tweets, "user_info": user_info}

                # 获取推文 - 适应新的API响应结构
                data = response_data.get("data", {})
                tweets = data.get("tweets", [])
                all_tweets.extend(tweets)

                # 检查是否有更多页 - 适应新的API响应结构
                has_next_page = response_data.get("has_next_page", False)
                if not has_next_page or len(all_tweets) >= self.limit:
                    break

                # 更新游标和页数 - 适应新的API响应结构
                current_cursor = response_data.get("next_cursor", "")
                current_page += 1

                # 如果没有下一页的游标，退出循环
                if not current_cursor:
                    break

                # 简单的速率限制
                await asyncio.sleep(0.5)

            # 限制返回的推文数量
            if len(all_tweets) > self.limit:
                all_tweets = all_tweets[:self.limit]

            return {
                "tweets": all_tweets,
                "user_info": user_info,
                "total_count": len(all_tweets),
                "next_cursor": current_cursor if current_page < max_pages else ""
            }

        except Exception as e:
            error_msg = f"获取推文时出错: {str(e)}"
            self.logger.error(error_msg)
            self.logger.debug(traceback.format_exc())
            return {"error": error_msg, "tweets": all_tweets, "user_info": user_info}


    async def execute(self) -> bool:
        """执行节点逻辑，获取Twitter用户的最近推文"""
        start_time = time.time()
        try:
            self.logger.info(f"执行XListenerNode，获取用户 {self.account} 的推文")

            # 检查API密钥
            if not self.api_key:
                error_msg = "未提供Twitter API密钥"
                self.logger.error(error_msg)
                await self.set_status(NodeStatus.FAILED, error_msg)
                return False

            # 检查用户标识符
            if not self.account:
                error_msg = "必须提供account参数"
                self.logger.error(error_msg)
                await self.set_status(NodeStatus.FAILED, error_msg)
                return False

            await self.set_status(NodeStatus.RUNNING)

            # 获取推文
            result = await self.fetch_tweets()

            # 检查是否有错误
            if "error" in result:
                error_msg = result["error"]
                self.logger.error(error_msg)
                await self.set_status(NodeStatus.FAILED, error_msg)
                return False

            # 发送推文数据
            tweets_data = {
                "tweets": result["tweets"],
                "count": len(result["tweets"]),
                "user": self.account,
                "timestamp": time.time(),
                "next_cursor": result.get("next_cursor", "")
            }

            if await self.send_signal(TWEETS_OUTPUT_HANDLE, SignalType.DATASET, payload=tweets_data):
                self.logger.info(f"成功发送 {len(result['tweets'])} 条推文数据")
            else:
                self.logger.warning("发送推文数据失败")

            # 设置完成状态
            execution_time = time.time() - start_time
            self.logger.info(f"XListenerNode执行完成，耗时: {execution_time:.2f}秒")
            await self.set_status(NodeStatus.COMPLETED)
            return True

        except asyncio.CancelledError:
            # 任务被取消
            await self.set_status(NodeStatus.TERMINATED, "任务被取消")
            return True
        except Exception as e:
            error_msg = f"XListenerNode执行错误: {str(e)}"
            self.logger.error(error_msg)
            self.logger.debug(traceback.format_exc())
            await self.set_status(NodeStatus.FAILED, error_msg)
            return False
