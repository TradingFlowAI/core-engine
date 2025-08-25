import asyncio
# Removed logging import - using persist_log from NodeBase
import re
import traceback
import time
import os
from typing import Any, Dict, List, Optional, Tuple, Union

import requests
from tradingflow.station.common.edge import Edge
from tradingflow.depot.python.config import CONFIG

from tradingflow.station.common.node_decorators import register_node_type
from tradingflow.station.common.signal_types import SignalType
from tradingflow.station.nodes.node_base import NodeBase, NodeStatus

# 定义输入输出处理器名称
# 输入句柄
ACCOUNT_INPUT_HANDLE = "account"  # X账号输入
KEYWORDS_INPUT_HANDLE = "keywords"  # 关键词输入

# 输出句柄
LATEST_TWEETS_OUTPUT_HANDLE = "latest_tweets"  # 最新推文输出


def is_user_id(identifier: str) -> bool:
    """判断是否为 Twitter user ID（纯数字）"""
    return re.fullmatch(r"\d+", identifier) is not None


@register_node_type(
    "x_listener_node",
    default_params={
        "accounts": [],  # X账号列表，支持UserId和UserName
        "limit": 20,  # 获取的推文数量限制
        "keywords": "",  # 关键词过滤，多个关键词用逗号分隔
        "search_mode": "user_tweets",  # 搜索模式: "user_tweets" 或 "advanced_search"
        "query_type": "Latest",  # 搜索类型: "Latest" 或 "Top"
        "api_key": "",  # Twitter API密钥
    },
)
class XListenerNode(NodeBase):
    """
    Twitter监听器节点 - 用于获取指定用户的最近推文或进行高级搜索

    输入参数:
    - accounts: X 的用户名或id列表（用户推文模式下使用）
    - keywords: 关键词过滤，多个关键词用逗号分隔
    - search_mode: 搜索模式 ("user_tweets" 或 "advanced_search")
    - query_type: 搜索类型 ("Latest" 或 "Top")
    - limit: 获取的推文数量限制
    - api_key: Twitter API密钥

    输出信号:
    - latest_tweets: 获取的最新推文数据
    """

    def __init__(
            self,
            flow_id: str,
            component_id: int,
            cycle: int,
            node_id: str,
            name: str,
            accounts: List[str] = None,
            limit: int = 20,
            keywords: str = "",
            search_mode: str = "user_tweets",
            query_type: str = "Latest",
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
        self.accounts = accounts or []
        self.keywords = keywords.strip() if keywords else ""
        self.search_mode = search_mode
        self.query_type = query_type
        self.limit = max(1, min(100, limit))  # 限制在1-100之间
        self.api_key = api_key or os.environ.get("TWITTER_API_KEY") or CONFIG.get("TWITTER_API_KEY", "")

        # API相关
        self.user_tweets_url = "https://api.twitterapi.io/twitter/user/last_tweets"
        self.advanced_search_url = "https://api.twitterapi.io/twitter/tweet/advanced_search"
        self.headers = {"X-API-Key": self.api_key}

        # Logger removed - using persist_log from NodeBase

    def _register_input_handles(self) -> None:
        """注册输入句柄"""
        self.register_input_handle(
            name=ACCOUNT_INPUT_HANDLE,
            data_type=str,
            description="Accounts - X 的用户名或id，支持userId和userName",
            example="elonmusk",
            auto_update_attr="account",
        )
        self.register_input_handle(
            name=KEYWORDS_INPUT_HANDLE,
            data_type=str,
            description="Keywords - 关键词过滤，多个关键词用逗号分隔",
            example="AI, Tesla, SpaceX",
            auto_update_attr="keywords",
        )

    async def _build_search_query(self) -> str:
        """
        Build advanced search query string

        Returns:
            str: Search query string
        """
        query_parts = []

        # 处理关键词
        if self.keywords:
            keywords_list = [kw.strip() for kw in self.keywords.split(',') if kw.strip()]
            if keywords_list:
                # 如果有多个关键词，使用 OR 连接
                if len(keywords_list) > 1:
                    keywords_query = ' OR '.join([f'"{kw}"' for kw in keywords_list])
                    query_parts.append(f'({keywords_query})')
                else:
                    query_parts.append(f'"{keywords_list[0]}"')

        # 如果指定了用户，添加 from: 操作符
        if self.account and self.search_mode == "advanced_search":
            if is_user_id(self.account):
                # 如果是用户ID，需要转换为用户名或使用其他方式
                await self.persist_log(f"Advanced search does not support user ID, skipping user filter: {self.account}", "WARNING")
            else:
                query_parts.append(f"from:{self.account}")

        # 如果没有任何查询条件，返回默认查询
        if not query_parts:
            return "twitter"  # 默认搜索

        return ' '.join(query_parts)

    async def _filter_tweets_by_keywords(self, tweets: List[Dict]) -> List[Dict]:
        """
        Filter tweets by keywords

        Args:
            tweets: List of tweets

        Returns:
            List[Dict]: Filtered list of tweets
        """
        if not self.keywords or not tweets:
            return tweets

        keywords_list = [kw.strip().lower() for kw in self.keywords.split(',') if kw.strip()]
        if not keywords_list:
            return tweets

        filtered_tweets = []
        for tweet in tweets:
            tweet_text = tweet.get('text', '').lower()
            # 检查是否包含任何关键词
            if any(keyword in tweet_text for keyword in keywords_list):
                filtered_tweets.append(tweet)

        await self.persist_log(f"Keyword filtering: {len(tweets)} -> {len(filtered_tweets)} tweets", "INFO")
        return filtered_tweets

    async def fetch_tweets_advanced_search(self, cursor="", max_pages=5) -> Dict[str, Any]:
        """
        使用高级搜索API获取推文

        Args:
            cursor: 分页游标
            max_pages: 最大页数

        Returns:
            Dict[str, Any]: 包含推文和用户信息的字典
        """
        query = await self._build_search_query()
        await self.persist_log(f"Using advanced search query: {query}", "INFO")

        all_tweets = []
        current_page = 0
        current_cursor = cursor

        try:
            while current_page < max_pages:
                # 构建查询参数
                params = {
                    "query": query,
                    "queryType": self.query_type
                }

                if current_cursor:
                    params["cursor"] = current_cursor

                # 使用线程池执行同步API调用
                loop = asyncio.get_event_loop()
                response = await loop.run_in_executor(
                    None,
                    lambda: requests.get(self.advanced_search_url, headers=self.headers, params=params)
                )

                # 检查响应状态
                if response.status_code != 200:
                    error_msg = f"Advanced search API request failed: {response.status_code} - {response.text}"
                    await self.persist_log(error_msg, "ERROR")
                    return {"error": error_msg, "tweets": all_tweets}

                # 解析响应
                response_data = response.json()

                # 获取推文
                tweets = response_data.get("tweets", [])
                all_tweets.extend(tweets)

                # 检查是否有更多页
                has_next_page = response_data.get("has_next_page", False)
                if not has_next_page or len(all_tweets) >= self.limit:
                    break

                # 更新游标和页数
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
                "total_count": len(all_tweets),
                "next_cursor": current_cursor if current_page < max_pages else ""
            }

        except Exception as e:
            error_msg = f"Advanced search execution error: {str(e)}"
            await self.persist_log(error_msg, "ERROR")
            await self.persist_log(traceback.format_exc(), "DEBUG")
            return {"error": error_msg, "tweets": all_tweets}

    async def fetch_tweets_for_account(self, account: str, cursor="", max_pages=5) -> Dict[str, Any]:
        """
        获取单个用户的最近推文

        Args:
            account: Twitter用户名或ID
            cursor: 分页游标
            max_pages: 最大页数

        Returns:
            Dict[str, Any]: 包含推文和用户信息的字典
        """
        if not account:
            error_msg = "Account parameter is required"
            await self.persist_log(error_msg, "ERROR")
            return {"error": error_msg, "tweets": []}

        if is_user_id(account):
            user_id = account
            user_name = None
        else:
            user_name = account
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
                    lambda: requests.get(self.user_tweets_url, headers=self.headers, params=params)
                )

                # 检查响应状态
                if response.status_code != 200:
                    error_msg = f"API request failed: {response.status_code} - {response.text}"
                    await self.persist_log(error_msg, "ERROR")
                    return {"error": error_msg, "tweets": all_tweets, "user_info": user_info}

                # 解析响应
                response_data = response.json()
                if response_data.get("status") != "success":
                    error_msg = f"API returned error: {response_data.get('msg') or response_data.get('message')}"
                    await self.persist_log(error_msg, "ERROR")
                    return {"error": error_msg, "tweets": all_tweets, "user_info": user_info}

                # 获取推文 - 适应新的API响应结构
                data = response_data.get("data", {})
                tweets = data.get("tweets", [])

                # Filter tweets by keywords
                if self.keywords:
                    tweets = await self._filter_tweets_by_keywords(tweets)

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
            error_msg = f"Error fetching tweets: {str(e)}"
            await self.persist_log(error_msg, "ERROR")
            await self.persist_log(traceback.format_exc(), "DEBUG")
            return {"error": error_msg, "tweets": all_tweets, "user_info": user_info}

    async def fetch_tweets_for_all_accounts(self) -> Dict[str, Any]:
        """
        获取所有账户的推文

        Returns:
            Dict[str, Any]: 包含所有推文的字典
        """
        all_tweets = []
        all_errors = []
        
        await self.persist_log(f"Starting to fetch tweets from {len(self.accounts)} accounts", "INFO")
        
        for account in self.accounts:
            await self.persist_log(f"Fetching tweets from account: {account}", "INFO")
            
            # 为每个账户获取推文
            account_result = await self.fetch_tweets_for_account(account)
            
            if "error" in account_result:
                error_msg = f"Failed to fetch from account {account}: {account_result['error']}"
                await self.persist_log(error_msg, "WARNING")
                all_errors.append(error_msg)
            else:
                tweets = account_result.get("tweets", [])
                # 为每条推文添加来源账户信息
                for tweet in tweets:
                    tweet["source_account"] = account
                
                all_tweets.extend(tweets)
                await self.persist_log(f"Fetched {len(tweets)} tweets from account {account}", "INFO")
            
            # 简单的速率限制，避免API调用过快
            await asyncio.sleep(0.5)
        
        # 按时间排序所有推文（如果有时间戳）
        try:
            all_tweets.sort(key=lambda x: x.get('created_at', ''), reverse=True)
        except:
            pass  # 如果排序失败，保持原顺序
        
        # 限制总推文数量
        if len(all_tweets) > self.limit:
            all_tweets = all_tweets[:self.limit]
        
        await self.persist_log(f"Total fetched {len(all_tweets)} tweets", "INFO")
        
        result = {
            "tweets": all_tweets,
            "total_count": len(all_tweets),
            "accounts_processed": len(self.accounts),
            "errors": all_errors
        }
        
        # 如果所有账户都失败了，返回错误
        if len(all_errors) == len(self.accounts) and len(all_tweets) == 0:
            result["error"] = f"All accounts failed to fetch: {'; '.join(all_errors)}"
        
        return result

    async def execute(self) -> bool:
        """执行节点逻辑，获取Twitter用户的最近推文或进行高级搜索"""
        start_time = time.time()
        try:
            mode_desc = "advanced search" if self.search_mode == "advanced_search" else f"tweets from users {', '.join(self.accounts)}"
            await self.persist_log(f"Executing XListenerNode, mode: {self.search_mode}, fetching {mode_desc}", "INFO")

            # 检查API密钥
            if not self.api_key:
                error_msg = "Twitter API key not provided"
                self.logger.error(error_msg)
                await self.set_status(NodeStatus.FAILED, error_msg)
                return False

            # 根据搜索模式检查参数
            if self.search_mode == "user_tweets":
                if not self.accounts:
                    error_msg = "Accounts parameter is required when fetching user tweets"
                    await self.persist_log(error_msg, "ERROR")
                    await self.set_status(NodeStatus.FAILED, error_msg)
                    return False
            elif self.search_mode == "advanced_search":
                if not self.keywords and not self.accounts:
                    error_msg = "Keywords or accounts parameter is required for advanced search"
                    await self.persist_log(error_msg, "ERROR")
                    await self.set_status(NodeStatus.FAILED, error_msg)
                    return False

            await self.set_status(NodeStatus.RUNNING)

            # 根据搜索模式获取推文
            if self.search_mode == "advanced_search":
                result = await self.fetch_tweets_advanced_search()
            else:
                result = await self.fetch_tweets_for_all_accounts()

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
                "accounts": self.accounts,
                "keywords": self.keywords,
                "search_mode": self.search_mode,
                "query_type": self.query_type,
                "timestamp": time.time(),
                "next_cursor": result.get("next_cursor", "")
            }

            if await self.send_signal(LATEST_TWEETS_OUTPUT_HANDLE, SignalType.DATASET, payload=tweets_data):
                await self.persist_log(f"Successfully sent {len(result['tweets'])} tweet data records", "INFO")
            else:
                await self.persist_log("Failed to send tweet data", "WARNING")

            # Set completion status
            execution_time = time.time() - start_time
            await self.persist_log(f"XListenerNode execution completed, time taken: {execution_time:.2f} seconds", "INFO")
            await self.set_status(NodeStatus.COMPLETED)
            return True

        except asyncio.CancelledError:
            # Task was cancelled
            await self.set_status(NodeStatus.TERMINATED, "Task was cancelled")
            return True
        except Exception as e:
            error_msg = f"XListenerNode execution error: {str(e)}"
            await self.persist_log(error_msg, "ERROR")
            await self.persist_log(traceback.format_exc(), "DEBUG")
            await self.set_status(NodeStatus.FAILED, error_msg)
            return False
