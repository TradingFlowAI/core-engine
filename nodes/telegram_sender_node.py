import asyncio
import logging
import traceback
import json
import os
import time
from typing import Any, Dict, List, Optional, Union

import aiohttp
from tradingflow.depot.config import CONFIG
from tradingflow.station.common.edge import Edge

from tradingflow.station.common.node_decorators import register_node_type
from tradingflow.station.common.signal_types import SignalType
from tradingflow.station.nodes.node_base import NodeBase, NodeStatus

# 定义输入输出处理器名称
MESSAGE_INPUT_HANDLE = "message_input_handle"
STATUS_OUTPUT_HANDLE = "status_output_handle"
ERROR_HANDLE = "error_handle"


@register_node_type(
    "telegram_sender_node",
    default_params={
        "bot_token": "",  # Telegram Bot Token
        "chat_id": "",    # 接收消息的聊天ID
        "message_prefix": "",  # 消息前缀，可选
        "parse_mode": "HTML",  # 消息解析模式：HTML, Markdown, MarkdownV2
        "disable_web_page_preview": True,  # 是否禁用网页预览
        "disable_notification": False,  # 是否静默发送消息
        "timeout": 30,  # 请求超时时间（秒）
        "retry_count": 3,  # 失败重试次数
    },
)
class TelegramSenderNode(NodeBase):
    """
    Telegram 发送器节点 - 用于将消息发送到 Telegram

    输入参数:
    - bot_token: Telegram Bot Token
    - chat_id: 接收消息的聊天ID
    - message_prefix: 消息前缀，可选
    - parse_mode: 消息解析模式：HTML, Markdown, MarkdownV2
    - disable_web_page_preview: 是否禁用网页预览
    - disable_notification: 是否静默发送消息
    - timeout: 请求超时时间（秒）
    - retry_count: 失败重试次数

    输入信号:
    - MESSAGE_INPUT_HANDLE: 要发送的消息内容

    输出信号:
    - STATUS_OUTPUT_HANDLE: 发送状态
    - ERROR_HANDLE: 错误信息
    """

    def __init__(
        self,
        flow_id: str,
        component_id: int,
        cycle: int,
        node_id: str,
        name: str,
        bot_token: str = "",
        chat_id: str = "",
        message_prefix: str = "",
        parse_mode: str = "HTML",
        disable_web_page_preview: bool = True,
        disable_notification: bool = False,
        timeout: int = 30,
        retry_count: int = 3,
        input_edges: List[Edge] = None,
        output_edges: List[Edge] = None,
        state_store=None,
        **kwargs,
    ):
        """
        初始化 Telegram 发送器节点

        Args:
            flow_id: 流程ID
            component_id: 组件ID
            cycle: 节点执行周期
            node_id: 节点唯一标识符
            name: 节点名称
            bot_token: Telegram Bot Token
            chat_id: 接收消息的聊天ID
            message_prefix: 消息前缀，可选
            parse_mode: 消息解析模式：HTML, Markdown, MarkdownV2
            disable_web_page_preview: 是否禁用网页预览
            disable_notification: 是否静默发送消息
            timeout: 请求超时时间（秒）
            retry_count: 失败重试次数
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
        self.bot_token = bot_token or os.environ.get("TELEGRAM_BOT_TOKEN", "") or CONFIG.get("TELEGRAM_BOT_TOKEN", "")
        self.chat_id = chat_id or os.environ.get("TELEGRAM_CHAT_ID", "") or CONFIG.get("TELEGRAM_CHAT_ID", "")
        self.message_prefix = message_prefix
        self.parse_mode = parse_mode if parse_mode in ["HTML", "Markdown", "MarkdownV2"] else "HTML"
        self.disable_web_page_preview = disable_web_page_preview
        self.disable_notification = disable_notification
        self.timeout = max(1, min(300, timeout))  # 限制在1-300秒之间
        self.retry_count = max(0, min(10, retry_count))  # 限制在0-10次之间

        # 日志设置
        self.logger = logging.getLogger(f"TelegramSenderNode.{node_id}")

    async def send_telegram_message(self, message: str) -> Dict[str, Any]:
        """
        发送 Telegram 消息

        Args:
            message: 要发送的消息内容

        Returns:
            Dict[str, Any]: 包含发送结果的字典
        """
        # 检查必要参数
        if not self.bot_token:
            error_msg = "Bot token is required"
            self.logger.error(error_msg)
            return {"success": False, "error": error_msg}

        if not self.chat_id:
            error_msg = "Chat ID is required"
            self.logger.error(error_msg)
            return {"success": False, "error": error_msg}

        # 添加消息前缀
        if self.message_prefix:
            message = f"{self.message_prefix}\n\n{message}"

        # 构建 API URL
        url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"

        # 构建请求参数
        params = {
            "chat_id": self.chat_id,
            "text": message,
            "parse_mode": self.parse_mode,
            "disable_web_page_preview": self.disable_web_page_preview,
            "disable_notification": self.disable_notification,
        }

        # 重试机制
        for attempt in range(self.retry_count + 1):
            try:
                # 创建异步 HTTP 会话
                async with aiohttp.ClientSession() as session:
                    # 发送 POST 请求
                    async with session.post(url, json=params, timeout=self.timeout) as response:
                        # 解析响应
                        response_data = await response.json()

                        # 检查响应状态
                        if response.status == 200 and response_data.get("ok"):
                            self.logger.info(f"Successfully sent Telegram message to chat {self.chat_id}")
                            return {
                                "success": True,
                                "message_id": response_data.get("result", {}).get("message_id"),
                                "chat_id": self.chat_id,
                                "timestamp": time.time(),
                            }
                        else:
                            error_msg = f"Failed to send Telegram message: {response_data.get('description', 'Unknown error')}"
                            self.logger.error(error_msg)

                            # 如果是最后一次尝试，返回错误信息
                            if attempt == self.retry_count:
                                return {
                                    "success": False,
                                    "error": error_msg,
                                    "status_code": response.status,
                                    "response": response_data,
                                }

                            # 否则等待后重试
                            await asyncio.sleep(2 ** attempt)  # 指数退避策略

            except aiohttp.ClientError as e:
                error_msg = f"HTTP request error: {str(e)}"
                self.logger.error(error_msg)

                if attempt == self.retry_count:
                    return {"success": False, "error": error_msg}

                await asyncio.sleep(2 ** attempt)

            except asyncio.TimeoutError:
                error_msg = f"Request timed out after {self.timeout} seconds"
                self.logger.error(error_msg)

                if attempt == self.retry_count:
                    return {"success": False, "error": error_msg}

                await asyncio.sleep(2 ** attempt)

            except Exception as e:
                error_msg = f"Unexpected error: {str(e)}"
                self.logger.error(error_msg)
                self.logger.debug(traceback.format_exc())

                if attempt == self.retry_count:
                    return {"success": False, "error": error_msg}

                await asyncio.sleep(2 ** attempt)

        # 如果所有重试都失败
        return {"success": False, "error": "All retry attempts failed"}

    def format_message(self, data: Any) -> str:
        """
        格式化消息内容

        Args:
            data: 要格式化的数据

        Returns:
            str: 格式化后的消息字符串
        """
        try:
            # 如果是字符串，直接返回
            if isinstance(data, str):
                return data

            # 如果是字典或列表，转换为格式化的JSON字符串
            if isinstance(data, (dict, list)):
                # 检查是否是RSS数据
                if isinstance(data, dict) and "items" in data and isinstance(data["items"], list):
                    # RSS数据特殊处理
                    feed_title = data.get("title", "RSS Feed")
                    items = data.get("items", [])

                    if self.parse_mode == "HTML":
                        message = f"<b>{feed_title}</b>\n\n"
                        for i, item in enumerate(items[:5], 1):  # 只显示前5条
                            title = item.get("title", "No Title")
                            link = item.get("link", "")
                            published = item.get("published", "")

                            message += f"{i}. <a href='{link}'>{title}</a>\n"
                            if published:
                                message += f"   <i>{published}</i>\n"
                            message += "\n"
                    else:
                        # Markdown格式
                        message = f"**{feed_title}**\n\n"
                        for i, item in enumerate(items[:5], 1):
                            title = item.get("title", "No Title")
                            link = item.get("link", "")
                            published = item.get("published", "")

                            message += f"{i}. [{title}]({link})\n"
                            if published:
                                message += f"   *{published}*\n"
                            message += "\n"

                    return message

                # 其他字典或列表，转为JSON
                return json.dumps(data, indent=2, ensure_ascii=False)

            # 其他类型，转换为字符串
            return str(data)

        except Exception as e:
            self.logger.error(f"Error formatting message: {str(e)}")
            return f"Error formatting message: {str(e)}\nRaw data type: {type(data).__name__}"

    async def execute(self) -> bool:
        """执行节点逻辑，发送 Telegram 消息"""
        start_time = time.time()
        try:
            self.logger.info(f"Executing TelegramSenderNode")

            # 验证必要参数
            if not self.bot_token:
                error_msg = "Bot token is required"
                self.logger.error(error_msg)
                await self.set_status(NodeStatus.FAILED, error_msg)
                await self.send_signal(ERROR_HANDLE, SignalType.TEXT, payload=error_msg)
                return False

            if not self.chat_id:
                error_msg = "Chat ID is required"
                self.logger.error(error_msg)
                await self.set_status(NodeStatus.FAILED, error_msg)
                await self.send_signal(ERROR_HANDLE, SignalType.TEXT, payload=error_msg)
                return False

            await self.set_status(NodeStatus.RUNNING)

            # 获取输入消息
            input_data = await self.get_input_signal(MESSAGE_INPUT_HANDLE)

            if input_data is None:
                error_msg = "No input message received"
                self.logger.warning(error_msg)
                await self.set_status(NodeStatus.FAILED, error_msg)
                await self.send_signal(ERROR_HANDLE, SignalType.TEXT, payload=error_msg)
                return False

            # 格式化消息
            formatted_message = self.format_message(input_data)

            # 发送消息
            result = await self.send_telegram_message(formatted_message)

            # 检查发送结果
            if result.get("success"):
                self.logger.info(f"Successfully sent Telegram message to chat {self.chat_id}")

                # 发送状态信号
                status_data = {
                    "success": True,
                    "message_id": result.get("message_id"),
                    "chat_id": self.chat_id,
                    "timestamp": time.time(),
                    "execution_time": time.time() - start_time
                }

                await self.send_signal(STATUS_OUTPUT_HANDLE, SignalType.DATASET, payload=status_data)
                await self.set_status(NodeStatus.COMPLETED)
                return True
            else:
                error_msg = f"Failed to send Telegram message: {result.get('error', 'Unknown error')}"
                self.logger.error(error_msg)
                await self.set_status(NodeStatus.FAILED, error_msg)
                await self.send_signal(ERROR_HANDLE, SignalType.TEXT, payload=error_msg)
                return False

        except asyncio.CancelledError:
            # 任务被取消
            await self.set_status(NodeStatus.TERMINATED, "Task cancelled")
            return True
        except Exception as e:
            error_msg = f"Error executing TelegramSenderNode: {str(e)}"
            self.logger.error(error_msg)
            self.logger.debug(traceback.format_exc())
            await self.set_status(NodeStatus.FAILED, error_msg)
            await self.send_signal(ERROR_HANDLE, SignalType.TEXT, payload=error_msg)
            return False
