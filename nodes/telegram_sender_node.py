import asyncio
# Removed logging import - using persist_log from NodeBase
import traceback
import json
import os
import time
from typing import Any, Dict, List, Optional, Union

import aiohttp
from weather_depot.config import CONFIG
from common.edge import Edge

from common.node_decorators import register_node_type
from common.signal_types import SignalType
from nodes.node_base import NodeBase, NodeStatus

# Define input and output handle names
# 🔥 修复：统一为单一输出 result
ACCOUNT_INPUT_HANDLE = "account_to_send"
MESSAGE_INPUT_HANDLE = "messages"
RESULT_OUTPUT_HANDLE = "result"


@register_node_type(
    "telegram_sender_node",
    default_params={
        "bot_token": "",  # Telegram Bot Token
        "chat_id": "",    # Chat ID to receive messages
        "message_prefix": "",  # Message prefix, optional
        "parse_mode": "HTML",  # Message parsing mode: HTML, Markdown, MarkdownV2
        "disable_web_page_preview": True,  # Whether to disable web page preview
        "disable_notification": False,  # Whether to send message silently
        "timeout": 30,  # Request timeout (seconds)
        "retry_count": 3,  # Number of retries on failure
    },
)
class TelegramSenderNode(NodeBase):
    """
    Telegram Sender Node - Used to send messages to Telegram

    Input parameters:
    - bot_token: Telegram Bot Token
    - chat_id: Chat ID to receive messages
    - message_prefix: Message prefix, optional
    - parse_mode: Message parsing mode: HTML, Markdown, MarkdownV2
    - disable_web_page_preview: Whether to disable web page preview
    - disable_notification: Whether to send message silently
    - timeout: Request timeout (seconds)
    - retry_count: Number of retries on failure

    Input signals:
    - MESSAGE_INPUT_HANDLE: Message content to send

    Output signals:
    - RESULT_OUTPUT_HANDLE: Send result including success status, message_id, timestamp, and error info
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
        Initialize Telegram Sender Node

        Args:
            flow_id: Flow ID
            component_id: Component ID
            cycle: Node execution cycle
            node_id: Node unique identifier
            name: Node name
            bot_token: Telegram Bot Token
            chat_id: Chat ID to receive messages
            message_prefix: Message prefix, optional
            parse_mode: Message parsing mode: HTML, Markdown, MarkdownV2
            disable_web_page_preview: Whether to disable web page preview
            disable_notification: Whether to send message silently
            timeout: Request timeout (seconds)
            retry_count: Number of retries on failure
            input_edges: Input edge list
            output_edges: Output edge list
            state_store: State storage
            **kwargs: Other parameters passed to base class
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

        # Save parameters
        self.bot_token = bot_token or os.environ.get("TELEGRAM_BOT_TOKEN", "") or CONFIG.get("TELEGRAM_BOT_TOKEN", "")
        self.chat_id = chat_id or os.environ.get("TELEGRAM_CHAT_ID", "") or CONFIG.get("TELEGRAM_CHAT_ID", "")
        self.message_prefix = message_prefix
        self.parse_mode = parse_mode if parse_mode in ["HTML", "Markdown", "MarkdownV2"] else "HTML"
        self.disable_web_page_preview = disable_web_page_preview
        self.disable_notification = disable_notification
        self.timeout = max(1, min(300, timeout))  # 限制在1-300秒之间
        self.retry_count = max(0, min(10, retry_count))  # 限制在0-10次之间

        # Logging will be handled by persist_log method

    def _register_input_handles(self) -> None:
        """Register input handles"""
        # 🔥 新增：注册 account_to_send 输入句柄
        self.register_input_handle(
            name=ACCOUNT_INPUT_HANDLE,
            data_type=str,
            description="Telegram Account - Username with chat ID metadata",
            example="@username",
            auto_update_attr="chat_id",  # 自动更新到 self.chat_id
        )

        self.register_input_handle(
            name=MESSAGE_INPUT_HANDLE,
            data_type=str,
            description="Message Input - Message content to send to Telegram",
            example="Hello from TradingFlow!",
        )

    def _register_output_handles(self) -> None:
        """Register output handles"""
        self.register_output_handle(
            name=RESULT_OUTPUT_HANDLE,
            data_type=dict,
            description="Result - Complete send result including success status, message info, and error details",
            example={
                "success": True,
                "message_id": 123,
                "chat_id": "123456789",
                "timestamp": 1234567890.0,
                "error": None
            },
        )

    async def send_telegram_message(self, message: str) -> Dict[str, Any]:
        """
        Send Telegram message

        Args:
            message: Message content to send

        Returns:
            Dict[str, Any]: Dictionary containing send result
        """
        # Check required parameters
        if not self.bot_token:
            error_msg = "Bot token and chat ID are required"
            await self.persist_log(error_msg, "ERROR")
            return {"success": False, "error": error_msg}

        if not self.chat_id:
            error_msg = "Chat ID is required"
            await self.persist_log(error_msg, "ERROR")
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
                            await self.persist_log(f"Successfully sent Telegram message to chat {self.chat_id}", "INFO")
                            return {
                                "success": True,
                                "message_id": response_data.get("result", {}).get("message_id"),
                                "chat_id": self.chat_id,
                                "timestamp": time.time(),
                            }
                        else:
                            error_msg = f"Failed to send Telegram message: {response_data.get('description', 'Unknown error')}"
                            await self.persist_log(error_msg, "ERROR")

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
                await self.persist_log(error_msg, "ERROR")

                if attempt == self.retry_count:
                    return {"success": False, "error": error_msg}

                await asyncio.sleep(2 ** attempt)

            except asyncio.TimeoutError:
                error_msg = f"Request timed out after {self.timeout} seconds"
                await self.persist_log(error_msg, "ERROR")

                if attempt == self.retry_count:
                    return {"success": False, "error": error_msg}

                await asyncio.sleep(2 ** attempt)

            except Exception as e:
                error_msg = f"Unexpected error: {str(e)}"
                await self.persist_log(error_msg, "ERROR")
                await self.persist_log(traceback.format_exc(), "DEBUG")

                if attempt == self.retry_count:
                    return {"success": False, "error": error_msg}

                await asyncio.sleep(2 ** attempt)

        # 如果所有重试都失败
        return {"success": False, "error": "All retry attempts failed"}

    async def format_message(self, data: Any) -> str:
        """
        Format message content

        Args:
            data: Data to format

        Returns:
            str: Formatted message string
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
            await self.persist_log(f"Error formatting message: {str(e)}", "ERROR")
            return f"Error formatting message: {str(e)}\nRaw data type: {type(data).__name__}"

    async def execute(self) -> bool:
        """Execute node logic, send Telegram message"""
        start_time = time.time()
        try:
            await self.persist_log(f"Executing TelegramSenderNode", "INFO")

            # 🔥 修复：优先使用连线传入的 account_to_send
            account_data = self.get_input_handle_data(ACCOUNT_INPUT_HANDLE)
            if account_data:
                await self.persist_log(f"Using connected account input: {account_data}", "INFO")

                # 解析 account_data
                if isinstance(account_data, dict):
                    # 从 metadata 中提取 chat_id
                    metadata = account_data.get('_metadata', {})
                    extracted_chat_id = metadata.get('chat_id') or account_data.get('chat_id')
                    if extracted_chat_id:
                        self.chat_id = extracted_chat_id
                        await self.persist_log(f"Extracted chat_id from metadata: {self.chat_id}", "INFO")
                elif isinstance(account_data, str):
                    # 如果是字符串，尝试作为 chat_id 使用
                    if account_data.startswith('@'):
                        await self.persist_log(f"Username format detected: {account_data}, using configured chat_id", "INFO")
                    else:
                        self.chat_id = account_data
                        await self.persist_log(f"Using chat_id from input: {self.chat_id}", "INFO")
            else:
                await self.persist_log("No connected account input, using configured chat_id", "INFO")

            # Validate required parameters
            if not self.bot_token:
                error_msg = "Bot token is required"
                await self.persist_log(error_msg, "ERROR")
                await self.set_status(NodeStatus.FAILED, error_msg)
                await self.send_signal(RESULT_OUTPUT_HANDLE, SignalType.DATASET, payload={
                    "success": False,
                    "error": error_msg,
                    "timestamp": time.time()
                })
                return False

            if not self.chat_id:
                error_msg = "Chat ID is required (either from input or configuration)"
                await self.persist_log(error_msg, "ERROR")
                await self.set_status(NodeStatus.FAILED, error_msg)
                await self.send_signal(RESULT_OUTPUT_HANDLE, SignalType.DATASET, payload={
                    "success": False,
                    "error": error_msg,
                    "timestamp": time.time()
                })
                return False

            await self.set_status(NodeStatus.RUNNING)

            # 🔥 修复：获取消息输入（支持连线和信号两种方式）
            input_data = self.get_input_handle_data(MESSAGE_INPUT_HANDLE)
            if input_data is None:
                input_data = await self.get_input_signal(MESSAGE_INPUT_HANDLE)

            if input_data is None:
                error_msg = "No input message received"
                await self.persist_log(error_msg, "WARNING")
                await self.set_status(NodeStatus.FAILED, error_msg)
                await self.send_signal(RESULT_OUTPUT_HANDLE, SignalType.DATASET, payload={
                    "success": False,
                    "error": error_msg,
                    "timestamp": time.time()
                })
                return False

            # Format message
            formatted_message = await self.format_message(input_data)

            # Send message
            result = await self.send_telegram_message(formatted_message)

            # Check send result and send unified result output
            if result.get("success"):
                await self.persist_log(f"Successfully sent Telegram message to chat {self.chat_id}", "INFO")

                # Send unified result signal
                result_data = {
                    "success": True,
                    "message_id": result.get("message_id"),
                    "chat_id": self.chat_id,
                    "timestamp": time.time(),
                    "execution_time": time.time() - start_time,
                    "error": None
                }

                await self.send_signal(RESULT_OUTPUT_HANDLE, SignalType.DATASET, payload=result_data)
                await self.set_status(NodeStatus.COMPLETED)
                return True
            else:
                error_msg = f"Failed to send Telegram message: {result.get('error', 'Unknown error')}"
                await self.persist_log(error_msg, "ERROR")
                await self.set_status(NodeStatus.FAILED, error_msg)

                # Send unified result signal with error
                result_data = {
                    "success": False,
                    "error": error_msg,
                    "timestamp": time.time(),
                    "execution_time": time.time() - start_time,
                    "message_id": None,
                    "chat_id": self.chat_id
                }
                await self.send_signal(RESULT_OUTPUT_HANDLE, SignalType.DATASET, payload=result_data)
                return False

        except asyncio.CancelledError:
            # Task cancelled
            await self.set_status(NodeStatus.TERMINATED, "Task cancelled")
            return True
        except Exception as e:
            error_msg = f"Error executing TelegramSenderNode: {str(e)}"
            await self.persist_log(error_msg, "ERROR")
            await self.persist_log(traceback.format_exc(), "DEBUG")
            await self.set_status(NodeStatus.FAILED, error_msg)
            await self.send_signal(RESULT_OUTPUT_HANDLE, SignalType.DATASET, payload={
                "success": False,
                "error": error_msg,
                "timestamp": time.time()
            })
            return False
