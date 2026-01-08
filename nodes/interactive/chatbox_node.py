"""
ChatBox Node - Constant Interactive Node

提供对话交互界面，允许用户与 LLM 进行实时对话。
这是一个 Constant Node，在 Flow 启动时运行，持续监听用户消息。

特点:
- Constant Node：持续运行，不参与常规 cycle 调度
- 对话历史：持久化保存对话记录
- LLM 集成：调用上游 AI Model Node 或内置 LLM
- 实时响应：通过 WebSocket 推送消息
"""

import asyncio
import json
from datetime import datetime, timezone
from typing import Any, Dict, List

from common.edge import Edge
from common.node_decorators import register_node_type
from common.signal_types import SignalType
from common.constant_node_manager import ConstantNodeManager
from common.interactive_node_manager import get_interactive_manager
from nodes.node_base import NodeBase, NodeStatus


# Input handles
SYSTEM_PROMPT_HANDLE = "system_prompt"  # 系统提示
AI_MODEL_HANDLE = "ai_model"  # 连接的 AI Model
MAX_HISTORY_HANDLE = "max_history"  # 最大历史消息数
INITIAL_MESSAGE_HANDLE = "initial_message"  # 初始问候语

# Output handles - 单一 data 输出
DATA_HANDLE = "data"  # 包含 last_message 和 conversation


@register_node_type(
    "chatbox_node",
    default_params={
        "system_prompt": "You are a helpful assistant.",
        "max_history": 50,
        "initial_message": "Hello! How can I help you today?",
    },
)
class ChatBoxNode(NodeBase):
    """
    ChatBox Node - 对话交互界面

    Input:
    - system_prompt: 系统提示（定义 AI 行为）
    - ai_model: 可选的 AI Model Node 连接
    - max_history: 最大保留的历史消息数
    - initial_message: 初始问候语

    Output:
    - last_message: 最后一条消息（用户或 AI）
    - conversation: 完整对话历史

    Credits Cost: 基于消息数量计费
    """

    def __init__(
        self,
        flow_id: str,
        component_id: int,
        cycle: int,
        node_id: str,
        name: str,
        system_prompt: str = "You are a helpful assistant.",
        max_history: int = 50,
        initial_message: str = "Hello! How can I help you today?",
        input_edges: List[Edge] = None,
        output_edges: List[Edge] = None,
        state_store=None,
        **kwargs,
    ):
        # 从 kwargs 中移除 node_type 避免重复参数
        kwargs.pop("node_type", None)
        super().__init__(
            flow_id=flow_id,
            component_id=component_id,
            cycle=cycle,
            node_id=node_id,
            name=name,
            input_edges=input_edges,
            output_edges=output_edges,
            state_store=state_store,
            node_type="chatbox_node",
            **kwargs,
        )

        self.system_prompt = system_prompt
        self.max_history = max_history
        self.initial_message = initial_message
        
        # AI Model 连接数据
        self.ai_model = None

        # 对话历史
        self._conversation: List[Dict[str, Any]] = []

        # 运行状态
        self._is_running = False
        self._message_queue: asyncio.Queue = asyncio.Queue()

    def _register_input_handles(self) -> None:
        """注册 input handles"""
        self.register_input_handle(
            name=SYSTEM_PROMPT_HANDLE,
            data_type=str,
            description="系统提示（定义 AI 行为）",
            example="You are a helpful assistant.",
            auto_update_attr="system_prompt",
        )
        self.register_input_handle(
            name=AI_MODEL_HANDLE,
            data_type=object,
            description="连接的 AI Model",
            example=None,
            auto_update_attr="ai_model",
        )
        self.register_input_handle(
            name=MAX_HISTORY_HANDLE,
            data_type=int,
            description="最大历史消息数",
            example=50,
            auto_update_attr="max_history",
        )
        self.register_input_handle(
            name=INITIAL_MESSAGE_HANDLE,
            data_type=str,
            description="初始问候语",
            example="Hello! How can I help you today?",
            auto_update_attr="initial_message",
        )

    async def execute(self) -> bool:
        """
        执行 ChatBox Node 逻辑

        作为 Constant Node，这个方法主要负责：
        1. 注册到 ConstantNodeManager
        2. 初始化对话
        3. 启动消息处理循环

        Returns:
            bool: 是否成功启动
        """
        try:
            await self.persist_log(
                f"Starting ChatBoxNode: system_prompt={self.system_prompt[:30]}..."
            )

            await self.set_status(NodeStatus.RUNNING)

            # 尝试从输入获取动态值
            input_system_prompt = self.get_input_handle_data(SYSTEM_PROMPT_HANDLE)
            if input_system_prompt:
                self.system_prompt = str(input_system_prompt)

            input_max_history = self.get_input_handle_data(MAX_HISTORY_HANDLE)
            if input_max_history and isinstance(input_max_history, (int, float)):
                self.max_history = int(input_max_history)

            input_initial_message = self.get_input_handle_data(INITIAL_MESSAGE_HANDLE)
            if input_initial_message:
                self.initial_message = str(input_initial_message)

            # 加载历史对话
            await self._load_conversation_history()

            # 如果没有历史，添加初始消息
            if not self._conversation and self.initial_message:
                await self._add_message("assistant", self.initial_message)

            # 注册到 ConstantNodeManager
            constant_manager = ConstantNodeManager.get_instance()
            await constant_manager.initialize()

            registration = await constant_manager.register_constant_node(
                flow_id=self.flow_id,
                node_id=self.node_id,
                node_type="chatbox_node",
                node_config={
                    "system_prompt": self.system_prompt,
                    "max_history": self.max_history,
                },
            )

            await self.persist_log(
                f"ChatBox registered as Constant Node: {registration.get('status')}"
            )

            # 启动 Constant Node 任务
            await constant_manager.start_constant_node(
                flow_id=self.flow_id,
                node_id=self.node_id,
                executor_func=self._message_loop,
            )

            # 发布初始状态
            await self._publish_chat_update("init", {
                "conversation": self._conversation,
                "system_prompt": self.system_prompt,
            })

            return True

        except Exception as e:
            error_message = f"ChatBoxNode startup failed: {str(e)}"
            await self.persist_log(error_message, "ERROR")
            await self.set_status(NodeStatus.FAILED, error_message)
            return False

    async def _message_loop(self) -> None:
        """
        消息处理主循环

        持续监听用户消息并生成响应
        """
        self._is_running = True
        interactive_manager = get_interactive_manager()
        await interactive_manager.initialize()

        await self.persist_log("ChatBox message loop started", "DEBUG")

        try:
            while self._is_running:
                # 等待用户消息（通过交互 API 提交）
                try:
                    # 检查是否有待处理的交互请求
                    pending = await interactive_manager.get_pending_interaction(
                        flow_id=self.flow_id,
                        cycle=0,  # Constant Node 使用 cycle=0
                        node_id=self.node_id,
                    )

                    if pending and pending.get("status") == "pending":
                        # 有用户消息，处理它
                        response = await interactive_manager.get_response(
                            flow_id=self.flow_id,
                            cycle=0,
                            node_id=self.node_id,
                        )

                        if response:
                            user_message = response.get("response", {}).get("message", "")
                            if user_message:
                                await self._handle_user_message(user_message)

                except Exception as e:
                    await self.persist_log(f"Error checking messages: {e}", "WARNING")

                # 短暂休眠避免过度轮询
                await asyncio.sleep(0.5)

        except asyncio.CancelledError:
            await self.persist_log("ChatBox message loop cancelled", "INFO")
        except Exception as e:
            await self.persist_log(f"ChatBox message loop error: {e}", "ERROR")
        finally:
            self._is_running = False

    async def _handle_user_message(self, message: str) -> None:
        """
        处理用户消息

        Args:
            message: 用户发送的消息
        """
        # 添加用户消息
        await self._add_message("user", message)

        # 发布用户消息事件
        await self._publish_chat_update("user_message", {
            "role": "user",
            "content": message,
        })

        # 生成 AI 响应
        try:
            # 检查是否有连接的 AI Model
            ai_model_signal = self.get_input_handle_data(AI_MODEL_HANDLE)

            if ai_model_signal:
                # 使用连接的 AI Model
                ai_response = await self._call_connected_ai_model(ai_model_signal, message)
            else:
                # 使用内置的简单响应（后续可以集成 LLM API）
                ai_response = await self._generate_fallback_response(message)

            # 添加 AI 响应
            await self._add_message("assistant", ai_response)

            # 发布 AI 响应事件
            await self._publish_chat_update("assistant_message", {
                "role": "assistant",
                "content": ai_response,
            })

            # 发送单一 data 输出（包含 last_message 和 conversation）
            output_data = {
                "last_message": {"role": "assistant", "content": ai_response},
                "conversation": self._conversation,
            }

            await self.send_signal(
                DATA_HANDLE,
                SignalType.ANY,
                payload=output_data,
            )

        except Exception as e:
            error_msg = f"Error generating response: {e}"
            await self.persist_log(error_msg, "ERROR")
            await self._add_message("system", f"Error: {error_msg}")

    async def _call_connected_ai_model(self, model_config: Any, message: str) -> str:
        """
        调用连接的 AI Model Node

        Args:
            model_config: AI Model 配置
            message: 用户消息

        Returns:
            AI 响应
        """
        # TODO: 实现通过信号调用 AI Model Node
        # 这需要一种机制让 ChatBox 触发上游 AI Model 并等待响应
        return f"[AI Model integration pending] Echo: {message}"

    async def _generate_fallback_response(self, message: str) -> str:
        """
        生成回退响应（当没有 AI Model 连接时）

        Args:
            message: 用户消息

        Returns:
            简单响应
        """
        # 简单的回退响应
        # 后续可以直接集成 OpenAI/Claude API
        return f"I received your message: '{message}'. Please connect an AI Model node for intelligent responses."

    async def _add_message(self, role: str, content: str) -> None:
        """
        添加消息到对话历史

        Args:
            role: 消息角色 (user/assistant/system)
            content: 消息内容
        """
        message = {
            "role": role,
            "content": content,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        self._conversation.append(message)

        # 限制历史长度
        if len(self._conversation) > self.max_history:
            self._conversation = self._conversation[-self.max_history:]

        # 持久化对话历史
        await self._save_conversation_history()

    async def _load_conversation_history(self) -> None:
        """从 Redis 加载对话历史"""
        try:
            if self.state_store and hasattr(self.state_store, "redis_client"):
                key = f"chatbox:conversation:{self.flow_id}:{self.node_id}"
                data = await self.state_store.redis_client.get(key)
                if data:
                    self._conversation = json.loads(data)
                    await self.persist_log(
                        f"Loaded {len(self._conversation)} messages from history",
                        "DEBUG"
                    )
        except Exception as e:
            await self.persist_log(f"Error loading conversation history: {e}", "WARNING")

    async def _save_conversation_history(self) -> None:
        """保存对话历史到 Redis"""
        try:
            if self.state_store and hasattr(self.state_store, "redis_client"):
                key = f"chatbox:conversation:{self.flow_id}:{self.node_id}"
                await self.state_store.redis_client.set(
                    key,
                    json.dumps(self._conversation),
                    ex=86400 * 7,  # 7 天过期
                )
        except Exception as e:
            await self.persist_log(f"Error saving conversation history: {e}", "WARNING")

    async def _publish_chat_update(self, event_type: str, data: Dict[str, Any]) -> None:
        """
        发布聊天更新到 Redis Pub/Sub

        Args:
            event_type: 事件类型 (init/user_message/assistant_message)
            data: 事件数据
        """
        try:
            if self.state_store and hasattr(self.state_store, "redis_client"):
                update_event = {
                    "type": "chatbox_update",
                    "event": event_type,
                    "flow_id": self.flow_id,
                    "cycle": 0,  # Constant Node 使用 cycle=0
                    "node_id": self.node_id,
                    "data": data,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }

                channel = f"interaction:flow:{self.flow_id}"
                await self.state_store.redis_client.publish(
                    channel, json.dumps(update_event, default=str)
                )

        except Exception as e:
            await self.persist_log(f"Error publishing chat update: {e}", "WARNING")

    async def stop(self) -> None:
        """停止 ChatBox Node"""
        self._is_running = False

        # 从 ConstantNodeManager 停止
        constant_manager = ConstantNodeManager.get_instance()
        await constant_manager.stop_constant_node(
            flow_id=self.flow_id,
            node_id=self.node_id,
            reason="node_stopped",
        )

        await self.set_status(NodeStatus.TERMINATED)
        await self.persist_log("ChatBox node stopped")

    def calculate_credits_cost(self) -> int:
        """ChatBox Node 的 credits 消耗基于消息数量"""
        # 每条消息消耗 1 credit
        return len(self._conversation)
