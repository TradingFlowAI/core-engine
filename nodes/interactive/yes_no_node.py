"""
Yes/No Node - Interactive Node

允许用户在 Flow 执行过程中做出确认/拒绝决策。
这是一个阻塞式 Interactive Node，会暂停 Flow 执行直到用户响应或超时。

特点:
- 阻塞：暂停 Flow 执行等待用户响应
- 超时：支持可配置的超时时间和超时动作
- 决策输出：输出用户的决策结果（approved/rejected/timeout）
"""

from datetime import datetime, timezone
from typing import Dict, List, Optional

from common.edge import Edge
from common.node_decorators import register_node_type
from common.signal_types import SignalType
from common.interactive_node_manager import get_interactive_manager
from nodes.node_base import NodeBase, NodeStatus


# Input handles
PROMPT_HANDLE = "prompt"  # 提示信息
CONTEXT_DATA_HANDLE = "context_data"  # 上下文数据
TIMEOUT_SECONDS_HANDLE = "timeout_seconds"  # 超时时间
TIMEOUT_ACTION_HANDLE = "timeout_action"  # 超时动作
YES_LABEL_HANDLE = "yes_label"  # Yes 按钮标签
NO_LABEL_HANDLE = "no_label"  # No 按钮标签

# Output handles - 单一 data 输出
DATA_HANDLE = "data"  # 包含 decision 和 metadata


@register_node_type(
    "yes_no_node",
    default_params={
        "prompt": "Do you want to proceed?",
        "timeout_seconds": 300,
        "timeout_action": "reject",
        "yes_label": "Yes",
        "no_label": "No",
    },
)
class YesNoNode(NodeBase):
    """
    Yes/No Decision Node - 用户确认/拒绝决策

    Input:
    - prompt: 提示信息
    - context_data: 可选的上下文数据
    - timeout_seconds: 超时时间（秒），默认 300（5分钟）
    - timeout_action: 超时动作 (reject, approve, skip)
    - yes_label: Yes 按钮标签
    - no_label: No 按钮标签

    Output:
    - decision: 决策结果 ("approved", "rejected", "timeout")
    - decision_metadata: 决策元数据（时间戳、用户ID等）

    Credits Cost: 2 credits
    """

    def __init__(
        self,
        flow_id: str,
        component_id: int,
        cycle: int,
        node_id: str,
        name: str,
        prompt: str = "Do you want to proceed?",
        context_data: Optional[Dict] = None,
        timeout_seconds: int = 300,
        timeout_action: str = "reject",
        yes_label: str = "Yes",
        no_label: str = "No",
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
            node_type="yes_no_node",
            **kwargs,
        )

        self.prompt = prompt
        self.context_data = context_data or {}
        self.timeout_seconds = timeout_seconds
        self.timeout_action = timeout_action
        self.yes_label = yes_label
        self.no_label = no_label

    def _register_input_handles(self) -> None:
        """注册 input handles"""
        self.register_input_handle(
            name=PROMPT_HANDLE,
            data_type=str,
            description="提示信息",
            example="Do you want to proceed?",
            auto_update_attr="prompt",
        )
        self.register_input_handle(
            name=CONTEXT_DATA_HANDLE,
            data_type=dict,
            description="上下文数据",
            example={"key": "value"},
            auto_update_attr="context_data",
        )
        self.register_input_handle(
            name=TIMEOUT_SECONDS_HANDLE,
            data_type=int,
            description="超时时间（秒）",
            example=300,
            auto_update_attr="timeout_seconds",
        )
        self.register_input_handle(
            name=TIMEOUT_ACTION_HANDLE,
            data_type=str,
            description="超时动作 (reject, approve, skip)",
            example="reject",
            auto_update_attr="timeout_action",
        )
        self.register_input_handle(
            name=YES_LABEL_HANDLE,
            data_type=str,
            description="Yes 按钮标签",
            example="Yes",
            auto_update_attr="yes_label",
        )
        self.register_input_handle(
            name=NO_LABEL_HANDLE,
            data_type=str,
            description="No 按钮标签",
            example="No",
            auto_update_attr="no_label",
        )

    async def execute(self) -> bool:
        """
        执行 Yes/No Node 逻辑

        1. 发布交互请求
        2. 设置状态为 awaiting_input
        3. 等待用户响应或超时
        4. 根据响应发送决策信号

        Returns:
            bool: 是否执行成功
        """
        try:
            await self.persist_log(
                f"Executing YesNoNode: prompt={self.prompt[:50]}..., timeout={self.timeout_seconds}s"
            )

            await self.set_status(NodeStatus.RUNNING)

            # 尝试从输入获取动态值
            input_prompt = self.get_input_handle_data(PROMPT_HANDLE)
            if input_prompt:
                self.prompt = str(input_prompt)

            input_context = self.get_input_handle_data(CONTEXT_DATA_HANDLE)
            if input_context:
                if isinstance(input_context, dict):
                    self.context_data.update(input_context)
                else:
                    self.context_data["input"] = input_context

            input_timeout = self.get_input_handle_data(TIMEOUT_SECONDS_HANDLE)
            if input_timeout and isinstance(input_timeout, (int, float)):
                self.timeout_seconds = int(input_timeout)

            input_timeout_action = self.get_input_handle_data(TIMEOUT_ACTION_HANDLE)
            if input_timeout_action and input_timeout_action in ["reject", "approve", "skip"]:
                self.timeout_action = input_timeout_action

            # 获取 Interactive Node Manager
            manager = get_interactive_manager()
            await manager.initialize()

            # 发布交互请求
            await self.persist_log("Publishing interaction request...")
            await self.set_status(NodeStatus.AWAITING_INPUT)

            await manager.request_interaction(
                flow_id=self.flow_id,
                cycle=self.cycle,
                node_id=self.node_id,
                node_type="yes_no_node",
                interaction_type="confirmation",
                prompt=self.prompt,
                context_data=self.context_data,
                timeout_seconds=self.timeout_seconds,
                timeout_action=self.timeout_action,
                options={
                    "yes_label": self.yes_label,
                    "no_label": self.no_label,
                },
            )

            await self.persist_log(
                f"Interaction request published, waiting for response (timeout: {self.timeout_seconds}s)"
            )

            # 使用新的持久化暂停机制
            # 首先检查是否有恢复上下文（服务重启后恢复的情况）
            resume_context = await self.get_resume_context()

            if resume_context and resume_context.get("input_data"):
                # 从恢复上下文获取用户输入
                response = {
                    "response": resume_context.get("input_data"),
                    "user_id": resume_context.get("input_data", {}).get("user_id"),
                    "submitted_at": resume_context.get("resumed_at"),
                }
                await self.persist_log("Resumed with saved user response")
            else:
                # 使用新的 await_input 方法进行持久化暂停
                pause_data = await self.await_input(
                    prompt=self.prompt,
                    timeout_seconds=self.timeout_seconds,
                    input_options={
                        "yes_label": self.yes_label,
                        "no_label": self.no_label,
                    },
                    resume_context={
                        "timeout_action": self.timeout_action,
                        "context_data": self.context_data,
                    },
                )

                # 等待用户响应（兼容旧的轮询方式）
                response = await manager.wait_for_response(
                    flow_id=self.flow_id,
                    cycle=self.cycle,
                    node_id=self.node_id,
                    timeout_seconds=self.timeout_seconds,
                    poll_interval=1.0,
                )

            # 处理响应
            if response:
                user_decision = response.get("response", {})
                decision_value = user_decision.get("decision", "rejected")
                user_id = response.get("user_id")

                await self.persist_log(
                    f"User response received: decision={decision_value}, user_id={user_id}"
                )

                decision_metadata = {
                    "decision": decision_value,
                    "user_id": user_id,
                    "responded_at": response.get("submitted_at"),
                    "prompt": self.prompt,
                    "is_timeout": False,
                }
            else:
                # 超时处理
                await self.persist_log(
                    f"Interaction timeout, applying timeout action: {self.timeout_action}"
                )

                if self.timeout_action == "approve":
                    decision_value = "approved"
                elif self.timeout_action == "skip":
                    decision_value = "skipped"
                else:  # reject
                    decision_value = "rejected"

                decision_metadata = {
                    "decision": decision_value,
                    "user_id": None,
                    "responded_at": datetime.now(timezone.utc).isoformat(),
                    "prompt": self.prompt,
                    "is_timeout": True,
                    "timeout_action": self.timeout_action,
                }

            # 发送单一 data 输出（包含 decision 和 metadata）
            output_data = {
                "decision": decision_value == "approved",  # bool
                "decision_value": decision_value,  # "approved"/"rejected"/"skipped"
                "metadata": decision_metadata,
            }

            await self.send_signal(
                DATA_HANDLE,
                SignalType.ANY,
                payload=output_data,
            )

            await self.persist_log(
                f"Decision data sent: {decision_value}"
            )

            # 清除暂停状态（如果有）
            from flow.pause_manager import get_pause_manager
            pause_manager = get_pause_manager()
            await pause_manager.initialize()
            if await pause_manager.is_node_paused(self.flow_id, self.node_id):
                await pause_manager.resume_node(self.flow_id, self.node_id)

            await self.set_status(NodeStatus.COMPLETED)
            return True

        except Exception as e:
            error_message = f"YesNoNode execution failed: {str(e)}"
            await self.persist_log(error_message, "ERROR")
            await self.set_status(NodeStatus.FAILED, error_message)
            return False

    def calculate_credits_cost(self) -> int:
        """Yes/No Node 的 credits 消耗"""
        return 2
