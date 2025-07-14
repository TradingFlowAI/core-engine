import asyncio
import json
import logging
import traceback
from typing import Any, Dict, List, Optional

import httpx
from tradingflow.depot.config import CONFIG
from tradingflow.station.common.edge import Edge

from tradingflow.station.common.node_decorators import register_node_type
from tradingflow.station.common.signal_formats import SignalFormats
from tradingflow.station.common.signal_types import Signal, SignalType
from tradingflow.station.nodes.node_base import NodeBase, NodeStatus

SYSTEM_PROMPT_HANDLE = "system_prompt_handle"
PROMPT_HANDLE = "prompt_handle"


@register_node_type(
    "ai_model_node",
    default_params={
        "model_name": "gpt-3.5-turbo",
        "temperature": 0.7,
        "system_prompt": "You are a helpful assistant.",
        "prompt": "Please analyze the following information:",
        "max_tokens": 1000,
        "output_signal_type": SignalType.AI_RESPONSE.value,  # 输出信号类型
        "output_format_prompt": True,  # 是否在提示词中添加输出格式要求
    },
)
class AIModelNode(NodeBase):
    """
    AI大模型节点 - 接收各类信号作为上下文，调用大模型获取响应

    输入参数:
    - model_name: 大模型名称，如 "gpt-3.5-turbo", "gpt-4" 等
    - temperature: 温度参数，控制随机性，0.0-2.0
    - system_prompt: 系统提示词，设置AI的角色和行为
    - prompt: 主提示词，在上下文前添加的指导语
    - max_tokens: 最大返回token数
    - output_signal_type: 输出信号类型

    输入信号:
    - 任意信号类型：所有接收到的信号将被转换为文本上下文

    输出信号:
    - AI_RESPONSE：包含AI响应的信号
    """

    def __init__(
        self,
        flow_id: str,
        component_id: int,
        cycle: int,
        node_id: str,
        name: str,
        model_name: str,
        temperature: float = 0.7,
        system_prompt: str = "You are a helpful assistant.",
        prompt: str = "Please analyze the following information:",
        max_tokens: int = 1000,
        output_signal_type: str = "AI_RESPONSE",
        output_format_prompt: bool = True,
        input_edges: List[Edge] = None,
        output_edges: List[Edge] = None,
        state_store=None,
        **kwargs,
    ):
        """
        初始化AI大模型节点

        Args:
            node_id: 节点唯一标识符
            name: 节点名称
            model_name: 大模型名称，如 "gpt-3.5-turbo", "gpt-4" 等
            temperature: 温度参数，控制随机性（0.0-2.0）
            system_prompt: 系统提示词，设置AI的角色和行为
            prompt: 主提示词，在上下文前添加的指导语
            api_key: API密钥
            api_endpoint: API端点URL
            max_tokens: 最大返回token数
            output_signal_type: 输出信号类型
            input_edges: 输入边缘
            output_edges: 输出边缘
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
        )

        # 保存参数
        self.model_name = "deepseek-r1-250120"
        self.temperature = max(0.0, min(2.0, temperature))
        self.system_prompt = system_prompt
        self.prompt = prompt
        self.api_key = CONFIG["ARK_API_KEY"]
        self.api_endpoint = CONFIG["AI_MODEL_NODE_ENDPOINT"]
        self.max_tokens = max(1, min(max_tokens, 4000))
        self.output_signal_type = output_signal_type
        self.output_format_prompt = output_format_prompt

        # 结果
        self.ai_response = None

        # 日志设置
        self.logger = logging.getLogger(f"AIModel.{node_id}")

    def _signal_to_text(self, signal: Signal) -> str:
        """
        将信号转换为文本上下文

        Args:
            signal: 输入信号

        Returns:
            str: 转换后的文本
        """
        try:
            # 添加信号类型和来源信息
            text = f"Signal Type: {signal.type.value}\n"
            text += f"Source Node: {signal.source_node_id}\n"

            # 处理payload
            if signal.payload:
                text += "Content:\n"

                # 尝试格式化不同类型的payload
                if isinstance(signal.payload, dict):
                    # 对于字典，尝试格式化为JSON
                    try:
                        if "klines" in signal.payload and isinstance(
                            signal.payload["klines"], list
                        ):
                            # 特殊处理K线数据，太大时只取部分
                            klines_copy = signal.payload.copy()
                            if len(signal.payload["klines"]) > 5:
                                klines_copy["klines"] = signal.payload["klines"][:5]
                                klines_copy["klines_note"] = (
                                    f"(Showing first 5 of {len(signal.payload['klines'])} klines)"
                                )
                            text += json.dumps(klines_copy, indent=2)
                        else:
                            text += json.dumps(signal.payload, indent=2)
                    except:
                        # 如果JSON转换失败，使用字符串表示
                        text += str(signal.payload)
                elif isinstance(signal.payload, list):
                    # 对于列表，视情况格式化
                    if len(signal.payload) > 5:
                        # 如果列表太长，只显示前5个元素
                        text += (
                            str(signal.payload[:5])
                            + f" ... (and {len(signal.payload)-5} more items)"
                        )
                    else:
                        text += str(signal.payload)
                else:
                    # 其他类型直接转为字符串
                    text += str(signal.payload)
            else:
                text += "(No content)"

            return text
        except Exception as e:
            self.logger.error(f"Error converting signal to text: {str(e)}")
            return f"[Error parsing signal: {str(e)}]"

    def _prepare_context(self) -> str:
        """
        准备发送给AI模型的完整上下文

        Returns:
            str: 完整上下文文本
        """
        # 先添加主提示词
        context = self.prompt + "\n\n"

        # 如果启用了输出格式提示，添加格式要求
        if self.output_format_prompt:
            # 获取输出信号类型的格式描述
            format_desc = SignalFormats.get_format_description(self.output_signal_type)
            if format_desc:
                context += (
                    f"\n输出要求: 请确保你的回复遵循以下格式规范:\n{format_desc}\n"
                )

                # 如果是DEX_TRADE这样的特殊信号，添加具体的结构指导
                if self.output_signal_type == SignalType.DEX_TRADE.value:
                    context += (
                        "\n请在你的回复中明确包含一个格式化的交易信号部分，如下所示:\n"
                        "```json\n"
                        "{\n"
                        '  "trading_pair": "ETH/USDT",\n'
                        '  "action": "buy",\n'
                        '  "amount": 0.5,\n'
                        '  "price": 3000.0,\n'
                        '  "reason": "突破阻力位后的追涨信号"\n'
                        "}\n"
                        "```\n"
                    )

        # 如果有输入信号，添加信号内容
        if self._input_signals:
            for i, (handle, signal) in enumerate(self._input_signals.items(), 1):
                if signal is not None:
                    signal_text = self._signal_to_text(signal)
                    context += (
                        f"\n--- Input {i} (Handle: {handle}) ---\n{signal_text}\n"
                    )
        else:
            context += "(No input signals received)"

        return context

    async def _extract_structured_data(self, ai_response: str) -> Dict[str, Any]:
        """
        从AI回复中提取结构化数据

        Args:
            ai_response: AI模型的回复文本

        Returns:
            Dict[str, Any]: 提取的结构化数据
        """
        try:
            # 1. 尝试从JSON代码块中提取
            import re

            json_pattern = r"```(?:json)?\s*({[\s\S]*?})\s*```"
            matches = re.findall(json_pattern, ai_response)

            # 如果找到JSON代码块
            if matches:
                for json_str in matches:
                    try:
                        data = json.loads(json_str)
                        # 验证数据是否符合信号格式要求
                        valid, _ = SignalFormats.validate(self.output_signal_type, data)
                        if valid:
                            return data
                    except json.JSONDecodeError:
                        continue

            # 2. 尝试从回复中提取键值对
            if self.output_signal_type == SignalType.DEX_TRADE:
                # 针对交易信号的提取逻辑
                data = {}

                # 提取交易对
                pair_match = re.search(
                    r"交易对:?\s*([A-Z0-9]+/[A-Z0-9]+)", ai_response, re.IGNORECASE
                )
                if pair_match:
                    data["trading_pair"] = pair_match.group(1)

                # 提取操作类型
                action_match = re.search(
                    r"操作:?\s*(买入|卖出|buy|sell)", ai_response, re.IGNORECASE
                )
                if action_match:
                    action = action_match.group(1)
                    data["action"] = "buy" if action in ["买入", "buy"] else "sell"

                # 提取数量
                amount_match = re.search(r"数量:?\s*(\d+(\.\d+)?)", ai_response)
                if amount_match:
                    data["amount"] = float(amount_match.group(1))

                # 提取价格(可选)
                price_match = re.search(r"价格:?\s*(\d+(\.\d+)?)", ai_response)
                if price_match:
                    data["price"] = float(price_match.group(1))

                # 提取理由(可选)
                reason_match = re.search(r"理由:?\s*(.+?)(?:\n|$)", ai_response)
                if reason_match:
                    data["reason"] = reason_match.group(1).strip()

                # 验证必要字段是否存在
                valid, _ = SignalFormats.validate(self.output_signal_type, data)
                if valid:
                    return data

            # 3. 如果以上方法都失败，返回一个包含原始回复的基本数据
            return {
                "response": ai_response,
                "parsed": False,
                "warning": "无法提取符合格式要求的数据",
            }

        except Exception as e:
            self.logger.error(f"Error extracting structured data: {str(e)}")
            return {"response": ai_response, "error": str(e)}

    async def call_ai_model(self, context: str) -> Optional[str]:
        """
        调用AI大模型API

        Args:
            context: 完整上下文文本

        Returns:
            Optional[str]: AI响应文本，失败则返回None
        """
        if not self.api_key:
            self.logger.error("API key not provided")
            return None

        try:
            # 准备请求数据
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.api_key}",
            }

            # 创建消息列表
            messages = [
                {"role": "system", "content": self.system_prompt},
                {"role": "user", "content": context},
            ]

            data = {
                "model": self.model_name,
                "messages": messages,
                "temperature": self.temperature,
                "max_tokens": self.max_tokens,
            }

            self.logger.info(f"Calling AI model: {self.model_name}")

            # 发送请求
            async with httpx.AsyncClient(timeout=60.0) as client:
                response = await client.post(
                    self.api_endpoint, headers=headers, json=data
                )

                # 检查响应
                response.raise_for_status()
                response_data = response.json()

                # 提取响应文本
                if "choices" in response_data and len(response_data["choices"]) > 0:
                    ai_text = response_data["choices"][0]["message"]["content"]
                    self.logger.info("AI response received successfully")
                    return ai_text
                else:
                    self.logger.error(f"Invalid response format: {response_data}")
                    return None

        except httpx.HTTPStatusError as e:
            self.logger.error(
                f"HTTP error: {e.response.status_code} - {e.response.text}"
            )
            return None
        except httpx.RequestError as e:
            self.logger.error(f"Request error: {str(e)}")
            return None
        except Exception as e:
            self.logger.error(f"Error calling AI model: {str(e)}")
            return None

    async def execute(self) -> bool:
        """
        执行节点逻辑

        Returns:
            bool: 执行是否成功
        """
        try:
            self.logger.info("Starting AI model node execution")
            await self.set_status(NodeStatus.RUNNING)

            # 准备上下文
            context = self._prepare_context()
            input_signals_count = sum(
                1 for signal in self._input_signals.values() if signal is not None
            )
            self.logger.info(f"Prepared context with {input_signals_count} signals")

            # 调用AI模型
            self.logger.info(f"Calling AI model: {self.model_name}")
            ai_response = await self.call_ai_model(context)

            if not ai_response:
                await self.set_status(
                    NodeStatus.FAILED, "Failed to get response from AI model"
                )
                return False

            self.ai_response = ai_response
            self.logger.info(f"Got AI response (length: {len(ai_response)})")

            # 构建基本响应payload
            base_payload = {
                "model_name": self.model_name,
                "response": ai_response,
                "input_signals_count": input_signals_count,
                "input_signal_types": [
                    signal.type.value
                    for signal in self._input_signals.values()
                    if signal is not None
                ],
            }

            # 处理特殊信号类型
            if self.output_signal_type != SignalType.AI_RESPONSE:
                # 从AI响应中提取结构化数据
                structured_data = await self._extract_structured_data(ai_response)

                # 验证数据是否符合格式要求
                valid, error_msg = SignalFormats.validate(
                    self.output_signal_type, structured_data
                )

                if valid:
                    # 合并数据
                    payload = {**structured_data, **base_payload}
                else:
                    # 数据不符合要求，记录警告并回退到基本响应
                    self.logger.warning(
                        f"Extracted data does not meet {self.output_signal_type} format requirements: {error_msg}"
                    )
                    # 添加原始提取数据和警告信息
                    payload = {
                        **base_payload,
                        "structured_data": structured_data,
                        "format_error": error_msg,
                    }
            else:
                # 对于AI_RESPONSE类型，使用基本payload
                payload = base_payload

            # 发送AI响应信号 - 使用默认输出handle
            # output_handle = next(
            #     (
            #         edge["source_handle"]
            #         for edge in self._output_edges
            #         if edge.source_node == self.node_id
            #     ),
            #     "default",
            # )

            # if await self.send_signal(
            #     output_handle, self.output_signal_type, payload=payload
            # ):
            # FIXME: mock
            output_handle = "signal"
            # if await self.send_signal(
            #     output_handle,
            #     "dex_trade",
            #     payload={"amount_in_handle": 0.5},
            # ):
            if await self.send_stop_execution_signal(
                reason=f"AI model node {self.node_id} decided not to trade",
            ):
                self.logger.info(
                    f"Successfully sent {self.output_signal_type} signal via handle {output_handle}"
                )
                await self.set_status(NodeStatus.COMPLETED)
                return True
            else:
                error_message = f"Failed to send {self.output_signal_type} signal"
                self.logger.error(error_message)
                await self.set_status(NodeStatus.FAILED, error_message)
                return False

        except asyncio.CancelledError:
            # 任务被取消
            self.logger.info("AI model node execution cancelled")
            await self.set_status(NodeStatus.TERMINATED, "Task cancelled")
            return True
        except Exception as e:
            error_message = f"Error executing AIModelNode: {str(e)}"
            self.logger.error(error_message)
            self.logger.error(traceback.format_exc())
            await self.set_status(NodeStatus.FAILED, error_message)
            return False

    async def _on_signal_received_system_prompt_handle(self, signal: Signal) -> bool:
        """
        处理系统提示词信号

        Args:
            signal: 输入信号

        Returns:
            bool: 是否成功处理信号
        """
        # 更新系统提示词
        self.system_prompt = signal.payload.get("system_prompt", self.system_prompt)
        self.logger.info("Updated system prompt: %s", self.system_prompt)
        return True

    async def _on_signal_received_prompt_handle(self, signal: Signal) -> bool:
        """
        处理主提示词信号

        Args:
            signal: 输入信号

        Returns:
            bool: 是否成功处理信号
        """
        # 更新主提示词
        self.prompt = signal.payload.get("prompt", self.prompt)
        self.logger.info("Updated prompt: %s", self.prompt)
        return True

    async def _get_vault_portfolio(self) -> Dict[str, Any]:
        """
        获取Vault的投资组合信息

        Returns:
            Dict[str, Any]: 投资组合信息
        """
