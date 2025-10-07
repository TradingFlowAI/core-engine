import asyncio
import json
# Removed logging import - using persist_log from NodeBase
import time
import traceback
from typing import Any, Dict, List, Optional

import httpx
from weather_depot.config import CONFIG
from common.edge import Edge

from common.node_decorators import register_node_type
from common.signal_formats import SignalFormats
from common.signal_types import Signal, SignalType
from nodes.node_base import NodeBase, NodeStatus

# Define input and output handle names
# Input handles
MODEL_INPUT_HANDLE = "model"  # Model input
PROMPT_INPUT_HANDLE = "prompt"  # Prompt input
PARAMETERS_INPUT_HANDLE = "parameters"  # Parameters input

# Output handles
AI_RESPONSE_OUTPUT_HANDLE = "ai_response"  # AI response output


@register_node_type(
    "ai_model_node",
    default_params={
        "model_name": "gpt-3.5-turbo",
        "temperature": 0.7,
        "system_prompt": "You are a helpful assistant.",
        "prompt": "Please analyze the following information:",
        "max_tokens": 1000,
        "auto_format_output": True,  # Whether to automatically generate JSON format requirements based on output connections
    },)
class AIModelNode(NodeBase):
    """
    AI Large Model Node - Receives various signals as context, calls large model to get response
    Automatically generates JSON format requirements based on output connection status

    Input parameters:
    - model: Large model name, such as "gpt-3.5-turbo", "gpt-4", etc.
    - prompt: Main prompt, guiding AI on how to process input data
    - parameters: Model parameters, such as temperature, max_tokens, etc.
    - auto_format_output: Whether to automatically generate JSON format requirements based on output connections

    Output signals:
    - ai_response: AI response data, automatically adjusted format based on output connections
    """

    def __init__(
        self,
        flow_id: str,
        component_id: int,
        cycle: int,
        node_id: str,
        name: str,
        model_name: str = "gpt-3.5-turbo",
        temperature: float = 0.7,
        system_prompt: str = "You are a helpful assistant.",
        prompt: str = "Please analyze the following information:",
        max_tokens: int = 1000,
        parameters: Dict[str, Any] = None,
        auto_format_output: bool = True,
        input_edges: List[Edge] = None,
        output_edges: List[Edge] = None,
        state_store=None,
        **kwargs,
    ):
        """
        Initialize AI Large Model Node

        Args:
            node_id: Node unique identifier
            name: Node name
            model_name: Large model name, such as "gpt-3.5-turbo", "gpt-4", etc.
            temperature: Temperature parameter, controls randomness (0.0-2.0)
            system_prompt: System prompt, sets AI's role and behavior
            prompt: Main prompt, guidance added before context
            api_key: API key
            api_endpoint: API endpoint URL
            max_tokens: Maximum return token count
            output_signal_type: Output signal type
            input_edges: Input edges
            output_edges: Output edges
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
        )

        # Save parameters
        self.model_name = model_name or "openai/gpt-3.5-turbo"
        self.system_prompt = system_prompt
        self.prompt = prompt

        # OpenRouter configuration
        self.api_key = CONFIG["OPENROUTER_API_KEY"]
        self.api_endpoint = "https://openrouter.ai/api/v1/chat/completions"
        self.site_url = "https://tradingflows.ai"
        self.site_name = "TradingFlow"
        self.auto_format_output = auto_format_output

        # Process model parameters
        self.parameters = parameters or {}
        self.temperature = self.parameters.get("temperature", max(0.0, min(2.0, temperature)))
        self.max_tokens = self.parameters.get("max_tokens", max(1, min(max_tokens, 4000)))

        # Results
        self.ai_response = None

        # Logging will be handled by persist_log method

    def _register_input_handles(self) -> None:
        """Register input handles"""
        self.register_input_handle(
            name=MODEL_INPUT_HANDLE,
            data_type=str,
            description="Model - 大模型名称，如 'gpt-3.5-turbo', 'gpt-4' 等",
            example="gpt-3.5-turbo",
            auto_update_attr="model_name",
        )
        self.register_input_handle(
            name=PROMPT_INPUT_HANDLE,
            data_type=str,
            description="Prompt - 主提示词，指导 AI 如何处理输入数据",
            example="Please analyze the trading data and provide recommendations:",
            auto_update_attr="prompt",
        )
        self.register_input_handle(
            name=PARAMETERS_INPUT_HANDLE,
            data_type=dict,
            description="Parameters - 模型参数，如 temperature, max_tokens 等",
            example={"temperature": 0.7, "max_tokens": 1000},
            auto_update_attr="parameters",
        )

    async def _analyze_output_format_requirements(self) -> Dict[str, Any]:
        """
        分析输出句柄的连接情况，确定需要输出的JSON格式

        Returns:
            Dict[str, Any]: 包含输出格式要求的字典
        """
        required_fields = set()
        field_descriptions = {}
        field_examples = {}

        await self.persist_log(f"Analyzing {len(self._output_edges)} output edges for format requirements", "DEBUG")

        # Iterate over all output edges to analyze target node input formats
        for i, edge in enumerate(self._output_edges):
            target_handle = edge.target_node_handle
            target_node = edge.target_node
            source_handle = edge.source_node_handle

            await self.persist_log(
                f"Edge {i}: source={edge.get('source_node')} -> target={edge.get('target_node')}, "
                f"source_handle={edge.get('source_handle')}, target_handle={edge.get('target_handle')}", "DEBUG"
            )

            # Add descriptions and examples based on common handle names
            if "chain" in target_handle.lower():
                field_descriptions[target_handle] = "Blockchain network identifier"
                field_examples[target_handle] = "aptos"
            elif "amount" in target_handle.lower():
                field_descriptions[target_handle] = "Transaction amount, numeric type"
                field_examples[target_handle] = 100.0
            elif "token" in target_handle.lower():
                if "from" in target_handle.lower():
                    field_descriptions[target_handle] = "源代币符号"
                    field_examples[target_handle] = "USDT"
                elif "to" in target_handle.lower():
                    field_descriptions[target_handle] = "目标代币符号"
                    field_examples[target_handle] = "BTC"
                else:
                    field_descriptions[target_handle] = "代币符号"
                    field_examples[target_handle] = "ETH"
            elif "price" in target_handle.lower():
                field_descriptions[target_handle] = "价格，数值类型"
                field_examples[target_handle] = 50000.0
            elif "action" in target_handle.lower():
                field_descriptions[target_handle] = "操作类型"
                field_examples[target_handle] = "buy"
            elif "vault" in target_handle.lower():
                field_descriptions[target_handle] = "Vault地址"
                field_examples[target_handle] = "0x123..."
            elif "slippage" in target_handle.lower() or "tolerance" in target_handle.lower():
                field_descriptions[target_handle] = "滑点容忍度，百分比"
                field_examples[target_handle] = 1.0
            else:
                field_descriptions[target_handle] = f"必需字段: {target_handle}"
                field_examples[target_handle] = "value"

        return {
            "required_fields": list(required_fields),
            "field_descriptions": field_descriptions,
            "field_examples": field_examples
        }

    async def _extract_structured_data_from_response(self, ai_response: str) -> Optional[Dict[str, Any]]:
        """
        从AI响应中提取结构化数据

        Args:
            ai_response: AI模型的响应文本

        Returns:
            Optional[Dict[str, Any]]: 提取的结构化数据，如果提取失败则返回None
        """
        try:
            # 尝试从响应中提取JSON块
            import re

            # 查找JSON代码块
            json_pattern = r'```(?:json)?\s*({[^}]*}[^`]*)```'
            json_matches = re.findall(json_pattern, ai_response, re.DOTALL | re.IGNORECASE)

            if json_matches:
                # 尝试解析第一个JSON块
                json_str = json_matches[0].strip()
                try:
                    structured_data = json.loads(json_str)
                    await self.persist_log(f"Extracted JSON from code block: {json_str[:100]}...", "INFO")
                    return structured_data
                except json.JSONDecodeError as e:
                    await self.persist_log(f"Failed to parse JSON from code block: {e}", "WARNING")

            # 如果没有找到代码块，尝试直接查找JSON对象
            json_object_pattern = r'{[^{}]*(?:{[^{}]*}[^{}]*)*}'
            json_objects = re.findall(json_object_pattern, ai_response)

            for json_obj in json_objects:
                try:
                    structured_data = json.loads(json_obj)
                    await self.persist_log(f"Extracted JSON object: {json_obj[:100]}...", "INFO")
                    return structured_data
                except json.JSONDecodeError:
                    continue

            # 如果都没有找到，尝试基于输出连接的字段名从文本中提取信息
            if self._output_edges:
                format_requirements = self._analyze_output_format_requirements()
                extracted_data = {}

                for field in format_requirements["required_fields"]:
                    # 简单的文本匹配提取
                    field_pattern = field + r'["\']?\s*[:=]\s*["\']?([^,\n\r"\'}]+)["\']?'
                    match = re.search(field_pattern, ai_response, re.IGNORECASE)
                    if match:
                        value = match.group(1).strip()
                        # 尝试转换数据类型
                        if value.lower() in ['true', 'false']:
                            extracted_data[field] = value.lower() == 'true'
                        elif value.replace('.', '', 1).isdigit():
                            extracted_data[field] = float(value) if '.' in value else int(value)
                        else:
                            extracted_data[field] = value

                if extracted_data:
                    await self.persist_log(f"Extracted data from text patterns: {extracted_data}", "INFO")
                    return extracted_data

            return None

        except Exception as e:
            await self.persist_log(f"Error in _extract_structured_data_from_response: {str(e)}", "ERROR")
            return None

    async def _signal_to_text(self, signal: Signal) -> str:
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
            await self.persist_log(f"Error converting signal to text: {str(e)}", "ERROR")
            return f"[Error parsing signal: {str(e)}]"

    async def _prepare_context(self) -> str:
        """
        准备发送给AI模型的完整上下文

        Returns:
            str: 完整上下文文本
        """
        # 先添加主提示词
        context = self.prompt + "\n\n"

        # 如果启用了自动输出格式，根据输出连接生成JSON格式要求
        if self.auto_format_output and self._output_edges:
            format_requirements = self._analyze_output_format_requirements()

            if format_requirements["required_fields"]:
                context += "\n输出格式要求:\n"
                context += "请确保你的回复包含一个符合以下格式的JSON对象：\n\n"

                # 生成JSON示例
                json_example = {}
                for field in format_requirements["required_fields"]:
                    json_example[field] = format_requirements["field_examples"].get(field, "value")

                context += "```json\n"
                context += json.dumps(json_example, indent=2, ensure_ascii=False)
                context += "\n```\n\n"

                # 添加字段说明
                context += "字段说明：\n"
                for field in format_requirements["required_fields"]:
                    desc = format_requirements["field_descriptions"].get(field, f"必需字段: {field}")
                    context += f"- {field}: {desc}\n"

                context += "\n请在你的分析后，提供一个符合上述格式的JSON对象。\n\n"

        # 如果有输入信号，添加信号内容
        if self._input_signals:
            for i, (handle, signal) in enumerate(self._input_signals.items(), 1):
                if signal is not None:
                    signal_text = await self._signal_to_text(signal)
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
            await self.persist_log(f"Error extracting structured data: {str(e)}", "ERROR")
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
            self.persist_log("ERROR", "OpenRouter API key not provided")
            return None
        try:
            self.persist_log("INFO", f"Calling OpenRouter API with model: {self.model_name}")
            self.persist_log("DEBUG", f"Context: {context[:200]}...")

            # Prepare messages in OpenAI chat format
            messages = []
            if self.system_prompt:
                messages.append({"role": "system", "content": self.system_prompt})
            messages.append({"role": "user", "content": context})

            data = {
                "model": self.model_name,
                "messages": messages,
                "max_tokens": self.max_tokens,
                "temperature": self.temperature,
                "stream": False,
            }

            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.api_key}",
                "HTTP-Referer": self.site_url,
                "X-Title": self.site_name,
            }

            async with httpx.AsyncClient(timeout=60.0) as client:
                response = await client.post(self.api_endpoint, headers=headers, json=data)
                response.raise_for_status()
                response_data = response.json()

                self.persist_log("DEBUG", f"Raw response: {response_data}")

                # Extract content from OpenAI-format response
                if "choices" in response_data and len(response_data["choices"]) > 0:
                    choice = response_data["choices"][0]
                    message = choice.get("message", {})
                    content = message.get("content", "").strip()
                    if content:
                        self.persist_log("INFO", f"AI response received: {content[:100]}...")
                        return content
                    else:
                        self.persist_log("WARNING", "No content in AI response")
                        return None
                else:
                    self.persist_log("WARNING", f"Unexpected response format: {response_data}")
                    return None

        except httpx.HTTPStatusError as e:
            self.persist_log("ERROR", f"HTTP error calling OpenRouter API: {e.response.status_code} - {e.response.text}")
            return None
        except Exception as e:
            self.persist_log("ERROR", f"Error calling OpenRouter API: {str(e)}")
            self.persist_log("ERROR", f"Traceback: {traceback.format_exc()}")
            return None

    async def execute(self) -> bool:
        """
        执行节点逻辑

        Returns:
            bool: 执行是否成功
        """
        try:
            await self.persist_log("Starting AI model node execution", "INFO")
            await self.set_status(NodeStatus.RUNNING)

            # 准备上下文
            context = await self._prepare_context()
            input_signals_count = sum(
                1 for signal in self._input_signals.values() if signal is not None
            )
            await self.persist_log(f"Prepared context with {input_signals_count} signals", "INFO")

            # 调用AI模型
            await self.persist_log(f"Calling AI model: {self.model_name}", "INFO")
            ai_response = await self.call_ai_model(context)

            if not ai_response:
                await self.set_status(
                    NodeStatus.FAILED, "Failed to get response from AI model"
                )
                return False

            self.ai_response = ai_response
            await self.persist_log(f"Got AI response (length: {len(ai_response)})", "INFO")

            # 构建 AI 响应 payload
            payload = {
                "model_name": self.model_name,
                "response": ai_response,
                "input_signals_count": input_signals_count,
                "input_signal_types": [
                    signal.type.value
                    for signal in self._input_signals.values()
                    if signal is not None
                ],
                "timestamp": time.time()
            }

            # 如果启用了自动输出格式且有输出连接，尝试提取结构化数据
            if self.auto_format_output and self._output_edges:
                try:
                    structured_data = await self._extract_structured_data_from_response(ai_response)
                    if structured_data:
                        # 将结构化数据添加到 payload 中
                        payload.update(structured_data)
                        await self.persist_log(f"Successfully extracted structured data: {list(structured_data.keys())}", "INFO")
                    else:
                        await self.persist_log("Failed to extract structured data from AI response", "WARNING")
                except Exception as e:
                    await self.persist_log(f"Error extracting structured data: {str(e)}", "WARNING")

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
                await self.persist_log(
                    f"Successfully sent {self.output_signal_type} signal via handle {output_handle}", "INFO"
                )
                await self.set_status(NodeStatus.COMPLETED)
                return True
            else:
                error_message = f"Failed to send {self.output_signal_type} signal"
                await self.persist_log(error_message, "ERROR")
                await self.set_status(NodeStatus.FAILED, error_message)
                return False

        except asyncio.CancelledError:
            # 任务被取消
            await self.persist_log("AI model node execution cancelled", "INFO")
            await self.set_status(NodeStatus.TERMINATED, "Task cancelled")
            return True
        except Exception as e:
            error_message = f"Error executing AIModelNode: {str(e)}"
            await self.persist_log(error_message, "ERROR")
            await self.persist_log(traceback.format_exc(), "ERROR")
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
        await self.persist_log(f"Updated system prompt: {self.system_prompt}", "INFO")
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
        await self.persist_log(f"Updated prompt: {self.prompt}", "INFO")
        return True

    async def _get_vault_portfolio(self) -> Dict[str, Any]:
        """
        获取Vault的投资组合信息

        Returns:
            Dict[str, Any]: 投资组合信息
        """
