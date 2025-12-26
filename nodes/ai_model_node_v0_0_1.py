import asyncio
import json
# Removed logging import - using persist_log from NodeBase
import time
import traceback
from typing import Any, Dict, List, Optional

import httpx
from infra.config import CONFIG
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

        # Default output signal type (can be overridden via kwargs if needed)
        self.output_signal_type = kwargs.get("output_signal_type", SignalType.JSON_DATA)

        # Process model parameters (support dict, list, or JSON string)
        self.raw_parameters = parameters
        self.parameters = self._normalize_parameters(parameters)
        self.temperature = self._coerce_numeric(
            self.parameters.get("temperature"),
            max(0.0, min(2.0, temperature)),
            min_value=0.0,
            max_value=2.0,
            integer=False,
        )
        self.max_tokens = self._coerce_numeric(
            self.parameters.get("max_tokens"),
            max(1, min(max_tokens, 4000)),
            min_value=1,
            max_value=4000,
            integer=True,
        )

        # Results
        self.ai_response = None

        # Credits pricing for AI models (5-30 credits per execution)
        self.ai_model_credits = self._calculate_model_credits()

        # Logging will be handled by persist_log method

    def _normalize_parameters(self, parameters: Any) -> Dict[str, Any]:
        """
        Normalize parameters supplied from frontend/backends.

        Supports:
          - dict: used directly
          - list[paramMatrix items]: convert to {name: value}
          - JSON string: parsed into dict if possible
        """
        if not parameters:
            return {}

        if isinstance(parameters, dict):
            return parameters

        if isinstance(parameters, str):
            try:
                parsed = json.loads(parameters)
                return parsed if isinstance(parsed, dict) else {}
            except json.JSONDecodeError:
                return {}

        if isinstance(parameters, list):
            normalized: Dict[str, Any] = {}
            for entry in parameters:
                if not isinstance(entry, dict):
                    continue
                name = entry.get("name") or entry.get("id")
                if not name:
                    continue
                value = entry.get("value")
                if value is None:
                    # fall back to remaining fields so we don't lose contextual data
                    value = {k: v for k, v in entry.items() if k not in {"name", "id"}}
                normalized[name] = value
            return normalized

        return {}

    def _coerce_numeric(
        self,
        value: Any,
        default: float,
        min_value: Optional[float] = None,
        max_value: Optional[float] = None,
        integer: bool = False,
    ) -> Any:
        """Safely convert numeric parameter values with optional bounds."""
        if value is None:
            numeric = float(default)
        else:
            try:
                if isinstance(value, str):
                    value = value.strip()
                numeric = float(value)
            except (TypeError, ValueError):
                numeric = float(default)

        if min_value is not None:
            numeric = max(min_value, numeric)
        if max_value is not None:
            numeric = min(max_value, numeric)

        return int(numeric) if integer else numeric

    def _calculate_model_credits(self) -> int:
        """
        根据模型类型计算 Credits 消耗

        定价策略：
        - GPT-3.5 / Claude Haiku: 5 credits (最便宜)
        - GPT-4 / Claude Sonnet: 10 credits (标准)
        - GPT-4 Turbo: 15 credits (增强)
        - GPT-4o / o1-preview: 20 credits (高级)
        - Claude Opus / o1: 30 credits (旗舰)
        - 默认: 10 credits (标准)

        Returns:
            int: Credits 消耗数量
        """
        model_lower = self.model_name.lower()

        # GPT-3.5 系列 (最便宜)
        if 'gpt-3.5' in model_lower or '3.5' in model_lower:
            return 5

        # Claude Haiku (最快最便宜)
        if 'haiku' in model_lower:
            return 5

        # Claude Opus 或 o1 (旗舰模型)
        if 'opus' in model_lower or model_lower == 'o1' or 'o1-mini' not in model_lower and 'o1' in model_lower:
            return 30

        # GPT-4o / o1-preview (高级)
        if 'gpt-4o' in model_lower or 'o1-preview' in model_lower:
            return 20

        # GPT-4 Turbo (增强)
        if 'gpt-4-turbo' in model_lower or 'turbo' in model_lower:
            return 15

        # GPT-4 标准版或 Claude Sonnet
        if 'gpt-4' in model_lower or 'sonnet' in model_lower:
            return 10

        # 默认标准定价
        return 10

    async def _charge_credits_sync(self) -> None:
        """
        覆盖基类的 Credits 扣除方法，使用 AI 模型特殊定价

        Raises:
            InsufficientCreditsException: 余额不足时抛出
        """
        if not self.enable_credits:
            await self.persist_log(f"Credits tracking is disabled for node {self.node_id}", "DEBUG")
            return

        if not self.user_id:
            await self.persist_log("No user_id provided, skipping credits charge", "WARNING")
            return

        try:
            from infra.exceptions.tf_exception import InsufficientCreditsException

            credits_cost = self.ai_model_credits
            await self.persist_log(
                f"Charging {credits_cost} credits for AI model: {self.model_name}", "INFO"
            )

            # 获取 weather_control URL
            weather_control_url = CONFIG.get(
                "WEATHER_CONTROL_URL",
                "http://weather-control:3050"
            )

            # 调用同步扣费 API
            async with httpx.AsyncClient(timeout=5.0) as client:
                response = await client.post(
                    f"{weather_control_url}/api/v1/credits/charge",
                    json={
                        "userId": self.user_id,
                        "amount": credits_cost,
                        "nodeId": self.node_id,
                        "nodeType": 'ai_model_node',
                        "flowId": self.flow_id,
                        "metadata": {
                            "model_name": self.model_name,
                            "credits_amount": credits_cost
                        }
                    }
                )

                # 检查是否余额不足
                if response.status_code == 402:  # Payment Required
                    data = response.json()
                    balance = data.get("balance", 0)

                    await self.persist_log(
                        f"Insufficient credits: user={self.user_id}, "
                        f"required={credits_cost}, balance={balance}", "ERROR"
                    )

                    raise InsufficientCreditsException(
                        message=f"Insufficient credits to execute AI model node {self.node_id}",
                        node_id=self.node_id,
                        user_id=self.user_id,
                        required_credits=credits_cost,
                        current_balance=balance,
                    )

                # 其他错误
                response.raise_for_status()

                # 成功
                result = response.json()
                remaining_balance = result.get("data", {}).get("balance", 0)

                await self.persist_log(
                    f"Credits charged successfully: user={self.user_id}, "
                    f"node={self.node_id}, model={self.model_name}, cost={credits_cost}, "
                    f"remaining={remaining_balance}", "INFO"
                )

        except InsufficientCreditsException:
            # 重新抛出余额不足异常
            raise
        except httpx.TimeoutException as e:
            await self.persist_log(f"Timeout charging credits: {str(e)}", "ERROR")
            raise Exception(f"Credits service timeout: {str(e)}")
        except Exception as e:
            await self.persist_log(f"Error charging credits: {str(e)}", "ERROR")
            await self.persist_log(traceback.format_exc(), "ERROR")
            raise Exception(f"Failed to charge credits: {str(e)}")

    def _register_input_handles(self) -> None:
        """
        Register input handles

        🔧 Dynamic Parameters Handle Support:
        - model and prompt are static handles
        - Other handles (like tweet_content, market_data) are dynamically registered as parameter handles
        - Each parameter handle accepts data that will be included in the AI prompt
        """
        # Register static handles
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

        # 🔧 Register dynamic parameter handles based on input_edges
        # Each edge targeting a handle other than 'model' or 'prompt' is treated as a parameter
        if hasattr(self, '_input_edges') and self._input_edges:
            registered_params = set()
            for edge in self._input_edges:
                target_handle = edge.target_node_handle
                # Skip static handles
                if target_handle in [MODEL_INPUT_HANDLE, PROMPT_INPUT_HANDLE]:
                    continue
                # Skip already registered
                if target_handle in registered_params:
                    continue

                # Register as dynamic parameter handle
                self.register_input_handle(
                    name=target_handle,
                    data_type=str,  # Accept any string data
                    description=f"Parameter: {target_handle} - Dynamic parameter input",
                    example=f"Value for {target_handle}",
                    auto_update_attr=None,  # Don't auto-update, will be collected in parameters dict
                )
                registered_params.add(target_handle)
                self.logger.info(f"Registered dynamic parameter handle: {target_handle}")

        # Log if no dynamic parameters were registered
        if not (hasattr(self, '_input_edges') and self._input_edges):
            self.logger.debug("No input_edges provided, skipping dynamic parameter handle registration")

    def _register_output_handles(self) -> None:
        """Register output handles"""
        self.register_output_handle(
            name=AI_RESPONSE_OUTPUT_HANDLE,
            data_type=dict,
            description="AI Response - AI模型的响应数据，包含分析结果和结构化数据",
            example={"response": "Analysis result...", "action": "buy", "confidence": 0.85},
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
            if isinstance(edge, dict):
                target_handle = edge.get("target_handle") or edge.get("target_node_handle")
                target_node = edge.get("target") or edge.get("target_node")
                source_handle = edge.get("source_handle") or edge.get("source_node_handle")
                source_node = edge.get("source") or edge.get("source_node")
            else:
                target_handle = getattr(edge, "target_node_handle", None)
                target_node = getattr(edge, "target_node", None)
                source_handle = getattr(edge, "source_node_handle", None)
                source_node = getattr(edge, "source_node", None)

            await self.persist_log(
                f"Edge {i}: source={source_node} -> target={target_node}, "
                f"source_handle={source_handle}, target_handle={target_handle}",
                "DEBUG",
            )

            if not target_handle:
                continue
            required_fields.add(target_handle)

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
                format_requirements = await self._analyze_output_format_requirements()
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
        支持 {param_name} 占位符，从 parameters 字段中提取值并替换

        Returns:
            str: 完整上下文文本
        """
        # 先进行参数替换
        context = self.prompt

        # 从 parameters 字典中提取参数并替换占位符
        if self.parameters and isinstance(self.parameters, dict):
            await self.persist_log(f"Replacing parameters in prompt: {list(self.parameters.keys())}", "DEBUG")
            for param_name, param_value in self.parameters.items():
                # 支持 {param_name} 格式的占位符
                placeholder = f"{{{param_name}}}"
                if placeholder in context:
                    # 如果参数值是字典或列表，转为 JSON 字符串
                    if isinstance(param_value, (dict, list)):
                        param_str = json.dumps(param_value, ensure_ascii=False, indent=2)
                    else:
                        param_str = str(param_value)

                    context = context.replace(placeholder, param_str)
                    await self.persist_log(f"Replaced {placeholder} with value (length: {len(param_str)})", "DEBUG")

        context += "\n\n"

        # 如果启用了自动输出格式，根据输出连接生成JSON格式要求
        if self.auto_format_output and self._output_edges:
            format_requirements = await self._analyze_output_format_requirements()

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

        # 处理输入信号：将信号数据填充到 parameters 中（用于动态数据）
        if self._input_signals:
            await self.persist_log(f"Processing {len(self._input_signals)} input signals", "DEBUG")
            for handle, signal in self._input_signals.items():
                if signal is not None and handle != 'model' and handle != 'prompt':
                    # 如果信号的 handle 对应一个参数占位符，用信号数据填充
                    placeholder = f"{{{handle}}}"
                    if placeholder in context and signal.payload:
                        # 将信号 payload 转为文本
                        signal_text = await self._signal_to_text(signal)
                        context = context.replace(placeholder, signal_text)
                        await self.persist_log(f"Replaced {placeholder} with signal data from {signal.source_node_id}", "INFO")
                    else:
                        # 否则将信号内容作为额外上下文添加
                        signal_text = await self._signal_to_text(signal)
                        context += f"\n--- Input Signal (Handle: {handle}) ---\n{signal_text}\n"
        else:
            context += "(No input signals received)\n"

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
            await self.persist_log("OpenRouter API key not provided", "ERROR")
            return None
        try:
            await self.persist_log(f"Calling OpenRouter API with model: {self.model_name}", "INFO")
            await self.persist_log(f"Context: {context[:200]}...", "DEBUG")

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

                await self.persist_log(f"Raw response: {response_data}", "DEBUG")

                # Extract content from OpenAI-format response
                if "choices" in response_data and len(response_data["choices"]) > 0:
                    choice = response_data["choices"][0]
                    message = choice.get("message", {})
                    content = message.get("content", "").strip()
                    if content:
                        await self.persist_log(f"AI response received: {content[:100]}...", "INFO")
                        return content
                    else:
                        await self.persist_log("No content in AI response", "WARNING")
                        return None
                else:
                    await self.persist_log(f"Unexpected response format: {response_data}", "WARNING")
                    return None

        except httpx.HTTPStatusError as e:
            await self.persist_log(f"HTTP error calling OpenRouter API: {e.response.status_code} - {e.response.text}", "ERROR")
            return None
        except Exception as e:
            await self.persist_log(f"Error calling OpenRouter API: {str(e)}", "ERROR")
            await self.persist_log(f"Traceback: {traceback.format_exc()}", "ERROR")
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

            # 发送AI响应信号
            output_handle = AI_RESPONSE_OUTPUT_HANDLE

            if await self.send_signal(
                source_handle=output_handle,
                signal_type=SignalType.JSON_DATA,
                payload=payload,
            ):
                await self.persist_log(
                    f"Successfully sent AI response signal via handle {output_handle}", "INFO"
                )
                await self.set_status(NodeStatus.COMPLETED)
                return True
            else:
                error_message = "Failed to send AI response signal"
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
