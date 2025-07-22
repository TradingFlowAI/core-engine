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

# 定义输入输出处理器名称
# 输入句柄
MODEL_INPUT_HANDLE = "model"  # 模型输入
PROMPT_INPUT_HANDLE = "prompt"  # 提示词输入
PARAMETERS_INPUT_HANDLE = "parameters"  # 参数输入

# 输出句柄
AI_RESPONSE_OUTPUT_HANDLE = "ai_response"  # AI响应输出


@register_node_type(
    "ai_model_node",
    default_params={
        "model_name": "gpt-3.5-turbo",
        "temperature": 0.7,
        "system_prompt": "You are a helpful assistant.",
        "prompt": "Please analyze the following information:",
        "max_tokens": 1000,
        "auto_format_output": True,  # 是否自动根据输出连接生成JSON格式要求
    },
)
class AIModelNode(NodeBase):
    """
    AI大模型节点 - 接收各类信号作为上下文，调用大模型获取响应
    自动根据输出连接情况生成JSON格式要求

    输入参数:
    - model: 大模型名称，如 "gpt-3.5-turbo", "gpt-4" 等
    - prompt: 主提示词，指导 AI 如何处理输入数据
    - parameters: 模型参数，如 temperature, max_tokens 等
    - auto_format_output: 是否自动根据输出连接生成JSON格式要求

    输出信号:
    - ai_response: AI响应数据，根据输出连接自动调整格式
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
        self.model_name = model_name or "deepseek-r1-250120"
        self.system_prompt = system_prompt
        self.prompt = prompt
        self.api_key = CONFIG["ARK_API_KEY"]
        self.api_endpoint = CONFIG["AI_MODEL_NODE_ENDPOINT"]
        self.auto_format_output = auto_format_output
        
        # 处理模型参数
        self.parameters = parameters or {}
        self.temperature = self.parameters.get("temperature", max(0.0, min(2.0, temperature)))
        self.max_tokens = self.parameters.get("max_tokens", max(1, min(max_tokens, 4000)))

        # 结果
        self.ai_response = None

        # 日志设置
        self.logger = logging.getLogger(f"AIModel.{node_id}")
    
    def _register_input_handles(self) -> None:
        """注册输入句柄"""
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
    
    def _analyze_output_format_requirements(self) -> Dict[str, Any]:
        """
        分析输出句柄的连接情况，确定需要输出的JSON格式
        
        Returns:
            Dict[str, Any]: 包含输出格式要求的字典
        """
        required_fields = set()
        field_descriptions = {}
        field_examples = {}
        
        self.logger.debug(f"Analyzing {len(self._output_edges)} output edges for format requirements")
        
        # 遍历所有输出边，分析目标节点需要的输入格式
        for edge in self._output_edges:
            target_handle = edge.target_node_handle
            target_node = edge.target_node
            source_handle = edge.source_node_handle
            
            self.logger.debug(
                f"Output edge: {source_handle} -> {target_node}.{target_handle}"
            )
            
            # 将目标句柄名称作为必需的JSON字段
            required_fields.add(target_handle)
            
            # 根据常见句柄名称添加描述和示例
            if "chain" in target_handle.lower():
                field_descriptions[target_handle] = "区块链网络标识符"
                field_examples[target_handle] = "aptos"
            elif "amount" in target_handle.lower():
                field_descriptions[target_handle] = "交易金额，数值类型"
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
                    self.logger.info(f"Extracted JSON from code block: {json_str[:100]}...")
                    return structured_data
                except json.JSONDecodeError as e:
                    self.logger.warning(f"Failed to parse JSON from code block: {e}")
            
            # 如果没有找到代码块，尝试直接查找JSON对象
            json_object_pattern = r'{[^{}]*(?:{[^{}]*}[^{}]*)*}'
            json_objects = re.findall(json_object_pattern, ai_response)
            
            for json_obj in json_objects:
                try:
                    structured_data = json.loads(json_obj)
                    self.logger.info(f"Extracted JSON object: {json_obj[:100]}...")
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
                    self.logger.info(f"Extracted data from text patterns: {extracted_data}")
                    return extracted_data
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error in _extract_structured_data_from_response: {str(e)}")
            return None

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
                        self.logger.info(f"Successfully extracted structured data: {list(structured_data.keys())}")
                    else:
                        self.logger.warning("Failed to extract structured data from AI response")
                except Exception as e:
                    self.logger.warning(f"Error extracting structured data: {str(e)}")

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
