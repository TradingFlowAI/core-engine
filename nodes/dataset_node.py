import asyncio
import traceback
import json
import os
import time
import pathlib
import re
from typing import Any, Dict, List, Optional, Tuple, Union
from urllib.parse import urlparse

import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError, SpreadsheetNotFound, WorksheetNotFound
from weather_depot.config import CONFIG
from common.edge import Edge

from common.node_decorators import register_node_type
from common.signal_types import SignalType
from nodes.node_base import NodeBase, NodeStatus


@register_node_type(
    "dataset_node",
    default_params={
        "spreadsheet_id": "",
        "worksheet_name": "Sheet1",
        "mode": "read",  # read, write, append
        "range": "A1:Z1000",
        "header_row": True,
        "node_category": "base",
        "display_name": "Dataset Node",
        "version": "0.0.2",
    },
)
class DatasetNode(NodeBase):
    """
    Google Sheets 数据集节点 - 用于读取和写入 Google Sheets 数据

    输入参数:
    - spreadsheet_id: Google Sheets 文档 ID
    - worksheet_name: 工作表名称，默认为 'Sheet1'
    - mode: 操作模式，'read', 'write' 或 'append'
    - range: 数据范围，例如 'A1:Z1000'
    - header_row: 是否包含表头行，默认为 True

    输入信号 (用于写入模式):
    - DATA_INPUT: 要写入的数据

    输出信号 (用于读取模式):
    - DATA_OUTPUT: 读取的数据
    """

    def __init__(
        self,
        flow_id: str,
        component_id: int,
        cycle: int,
        node_id: str,
        name: str,
        spreadsheet_id: str = "",
        worksheet_name: str = "Sheet1",
        mode: str = "read",
        range: str = "A1:Z1000",
        header_row: bool = True,
        input_edges: List[Edge] = None,
        output_edges: List[Edge] = None,
        state_store=None,
        **kwargs,
    ):
        """
        初始化 Google Sheets 数据集节点

        Args:
            flow_id: 流程ID
            component_id: 组件ID
            cycle: 节点执行周期
            node_id: 节点唯一标识符
            name: 节点名称
            spreadsheet_id: Google Sheets 文档 ID
            worksheet_name: 工作表名称
            mode: 操作模式 ('read', 'write', 'append')
            range: 数据范围
            header_row: 是否包含表头行
            input_edges: 输入边列表
            output_edges: 输出边列表
            state_store: 状态存储
            **kwargs: 传递给基类的其他参数
        """
        # 设置基类节点元数据
        kwargs.setdefault('version', '0.0.2')
        kwargs.setdefault('display_name', 'Dataset Node')
        kwargs.setdefault('node_category', 'base')
        kwargs.setdefault('description', 'A flexible node for reading/writing data from various sources')
        kwargs.setdefault('author', 'TradingFlow Team')
        kwargs.setdefault('tags', ['data', 'io', 'google-sheets'])

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

        # 从kwargs中获取doc_link，然后提取spreadsheet_id
        doc_link = kwargs.get('doc_link', spreadsheet_id)  # 优先使用kwargs中的doc_link，fallback到参数
        self._original_spreadsheet_input = doc_link  # 保存原始输入用于日志
        self.spreadsheet_id = self._extract_spreadsheet_id(doc_link)
        self.worksheet_name = worksheet_name
        self.mode = mode.lower()
        self.range = range
        self.header_row = header_row

        # 获取凭证路径，支持相对路径
        credentials_path = CONFIG.get("GOOGLE_CREDENTIALS_PATH", "")
        if credentials_path and not os.path.isabs(credentials_path):
            # 如果是相对路径，则相对于3_weather_cluster目录（项目根目录）
            # 获取项目根目录路径
            current_file = pathlib.Path(__file__)
            # 目录结构: 3_weather_cluster/tradingflow/station/nodes/dataset_node.py
            # 所以需要向上3级目录到达3_weather_cluster
            project_root = current_file.parent.parent.parent.parent
            credentials_path = os.path.join(project_root, credentials_path)

        self.credentials_path = credentials_path

        # Google Sheets 客户端
        self.client = None
        self.spreadsheet = None
        self.worksheet = None

    @staticmethod
    def _extract_spreadsheet_id(spreadsheet_input: str) -> str:
        """
        从Google Sheets URL或直接ID中提取spreadsheet ID

        支持的格式:
        1. 直接ID: "1uQvzsNIkaan67wFijnDiixusw9qNZnedW4Q3dWJASBM"
        2. 完整URL: "https://docs.google.com/spreadsheets/d/1uQvzsNIkaan67wFijnDiixusw9qNZnedW4Q3dWJASBM/edit?usp=sharing"
        3. 简化URL: "docs.google.com/spreadsheets/d/1uQvzsNIkaan67wFijnDiixusw9qNZnedW4Q3dWJASBM"

        Args:
            spreadsheet_input: Google Sheets URL或spreadsheet ID

        Returns:
            str: 提取的spreadsheet ID，如果无法提取则返回原始输入
        """
        if not spreadsheet_input or not isinstance(spreadsheet_input, str):
            return spreadsheet_input or ""

        # 去除首尾空白
        spreadsheet_input = spreadsheet_input.strip()

        # 如果已经是一个看起来像ID的字符串（不包含斜杠且长度合适），直接返回
        if '/' not in spreadsheet_input and len(spreadsheet_input) >= 20:
            return spreadsheet_input

        # 使用正则表达式提取spreadsheet ID
        # Google Sheets ID 通常是44个字符的字母数字字符串
        patterns = [
            r'/spreadsheets/d/([a-zA-Z0-9-_]+)',  # 标准格式
            r'id=([a-zA-Z0-9-_]+)',               # 某些格式可能使用id参数
            r'spreadsheets/d/([a-zA-Z0-9-_]+)',   # 没有前导斜杠的格式
        ]

        for pattern in patterns:
            match = re.search(pattern, spreadsheet_input)
            if match:
                extracted_id = match.group(1)
                # 验证ID长度（Google Sheets ID通常是44个字符）
                if len(extracted_id) >= 20:  # 允许一些变化
                    return extracted_id

        # 如果无法提取，返回原始输入（可能已经是有效的ID）
        return spreadsheet_input

    async def initialize_client(self) -> bool:
        """初始化 Google Sheets API 客户端，带有重试机制"""
        max_retries = 3
        retry_count = 0
        base_delay = 1  # 基础延迟为1秒

        while retry_count <= max_retries:
            try:
                # 检查凭证路径
                if not self.credentials_path:
                    error_msg = "Google Sheets credentials file not found"
                    await self.persist_log(error_msg, "ERROR")
                    await self.set_status(NodeStatus.FAILED, error_msg)
                    return False

                # 设置 Google Sheets API 范围
                scopes = [
                    "https://www.googleapis.com/auth/spreadsheets",
                    "https://www.googleapis.com/auth/drive",
                ]

                # 创建凭证
                credentials = Credentials.from_service_account_file(
                    self.credentials_path, scopes=scopes
                )

                # 创建客户端
                self.client = gspread.authorize(credentials)
                await self.persist_log("Google Sheets client initialized", "INFO")

                # 检查 spreadsheet_id
                if not self.spreadsheet_id:
                    error_msg = "Spreadsheet ID not provided"
                    await self.persist_log(error_msg, "ERROR")
                    await self.set_status(NodeStatus.FAILED, error_msg)
                    return False

                # 打开电子表格
                try:
                    self.spreadsheet = self.client.open_by_key(self.spreadsheet_id)
                    await self.persist_log(f"Opened spreadsheet: {self.spreadsheet.title}", "INFO")
                except SpreadsheetNotFound:
                    error_msg = f"Spreadsheet with ID '{self.spreadsheet_id}' not found"
                    await self.persist_log(error_msg, "ERROR")
                    await self.set_status(NodeStatus.FAILED, error_msg)
                    return False
                except APIError as e:
                    if "permission" in str(e).lower():
                        error_msg = f"Permission denied to access spreadsheet: {str(e)}"
                    else:
                        error_msg = f"API error accessing spreadsheet: {str(e)}"
                    await self.persist_log(error_msg, "ERROR")
                    await self.set_status(NodeStatus.FAILED, error_msg)
                    return False

                # 打开工作表
                try:
                    self.worksheet = self.spreadsheet.worksheet(self.worksheet_name)
                    await self.persist_log(f"Opened worksheet: {self.worksheet_name}", "INFO")
                except WorksheetNotFound:
                    error_msg = f"Worksheet '{self.worksheet_name}' not found"
                    await self.persist_log(error_msg, "ERROR")
                    await self.set_status(NodeStatus.FAILED, error_msg)
                    return False

                return True

            except Exception as e:
                retry_count += 1
                if retry_count <= max_retries:
                    # 计算指数退避延迟
                    delay = base_delay * (2 ** (retry_count - 1))
                    await self.persist_log(
                        f"Failed to initialize Google Sheets client (attempt {retry_count}/{max_retries}): {str(e)}. "
                        f"Retrying in {delay}s...", "WARNING")
                    await asyncio.sleep(delay)
                else:
                    error_msg = f"Failed to initialize Google Sheets client: {str(e)}"
                    await self.persist_log(error_msg, "ERROR")
                    await self.set_status(NodeStatus.FAILED, error_msg)
                    return False

    async def read_data(self) -> Optional[Dict[str, Any]]:
        """读取 Google Sheets 数据并转换为完整的 JSON object"""
        if not self.worksheet:
            await self.persist_log("Worksheet not initialized", "ERROR")
            return None

        try:
            # 使用线程池执行同步 API 调用
            loop = asyncio.get_event_loop()
            values = await loop.run_in_executor(
                None, lambda: self.worksheet.get_values(self.range)
            )

            if not values:
                await self.persist_log(f"No data found in range {self.range}", "WARNING")
                return {}

            # 将 Google Sheets 数据转换为 JSON object
            # 如果有表头，使用表头作为键；否则使用 col_0, col_1 等
            result_data = []

            if self.header_row and len(values) > 0:
                headers = values[0]
                data_rows = values[1:]

                # 将每一行转换为字典
                for row in data_rows:
                    row_dict = {}
                    for i, header in enumerate(headers):
                        value = row[i] if i < len(row) else ""
                        row_dict[header] = value
                    result_data.append(row_dict)
            else:
                # 没有表头，使用列索引作为键
                for row in values:
                    row_dict = {}
                    for i, value in enumerate(row):
                        row_dict[f"col_{i}"] = value
                    result_data.append(row_dict)

            # 返回包含所有行的数组
            result = {
                "data": result_data,
                "_metadata": {
                    "spreadsheet_id": self.spreadsheet_id,
                    "spreadsheet_title": self.spreadsheet.title,
                    "worksheet_name": self.worksheet_name,
                    "range": self.range,
                    "row_count": len(result_data)
                }
            }

            await self.persist_log(
                f"Successfully read {len(result_data)} rows from Google Sheets: {self.spreadsheet.title} - {self.worksheet_name}", "INFO"
            )
            return result

        except APIError as e:
            error_msg = f"Google Sheets API error: {str(e)}"
            await self.persist_log(error_msg, "ERROR")
            await self.set_status(NodeStatus.FAILED, error_msg)
            return None
        except Exception as e:
            error_msg = f"Unexpected error reading data: {str(e)}"
            await self.persist_log(error_msg, "ERROR")
            await self.set_status(NodeStatus.FAILED, error_msg)
            return None

    async def _transform_input_data(self, raw_data: Any) -> Optional[Dict[str, Any]]:
        """
        将各种输入数据格式转换为Google Sheets期望的标准格式

        支持的输入格式:
        1. 标准格式: {"data": [[row1], [row2]], "headers": [col1, col2]}
        2. JSON对象: {"key1": "value1", "key2": "value2"}
        3. 一维列表: ["item1", "item2", "item3"]
        4. 二维列表: [["row1col1", "row1col2"], ["row2col1", "row2col2"]]
        5. 多维嵌套数据: 复杂的嵌套结构
        6. 字符串: "simple text" 或 JSON字符串
        7. CodeNode输出格式: {"bitcoin_summary": "...", "top_holders": [...], ...}

        Args:
            raw_data: 原始输入数据

        Returns:
            Dict[str, Any]: 转换后的标准格式数据，如果转换失败返回None
        """
        try:
            await self.persist_log(f"Transforming input data of type: {type(raw_data)}", "DEBUG")

            # 如果已经是标准格式，直接验证并返回
            if isinstance(raw_data, dict) and "data" in raw_data:
                await self.persist_log("Input data is already in standard format", "DEBUG")
                return raw_data

            # 处理字符串输入
            if isinstance(raw_data, str):
                return await self._transform_string_data(raw_data)

            # 处理列表输入
            elif isinstance(raw_data, list):
                return self._transform_list_data(raw_data)

            # 处理字典输入（非标准格式）
            elif isinstance(raw_data, dict):
                return self._transform_dict_data(raw_data)

            # 处理其他类型（数字、布尔值等）
            else:
                return self._transform_primitive_data(raw_data)

        except Exception as e:
            await self.persist_log(f"Error transforming input data: {str(e)}", "ERROR")
            await self.persist_log(traceback.format_exc(), "DEBUG")
            return None

    async def _transform_string_data(self, data: str) -> Dict[str, Any]:
        """转换字符串数据"""
        data = data.strip()

        # 尝试解析为JSON
        if data.startswith(('{', '[')):
            try:
                parsed_data = json.loads(data)
                await self.persist_log("Successfully parsed string as JSON", "DEBUG")
                return await self._transform_input_data(parsed_data)  # 递归处理解析后的数据
            except json.JSONDecodeError:
                await self.persist_log("String is not valid JSON, treating as plain text", "DEBUG")

        # 处理多行文本（按行分割）
        if '\n' in data:
            lines = [line.strip() for line in data.split('\n') if line.strip()]
            return {
                "data": [[line] for line in lines],
                "headers": ["content"]
            }

        # 处理单行文本
        return {
            "data": [[data]],
            "headers": ["content"]
        }

    def _transform_list_data(self, data: list) -> Dict[str, Any]:
        """转换列表数据"""
        if not data:
            return {"data": [], "headers": []}

        # 检查是否为二维列表
        if isinstance(data[0], list):
            # 二维列表，直接作为数据行
            if not data[0]:  # 空的内层列表
                return {"data": [], "headers": []}

            # 生成默认列名
            max_cols = max(len(row) if isinstance(row, list) else 1 for row in data)
            headers = [f"col_{i+1}" for i in range(max_cols)]

            # 确保所有行长度一致
            normalized_data = []
            for row in data:
                if isinstance(row, list):
                    # 补齐或截断到统一长度
                    normalized_row = row[:max_cols] + [''] * (max_cols - len(row))
                else:
                    # 非列表项转换为字符串并放在第一列
                    normalized_row = [str(row)] + [''] * (max_cols - 1)
                normalized_data.append(normalized_row)

            return {
                "data": normalized_data,
                "headers": headers
            }

        else:
            # 一维列表，转换为单列数据
            return {
                "data": [[str(item)] for item in data],
                "headers": ["item"]
            }

    def _transform_dict_data(self, data: dict) -> Dict[str, Any]:
        """转换字典数据"""
        # 过滤掉内部字段（以_开头的字段）
        filtered_data = {k: v for k, v in data.items() if not k.startswith('_')}

        if not filtered_data:
            return {"data": [], "headers": []}

        # 检查是否包含列表值（可能是表格数据）
        list_values = {k: v for k, v in filtered_data.items() if isinstance(v, list)}

        if list_values:
            # 如果有列表值，尝试构建表格
            return self._transform_dict_with_lists(filtered_data, list_values)
        else:
            # 简单键值对，转换为两列表格
            return {
                "data": [[str(k), str(v)] for k, v in filtered_data.items()],
                "headers": ["key", "value"]
            }

    def _transform_dict_with_lists(self, data: dict, list_values: dict) -> Dict[str, Any]:
        """处理包含列表的字典数据 - 保留所有信息"""
        # 获取非列表字段（元数据）
        non_list_data = {k: v for k, v in data.items() if not isinstance(v, list)}

        # 如果只有一个列表字段，且其他字段是元数据
        if len(list_values) == 1:
            list_key, list_value = next(iter(list_values.items()))

            # 如果列表包含字典，添加元数据列
            if list_value and isinstance(list_value[0], dict):
                result = self._transform_list_of_dicts(list_value)
                # 为每一行添加元数据列
                if non_list_data:
                    # 添加元数据列到表头
                    meta_headers = sorted(non_list_data.keys())
                    result["headers"].extend(meta_headers)

                    # 为每一行数据添加元数据值
                    meta_values = [str(non_list_data[key]) for key in meta_headers]
                    for row in result["data"]:
                        row.extend(meta_values)

                return result

            # 如果列表包含简单值，创建综合表格
            elif list_value:
                # 创建表头：列表字段 + 元数据字段
                headers = [list_key]
                if non_list_data:
                    headers.extend(sorted(non_list_data.keys()))

                # 创建数据行
                rows = []
                meta_values = [str(non_list_data[key]) for key in sorted(non_list_data.keys())] if non_list_data else []

                for item in list_value:
                    row = [str(item)]
                    row.extend(meta_values)
                    rows.append(row)

                # 如果没有列表数据但有元数据，添加一行元数据
                if not rows and non_list_data:
                    rows.append([''] + meta_values)

                return {
                    "data": rows,
                    "headers": headers
                }

        # 多个列表或复杂情况，使用扩展的键值对格式
        return self._create_comprehensive_key_value_table(data, list_values)

    def _create_comprehensive_key_value_table(self, data: dict, list_values: dict) -> Dict[str, Any]:
        """创建综合的键值对表格，包含所有信息"""
        rows = []

        # 先添加非列表字段（元数据）
        for key, value in data.items():
            if not isinstance(value, list):
                rows.append([str(key), str(value), "metadata"])

        # 再添加列表字段
        for key, value in data.items():
            if isinstance(value, list):
                if not value:  # 空列表
                    rows.append([str(key), "", "list"])
                else:
                    # 为列表中的每个元素创建一行
                    for i, item in enumerate(value):
                        if isinstance(item, dict):
                            # 字典项转换为JSON字符串
                            item_str = json.dumps(item, ensure_ascii=False)
                        else:
                            item_str = str(item)

                        # 第一个元素显示字段名，后续元素不显示
                        field_name = str(key) if i == 0 else f"{key}[{i}]"
                        rows.append([field_name, item_str, "list_item"])

        return {
            "data": rows,
            "headers": ["field", "value", "type"]
        }

    def _transform_list_of_dicts(self, data: list) -> Dict[str, Any]:
        """转换字典列表为表格格式"""
        if not data or not isinstance(data[0], dict):
            return {"data": [], "headers": []}

        # 收集所有可能的键作为列名
        all_keys = set()
        for item in data:
            if isinstance(item, dict):
                all_keys.update(item.keys())

        headers = sorted(list(all_keys))

        # 构建数据行
        rows = []
        for item in data:
            if isinstance(item, dict):
                row = [str(item.get(key, '')) for key in headers]
                rows.append(row)

        return {
            "data": rows,
            "headers": headers
        }

    def _transform_primitive_data(self, data: Any) -> Dict[str, Any]:
        """转换基本数据类型"""
        return {
            "data": [[str(data)]],
            "headers": ["value"]
        }

    async def _extract_sheet_name_from_edge_key(self, edge_key: str) -> str:
        """
        从edge_key中提取工作表名称

        edge_key格式通常为: "source_node_id:output_handle->target_node_id:input_handle"
        我们使用input_handle作为工作表名称

        Args:
            edge_key: 边的键名

        Returns:
            str: 工作表名称
        """
        try:
            # 解析edge_key格式
            if '->' in edge_key:
                # 格式: "source_node_id:output_handle->target_node_id:input_handle"
                target_part = edge_key.split('->')[-1]  # 获取目标部分
                if ':' in target_part:
                    input_handle = target_part.split(':')[-1]  # 获取input_handle
                    # 清理句柄名称，移除可能的后缀
                    clean_handle = input_handle.replace('-handle', '').replace('_handle', '')
                    return clean_handle

            # 如果解析失败，使用整个edge_key作为工作表名
            # 但需要清理不合法的字符
            clean_key = edge_key.replace(':', '_').replace('->', '_').replace('-', '_')
            return clean_key[:31]  # Google Sheets工作表名最多31个字符

        except Exception as e:
            await self.persist_log(f"Failed to extract sheet name from edge_key '{edge_key}': {str(e)}", "WARNING")
            # 默认工作表名
            return "data"

    async def write_data_to_sheet(self, data: Dict[str, Any], sheet_name: str) -> bool:
        """
        将数据写入指定的工作表

        Args:
            data: 要写入的数据（已转换为标准格式）
            sheet_name: 目标工作表名称

        Returns:
            bool: 写入是否成功
        """
        if not self.client or not self.spreadsheet:
            await self.persist_log("Google Sheets client or spreadsheet not initialized", "ERROR")
            return False

        # 验证数据
        is_valid, error_msg = self.validate_data(data)
        if not is_valid:
            await self.persist_log(f"Data validation failed for sheet '{sheet_name}': {error_msg}", "ERROR")
            return False

        try:
            # 获取或创建工作表
            target_worksheet = await self._get_or_create_worksheet(sheet_name)
            if not target_worksheet:
                await self.persist_log(f"Failed to get or create worksheet '{sheet_name}'", "ERROR")
                return False

            # 提取数据
            headers = data.get("headers", [])
            rows = data.get("data", [])

            if not rows:
                await self.persist_log(f"No data to write to sheet '{sheet_name}'", "WARNING")
                return True  # 没有数据也算成功

            # 准备写入的值
            values_to_write = []
            if self.header_row and headers:
                values_to_write.append(headers)
            values_to_write.extend(rows)

            # 使用线程池执行同步 API 调用
            loop = asyncio.get_event_loop()

            if self.mode == "append":
                # 获取当前行数
                current_values = await loop.run_in_executor(
                    None, lambda: target_worksheet.get_values()
                )
                start_row = len(current_values) + 1
                end_row = start_row + len(values_to_write) - 1
                cols = len(values_to_write[0]) if values_to_write else 0
                range_to_update = f"A{start_row}:{chr(65+cols-1)}{end_row}" if cols > 0 else ""
            else:  # write mode
                range_to_update = self.range

            # 执行更新，带重试机制
            if range_to_update:
                max_retries = 3
                retry_count = 0
                base_delay = 1  # 基础延迟为1秒

                while retry_count <= max_retries:
                    try:
                        await loop.run_in_executor(
                            None,
                            lambda: target_worksheet.update(range_to_update, values_to_write),
                        )
                        await self.persist_log(
                            f"{'Appended' if self.mode == 'append' else 'Wrote'} {len(values_to_write)} rows to sheet '{sheet_name}'",
                            "INFO")
                        return True
                    except APIError as e:
                        retry_count += 1
                        if "rate_limit" in str(e).lower() and retry_count <= max_retries:
                            # 计算指数退避延迟
                            delay = base_delay * (2 ** (retry_count - 1))
                            await self.persist_log(
                                f"Rate limit exceeded for sheet '{sheet_name}' (attempt {retry_count}/{max_retries}). "
                                f"Retrying in {delay}s...", "WARNING")
                            await asyncio.sleep(delay)
                        else:
                            # 其他API错误或重试次数已用完
                            raise

                # 如果达到这里，说明所有重试都失败了
                await self.persist_log(f"Failed to update sheet '{sheet_name}' after {max_retries} attempts", "ERROR")
                return False
            else:
                await self.persist_log(f"No data range to update for sheet '{sheet_name}'", "WARNING")
                return False

        except Exception as e:
            error_msg = f"Error writing data to sheet '{sheet_name}': {str(e)}"
            await self.persist_log(error_msg, "ERROR")
            await self.persist_log(traceback.format_exc(), "DEBUG")
            return False

    async def _get_or_create_worksheet(self, sheet_name: str):
        """
        获取或创建指定名称的工作表

        Args:
            sheet_name: 工作表名称

        Returns:
            Worksheet: 工作表对象，失败返回None
        """
        try:
            # 首先尝试获取现有工作表
            try:
                worksheet = self.spreadsheet.worksheet(sheet_name)
                await self.persist_log(f"Found existing worksheet: {sheet_name}", "DEBUG")
                return worksheet
            except WorksheetNotFound:
                await self.persist_log(f"Worksheet '{sheet_name}' not found, creating new one", "INFO")

            # 如果工作表不存在，创建新的
            loop = asyncio.get_event_loop()
            worksheet = await loop.run_in_executor(
                None,
                lambda: self.spreadsheet.add_worksheet(title=sheet_name, rows=1000, cols=26)
            )
            await self.persist_log(f"Created new worksheet: {sheet_name}", "INFO")
            return worksheet

        except Exception as e:
            await self.persist_log(f"Failed to get or create worksheet '{sheet_name}': {str(e)}", "ERROR")
            return None

    async def _get_output_edges_info(self) -> List[Dict[str, str]]:
        """
        获取输出边的信息

        Returns:
            List[Dict]: 输出边信息列表，每个元素包含 edge_key 和 output_handle
        """
        output_edges_info = []

        try:
            # 从输出边中提取信息
            if hasattr(self, 'output_edges') and self.output_edges:
                for edge in self.output_edges:
                    if hasattr(edge, 'source_handle') and hasattr(edge, 'get_edge_key'):
                        output_handle = edge.source_handle
                        edge_key = edge.get_edge_key()
                        output_edges_info.append({
                            'edge_key': edge_key,
                            'output_handle': output_handle
                        })

            await self.persist_log(f"Found {len(output_edges_info)} output edges", "DEBUG")
            return output_edges_info

        except Exception as e:
            await self.persist_log(f"Failed to get output edges info: {str(e)}", "WARNING")
            return []

    async def _extract_sheet_name_from_output_handle(self, output_handle: str) -> str:
        """
        从输出句柄名称中提取工作表名称

        Args:
            output_handle: 输出句柄名称

        Returns:
            str: 工作表名称
        """
        try:
            # 清理句柄名称，移除可能的后缀
            clean_handle = output_handle.replace('-handle', '').replace('_handle', '')

            # 确保工作表名称符合Google Sheets要求
            # Google Sheets工作表名最多31个字符，不能包含某些特殊字符
            clean_name = clean_handle.replace(':', '_').replace('/', '_').replace('\\', '_')
            clean_name = clean_name.replace('[', '_').replace(']', '_').replace('*', '_')
            clean_name = clean_name.replace('?', '_').replace('\n', '_').replace('\r', '_')

            return clean_name[:31]  # 限制长度

        except Exception as e:
            await self.persist_log(f"Failed to extract sheet name from output_handle '{output_handle}': {str(e)}", "WARNING")
            # 默认工作表名
            return "data"

    async def read_data_from_sheet(self, sheet_name: str) -> Optional[Dict[str, Any]]:
        """
        从指定的工作表读取数据

        Args:
            sheet_name: 工作表名称

        Returns:
            Dict[str, Any]: 读取的数据，失败返回None
        """
        if not self.client or not self.spreadsheet:
            await self.persist_log("Google Sheets client or spreadsheet not initialized", "ERROR")
            return None

        try:
            # 尝试获取指定的工作表
            target_worksheet = None

            try:
                target_worksheet = self.spreadsheet.worksheet(sheet_name)
                await self.persist_log(f"Found worksheet: {sheet_name}", "DEBUG")
            except WorksheetNotFound:
                await self.persist_log(f"Worksheet '{sheet_name}' not found, trying to use first available worksheet", "INFO")

                # 如果指定的工作表不存在，使用第一个可用的工作表
                worksheets = self.spreadsheet.worksheets()
                if worksheets:
                    target_worksheet = worksheets[0]
                    await self.persist_log(f"Using first available worksheet: {target_worksheet.title}", "INFO")
                else:
                    await self.persist_log("No worksheets found in the spreadsheet", "ERROR")
                    return None

            if not target_worksheet:
                await self.persist_log(f"Failed to get worksheet '{sheet_name}'", "ERROR")
                return None

            # 使用线程池执行同步 API 调用
            loop = asyncio.get_event_loop()
            values = await loop.run_in_executor(
                None, lambda: target_worksheet.get_values(self.range)
            )

            if not values:
                await self.persist_log(f"No data found in range {self.range} of sheet '{target_worksheet.title}'", "WARNING")
                return {"data": [], "headers": []}

            # 处理数据
            if self.header_row and len(values) > 0:
                headers = values[0]
                data = values[1:]
            else:
                headers = [f"col_{i}" for i in range(len(values[0]))] if values else []
                data = values

            result = {
                "spreadsheet_id": self.spreadsheet_id,
                "spreadsheet_title": self.spreadsheet.title,
                "worksheet_name": target_worksheet.title,
                "range": self.range,
                "headers": headers,
                "data": data,
                "row_count": len(data),
                "column_count": len(headers) if headers else 0,
            }

            await self.persist_log(
                f"Read {len(data)} rows from {self.spreadsheet.title}/{target_worksheet.title}", "INFO")
            return result

        except APIError as e:
            error_msg = f"Google Sheets API error reading sheet '{sheet_name}': {str(e)}"
            await self.persist_log(error_msg, "ERROR")
            return None
        except Exception as e:
            error_msg = f"Error reading data from Google Sheets: {str(e)}"
            await self.persist_log(error_msg, "ERROR")
            return None

    def validate_data(self, data: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """验证数据格式是否正确

        Returns:
            Tuple[bool, Optional[str]]: (是否有效, 错误信息)
        """
        # 检查数据是否为字典
        if not isinstance(data, dict):
            return False, f"Invalid data type: expected dict, got {type(data).__name__}"

        # 检查必要的字段
        if "data" not in data:
            return False, "Missing required field: 'data'"

        # 检查数据格式
        rows = data.get("data", [])
        if not isinstance(rows, list):
            return False, f"Invalid 'data' type: expected list, got {type(rows).__name__}"

        # 检查headers字段
        headers = data.get("headers", [])
        if not isinstance(headers, list):
            return False, f"Invalid 'headers' type: expected list, got {type(headers).__name__}"

        # 如果有数据行，检查每行的格式
        if rows:
            # 检查所有行的长度是否一致
            row_lengths = set(len(row) for row in rows if isinstance(row, list))
            if len(row_lengths) > 1:
                return False, "Inconsistent row lengths in data"

            # 检查行数据类型
            for i, row in enumerate(rows):
                if not isinstance(row, list):
                    return False, f"Invalid row type at index {i}: expected list, got {type(row).__name__}"

            # 如果有headers，检查headers长度与数据行长度是否匹配
            if headers and row_lengths and list(row_lengths)[0] != len(headers):
                return False, f"Headers length ({len(headers)}) does not match data row length ({list(row_lengths)[0]})"

        return True, None

    async def write_data(self, data: Dict[str, Any]) -> bool:
        """写入数据到 Google Sheets"""
        if not self.worksheet:
            await self.persist_log("Worksheet not initialized", "ERROR")
            return False

        # 验证数据
        is_valid, error_msg = self.validate_data(data)
        if not is_valid:
            await self.persist_log(f"Data validation failed: {error_msg}", "ERROR")
            await self.set_status(NodeStatus.FAILED, f"Data validation failed: {error_msg}")
            return False

        try:
            # 提取数据
            headers = data.get("headers", [])
            rows = data.get("data", [])

            if not rows:
                await self.persist_log("No data to write", "WARNING")
                return True  # 没有数据也算成功

            # 准备写入的值
            values_to_write = []
            if self.header_row and headers:
                values_to_write.append(headers)
            values_to_write.extend(rows)

            # 使用线程池执行同步 API 调用
            loop = asyncio.get_event_loop()

            if self.mode == "append":
                # 获取当前行数
                current_values = await loop.run_in_executor(
                    None, lambda: self.worksheet.get_values()
                )
                start_row = len(current_values) + 1
                end_row = start_row + len(values_to_write) - 1
                cols = len(values_to_write[0]) if values_to_write else 0
                range_to_update = f"A{start_row}:{chr(65+cols-1)}{end_row}" if cols > 0 else ""
            else:  # write mode
                range_to_update = self.range

            # 执行更新，带重试机制
            if range_to_update:
                max_retries = 3
                retry_count = 0
                base_delay = 1  # 基础延迟为1秒

                while retry_count <= max_retries:
                    try:
                        await loop.run_in_executor(
                            None,
                            lambda: self.worksheet.update(range_to_update, values_to_write),
                        )
                        await self.persist_log(
                            f"{'Appended' if self.mode == 'append' else 'Wrote'} {len(values_to_write)} rows to Google Sheets: {self.spreadsheet.title} - {self.worksheet.title}", "INFO"
                        )
                        return True
                    except APIError as e:
                        retry_count += 1
                        if "rate_limit" in str(e).lower() and retry_count <= max_retries:
                            # 计算指数退避延迟
                            delay = base_delay * (2 ** (retry_count - 1))
                            await self.persist_log(
                                f"Rate limit exceeded (attempt {retry_count}/{max_retries}). "
                                f"Retrying in {delay}s...", "WARNING"
                            )
                            await asyncio.sleep(delay)
                        else:
                            # 其他API错误或重试次数已用完
                            raise

                # 如果达到这里，说明所有重试都失败了
                await self.persist_log(f"Failed to update sheet after {max_retries} attempts", "ERROR")
                return False
            else:
                await self.persist_log("No data range to update", "WARNING")
                return False

        except APIError as e:
            error_msg = f"Google Sheets API error: {str(e)}"
            if "permission" in str(e).lower():
                error_msg = f"Permission denied to write to spreadsheet: {str(e)}"
            elif "rate_limit" in str(e).lower():
                error_msg = f"Rate limit exceeded: {str(e)}. Consider reducing request frequency."
            elif "quota" in str(e).lower():
                error_msg = f"API quota exceeded: {str(e)}. Please try again later."
            await self.persist_log(error_msg, "ERROR")
            await self.set_status(NodeStatus.FAILED, error_msg)
            return False
        except gspread.exceptions.GSpreadException as e:
            error_msg = f"Google Sheets error: {str(e)}"
            await self.persist_log(error_msg, "ERROR")
            await self.set_status(NodeStatus.FAILED, error_msg)
            return False
        except Exception as e:
            error_msg = f"Error writing data to Google Sheets: {str(e)}"
            await self.persist_log(error_msg, "ERROR")
            await self.set_status(NodeStatus.FAILED, error_msg)
            return False

    async def get_safe_credentials_path(self) -> str:
        """获取安全的凭证路径，支持环境变量和相对路径"""
        # 首先检查环境变量
        env_cred_path = os.environ.get("GOOGLE_CREDENTIALS_PATH")
        if env_cred_path:
            # 如果环境变量中的路径是相对路径，则相对于项目根目录
            if not os.path.isabs(env_cred_path):
                current_file = pathlib.Path(__file__)
                project_root = current_file.parent.parent.parent.parent
                env_cred_path = os.path.join(project_root, env_cred_path)

            if os.path.exists(env_cred_path):
                await self.persist_log(f"Using credentials from environment variable: {env_cred_path}", "INFO")
                return env_cred_path

        # 然后检查配置文件中的路径
        config_cred_path = self.credentials_path
        if config_cred_path and os.path.exists(config_cred_path):
            await self.persist_log(f"Using credentials from config: {config_cred_path}", "INFO")
            return config_cred_path

        # 最后返回空字符串
        await self.persist_log("No valid Google credentials path found", "WARNING")
        return ""

    async def execute(self) -> bool:
        """Execute node logic based on mode (read/write/append)"""
        start_time = time.time()
        try:
            await self.persist_log(
                f"Starting DatasetNode execution: mode={self.mode}, spreadsheet_id={self.spreadsheet_id}, worksheet_name={self.worksheet_name}", "INFO"
            )

            # Log original input if extracted from URL
            original_input = getattr(self, '_original_spreadsheet_input', None)
            if original_input and original_input != self.spreadsheet_id:
                await self.persist_log(f"Extracted spreadsheet ID '{self.spreadsheet_id}' from input: {original_input}", "INFO")

            # Validate mode
            if self.mode not in ["read", "write", "append"]:
                error_msg = f"Invalid mode: {self.mode}. Must be 'read', 'write', or 'append'"
                await self.persist_log(error_msg, "ERROR")
                await self.set_status(NodeStatus.FAILED, error_msg)
                return False

            # Validate spreadsheet_id
            if not self.spreadsheet_id:
                error_msg = "Spreadsheet ID is required"
                await self.persist_log(error_msg, "ERROR")
                await self.set_status(NodeStatus.FAILED, error_msg)
                return False

            # Initialize client
            if not await self.initialize_client():
                return False

            await self.set_status(NodeStatus.RUNNING)

            # Execute based on mode
            if self.mode == "read":
                data = await self.read_data()
                if data:
                    if await self.send_signal(DATA_OUTPUT_HANDLE, SignalType.DATASET, payload=data):
                        await self.persist_log("Successfully sent dataset signal", "INFO")
                        await self.set_status(NodeStatus.COMPLETED)
                        return True
                    else:
                        error_msg = "Failed to send dataset signal"
                        await self.persist_log(error_msg, "ERROR")
                        await self.set_status(NodeStatus.FAILED, error_msg)
                        return False
                else:
                    error_msg = "Failed to read data from Google Sheets"
                    await self.persist_log(error_msg, "WARNING")
                    await self.set_status(NodeStatus.FAILED, error_msg)
                    return False

            elif self.mode in ["write", "append"]:
                # Get input data from signals
                input_signals_data = {}
                for edge_key, signal in self._input_signals.items():
                    if signal and signal.payload:
                        input_signals_data[edge_key] = signal.payload

                if not input_signals_data:
                    error_msg = "No input data received for write operation"
                    await self.persist_log(error_msg, "WARNING")
                    await self.set_status(NodeStatus.FAILED, error_msg)
                    return False

                # 直接将JSON写入到指定的cell
                success = True
                for edge_key, json_data in input_signals_data.items():
                    # 序列化为JSON字符串
                    try:
                        json_string = json.dumps(json_data, ensure_ascii=False, indent=2)
                        await self.persist_log(f"Writing JSON data to cell A1: {len(json_string)} characters", "INFO")

                        # 写入到单个cell (A1)
                        loop = asyncio.get_event_loop()
                        await loop.run_in_executor(
                            None,
                            lambda: self.worksheet.update('A1', [[json_string]])
                        )
                        await self.persist_log("Successfully wrote JSON to Google Sheets cell A1", "INFO")
                    except Exception as e:
                        await self.persist_log(f"Failed to write JSON for signal {edge_key}: {str(e)}", "ERROR")
                        success = False
                        break

                if success:
                    await self.persist_log("Successfully wrote all data to Google Sheets", "INFO")
                    await self.set_status(NodeStatus.COMPLETED)
                    return True
                else:
                    error_msg = "Failed to write data to Google Sheets"
                    await self.persist_log(error_msg, "ERROR")
                    await self.set_status(NodeStatus.FAILED, error_msg)
                    return False

            else:
                error_msg = f"Unsupported mode: {self.mode}"
                await self.persist_log(error_msg, "ERROR")
                await self.set_status(NodeStatus.FAILED, error_msg)
                return False

        except asyncio.CancelledError:
            await self.set_status(NodeStatus.TERMINATED, "Task cancelled")
            return True
        except Exception as e:
            error_msg = f"DatasetNode execution error: {str(e)}"
            await self.persist_log(error_msg, "ERROR")
            await self.set_status(NodeStatus.FAILED, error_msg)
            return False
        finally:
            # Clean up resources
            self.client = None
            self.spreadsheet = None
            self.worksheet = None


# ============ 实例节点类 ============

@register_node_type(
    "dataset_input_node",
    default_params={
        "spreadsheet_id": "",
        "worksheet_name": "Sheet1",
        "mode": "read",
        "range": "A1:Z1000",
        "header_row": True,
        "node_category": "instance",
        "display_name": "Dataset Input",
        "base_node_type": "dataset_node",
        "version": "0.0.2",
    },
)
class DatasetInputNode(DatasetNode):
    """
    Dataset Input Node - 数据输入节点

    专门用于从 Google Sheets 读取数据并输出为完整 JSON 对象

    输入参数:
    - doc_link: 文档链接 (Google Sheets URL或ID)

    输出信号:
    - data: 完整的 JSON object
      格式: {
        "data": [{row1_dict}, {row2_dict}, ...],
        "_metadata": {"spreadsheet_id", "worksheet_name", "row_count", ...}
      }

    说明:
    - 如果 Google Sheet 有表头，每行将转换为字典，使用表头作为键
    - 如果没有表头，使用 col_0, col_1... 作为键
    - 所有数据打包在 "data" 数组中
    - 元数据保存在 "_metadata" 字段
    """

    def __init__(self, **kwargs):
        # 强制设置为读取模式
        kwargs['mode'] = 'read'

        # 设置实例节点元数据
        kwargs.setdefault('version', '0.0.2')
        kwargs.setdefault('display_name', 'Dataset Input Node')
        kwargs.setdefault('node_category', 'instance')
        kwargs.setdefault('base_node_type', 'dataset_node')
        kwargs.setdefault('description', 'Specialized node for reading data from external sources')
        kwargs.setdefault('author', 'TradingFlow Team')
        kwargs.setdefault('tags', ['data', 'input', 'read'])

        super().__init__(**kwargs)

        # Logging will be handled by persist_log method

    def _register_input_handles(self):
        """注册输入句柄 - Dataset Input特化"""
        from nodes.node_base import InputHandle

        # 注册doc_link输入句柄
        self._input_handles["doc_link"] = InputHandle(
            name="doc_link",
            data_type=str,
            description="Document Link - Google Sheets URL or ID",
            example="1Q1J0-2s0SsY4z6uo9LXxTgYyacpeMrLlY7zbQnBYKgY",
            auto_update_attr="spreadsheet_id"
        )


@register_node_type(
    "dataset_output_node",
    default_params={
        "spreadsheet_id": "",
        "worksheet_name": "Sheet1",
        "mode": "write",
        "range": "A1:Z1000",
        "header_row": True,
        "node_category": "instance",
        "display_name": "Dataset Output",
        "base_node_type": "dataset_node",
        "version": "0.0.2",
    },
)
class DatasetOutputNode(DatasetNode):
    """
    Dataset Output Node - 数据输出节点

    专门用于将任意 JSON 对象写入 Google Sheets

    输入参数:
    - data: 任意 JSON object (将被序列化并写入 cell A1)
    - doc_link: 文档链接 (Google Sheets URL或ID)

    输出格式:
    - JSON 对象会被序列化为美化的 JSON 字符串
    - 整个 JSON 字符串写入到 Sheet 的 A1 单元格
    - 支持任意嵌套的 JSON 结构

    示例输入:
    {
      "result": "success",
      "items": [1, 2, 3],
      "metadata": {"timestamp": "2024-01-01"}
    }
    """

    def __init__(self, **kwargs):
        # 强制设置为写入模式
        kwargs['mode'] = 'write'

        # 设置实例节点元数据
        kwargs.setdefault('version', '0.0.2')
        kwargs.setdefault('display_name', 'Dataset Output Node')
        kwargs.setdefault('node_category', 'instance')
        kwargs.setdefault('base_node_type', 'dataset_node')
        kwargs.setdefault('description', 'Specialized node for writing data to external sources')
        kwargs.setdefault('author', 'TradingFlow Team')
        kwargs.setdefault('tags', ['data', 'output', 'write'])

        super().__init__(**kwargs)

        # Logging will be handled by persist_log method

    def _register_input_handles(self):
        """注册输入句柄 - Dataset Output特化"""
        from nodes.node_base import InputHandle

        # 注册data输入句柄
        self._input_handles["data"] = InputHandle(
            name="data",
            data_type=dict,
            description="Data to write - Complete JSON object (will be serialized and written to cell A1)",
            example={"key1": "value1", "key2": [1, 2, 3], "key3": {"nested": "object"}}
        )

        # 注册doc_link输入句柄
        self._input_handles["doc_link"] = InputHandle(
            name="doc_link",
            data_type=str,
            description="Document Link - Google Sheets URL or ID",
            example="1Q1J0-2s0SsY4z6uo9LXxTgYyacpeMrLlY7zbQnBYKgY",
            auto_update_attr="spreadsheet_id"
        )
