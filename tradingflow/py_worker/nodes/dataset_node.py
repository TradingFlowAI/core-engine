import asyncio
import logging
import traceback
import json
import os
import time
import pathlib
from typing import Any, Dict, List, Optional, Tuple, Union

import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError, SpreadsheetNotFound, WorksheetNotFound
from tradingflow.common.config import CONFIG
from tradingflow.py_worker.common.edge import Edge

from tradingflow.py_worker.common.node_decorators import register_node_type
from tradingflow.py_worker.common.signal_types import SignalType
from tradingflow.py_worker.nodes.node_base import NodeBase, NodeStatus

# 定义输入输出处理器名称
DATA_INPUT_HANDLE = "data_input_handle"
DATA_OUTPUT_HANDLE = "data_output_handle"


@register_node_type(
    "dataset_node",
    default_params={
        "spreadsheet_id": "",
        "worksheet_name": "Sheet1",
        "mode": "read",  # read, write, append
        "range": "A1:Z1000",
        "header_row": True,
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
        self.spreadsheet_id = spreadsheet_id
        self.worksheet_name = worksheet_name
        self.mode = mode.lower()
        self.range = range
        self.header_row = header_row

        # 获取凭证路径，支持相对路径
        credentials_path = CONFIG.get("GOOGLE_CREDENTIALS_PATH", "")
        if credentials_path and not os.path.isabs(credentials_path):
            # 如果是相对路径，则相对于项目根目录
            # 获取项目根目录路径
            current_file = pathlib.Path(__file__)
            # 假设目录结构是 TradingFlow/python/py_worker/nodes/dataset_node.py
            project_root = current_file.parent.parent.parent.parent
            credentials_path = os.path.join(project_root, credentials_path)

        self.credentials_path = credentials_path

        # Google Sheets 客户端
        self.client = None
        self.spreadsheet = None
        self.worksheet = None

        # 日志设置
        self.logger = logging.getLogger(f"DatasetNode.{node_id}")

    async def initialize_client(self) -> bool:
        """初始化 Google Sheets API 客户端，带有重试机制"""
        max_retries = 3
        retry_count = 0
        base_delay = 1  # 基础延迟为1秒

        while retry_count <= max_retries:
            try:
                # 检查凭证路径
                if not self.credentials_path:
                    error_msg = "Google API credentials path not provided"
                    self.logger.error(error_msg)
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
                self.logger.info("Google Sheets client initialized")

                # 检查 spreadsheet_id
                if not self.spreadsheet_id:
                    error_msg = "Spreadsheet ID not provided"
                    self.logger.error(error_msg)
                    await self.set_status(NodeStatus.FAILED, error_msg)
                    return False

                # 打开电子表格
                try:
                    self.spreadsheet = self.client.open_by_key(self.spreadsheet_id)
                    self.logger.info(f"Opened spreadsheet: {self.spreadsheet.title}")
                except SpreadsheetNotFound:
                    error_msg = f"Spreadsheet with ID '{self.spreadsheet_id}' not found"
                    self.logger.error(error_msg)
                    await self.set_status(NodeStatus.FAILED, error_msg)
                    return False
                except APIError as e:
                    if "permission" in str(e).lower():
                        error_msg = f"Permission denied to access spreadsheet: {str(e)}"
                    else:
                        error_msg = f"API error accessing spreadsheet: {str(e)}"
                    self.logger.error(error_msg)
                    await self.set_status(NodeStatus.FAILED, error_msg)
                    return False

                # 打开工作表
                try:
                    self.worksheet = self.spreadsheet.worksheet(self.worksheet_name)
                    self.logger.info(f"Opened worksheet: {self.worksheet_name}")
                except WorksheetNotFound:
                    error_msg = f"Worksheet '{self.worksheet_name}' not found"
                    self.logger.error(error_msg)
                    await self.set_status(NodeStatus.FAILED, error_msg)
                    return False

                return True

            except Exception as e:
                retry_count += 1
                if retry_count <= max_retries:
                    # 计算指数退避延迟
                    delay = base_delay * (2 ** (retry_count - 1))
                    self.logger.warning(
                        f"Failed to initialize Google Sheets client (attempt {retry_count}/{max_retries}): {str(e)}. "
                        f"Retrying in {delay}s..."
                    )
                    await asyncio.sleep(delay)
                else:
                    error_msg = f"Failed to initialize Google Sheets client after {max_retries} attempts: {str(e)}"
                    self.logger.error(error_msg)
                    await self.set_status(NodeStatus.FAILED, error_msg)
                    self.logger.debug(traceback.format_exc())
                    return False

    async def read_data(self) -> Optional[Dict[str, Any]]:
        """读取 Google Sheets 数据"""
        if not self.worksheet:
            self.logger.error("Worksheet not initialized")
            return None

        try:
            # 使用线程池执行同步 API 调用
            loop = asyncio.get_event_loop()
            values = await loop.run_in_executor(
                None, lambda: self.worksheet.get_values(self.range)
            )

            if not values:
                self.logger.warning(f"No data found in range {self.range}")
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
                "worksheet_name": self.worksheet_name,
                "range": self.range,
                "headers": headers,
                "data": data,
                "row_count": len(data),
                "column_count": len(headers) if headers else 0,
            }

            self.logger.info(
                f"Read {len(data)} rows from {self.spreadsheet.title}/{self.worksheet_name}"
            )
            return result

        except APIError as e:
            error_msg = f"Google Sheets API error: {str(e)}"
            self.logger.error(error_msg)
            await self.set_status(NodeStatus.FAILED, error_msg)
            return None
        except Exception as e:
            error_msg = f"Error reading data: {str(e)}"
            self.logger.error(error_msg)
            await self.set_status(NodeStatus.FAILED, error_msg)
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
            self.logger.error("Worksheet not initialized")
            return False

        # 验证数据
        is_valid, error_msg = self.validate_data(data)
        if not is_valid:
            self.logger.error(f"Data validation failed: {error_msg}")
            await self.set_status(NodeStatus.FAILED, f"Data validation failed: {error_msg}")
            return False

        try:
            # 提取数据
            headers = data.get("headers", [])
            rows = data.get("data", [])

            if not rows:
                self.logger.warning("No data to write")
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
                        self.logger.info(
                            f"{'Appended' if self.mode == 'append' else 'Wrote'} {len(values_to_write)} rows to {self.spreadsheet.title}/{self.worksheet_name}"
                        )
                        return True
                    except APIError as e:
                        retry_count += 1
                        if "rate_limit" in str(e).lower() and retry_count <= max_retries:
                            # 计算指数退避延迟
                            delay = base_delay * (2 ** (retry_count - 1))
                            self.logger.warning(
                                f"Rate limit exceeded (attempt {retry_count}/{max_retries}). "
                                f"Retrying in {delay}s..."
                            )
                            await asyncio.sleep(delay)
                        else:
                            # 其他API错误或重试次数已用完
                            raise

                # 如果达到这里，说明所有重试都失败了
                self.logger.error(f"Failed to update sheet after {max_retries} attempts")
                return False
            else:
                self.logger.warning("No data range to update")
                return False

        except APIError as e:
            error_msg = f"Google Sheets API error: {str(e)}"
            if "permission" in str(e).lower():
                error_msg = f"Permission denied to write to spreadsheet: {str(e)}"
            elif "rate_limit" in str(e).lower():
                error_msg = f"Rate limit exceeded: {str(e)}. Consider reducing request frequency."
            elif "quota" in str(e).lower():
                error_msg = f"API quota exceeded: {str(e)}. Please try again later."
            self.logger.error(error_msg)
            await self.set_status(NodeStatus.FAILED, error_msg)
            return False
        except gspread.exceptions.GSpreadException as e:
            error_msg = f"Google Sheets error: {str(e)}"
            self.logger.error(error_msg)
            await self.set_status(NodeStatus.FAILED, error_msg)
            return False
        except Exception as e:
            error_msg = f"Error writing data: {str(e)}"
            self.logger.error(error_msg)
            self.logger.debug(traceback.format_exc())
            await self.set_status(NodeStatus.FAILED, error_msg)
            return False

    def get_safe_credentials_path(self) -> str:
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
                self.logger.info(f"Using credentials from environment variable: {env_cred_path}")
                return env_cred_path

        # 然后检查配置文件中的路径
        config_cred_path = self.credentials_path
        if config_cred_path and os.path.exists(config_cred_path):
            self.logger.info(f"Using credentials from config: {config_cred_path}")
            return config_cred_path

        # 尝试默认位置
        current_file = pathlib.Path(__file__)
        project_root = current_file.parent.parent.parent.parent
        default_path = os.path.join(project_root, "python/py_worker/config/google_credentials.json")
        if os.path.exists(default_path):
            self.logger.info(f"Using default credentials path: {default_path}")
            return default_path

        # 最后返回空字符串
        self.logger.warning("No valid Google credentials path found")
        return ""

    async def execute(self) -> bool:
        """执行节点逻辑，根据模式读取或写入数据"""
        start_time = time.time()
        try:
            self.logger.info(
                f"Executing DatasetNode in {self.mode} mode for spreadsheet {self.spreadsheet_id}"
            )

            # 检查模式是否有效
            if self.mode not in ["read", "write", "append"]:
                error_msg = f"Invalid mode: {self.mode}. Must be 'read', 'write', or 'append'"
                self.logger.error(error_msg)
                await self.set_status(NodeStatus.FAILED, error_msg)
                return False

            # 检查spreadsheet_id是否有效
            if not self.spreadsheet_id:
                error_msg = "Spreadsheet ID is required"
                self.logger.error(error_msg)
                await self.set_status(NodeStatus.FAILED, error_msg)
                return False

            # 初始化客户端
            if not await self.initialize_client():
                return False

            await self.set_status(NodeStatus.RUNNING)

            # 根据模式执行不同操作
            if self.mode == "read":
                # 读取数据
                data = await self.read_data()
                if data:
                    # 发送数据信号
                    if await self.send_signal(
                        DATA_OUTPUT_HANDLE, SignalType.DATASET, payload=data
                    ):
                        self.logger.info("Successfully sent dataset signal")
                        await self.set_status(NodeStatus.COMPLETED)
                        return True
                    else:
                        error_msg = "Failed to send dataset signal"
                        self.logger.error(error_msg)
                        await self.set_status(NodeStatus.FAILED, error_msg)
                        return False
                else:
                    error_msg = "Failed to read data from Google Sheets"
                    self.logger.warning(error_msg)
                    await self.set_status(NodeStatus.FAILED, error_msg)
                    return False

            elif self.mode in ["write", "append"]:
                # 获取输入数据
                input_data = await self.get_input_signal(DATA_INPUT_HANDLE)
                
                if not input_data:
                    error_msg = "No input data received for write operation"
                    self.logger.warning(error_msg)
                    await self.set_status(NodeStatus.FAILED, error_msg)
                    return False

                # 写入数据
                if await self.write_data(input_data):
                    self.logger.info("Successfully wrote data to Google Sheets")
                    await self.set_status(NodeStatus.COMPLETED)
                    return True
                else:
                    error_msg = "Failed to write data to Google Sheets"
                    self.logger.warning(error_msg)
                    await self.set_status(NodeStatus.FAILED, error_msg)
                    return False

            else:
                error_msg = f"Invalid mode: {self.mode}. Must be 'read', 'write', or 'append'"
                self.logger.error(error_msg)
                await self.set_status(NodeStatus.FAILED, error_msg)
                return False

        except asyncio.CancelledError:
            # 任务被取消
            await self.set_status(NodeStatus.TERMINATED, "Task cancelled")
            return True
        except Exception as e:
            error_msg = f"Error executing DatasetNode: {str(e)}"
            self.logger.error(error_msg)
            await self.set_status(NodeStatus.FAILED, error_msg)
            return False
        finally:
            # 清理资源
            self.client = None
            self.spreadsheet = None
            self.worksheet = None
