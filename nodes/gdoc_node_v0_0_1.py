import asyncio
import traceback
import os
import time
import pathlib
import re
from typing import Any, Dict, List, Optional

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from infra.config import CONFIG
from common.edge import Edge

from common.node_decorators import register_node_type
from common.signal_types import SignalType
from nodes.node_base import NodeBase, NodeStatus

STATUS_OUTPUT_HANDLE = "status_output_handle"
ERROR_HANDLE = "error_handle"


@register_node_type(
    "gdoc_output_node",
    default_params={
        "doc_id": "",
        "mode": "append",
        "node_category": "output",
        "display_name": "Google Doc Output",
        "version": "0.0.1",
    },
)
class GDocNode(NodeBase):
    """
    Google Docs 节点 - 用于写入内容到 Google Docs

    输入参数:
    - doc_id: Google Docs 文档 ID 或完整 URL
    - mode: 写入模式，'append'（追加到末尾）, 'replace'（替换全部）或 'prepend'（添加到开头）
    - content: 要写入的内容

    输出信号:
    - STATUS_OUTPUT_HANDLE: 操作状态
    - ERROR_HANDLE: 错误信息
    """

    def __init__(
        self,
        flow_id: str,
        component_id: int,
        cycle: int,
        node_id: str,
        name: str,
        doc_id: str = "",
        mode: str = "append",
        content: str = "",
        input_edges: List[Edge] = None,
        output_edges: List[Edge] = None,
        state_store=None,
        **kwargs,
    ):
        """
        初始化 Google Docs 节点

        Args:
            flow_id: 流程ID
            component_id: 组件ID
            cycle: 节点执行周期
            node_id: 节点唯一标识符
            name: 节点名称
            doc_id: Google Docs 文档 ID
            mode: 写入模式 ('append', 'replace', 'prepend')
            content: 要写入的内容
            input_edges: 输入边列表
            output_edges: 输出边列表
            state_store: 状态存储
            **kwargs: 传递给基类的其他参数
        """
        # 设置基类节点元数据
        kwargs.setdefault('version', '0.0.1')
        kwargs.setdefault('display_name', 'Google Doc Output')
        kwargs.setdefault('node_category', 'output')
        kwargs.setdefault('description', 'Write content to Google Docs')
        kwargs.setdefault('author', 'TradingFlow Team')
        kwargs.setdefault('tags', ['output', 'google-docs', 'document'])

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

        # 从kwargs中获取doc_link，然后提取doc_id
        doc_link = kwargs.get('doc_link', doc_id)  # 优先使用kwargs中的doc_link，fallback到参数
        self._original_doc_input = doc_link  # 保存原始输入用于日志
        self.doc_id = self._extract_doc_id(doc_link)
        self.mode = mode.lower() if mode else "append"
        self.content = kwargs.get('content', content)

        # 获取凭证路径，支持相对路径
        credentials_path = CONFIG.get("GOOGLE_CREDENTIALS_PATH", "")
        if credentials_path and not os.path.isabs(credentials_path):
            # 如果是相对路径，则相对于项目根目录
            current_file = pathlib.Path(__file__)
            project_root = current_file.parent.parent.parent.parent
            credentials_path = os.path.join(project_root, credentials_path)

        self.credentials_path = credentials_path

        # Google Docs 服务
        self.service = None

    def _register_output_handles(self) -> None:
        """注册输出句柄"""
        self.register_output_handle(
            name=STATUS_OUTPUT_HANDLE,
            data_type=dict,
            description="Operation status",
            example={
                "success": True,
                "doc_id": "1abc",
                "doc_title": "My Document",
                "characters_written": 100,
            },
        )
        self.register_output_handle(
            name=ERROR_HANDLE,
            data_type=dict,
            description="Error information if operation fails",
            example={
                "error": True,
                "message": "Permission denied",
                "code": 403,
            },
        )

    @staticmethod
    def _extract_doc_id(doc_input: str) -> str:
        """
        从Google Docs URL或直接ID中提取document ID

        支持的格式:
        1. 直接ID: "1uQvzsNIkaan67wFijnDiixusw9qNZnedW4Q3dWJASBM"
        2. 完整URL: "https://docs.google.com/document/d/1uQvzsNIkaan67wFijnDiixusw9qNZnedW4Q3dWJASBM/edit"
        3. 简化URL: "docs.google.com/document/d/1uQvzsNIkaan67wFijnDiixusw9qNZnedW4Q3dWJASBM"

        Args:
            doc_input: Google Docs URL或document ID

        Returns:
            str: 提取的document ID，如果无法提取则返回原始输入
        """
        if not doc_input or not isinstance(doc_input, str):
            return doc_input or ""

        # 去除首尾空白
        doc_input = doc_input.strip()

        # 如果已经是一个看起来像ID的字符串（不包含斜杠且长度合适），直接返回
        if '/' not in doc_input and len(doc_input) >= 20:
            return doc_input

        # 使用正则表达式提取document ID
        patterns = [
            r'/document/d/([a-zA-Z0-9-_]+)',  # 标准格式
            r'id=([a-zA-Z0-9-_]+)',           # 某些格式可能使用id参数
            r'document/d/([a-zA-Z0-9-_]+)',   # 没有前导斜杠的格式
        ]

        for pattern in patterns:
            match = re.search(pattern, doc_input)
            if match:
                extracted_id = match.group(1)
                if len(extracted_id) >= 20:  # 允许一些变化
                    return extracted_id

        # 如果无法提取，返回原始输入（可能已经是有效的ID）
        return doc_input

    async def initialize_service(self) -> bool:
        """初始化 Google Docs API 服务，带有重试机制"""
        max_retries = 3
        retry_count = 0
        base_delay = 1  # 基础延迟为1秒

        while retry_count <= max_retries:
            try:
                # 检查凭证路径
                if not self.credentials_path or not os.path.exists(self.credentials_path):
                    error_msg = f"Google credentials file not found: {self.credentials_path}"
                    await self.persist_log(error_msg, "ERROR")
                    await self.set_status(NodeStatus.FAILED, error_msg)
                    return False

                # 设置 Google Docs API 范围
                scopes = [
                    "https://www.googleapis.com/auth/documents",
                    "https://www.googleapis.com/auth/drive",
                ]

                # 创建凭证
                credentials = Credentials.from_service_account_file(
                    self.credentials_path, scopes=scopes
                )

                # 创建 Google Docs 服务
                loop = asyncio.get_event_loop()
                self.service = await loop.run_in_executor(
                    None,
                    lambda: build('docs', 'v1', credentials=credentials)
                )
                await self.persist_log("Google Docs service initialized", "INFO")

                # 验证 doc_id
                if not self.doc_id:
                    error_msg = "Document ID not provided"
                    await self.persist_log(error_msg, "ERROR")
                    await self.set_status(NodeStatus.FAILED, error_msg)
                    return False

                return True

            except Exception as e:
                retry_count += 1
                if retry_count <= max_retries:
                    delay = base_delay * (2 ** (retry_count - 1))
                    await self.persist_log(
                        f"Failed to initialize Google Docs service (attempt {retry_count}/{max_retries}): {str(e)}. "
                        f"Retrying in {delay}s...", "WARNING")
                    await asyncio.sleep(delay)
                else:
                    error_msg = f"Failed to initialize Google Docs service: {str(e)}"
                    await self.persist_log(error_msg, "ERROR")
                    await self.set_status(NodeStatus.FAILED, error_msg)
                    return False

    async def get_document_info(self) -> Optional[Dict[str, Any]]:
        """获取文档信息"""
        try:
            loop = asyncio.get_event_loop()
            document = await loop.run_in_executor(
                None,
                lambda: self.service.documents().get(documentId=self.doc_id).execute()
            )
            return {
                "doc_id": self.doc_id,
                "title": document.get('title', ''),
                "revision_id": document.get('revisionId', ''),
            }
        except HttpError as e:
            await self.persist_log(f"Failed to get document info: {str(e)}", "ERROR")
            return None

    async def get_document_end_index(self) -> int:
        """获取文档末尾的索引位置"""
        try:
            loop = asyncio.get_event_loop()
            document = await loop.run_in_executor(
                None,
                lambda: self.service.documents().get(documentId=self.doc_id).execute()
            )
            
            # 获取文档内容的结束索引
            body = document.get('body', {})
            content = body.get('content', [])
            
            if content:
                last_element = content[-1]
                return last_element.get('endIndex', 1) - 1
            return 1
        except Exception as e:
            await self.persist_log(f"Failed to get document end index: {str(e)}", "WARNING")
            return 1

    async def write_content(self, content: str) -> bool:
        """写入内容到 Google Docs"""
        if not self.service:
            await self.persist_log("Google Docs service not initialized", "ERROR")
            return False

        if not content:
            await self.persist_log("No content to write", "WARNING")
            return True

        try:
            loop = asyncio.get_event_loop()
            requests = []

            if self.mode == "replace":
                # 替换模式：先删除所有内容，再插入新内容
                end_index = await self.get_document_end_index()
                if end_index > 1:
                    requests.append({
                        'deleteContentRange': {
                            'range': {
                                'startIndex': 1,
                                'endIndex': end_index,
                            }
                        }
                    })
                requests.append({
                    'insertText': {
                        'location': {
                            'index': 1,
                        },
                        'text': content,
                    }
                })
            elif self.mode == "prepend":
                # 在开头插入内容
                requests.append({
                    'insertText': {
                        'location': {
                            'index': 1,
                        },
                        'text': content + "\n\n",
                    }
                })
            else:  # append (default)
                # 在末尾追加内容
                end_index = await self.get_document_end_index()
                requests.append({
                    'insertText': {
                        'location': {
                            'index': end_index,
                        },
                        'text': "\n\n" + content if end_index > 1 else content,
                    }
                })

            # 执行批量更新
            if requests:
                await loop.run_in_executor(
                    None,
                    lambda: self.service.documents().batchUpdate(
                        documentId=self.doc_id,
                        body={'requests': requests}
                    ).execute()
                )
                await self.persist_log(
                    f"Successfully wrote {len(content)} characters to Google Doc (mode: {self.mode})",
                    "INFO"
                )
                return True

            return True

        except HttpError as e:
            error_msg = f"Google Docs API error: {str(e)}"
            if "permission" in str(e).lower():
                error_msg = f"Permission denied to write to document: {str(e)}"
            await self.persist_log(error_msg, "ERROR")
            await self.set_status(NodeStatus.FAILED, error_msg)
            return False
        except Exception as e:
            error_msg = f"Error writing to Google Docs: {str(e)}"
            await self.persist_log(error_msg, "ERROR")
            await self.persist_log(traceback.format_exc(), "DEBUG")
            await self.set_status(NodeStatus.FAILED, error_msg)
            return False

    async def execute(self) -> bool:
        """Execute node logic to write content to Google Docs"""
        start_time = time.time()
        try:
            await self.persist_log(
                f"Starting GDocNode execution: mode={self.mode}, doc_id={self.doc_id}",
                "INFO"
            )

            # Log original input if extracted from URL
            if self._original_doc_input and self._original_doc_input != self.doc_id:
                await self.persist_log(
                    f"Extracted document ID '{self.doc_id}' from input: {self._original_doc_input}",
                    "INFO"
                )

            # Validate mode
            if self.mode not in ["append", "replace", "prepend"]:
                error_msg = f"Invalid mode: {self.mode}. Must be 'append', 'replace', or 'prepend'"
                await self.persist_log(error_msg, "ERROR")
                await self.set_status(NodeStatus.FAILED, error_msg)
                return False

            # Validate doc_id
            if not self.doc_id:
                error_msg = "Document ID is required"
                await self.persist_log(error_msg, "ERROR")
                await self.set_status(NodeStatus.FAILED, error_msg)
                return False

            # Initialize service
            if not await self.initialize_service():
                return False

            await self.set_status(NodeStatus.RUNNING)

            # Get content from input signals or parameters
            content_to_write = self.content

            # Try to get content from input signals
            for edge_key, signal in self._input_signals.items():
                if signal and signal.payload:
                    # 检查是否是 content 输入
                    if 'content' in edge_key.lower():
                        if isinstance(signal.payload, str):
                            content_to_write = signal.payload
                        elif isinstance(signal.payload, dict):
                            # 尝试提取文本内容
                            content_to_write = signal.payload.get('content', 
                                signal.payload.get('text', 
                                    signal.payload.get('message', str(signal.payload))))
                        else:
                            content_to_write = str(signal.payload)
                        await self.persist_log(
                            f"Received content from signal: {len(content_to_write)} characters",
                            "INFO"
                        )
                        break

            if not content_to_write:
                error_msg = "No content to write"
                await self.persist_log(error_msg, "WARNING")
                # 发送空内容也算成功，只是警告
                doc_info = await self.get_document_info()
                if doc_info:
                    await self.send_signal(
                        STATUS_OUTPUT_HANDLE,
                        SignalType.STATUS,
                        payload={
                            "success": True,
                            "warning": "No content to write",
                            **doc_info,
                            "characters_written": 0,
                        }
                    )
                await self.set_status(NodeStatus.COMPLETED)
                return True

            # Write content
            if await self.write_content(content_to_write):
                doc_info = await self.get_document_info()
                status_payload = {
                    "success": True,
                    "characters_written": len(content_to_write),
                    "mode": self.mode,
                }
                if doc_info:
                    status_payload.update(doc_info)

                await self.send_signal(
                    STATUS_OUTPUT_HANDLE,
                    SignalType.STATUS,
                    payload=status_payload
                )
                await self.persist_log("Successfully completed Google Doc write operation", "INFO")
                await self.set_status(NodeStatus.COMPLETED)
                return True
            else:
                error_payload = {
                    "error": True,
                    "message": "Failed to write content to Google Docs",
                    "doc_id": self.doc_id,
                }
                await self.send_signal(
                    ERROR_HANDLE,
                    SignalType.ERROR,
                    payload=error_payload
                )
                await self.set_status(NodeStatus.FAILED, "Failed to write content")
                return False

        except asyncio.CancelledError:
            await self.set_status(NodeStatus.TERMINATED, "Task cancelled")
            return True
        except Exception as e:
            error_msg = f"GDocNode execution error: {str(e)}"
            await self.persist_log(error_msg, "ERROR")
            await self.persist_log(traceback.format_exc(), "DEBUG")
            
            error_payload = {
                "error": True,
                "message": str(e),
                "doc_id": self.doc_id,
            }
            await self.send_signal(
                ERROR_HANDLE,
                SignalType.ERROR,
                payload=error_payload
            )
            await self.set_status(NodeStatus.FAILED, error_msg)
            return False
        finally:
            # Clean up resources
            self.service = None
            execution_time = time.time() - start_time
            await self.persist_log(f"GDocNode execution completed in {execution_time:.2f}s", "INFO")
