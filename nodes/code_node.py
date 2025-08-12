import ast
import asyncio
import dis
import io
import linecache
import os
import re
import sys
import threading
import time
import traceback
from contextlib import redirect_stderr, redirect_stdout
from typing import Any, Dict, List

import pandas as pd
import psutil

from tradingflow.station.common.edge import Edge
from tradingflow.station.common.node_decorators import register_node_type
from tradingflow.station.common.signal_types import SignalType
from tradingflow.station.nodes.node_base import NodeBase, NodeStatus

from .code_node_interpreter import ALLOWED_MODULES, RestrictedInterpreter

# Define input/output handle names
INPUT_DATA_HANDLE = "input_data"  # Input data handler
PYTHON_CODE_HANDLE = "python_code"  # Python code handler
CODE_OUTPUT_HANDLE = "output_data"  # Code execution result output
STDOUT_HANDLE = "stdout_output"  # Standard output
STDERR_HANDLE = "stderr_output"  # Standard error
DEBUG_HANDLE = "debug_output"  # Debug output

# Define default maximum Gas
DEFAULT_MAX_GAS = 1000000000 # 10_0000_0000

@register_node_type(
    "code_node",
    default_params={
        "python_code": "# Write Python code here\n# You can use input_data_0, input_data_1 etc. to access input data\n# Use output_data variable to store output results\n\noutput_data = {'result': 'Hello from Code Node!'}",
        "input_handles": [],  # Can customize multiple input handlers
        "output_handles": [],  # Can customize multiple output handlers
        "timeout": 30,  # Code execution timeout (seconds)
        "max_gas": DEFAULT_MAX_GAS,  # Maximum available gas (increased to support complex module imports)
        "base_gas": 100,  # Base Gas consumption
        "max_recursion": 1000,  # Maximum recursion depth
        "max_memory_mb": 500,  # Maximum memory usage limit (MB)
    },
)
class CodeNode(NodeBase):
    """
    Code execution node - Used to execute Python code

    Input parameters:
    - python_code: Python code to execute
    - input_handles: Custom input handler list
    - output_handles: Custom output handler list
    - timeout: Code execution timeout (seconds)

    Input signals:
    - Receive input data according to handlers defined in input_handles

    Output signals:
    - CODE_OUTPUT_HANDLE: Code execution result
    - STDOUT_HANDLE: Standard output
    - STDERR_HANDLE: Standard error
    - As well as custom output handlers defined by output_handles
    """

    def __init__(
        self,
        flow_id: str,
        component_id: int,
        cycle: int,
        node_id: str,
        name: str,
        python_code: str = "",
        input_handles: List[str] = None,
        output_handles: List[str] = None,
        timeout: int = 30,
        max_gas: int = 20000,
        base_gas: int = 100,
        max_recursion: int = 1000,
        max_memory_mb: int = 500,
        input_edges: List[Edge] = None,
        output_edges: List[Edge] = None,
        state_store=None,
        **kwargs,
    ):
        """
        Initialize code execution node

        Args:
            flow_id: Flow ID
            component_id: Component ID
            cycle: Node execution cycle
            node_id: Node unique identifier
            name: Node name
            python_code: Python code to execute
            input_handles: Custom input handler list
            output_handles: Custom output handler list
            timeout: Code execution timeout (seconds)
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
        self.python_code = python_code
        self.input_handles = input_handles or []
        self.output_handles = output_handles or []
        self.timeout = timeout
        self.max_gas = max_gas
        self.base_gas = base_gas
        self.max_recursion = max_recursion
        self.max_memory_mb = max_memory_mb

        # Initialize execution statistics
        self.gas_used = 0
        self.execution_time = 0
        self.memory_peak = 0
        self.loop_count = 0
        self.recursion_depth = 0

        # Save main thread's event loop for communication in worker threads
        self.loop = asyncio.get_event_loop()

    async def analyze_security(self, code: str) -> Dict[str, Any]:
        """
        Analyze code security, detect potential malicious code

        Returns:
            Dict containing security analysis results:
            - is_safe: Boolean indicating if code is safe to execute
            - violations: List of security violations found
            - risk_level: Low, Medium, High
        """
        result = {"is_safe": True, "violations": [], "risk_level": "Low"}

        # Define dangerous modules and functions - remove common data collection libraries
        dangerous_modules = {
            "os": "System operations",
            "subprocess": "Execute system commands",
            "shutil": "File operations",
            "socket": "Low-level network access",
            # "requests": "Network requests",  # Removed: allowed for data collection
            # "urllib": "Network access",    # Removed: allowed for data collection
            "pathlib": "File system access",
            "pickle": "Unsafe serialization",
            "multiprocessing": "Process operations",
            "threading": "Thread operations",
            "sys": "System access",
            "ctypes": "Low-level system calls",
            "importlib": "Dynamic imports",
            "builtins": "Built-in function access",
            # Add truly dangerous modules
            "eval": "Dynamic code execution",
            "exec": "Dynamic code execution",
            "__import__": "Dynamic imports",
            "compile": "Code compilation",
        }

        # Define whitelist of allowed modules for import
        allowed_modules = set(ALLOWED_MODULES.keys())

        dangerous_functions = {
            "eval": "动态代码执行",
            "exec": "动态代码执行",
            "__import__": "动态导入",
            "globals": "访问全局变量",
            "locals": "访问局部变量",
            # "getattr": "动态属性访问",  # 移除：常用于对象操作
            # "setattr": "动态属性设置",  # 移除：常用于对象操作
            # "delattr": "删除属性",      # 移除：常用于对象操作
            # "compile": "代码编译",      # 移除：已在危险模块中处理
            # "open": "文件操作",         # 移除：在数据处理中常用
            # "read": "文件读取",         # 移除：在数据处理中常用
            # "write": "文件写入",        # 移除：在数据处理中常用
        }

        try:
            # 解析代码为AST
            parsed_ast = ast.parse(code)

            # 检查导入语句
            for node in ast.walk(parsed_ast):
                # 检查导入模块
                if isinstance(node, ast.Import):
                    # 处理 import xxx 语句
                    for name in node.names:
                        module_name = name.name.split(".")[0]
                        # 检查危险模块
                        if module_name in dangerous_modules:
                            result["violations"].append(
                                f"危险模块导入: {module_name} - {dangerous_modules[module_name]}"
                            )
                            result["is_safe"] = False
                        # 检查模块白名单
                        elif module_name not in allowed_modules:
                            result["violations"].append(
                                f"非白名单模块导入: {module_name} - 仅允许导入白名单中的模块"
                            )
                            result["is_safe"] = False
                elif isinstance(node, ast.ImportFrom):
                    # 处理 from xxx import yyy 语句
                    if node.module:
                        module_name = node.module.split(".")[0]
                        # 检查危险模块
                        if module_name in dangerous_modules:
                            result["violations"].append(
                                f"危险模块导入: {module_name} - {dangerous_modules[module_name]}"
                            )
                            result["is_safe"] = False
                        # 检查模块白名单
                        elif module_name not in allowed_modules:
                            result["violations"].append(
                                f"非白名单模块导入: {module_name} - 仅允许导入白名单中的模块"
                            )
                            result["is_safe"] = False

                # 检查函数调用
                elif isinstance(node, ast.Call):
                    if (
                        isinstance(node.func, ast.Name)
                        and node.func.id in dangerous_functions
                    ):
                        result["violations"].append(
                            f"危险函数调用: {node.func.id} - {dangerous_functions[node.func.id]}"
                        )
                        result["is_safe"] = False

                    # 检查属性访问 (例如 os.system)
                    elif isinstance(node.func, ast.Attribute) and hasattr(
                        node.func, "attr"
                    ):
                        if node.func.attr in ["system", "popen", "spawn", "call"]:
                            result["violations"].append(
                                f"危险系统调用: {node.func.attr}"
                            )
                            result["is_safe"] = False

            # 使用正则表达式检查字符串中的危险模式
            string_literals = []
            for node in ast.walk(parsed_ast):
                if isinstance(node, ast.Str):
                    string_literals.append(node.s)

            # 检查字符串中的危险模式（只保留真正危险的）
            dangerous_patterns = [
                (r"__import__\s*\(", "动态导入尝试"),
                (r"eval\s*\(", "动态代码执行尝试"),
                (r"exec\s*\(", "动态代码执行尝试"),
                (r"os\.system", "系统命令执行尝试"),
                (r"subprocess\.", "系统命令执行尝试"),
                # (r"open\s*\(", "文件操作尝试"),  # 移除：在数据处理中常用
            ]

            for string in string_literals:
                for pattern, description in dangerous_patterns:
                    if re.search(pattern, string):
                        result["violations"].append(
                            f"字符串中的危险模式: {description}"
                        )
                        result["is_safe"] = False

            # 检查代码中的其他危险模式
            # 例如，检查是否尝试修改内置函数
            for node in ast.walk(parsed_ast):
                if isinstance(node, ast.Assign):
                    for target in node.targets:
                        if isinstance(target, ast.Attribute) and hasattr(
                            target, "attr"
                        ):
                            if target.attr in ["__builtins__", "__dict__"]:
                                result["violations"].append(
                                    f"尝试修改内置函数或全局字典: {target.attr}"
                                )
                                result["is_safe"] = False

            # 设置风险等级
            if len(result["violations"]) > 5:
                result["risk_level"] = "High"
            elif len(result["violations"]) > 0:
                result["risk_level"] = "Medium"

        except Exception as e:
            await self.persist_log(f"Security analysis error: {e}", "WARNING")
            result["violations"].append(f"代码分析错误: {str(e)}")
            result["is_safe"] = False
            result["risk_level"] = "Medium"

        return result

    async def estimate_gas(self, code: str) -> int:
        """估算代码执行的Gas消耗

        基于代码的复杂度和操作类型估算Gas消耗，类似以太坊的Gas计算机制
        """
        try:
            # 解析代码为AST
            parsed_ast = ast.parse(code)

            # 计算基本复杂度
            node_count = sum(1 for _ in ast.walk(parsed_ast))

            # 编译代码并获取字节码
            compiled_code = compile(parsed_ast, "<string>", "exec")
            bytecode = dis.Bytecode(compiled_code)
            instruction_count = len(list(bytecode))

            # 计算循环和条件语句的数量（这些通常更耗资源）
            loops = sum(
                1
                for node in ast.walk(parsed_ast)
                if isinstance(node, (ast.For, ast.While))
            )
            conditionals = sum(
                1
                for node in ast.walk(parsed_ast)
                if isinstance(node, (ast.If, ast.IfExp))
            )

            # 检查是否使用了pandas等高资源消耗库的操作
            pandas_ops = 0
            for node in ast.walk(parsed_ast):
                if (
                    isinstance(node, ast.Call)
                    and hasattr(node.func, "value")
                    and hasattr(node.func.value, "id")
                ):
                    if node.func.value.id in ["pd", "pandas"]:
                        pandas_ops += 5  # pandas操作消耗更多Gas

            # 计算总Gas
            gas = (
                self.base_gas
                + (node_count * 2)
                + (instruction_count * 3)
                + (loops * 10)
                + (conditionals * 5)
                + (pandas_ops * 20)
            )

            return gas

        except Exception as e:
            await self.persist_log(f"Gas estimation error: {e}, using default gas value", "WARNING")
            return self.base_gas * 5  # 如果估算失败，使用默认值

    def gas_tracking_callback(self, frame, event, arg) -> None:
        """跟踪代码执行并计算Gas消耗，同时检测无限循环和内存使用"""
        if event == "line":
            self.gas_used += 1  # 每执行一行代码增加1个Gas

            # 获取当前行的代码内容
            # try:
            #     current_line = linecache.getline(
            #         frame.f_code.co_filename, frame.f_lineno
            #     ).strip()
            #     self.logger.info(f"执行行: {frame.f_lineno}, 内容: {current_line}")
            # except Exception:
            #     # 如果无法获取行内容，忽略错误
            #     pass

            # 输出资源使用情况
            # self.logger.info(f"资源使用: Gas={self.gas_used}/{self.max_gas}")

            # 检查是否超出Gas限制
            if self.gas_used > self.max_gas:
                # 注意：在同步回调中不能使用 await，所以我们只能抛出异常
                # 引发异常以终止执行
                raise Exception(f"Gas limit exceeded: {self.gas_used}/{self.max_gas}")

            # 无限循环检测
            # 使用(文件名, 行号)作为循环的唯一标识
            location = (frame.f_code.co_filename, frame.f_lineno)

            # # 如果是循环内的代码，增加计数
            # if location in self.loop_detection:
            #     self.loop_detection[location] += 1

            #     # 如果同一行执行次数过多，可能是无限循环
            #     if self.loop_detection[location] > self.max_iterations:
            #         raise Exception(
            #             f"Potential infinite loop detected at line {frame.f_lineno}. "
            #             f"Executed {self.max_iterations} iterations."
            #         )
            # else:
            #     # 新位置，初始化计数
            #     self.loop_detection[location] = 1

            # 每 10000 行代码检查一次内存使用情况
            if self.gas_used % 10000 == 0:
                # 获取当前进程的内存使用情况
                try:
                    process = psutil.Process(os.getpid())
                    memory_info = process.memory_info()
                    memory_mb = memory_info.rss / (1024 * 1024)  # 转换为MB

                    # self.logger.info(
                    #     f"内存使用: {memory_mb:.2f}MB/{self.max_memory_mb}MB"
                    # )

                    # 如果内存使用超过限制，终止执行
                    if memory_mb > self.max_memory_mb:
                        raise Exception(
                            f"Memory usage exceeded: {memory_mb:.2f}MB/{self.max_memory_mb}MB"
                        )
                except Exception as e:
                    # 如果psutil不可用或出错，记录错误但继续执行
                    # 注意：在同步回调中不能使用 await，所以我们只能忽略这个错误
                    pass

        return self.gas_tracking_callback

    async def _setup_execution_environment(self):
        """设置代码执行环境，包括预导入常用模块"""
        local_vars = {
            "__name__": "__console__",
            "__doc__": None,
            "pd": pd,
            "output_data": None,
        }

        await self.persist_log("开始导入常用模块...", log_level="INFO")

        imported_modules = []
        failed_modules = []

        # 预导入requests
        try:
            import requests
            local_vars["requests"] = requests
            imported_modules.append("requests")
        except ImportError as e:
            failed_modules.append(f"requests: {str(e)}")
            await self.persist_log("requests module not available - install with: pip install requests", "WARNING")

        # 预导入BeautifulSoup和bs4
        try:
            from bs4 import BeautifulSoup
            import bs4
            local_vars["BeautifulSoup"] = BeautifulSoup
            local_vars["bs4"] = bs4
            imported_modules.extend(["BeautifulSoup", "bs4"])
        except ImportError as e:
            failed_modules.append(f"bs4/BeautifulSoup: {str(e)}")
            await self.persist_log("BeautifulSoup/bs4 module not available - install with: pip install beautifulsoup4", "WARNING")

        # 预导入urllib
        try:
            import urllib
            local_vars["urllib"] = urllib
            imported_modules.append("urllib")
        except ImportError as e:
            failed_modules.append(f"urllib: {str(e)}")
            await self.persist_log("urllib module not available", "WARNING")

        await self.persist_log(
            f"模块导入完成: 成功 {len(imported_modules)} 个，失败 {len(failed_modules)} 个",
            log_level="INFO" if len(failed_modules) == 0 else "WARNING",
            log_metadata={
                "imported_modules": imported_modules,
                "failed_modules": failed_modules,
                "success_count": len(imported_modules),
                "failure_count": len(failed_modules)
            }
        )

        return local_vars

    async def _validate_security(self):
        """执行安全检查并记录结果"""
        await self.persist_log(
            "开始代码安全检查...",
            log_level="INFO",
            log_metadata={"code_length": len(self.python_code)}
        )

        security_result = await self.analyze_security(self.python_code)

        if not security_result["is_safe"]:
            error_msg = "; ".join(security_result["violations"])
            await self.persist_log(
                f"安全检查失败: {error_msg}",
                log_level="ERROR",
                log_metadata={
                    "security_violations": security_result["violations"],
                    "risk_level": security_result["risk_level"]
                }
            )

            await self.persist_log(f"Security violations detected: {error_msg}", "WARNING")
            await self.send_signal(STDERR_HANDLE, SignalType.TEXT, payload=error_msg)
            await self.set_status(NodeStatus.FAILED, "代码安全检查失败")
            return False

        # 记录安全审计日志
        await self.persist_log(
            f"代码安全检查通过 (风险等级: {security_result['risk_level']})",
            log_level="INFO",
            log_metadata={
                "risk_level": security_result["risk_level"],
                "modules_checked": len(security_result.get("modules_checked", [])),
                "code_length": len(self.python_code)
            }
        )

        await self.persist_log(f"Security check passed for code execution in node {self.node_id}", "INFO")
        await self.persist_log(f"Security analysis result: {security_result}", "DEBUG")
        return True

    async def _prepare_execution_context(self, local_vars):
        """准备代码执行上下文"""
        # 捕获标准输出、标准错误和调试输出
        stdout_capture = io.StringIO()
        stderr_capture = io.StringIO()
        debug_capture = io.StringIO()

        # 创建安全解释器
        interpreter = RestrictedInterpreter(
            locals=local_vars, allowed_modules=ALLOWED_MODULES
        )

        await self.persist_log(
            f"代码执行环境准备完成，开始执行代码 (超时: {self.timeout}秒)",
            log_level="INFO",
            log_metadata={
                "timeout": self.timeout,
                "max_gas": self.max_gas,
                "max_memory_mb": self.max_memory_mb,
                "available_variables": list(local_vars.keys())
            }
        )

        return stdout_capture, stderr_capture, debug_capture, interpreter

    async def execute(self) -> bool:
        """执行节点逻辑，运行 Python 代码"""
        try:
            await self.persist_log(f"Executing CodeNode {self.node_id}", "INFO")
            await self.set_status(NodeStatus.RUNNING)

            # 安全检查
            if not await self._validate_security():
                return False

            # 估算初始Gas
            estimated_gas = self.estimate_gas(self.python_code)
            await self.persist_log(
                f"代码复杂度分析完成，估算Gas消耗: {estimated_gas}",
                log_level="INFO",
                log_metadata={
                    "estimated_gas": estimated_gas,
                    "max_gas": self.max_gas,
                    "code_lines": len(self.python_code.split('\n'))
                }
            )

            # 收集所有输入数据
            input_data_dict = await self._parse_input_data_dict()
            await self.persist_log(
                f"输入数据处理完成，收集到 {len(input_data_dict)} 个输入变量",
                log_level="INFO",
                log_metadata={
                    "input_variables": list(input_data_dict.keys()),
                    "input_count": len(input_data_dict)
                }
            )

            # 准备执行环境
            local_vars = await self._setup_execution_environment()
            local_vars.update(input_data_dict)

            # debug信息
            await self.persist_log(
                f"Local variables prepared for execution: {local_vars.keys()}", "DEBUG"
            )

            # 准备执行上下文
            stdout_capture, stderr_capture, debug_capture, interpreter = \
                await self._prepare_execution_context(local_vars)

            # 执行代码，带超时和Gas计算
            success = True
            start_time = time.time()
            self.gas_used = 0
            self.loop_detection = {}  # 重置循环检测

            # 初始化执行状态
            success = False

            # 创建一个Future对象用于线程间通信
            # 注意：不要在工作线程中使用主线程的事件循环
            await self.persist_log("Creating execution task Future", "INFO")
            execution_task = asyncio.Future()

            # 定义一个在当前线程执行代码的函数
            def run_code_with_trace():
                try:
                    with redirect_stdout(stdout_capture), redirect_stderr(
                        stderr_capture
                    ):
                        # 设置跟踪回调来监控Gas使用
                        sys.settrace(self.gas_tracking_callback)

                        try:
                            # 直接在当前线程执行代码
                            # 传递调试捕获器给解释器
                            interpreter.runcode(
                                compile(self.python_code, "<string>", "exec"),
                                debug_capture=debug_capture,
                            )

                            # 标记任务完成
                            # 使用线程安全的方式设置结果
                            # 使用保存的主线程事件循环
                            self.loop.call_soon_threadsafe(
                                lambda: (
                                    execution_task.set_result(True)
                                    if not execution_task.done()
                                    else None
                                )
                            )
                        except Exception as e:
                            # 捕获并记录异常
                            error_msg = f"Error executing code: {str(e)}"
                            # Note: Cannot use await persist_log in thread context, using stderr capture
                            stderr_capture.write(f"{error_msg}\n")
                            stderr_capture.write(traceback.format_exc())

                            # 设置异常
                            # 使用线程安全的方式设置异常
                            # 使用主线程的事件循环
                            # 在主线程中获取事件循环并存储为实例变量
                            # 捕获异常变量 e 到 lambda 函数中
                            error = e  # 在 lambda 外部保存异常引用
                            self.loop.call_soon_threadsafe(
                                lambda error=error: (
                                    execution_task.set_exception(error)
                                    if not execution_task.done()
                                    else None
                                )
                            )
                        finally:
                            # 确保无论如何都停止跟踪
                            sys.settrace(None)
                except Exception as e:
                    # 处理外部异常（如redirect_stdout失败）
                    sys.settrace(None)  # 确保停止跟踪
                    error_msg = f"Unexpected error in code execution thread: {str(e)}"
                    # Note: Cannot use await persist_log in thread context, using stderr capture

                    # 设置异常
                    # 使用线程安全的方式设置异常
                    # 使用保存的主线程事件循环
                    # 捕获异常变量 e 到 lambda 函数中
                    error = e  # 在 lambda 外部保存异常引用
                    self.loop.call_soon_threadsafe(
                        lambda error=error: (
                            execution_task.set_exception(error)
                            if not execution_task.done()
                            else None
                        )
                    )

            # 创建一个线程来执行代码
            code_thread = threading.Thread(
                target=run_code_with_trace, name="CodeExecutionThread"
            )
            code_thread.daemon = True  # 设置为守护线程
            code_thread.start()

            try:
                # 等待代码执行完成或超时
                await self.persist_log(f"Waiting for code execution to complete, timeout: {self.timeout} seconds", "INFO")
                await asyncio.wait_for(execution_task, timeout=self.timeout)
                # 如果成功完成，设置成功状态
                success = True
                await self.persist_log(
                    "代码执行成功完成",
                    log_level="INFO",
                    log_metadata={
                        "execution_time": time.time() - start_time,
                        "gas_used": self.gas_used
                    }
                )
            except asyncio.TimeoutError:
                error_msg = f"代码执行超时 (超过 {self.timeout} 秒)"
                await self.persist_log(
                    error_msg,
                    log_level="ERROR",
                    log_metadata={
                        "timeout_seconds": self.timeout,
                        "execution_time": time.time() - start_time,
                        "gas_used": self.gas_used
                    }
                )
                await self.persist_log(error_msg, "WARNING")
                stderr_capture.write(f"执行超时: 代码运行时间超过 {self.timeout} 秒\n")
                # 即使超时，我们也不能立即终止线程，因为这可能导致资源泄漏
                # 我们只能等待线程自然结束
                # 注意：如果代码中有无限循环，这里不会终止它，但由于线程是守护线程，当主线程结束时它会被终止
            except Exception as e:
                # 处理其他异常（如从线程传递的异常）
                error_msg = f"代码执行异常: {str(e)}"

                # 根据异常类型记录不同的日志
                if "Gas limit exceeded" in str(e):
                    await self.persist_log(
                        f"代码执行中止: Gas使用量超出限制 ({self.gas_used}/{self.max_gas})",
                        log_level="ERROR",
                        log_metadata={
                            "gas_used": self.gas_used,
                            "max_gas": self.max_gas,
                            "exception_type": "gas_limit_exceeded"
                        }
                    )
                    stderr_capture.write(f"Execution terminated: {str(e)}\n")
                elif "Potential infinite loop detected" in str(e):
                    await self.persist_log(
                        f"代码执行中止: 检测到潜在的无限循环",
                        log_level="ERROR",
                        log_metadata={
                            "exception_type": "infinite_loop_detected",
                            "gas_used": self.gas_used
                        }
                    )
                    stderr_capture.write(f"Execution terminated: {str(e)}\n")
                elif "Memory usage exceeded" in str(e):
                    await self.persist_log(
                        f"代码执行中止: 内存使用量超出限制 ({self.max_memory_mb}MB)",
                        log_level="ERROR",
                        log_metadata={
                            "max_memory_mb": self.max_memory_mb,
                            "exception_type": "memory_limit_exceeded"
                        }
                    )
                    stderr_capture.write(f"Execution terminated: {str(e)}\n")
                else:
                    await self.persist_log(
                        f"代码执行异常: {str(e)}",
                        log_level="ERROR",
                        log_metadata={
                            "exception_type": "execution_error",
                            "exception_message": str(e),
                            "gas_used": self.gas_used
                        }
                    )
                    # 避免重复输出异常信息（如果已经在线程中输出过）
                    if "Error executing code:" not in stderr_capture.getvalue():
                        stderr_capture.write(f"Error executing code: {str(e)}\n")
                        stderr_capture.write(traceback.format_exc())

                await self.persist_log(error_msg, "ERROR")
            finally:
                # 确保无论如何都停止跟踪
                sys.settrace(None)

                # 计算执行时间和最终Gas消耗
                self.execution_time = time.time() - start_time
                await self.persist_log(
                    f"Code execution total time: {self.execution_time:.4f}s, total Gas consumed: {self.gas_used}", "INFO"
                )

                # 根据执行时间额外增加Gas
                await self.persist_log("Calculating final gas usage based on execution time", "INFO")
                time_gas = int(self.execution_time * 10)  # 每秒10个Gas
                self.gas_used += time_gas
                await self.persist_log(
                    f"Final resource usage: Gas={self.gas_used}/{self.max_gas}", "INFO"
                )

                # 获取输出
                await self.persist_log("Getting captured stdout, stderr and debug output", "INFO")
                stdout_output = stdout_capture.getvalue()
                stderr_output = stderr_capture.getvalue()
                debug_output = debug_capture.getvalue()  # 获取调试输出
                if stdout_output:
                    await self.persist_log(f"Captured stdout content: {stdout_output}", "INFO")
                if stderr_output:
                    await self.persist_log(f"Captured stderr content: {stderr_output}", "WARNING")
                if debug_output:
                    await self.persist_log(f"Captured debug content: {debug_output}", "DEBUG")

                await self.persist_log(
                    f"Captured stdout length: {len(stdout_output)}, stderr length: {len(stderr_output)}, debug length: {len(debug_output)}", "INFO"
                )

            # 添加详细的执行统计信息
            gas_info = (
                f"\n--- Execution Stats ---\n"
                f"Gas used: {self.gas_used}/{self.max_gas}\n"
                f"Execution time: {self.execution_time:.3f}s\n"
                f"Memory limit: {self.max_memory_mb}MB\n"
            )
            debug_output += gas_info

            # 发送标准输出、标准错误和调试输出
            await self.send_signal(
                STDOUT_HANDLE, SignalType.TEXT, payload=stdout_output
            )
            if stderr_output:
                await self.send_signal(
                    STDERR_HANDLE, SignalType.TEXT, payload=stderr_output
                )
                if not success:
                    error_msg = f"Code execution failed: {stderr_output}"
                    await self.persist_log(error_msg, "ERROR")
                    await self.set_status(NodeStatus.FAILED, error_msg)
                    return False

            # 发送调试输出（如果有）
            if debug_output:
                await self.send_signal(
                    DEBUG_HANDLE, SignalType.TEXT, payload=debug_output
                )
                await self.persist_log(f"Sent debug output: {len(debug_output)} characters", "DEBUG")

            # 获取代码执行结果
            await self.persist_log("Getting execution result from local variables", "INFO")
            output_data = local_vars.get("output_data")
            await self.persist_log(f"Output data present: {output_data is not None}", "INFO")

            if output_data is not None:
                # 在输出中添加Gas和Credits信息
                await self.persist_log("Adding execution stats to output data", "INFO")
                if isinstance(output_data, dict):
                    output_data["_execution_stats"] = {
                        "gas_used": self.gas_used,
                        "execution_time": self.execution_time,
                        "max_gas": self.max_gas,
                        "max_memory_mb": self.max_memory_mb,
                    }

                await self.persist_log(
                    f"代码执行成功，产生输出数据 (类型: {type(output_data).__name__})",
                    log_level="INFO",
                    log_metadata={
                        "output_type": type(output_data).__name__,
                        "output_size": len(str(output_data)) if output_data else 0,
                        "final_gas_used": self.gas_used,
                        "execution_time": self.execution_time,
                        "custom_outputs": len(self.output_handles)
                    }
                )

                # 发送主输出
                await self.send_signal(
                    CODE_OUTPUT_HANDLE, SignalType.CODE_OUTPUT, payload=output_data
                )

                # 发送自定义输出
                custom_outputs_sent = 0
                for handle in self.output_handles:
                    if handle in local_vars:
                        await self.send_signal(
                            handle, SignalType.DATASET, payload=local_vars[handle]
                        )
                        custom_outputs_sent += 1

                if custom_outputs_sent > 0:
                    await self.persist_log(
                        f"发送了 {custom_outputs_sent} 个自定义输出信号",
                        log_level="INFO",
                        log_metadata={
                            "custom_outputs_sent": custom_outputs_sent,
                            "output_handles": self.output_handles
                        }
                    )

                await self.persist_log("Successfully executed code and sent output signals", "INFO")
                await self.set_status(NodeStatus.COMPLETED)
                return True
            else:
                error_msg = "Code execution did not produce output_data"
                await self.persist_log(
                    "代码执行失败: 未产生output_data变量",
                    log_level="ERROR",
                    log_metadata={
                        "available_variables": [k for k in local_vars.keys() if not k.startswith('_')],
                        "execution_time": self.execution_time,
                        "gas_used": self.gas_used
                    }
                )
                await self.persist_log(error_msg, "WARNING")
                await self.set_status(NodeStatus.FAILED, error_msg)
                return False

        except asyncio.CancelledError:
            # 任务被取消
            await self.set_status(NodeStatus.TERMINATED, "Task cancelled")
            return True
        except Exception as e:
            error_msg = f"Error in CodeNode execution: {str(e)}"
            await self.persist_log(error_msg, "ERROR")
            await self.persist_log(traceback.format_exc(), "DEBUG")
            await self.set_status(NodeStatus.FAILED, error_msg)
            return False

    async def _parse_input_data_dict(self):
        input_data_dict = {}
        for edge_key, signal in self._input_signals.items():
            await self.persist_log(
                f"Processing input signal for edge {edge_key}: {signal}, type(signal)= {type(signal)}", "DEBUG"
            )

            if signal is not None:
                # 从 edge_key 中解析出 source_handle
                # edge_key 格式: "source_node:source_handle->target_handle"
                if "->" in edge_key and ":" in edge_key:
                    source_part = edge_key.split("->")[0]  # "source_node:source_handle"
                    source_handle = source_part.split(":", 1)[1]  # "source_handle"

                    # 获取信号的实际数据
                    data = signal.payload if hasattr(signal, "payload") else signal

                    # 使用 source_handle 作为 key 存储数据
                    input_data_dict[source_handle] = data

                    await self.persist_log(
                        f"Added input data for source_handle '{source_handle}': {data}", "DEBUG"
                    )

                    # 如果数据是 dataset 格式，尝试转换为 pandas DataFrame
                    if isinstance(data, dict) and "headers" in data and "data" in data:
                        try:
                            df = pd.DataFrame(data["data"], columns=data["headers"])
                            input_data_dict[f"df_{source_handle}"] = df
                            await self.persist_log(
                                f"Created DataFrame for source_handle '{source_handle}'", "DEBUG"
                            )
                        except Exception as e:
                            await self.persist_log(
                                f"Failed to convert data to DataFrame for source_handle '{source_handle}': {e}", "WARNING"
                            )
                else:
                    await self.persist_log(
                        f"Invalid edge_key format: {edge_key}, expected 'source_node:source_handle->target_handle'", "WARNING"
                    )
            else:
                await self.persist_log(f"No signal received for edge: {edge_key}", "DEBUG")
        return input_data_dict

    def _register_input_handles(self) -> None:
        """注册输入句柄"""
        self.register_input_handle(
            name=INPUT_DATA_HANDLE,
            data_type=dict,
            description="Input data for the code execution",
            example={"input1": "value1", "input2": "value2"},
        )
        self.register_input_handle(
            name=PYTHON_CODE_HANDLE,
            data_type=str,
            description="Python code to execute",
            example="# Your Python code here",
            auto_update_attr="python_code",
        )
