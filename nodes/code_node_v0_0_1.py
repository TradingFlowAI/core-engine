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

from common.edge import Edge
from common.node_decorators import register_node_type
from common.signal_types import SignalType
from nodes.node_base import NodeBase, NodeStatus

from .code_node_interpreter import ALLOWED_MODULES, RestrictedInterpreter

# Define input/output handle names
INPUT_DATA_HANDLE = "input_data"  # Input data handler
PYTHON_CODE_HANDLE = "python_code"  # Python code handler
CODE_OUTPUT_HANDLE = "output_data"  # Code execution result output

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

        # Initialize input data for aggregation (required for auto_update_attr)
        self.input_data = {}

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
            "eval": "Dynamic code execution",
            "exec": "Dynamic code execution",
            "__import__": "Dynamic imports",
            "globals": "Access global variables",
            "locals": "Access local variables",
            # "getattr": "Dynamic attribute access",  # Removed: commonly used for object operations
            # "setattr": "Dynamic attribute setting",  # Removed: commonly used for object operations
            # "delattr": "Delete attributes",      # Removed: commonly used for object operations
            # "compile": "Code compilation",      # Removed: already handled in dangerous modules
            # "open": "File operations",         # Removed: commonly used in data processing
            # "read": "File reading",         # Removed: commonly used in data processing
            # "write": "File writing",        # Removed: commonly used in data processing
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
                            f"Dangerous function call: {node.func.id} - {dangerous_functions[node.func.id]}"
                        )
                        result["is_safe"] = False

                    # 检查属性访问 (例如 os.system)
                    elif isinstance(node.func, ast.Attribute) and hasattr(
                        node.func, "attr"
                    ):
                        if node.func.attr in ["system", "popen", "spawn", "call"]:
                            result["violations"].append(
                                f"Dangerous system call: {node.func.attr}"
                            )
                            result["is_safe"] = False

            # 使用正则表达式检查字符串中的危险模式
            string_literals = []
            for node in ast.walk(parsed_ast):
                if isinstance(node, ast.Str):
                    string_literals.append(node.s)

            # Check dangerous patterns in strings (only keep truly dangerous ones)
            dangerous_patterns = [
                (r"__import__\s*\(", "Dynamic import attempt"),
                (r"eval\s*\(", "Dynamic code execution attempt"),
                (r"exec\s*\(", "Dynamic code execution attempt"),
                (r"os\.system", "System command execution attempt"),
                (r"subprocess\.", "System command execution attempt"),
                # (r"open\s*\(", "File operation attempt"),  # Removed: commonly used in data processing
            ]

            for string in string_literals:
                for pattern, description in dangerous_patterns:
                    if re.search(pattern, string):
                        result["violations"].append(
                            f"Dangerous pattern in string: {description}"
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
                                    f"Attempt to modify built-in functions or global dictionary: {target.attr}"
                                )
                                result["is_safe"] = False

            # 设置风险等级
            if len(result["violations"]) > 5:
                result["risk_level"] = "High"
            elif len(result["violations"]) > 0:
                result["risk_level"] = "Medium"

        except Exception as e:
            await self.persist_log(f"Security analysis error: {e}", "WARNING")
            result["violations"].append(f"Code analysis error: {str(e)}")
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

        await self.persist_log("Starting to import common modules...", log_level="INFO")

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
            f"Module import completed: {len(imported_modules)} successful, {len(failed_modules)} failed",
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
            "Starting code security check...",
            log_level="INFO",
            log_metadata={"code_length": len(self.python_code)}
        )

        security_result = await self.analyze_security(self.python_code)

        if not security_result["is_safe"]:
            error_msg = "; ".join(security_result["violations"])
            await self.persist_log(
                f"Security check failed: {error_msg}",
                log_level="ERROR",
                log_metadata={
                    "security_violations": security_result["violations"],
                    "risk_level": security_result["risk_level"]
                }
            )

            await self.persist_log(f"Security violations detected: {error_msg}", "WARNING")
            await self.send_signal(STDERR_HANDLE, SignalType.TEXT, payload=error_msg)
            await self.set_status(NodeStatus.FAILED, "Code security check failed")
            return False

        # 记录安全审计日志
        await self.persist_log(
            f"Code security check passed (risk level: {security_result['risk_level']})",
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
            f"Code execution environment ready, starting code execution (timeout: {self.timeout}s)",
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

            # 统一通过 input handle 获取可能的动态输入
            python_code_value = self.get_input_handle_data(PYTHON_CODE_HANDLE)
            if python_code_value is not None:
                self.python_code = python_code_value

            # 安全检查
            if not await self._validate_security():
                return False

            # 估算初始Gas
            estimated_gas = await self.estimate_gas(self.python_code)
            await self.persist_log(
                f"Code complexity analysis completed, estimated gas consumption: {estimated_gas}",
                log_level="INFO",
                log_metadata={
                    "estimated_gas": estimated_gas,
                    "max_gas": self.max_gas,
                    "code_lines": len(self.python_code.split('\n'))
                }
            )

            # 使用统一的句柄读取逻辑，支持聚合句柄返回完整字典
            input_data_dict = self.get_input_handle_data(INPUT_DATA_HANDLE) or {}
            if not isinstance(input_data_dict, dict):
                input_data_dict = {}

            await self.persist_log(
                f"Using auto-aggregated input data, collected {len(input_data_dict)} input variables",
                log_level="INFO",
                log_metadata={
                    "input_variables": list(input_data_dict.keys()),
                    "input_count": len(input_data_dict)
                }
            )

            # 准备执行环境
            local_vars = await self._setup_execution_environment()
            local_vars.update(input_data_dict)

            # 添加统一的 input_data 变量，方便代码中使用 input_data.get() 方式访问
            local_vars['input_data'] = input_data_dict

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
            self.loop = asyncio.get_running_loop()

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
                    "Code execution completed successfully",
                    log_level="INFO",
                    log_metadata={
                        "execution_time": time.time() - start_time,
                        "gas_used": self.gas_used
                    }
                )
            except asyncio.TimeoutError:
                error_msg = f"Code execution timeout (exceeded {self.timeout} seconds)"
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
                stderr_capture.write(f"Execution timeout: code runtime exceeded {self.timeout} seconds\n")
                # 即使超时，我们也不能立即终止线程，因为这可能导致资源泄漏
                # 我们只能等待线程自然结束
                # 注意：如果代码中有无限循环，这里不会终止它，但由于线程是守护线程，当主线程结束时它会被终止
            except Exception as e:
                # 处理其他异常（如从线程传递的异常）
                error_msg = f"Code execution exception: {str(e)}"

                # 根据异常类型记录不同的日志
                if "Gas limit exceeded" in str(e):
                    await self.persist_log(
                        f"Code execution terminated: Gas usage exceeded limit ({self.gas_used}/{self.max_gas})",
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
                        f"Code execution terminated: Potential infinite loop detected",
                        log_level="ERROR",
                        log_metadata={
                            "exception_type": "infinite_loop_detected",
                            "gas_used": self.gas_used
                        }
                    )
                    stderr_capture.write(f"Execution terminated: {str(e)}\n")
                elif "Memory usage exceeded" in str(e):
                    await self.persist_log(
                        f"Code execution terminated: Memory usage exceeded limit ({self.max_memory_mb}MB)",
                        log_level="ERROR",
                        log_metadata={
                            "max_memory_mb": self.max_memory_mb,
                            "exception_type": "memory_limit_exceeded"
                        }
                    )
                    stderr_capture.write(f"Execution terminated: {str(e)}\n")
                else:
                    await self.persist_log(
                        f"Code execution exception: {str(e)}",
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
                
                # 根据执行时间额外增加Gas
                time_gas = int(self.execution_time * 10)  # 每秒10个Gas
                self.gas_used += time_gas

                # 获取输出
                stdout_output = stdout_capture.getvalue()
                stderr_output = stderr_capture.getvalue()
                debug_output = debug_capture.getvalue()  # 获取调试输出
                
                # 🔥 将 print 输出按行拆分，每行作为单独的 INFO 日志持久化
                if stdout_output:
                    stdout_lines = stdout_output.strip().split('\n')
                    for line in stdout_lines:
                        if line.strip():  # 跳过空行
                            await self.persist_log(
                                f"[print] {line}",
                                log_level="INFO",
                                log_metadata={
                                    "source": "user_print",
                                    "raw_output": line
                                }
                            )
                
                # stderr 作为 WARNING 日志（用户代码的警告/错误输出）
                if stderr_output:
                    stderr_lines = stderr_output.strip().split('\n')
                    for line in stderr_lines:
                        if line.strip():
                            await self.persist_log(
                                f"[stderr] {line}",
                                log_level="WARNING",
                                log_metadata={
                                    "source": "user_stderr",
                                    "raw_output": line
                                }
                            )
                
                # debug 输出保持 DEBUG 级别
                if debug_output:
                    await self.persist_log(f"Debug output: {debug_output}", "DEBUG")

            # 如果有 stderr 输出且执行失败，记录错误
            if stderr_output and not success:
                error_msg = f"Code execution failed: {stderr_output}"
                await self.persist_log(error_msg, "ERROR")
                await self.set_status(NodeStatus.FAILED, error_msg)
                return False

            # 获取代码执行结果
            output_data = local_vars.get("output_data")

            if output_data is not None:
                # 在输出中添加Gas和执行统计信息
                if isinstance(output_data, dict):
                    output_data["_execution_stats"] = {
                        "gas_used": self.gas_used,
                        "execution_time": self.execution_time,
                        "max_gas": self.max_gas,
                        "max_memory_mb": self.max_memory_mb,
                    }

                await self.persist_log(
                    f"Code execution completed (time: {self.execution_time:.3f}s, gas: {self.gas_used})",
                    log_level="INFO",
                    log_metadata={
                        "output_type": type(output_data).__name__,
                        "gas_used": self.gas_used,
                        "execution_time": self.execution_time,
                    }
                )

                # 发送主输出
                await self.send_signal(
                    CODE_OUTPUT_HANDLE, SignalType.CODE_OUTPUT, payload=output_data
                )

                # 发送自定义输出
                for handle in self.output_handles:
                    if handle in local_vars:
                        await self.send_signal(
                            handle, SignalType.DATASET, payload=local_vars[handle]
                        )

                await self.set_status(NodeStatus.COMPLETED)
                return True
            else:
                error_msg = "Code execution did not produce output_data"
                await self.persist_log(
                    "Code execution failed: did not generate output_data variable",
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


    def _register_input_handles(self) -> None:
        """注册输入句柄"""
        self.register_input_handle(
            name=INPUT_DATA_HANDLE,
            data_type=dict,
            description="Input data for the code execution",
            example={"input1": "value1", "input2": "value2"},
            auto_update_attr="input_data",
            is_aggregate=True,  # 支持多信号聚合为字典
        )
        self.register_input_handle(
            name=PYTHON_CODE_HANDLE,
            data_type=str,
            description="Python code to execute",
            example="# Your Python code here",
            auto_update_attr="python_code",
        )

    def _register_output_handles(self) -> None:
        """Register output handles"""
        # Single output handle for code execution result
        self.register_output_handle(
            name=CODE_OUTPUT_HANDLE,
            data_type=dict,
            description="Output Data - Code execution result (stdout/stderr/debug info available in node logs)",
            example={"result": "value", "processed_data": []},
        )
