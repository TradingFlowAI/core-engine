"""
Community Node Executor
Executes community-defined nodes (Python code or HTTP calls)
"""

import sys
import io
import json
import requests
from typing import Dict, Any, Optional
from contextlib import redirect_stdout, redirect_stderr
import time


class CommunityNodeExecutor:
    """
    社区节点执行器
    
    支持两种执行方式：
    1. Python 代码执行（受限环境）
    2. HTTP API 调用
    """
    
    # Python 代码执行的安全限制
    ALLOWED_BUILTINS = {
        'abs', 'all', 'any', 'bool', 'dict', 'enumerate', 'filter',
        'float', 'int', 'len', 'list', 'map', 'max', 'min', 'print',
        'range', 'round', 'set', 'sorted', 'str', 'sum', 'tuple', 'zip',
        'True', 'False', 'None'
    }
    
    # 允许的模块（白名单）
    ALLOWED_MODULES = {
        'json', 'math', 'datetime', 'decimal', 'statistics'
    }
    
    # HTTP 调用的默认配置
    DEFAULT_HTTP_TIMEOUT = 30  # 秒
    MAX_HTTP_TIMEOUT = 120  # 最大超时时间
    MAX_RESPONSE_SIZE = 10 * 1024 * 1024  # 10MB
    
    @staticmethod
    def execute_python(code: str, inputs: Dict[str, Any], 
                      timeout: int = 10) -> Dict[str, Any]:
        """
        在受限环境中执行 Python 代码
        
        Args:
            code: Python 代码字符串
            inputs: 输入数据字典
            timeout: 超时时间（秒）
            
        Returns:
            包含 outputs 和 logs 的字典
            
        Raises:
            TimeoutError: 执行超时
            Exception: 代码执行错误
        """
        try:
            # 创建受限的全局命名空间
            restricted_globals = {
                '__builtins__': {
                    name: getattr(__builtins__, name)
                    for name in CommunityNodeExecutor.ALLOWED_BUILTINS
                    if hasattr(__builtins__, name)
                }
            }
            
            # 添加允许的模块
            for module_name in CommunityNodeExecutor.ALLOWED_MODULES:
                try:
                    module = __import__(module_name)
                    restricted_globals[module_name] = module
                except ImportError:
                    pass
            
            # 准备局部命名空间
            local_namespace = {
                'inputs': inputs,
                'outputs': {}
            }
            
            # 捕获标准输出和错误
            stdout_capture = io.StringIO()
            stderr_capture = io.StringIO()
            
            start_time = time.time()
            
            # 执行代码
            with redirect_stdout(stdout_capture), redirect_stderr(stderr_capture):
                exec(code, restricted_globals, local_namespace)
            
            execution_time = time.time() - start_time
            
            # 检查超时
            if execution_time > timeout:
                raise TimeoutError(f"Execution exceeded {timeout} seconds")
            
            # 获取输出
            outputs = local_namespace.get('outputs', {})
            
            # 获取日志
            stdout_text = stdout_capture.getvalue()
            stderr_text = stderr_capture.getvalue()
            
            logs = []
            if stdout_text:
                logs.append({'level': 'info', 'message': stdout_text})
            if stderr_text:
                logs.append({'level': 'error', 'message': stderr_text})
            
            return {
                'success': True,
                'outputs': outputs,
                'logs': logs,
                'execution_time': execution_time
            }
            
        except TimeoutError as e:
            return {
                'success': False,
                'error': str(e),
                'error_type': 'timeout'
            }
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'error_type': type(e).__name__,
                'logs': [{'level': 'error', 'message': str(e)}]
            }
    
    @staticmethod
    def execute_http(config: Dict[str, Any], inputs: Dict[str, Any]) -> Dict[str, Any]:
        """
        执行 HTTP API 调用
        
        Args:
            config: HTTP 配置
                - url: API URL (required)
                - method: HTTP 方法 (GET, POST, etc)
                - headers: 请求头
                - timeout: 超时时间
            inputs: 输入数据（作为请求体或查询参数）
            
        Returns:
            包含 outputs 和元数据的字典
            
        Raises:
            ValueError: 配置错误
            requests.RequestException: HTTP 请求错误
        """
        try:
            # 验证配置
            url = config.get('url')
            if not url:
                raise ValueError("URL is required in HTTP config")
            
            method = config.get('method', 'POST').upper()
            headers = config.get('headers', {})
            timeout = min(
                config.get('timeout', CommunityNodeExecutor.DEFAULT_HTTP_TIMEOUT),
                CommunityNodeExecutor.MAX_HTTP_TIMEOUT
            )
            
            # 设置默认 Content-Type
            if 'Content-Type' not in headers:
                headers['Content-Type'] = 'application/json'
            
            # 准备请求
            request_kwargs = {
                'headers': headers,
                'timeout': timeout
            }
            
            start_time = time.time()
            
            # 根据方法发送请求
            if method in ['GET', 'DELETE']:
                # GET/DELETE 使用查询参数
                request_kwargs['params'] = inputs
                response = requests.request(method, url, **request_kwargs)
            else:
                # POST/PUT/PATCH 使用请求体
                request_kwargs['json'] = inputs
                response = requests.request(method, url, **request_kwargs)
            
            execution_time = time.time() - start_time
            
            # 检查响应大小
            content_length = len(response.content)
            if content_length > CommunityNodeExecutor.MAX_RESPONSE_SIZE:
                raise ValueError(f"Response too large: {content_length} bytes")
            
            # 解析响应
            try:
                response_data = response.json()
            except json.JSONDecodeError:
                response_data = {'raw': response.text}
            
            # 检查 HTTP 状态码
            response.raise_for_status()
            
            return {
                'success': True,
                'outputs': response_data,
                'metadata': {
                    'status_code': response.status_code,
                    'execution_time': execution_time,
                    'content_length': content_length
                }
            }
            
        except requests.Timeout:
            return {
                'success': False,
                'error': 'HTTP request timeout',
                'error_type': 'timeout'
            }
        except requests.RequestException as e:
            return {
                'success': False,
                'error': str(e),
                'error_type': 'http_error',
                'status_code': getattr(e.response, 'status_code', None) if hasattr(e, 'response') else None
            }
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'error_type': type(e).__name__
            }
    
    @staticmethod
    def execute(execution_type: str, execution_config: Dict[str, Any], 
                inputs: Dict[str, Any]) -> Dict[str, Any]:
        """
        统一的执行接口
        
        Args:
            execution_type: 'python' 或 'http'
            execution_config: 执行配置
            inputs: 输入数据
            
        Returns:
            执行结果字典
        """
        if execution_type == 'python':
            code = execution_config.get('pythonCode', '')
            timeout = execution_config.get('timeout', 10)
            return CommunityNodeExecutor.execute_python(code, inputs, timeout)
        
        elif execution_type == 'http':
            return CommunityNodeExecutor.execute_http(execution_config, inputs)
        
        else:
            return {
                'success': False,
                'error': f"Unsupported execution type: {execution_type}",
                'error_type': 'invalid_execution_type'
            }
    
    @staticmethod
    def validate_python_code(code: str) -> Dict[str, Any]:
        """
        验证 Python 代码是否安全
        
        Args:
            code: Python 代码
            
        Returns:
            验证结果
        """
        issues = []
        
        # 检查危险关键字
        dangerous_keywords = [
            'import os', 'import sys', '__import__',
            'eval', 'exec', 'compile',
            'open(', 'file(', 'input(',
            'subprocess', 'socket', 'urllib',
            '__builtins__', 'globals(', 'locals(',
            'delattr', 'setattr', 'getattr'
        ]
        
        for keyword in dangerous_keywords:
            if keyword in code:
                issues.append(f"Dangerous keyword detected: {keyword}")
        
        # 检查代码是否可以编译
        try:
            compile(code, '<string>', 'exec')
        except SyntaxError as e:
            issues.append(f"Syntax error: {e}")
        
        return {
            'valid': len(issues) == 0,
            'issues': issues
        }
    
    @staticmethod
    def validate_http_config(config: Dict[str, Any]) -> Dict[str, Any]:
        """
        验证 HTTP 配置
        
        Args:
            config: HTTP 配置
            
        Returns:
            验证结果
        """
        issues = []
        
        # 检查必需字段
        if 'url' not in config:
            issues.append("URL is required")
        else:
            url = config['url']
            # 检查 URL 协议
            if not url.startswith('http://') and not url.startswith('https://'):
                issues.append("URL must start with http:// or https://")
        
        # 检查 HTTP 方法
        if 'method' in config:
            method = config['method'].upper()
            if method not in ['GET', 'POST', 'PUT', 'PATCH', 'DELETE']:
                issues.append(f"Unsupported HTTP method: {method}")
        
        # 检查超时设置
        if 'timeout' in config:
            timeout = config['timeout']
            if not isinstance(timeout, (int, float)) or timeout <= 0:
                issues.append("Timeout must be a positive number")
            elif timeout > CommunityNodeExecutor.MAX_HTTP_TIMEOUT:
                issues.append(f"Timeout exceeds maximum: {CommunityNodeExecutor.MAX_HTTP_TIMEOUT}s")
        
        return {
            'valid': len(issues) == 0,
            'issues': issues
        }
