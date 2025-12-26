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
    Community Node Executor
    
    Supports two execution methods:
    1. Python code execution (restricted environment)
    2. HTTP API calls
    """
    
    # Security restrictions for Python code execution
    ALLOWED_BUILTINS = {
        'abs', 'all', 'any', 'bool', 'dict', 'enumerate', 'filter',
        'float', 'int', 'len', 'list', 'map', 'max', 'min', 'print',
        'range', 'round', 'set', 'sorted', 'str', 'sum', 'tuple', 'zip',
        'True', 'False', 'None'
    }
    
    # Allowed modules (whitelist)
    ALLOWED_MODULES = {
        'json', 'math', 'datetime', 'decimal', 'statistics'
    }
    
    # HTTP call default config
    DEFAULT_HTTP_TIMEOUT = 30  # seconds
    MAX_HTTP_TIMEOUT = 120  # max timeout
    MAX_RESPONSE_SIZE = 10 * 1024 * 1024  # 10MB
    
    @staticmethod
    def execute_python(code: str, inputs: Dict[str, Any], 
                      timeout: int = 10) -> Dict[str, Any]:
        """
        Execute Python code in restricted environment.
        
        Args:
            code: Python code string
            inputs: Input data dictionary
            timeout: Timeout (seconds)
            
        Returns:
            Dictionary containing outputs and logs
            
        Raises:
            TimeoutError: Execution timeout
            Exception: Code execution error
        """
        try:
            # Create restricted global namespace
            restricted_globals = {
                '__builtins__': {
                    name: getattr(__builtins__, name)
                    for name in CommunityNodeExecutor.ALLOWED_BUILTINS
                    if hasattr(__builtins__, name)
                }
            }
            
            # Add allowed modules
            for module_name in CommunityNodeExecutor.ALLOWED_MODULES:
                try:
                    module = __import__(module_name)
                    restricted_globals[module_name] = module
                except ImportError:
                    pass
            
            # Prepare local namespace
            local_namespace = {
                'inputs': inputs,
                'outputs': {}
            }
            
            # Capture stdout and stderr
            stdout_capture = io.StringIO()
            stderr_capture = io.StringIO()
            
            start_time = time.time()
            
            # Execute code
            with redirect_stdout(stdout_capture), redirect_stderr(stderr_capture):
                exec(code, restricted_globals, local_namespace)
            
            execution_time = time.time() - start_time
            
            # Check timeout
            if execution_time > timeout:
                raise TimeoutError(f"Execution exceeded {timeout} seconds")
            
            # Get outputs
            outputs = local_namespace.get('outputs', {})
            
            # Get logs
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
        Execute HTTP API call.
        
        Args:
            config: HTTP config
                - url: API URL (required)
                - method: HTTP method (GET, POST, etc)
                - headers: Request headers
                - timeout: Timeout
            inputs: Input data (as request body or query params)
            
        Returns:
            Dictionary containing outputs and metadata
            
        Raises:
            ValueError: Config error
            requests.RequestException: HTTP request error
        """
        try:
            # Validate config
            url = config.get('url')
            if not url:
                raise ValueError("URL is required in HTTP config")
            
            method = config.get('method', 'POST').upper()
            headers = config.get('headers', {})
            timeout = min(
                config.get('timeout', CommunityNodeExecutor.DEFAULT_HTTP_TIMEOUT),
                CommunityNodeExecutor.MAX_HTTP_TIMEOUT
            )
            
            # Set default Content-Type
            if 'Content-Type' not in headers:
                headers['Content-Type'] = 'application/json'
            
            # Prepare request
            request_kwargs = {
                'headers': headers,
                'timeout': timeout
            }
            
            start_time = time.time()
            
            # Send request based on method
            if method in ['GET', 'DELETE']:
                # GET/DELETE use query params
                request_kwargs['params'] = inputs
                response = requests.request(method, url, **request_kwargs)
            else:
                # POST/PUT/PATCH use request body
                request_kwargs['json'] = inputs
                response = requests.request(method, url, **request_kwargs)
            
            execution_time = time.time() - start_time
            
            # Check response size
            content_length = len(response.content)
            if content_length > CommunityNodeExecutor.MAX_RESPONSE_SIZE:
                raise ValueError(f"Response too large: {content_length} bytes")
            
            # Parse response
            try:
                response_data = response.json()
            except json.JSONDecodeError:
                response_data = {'raw': response.text}
            
            # Check HTTP status code
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
        Unified execution interface.
        
        Args:
            execution_type: 'python' or 'http'
            execution_config: Execution config
            inputs: Input data
            
        Returns:
            Execution result dictionary
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
        Validate if Python code is safe.
        
        Args:
            code: Python code
            
        Returns:
            Validation result
        """
        issues = []
        
        # Check dangerous keywords
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
        
        # Check if code can compile
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
        Validate HTTP config.
        
        Args:
            config: HTTP config
            
        Returns:
            Validation result
        """
        issues = []
        
        # Check required fields
        if 'url' not in config:
            issues.append("URL is required")
        else:
            url = config['url']
            # Check URL protocol
            if not url.startswith('http://') and not url.startswith('https://'):
                issues.append("URL must start with http:// or https://")
        
        # Check HTTP method
        if 'method' in config:
            method = config['method'].upper()
            if method not in ['GET', 'POST', 'PUT', 'PATCH', 'DELETE']:
                issues.append(f"Unsupported HTTP method: {method}")
        
        # Check timeout setting
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
