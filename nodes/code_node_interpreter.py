
import sys
import builtins
import importlib
from code import InteractiveInterpreter

# 允许使用的内置函数白名单
SAFE_BUILTINS = {
    'abs': abs,
    'all': all,
    'any': any,
    'bool': bool,
    'callable': callable,  # 添加callable函数到白名单
    'chr': chr,
    'complex': complex,
    'compile': compile,  # 添加compile函数到白名单
    'delattr': delattr,  # 添加delattr函数到白名单
    'dict': dict,
    'dir': dir,
    'divmod': divmod,
    'enumerate': enumerate,
    'filter': filter,
    'float': float,
    'format': format,
    'frozenset': frozenset,
    'hasattr': hasattr,  # 添加hasattr函数到白名单
    'hash': hash,
    'hex': hex,
    'int': int,
    'isinstance': isinstance,
    'issubclass': issubclass,
    'iter': iter,
    'len': len,
    'list': list,
    'map': map,
    'max': max,
    'min': min,
    'next': next,
    'oct': oct,
    'ord': ord,
    'pow': pow,
    'print': print,  # 允许打印，但会被重定向到捕获器
    'range': range,
    'repr': repr,
    'reversed': reversed,
    'round': round,
    'set': set,
    'slice': slice,
    'sorted': sorted,
    'str': str,
    'sum': sum,
    'tuple': tuple,
    'type': type,
    'zip': zip,
}

# 允许导入的模块白名单
ALLOWED_MODULES = {
    'math': 'math',
    'random': 'random',
    'datetime': 'datetime',
    'json': 'json',
    'collections': 'collections',
    'itertools': 'itertools',
    'functools': 'functools',
    're': 're',
    'pandas': 'pandas',
    'numpy': 'numpy',
    'pd': 'pandas',  # 别名
    'np': 'numpy',   # 别名
    # 网络和数据采集库
    'requests': 'requests',
    'urllib': 'urllib',
    'urllib3': 'urllib3',
    # HTML/XML解析库
    'bs4': 'bs4',
    'beautifulsoup4': 'bs4',  # 别名
    'lxml': 'lxml',
    'html5lib': 'html5lib',
    # 数据处理相关
    'csv': 'csv',
    'time': 'time',
    'calendar': 'calendar',
    'decimal': 'decimal',
    'fractions': 'fractions',
    'statistics': 'statistics',
    # 文本处理
    'string': 'string',
    'textwrap': 'textwrap',
    'unicodedata': 'unicodedata',
    # 数据格式
    'xml': 'xml',
    'html': 'html',
    'base64': 'base64',
    'binascii': 'binascii',
    'zlib': 'zlib',
    'gzip': 'gzip',
    # R&D Mode: 交易相关库 (供高级用户在 CodeNode 中直接使用)
    'ccxt': 'ccxt',
    'web3': 'web3',
    'eth_account': 'eth_account',
    'eth_abi': 'eth_abi',
    'eth_utils': 'eth_utils',
}

class RestrictedImporter:
    """安全导入器，只允许导入白名单中的模块"""

    def __init__(self, allowed_modules):
        self.allowed_modules = allowed_modules
        self.imported_modules = {}

    def import_module(self, name):
        """安全地导入模块，只允许白名单中的模块"""
        if name not in self.allowed_modules:
            raise ImportError(f"导入被禁止: 模块 '{name}' 不在允许列表中")

        # 如果是别名，使用实际的模块名
        actual_name = self.allowed_modules[name]

        # 如果已经导入过，直接返回
        if actual_name in self.imported_modules:
            return self.imported_modules[actual_name]

        # 安全导入模块
        try:
            module = importlib.import_module(actual_name)
            self.imported_modules[actual_name] = module
            return module
        except Exception as e:
            raise ImportError(f"导入模块 '{actual_name}' 失败: {str(e)}")


class RestrictedInterpreter(InteractiveInterpreter):
    """受限制的代码解释器，提供安全的执行环境"""

    # ★ Phase 0 安全加固: 需要从 builtins 中移除的危险函数
    # 这些函数可以绕过 AST 静态检查，通过间接方式访问 os.environ 等敏感资源
    BLOCKED_BUILTINS = frozenset([
        'eval', 'exec', 'compile',     # 动态代码执行
        'open',                          # 文件系统访问
        'globals', 'locals',             # 作用域探测
        'vars',                          # 对象内省（可泄露 __builtins__)
        'breakpoint',                    # 调试器入口
        'exit', 'quit',                  # 退出解释器
    ])

    def __init__(self, locals=None, allowed_modules=None):
        """安全的内置函数环境，提供安全的执行环境"""
        super().__init__(locals or {})
        self.allowed_modules = allowed_modules or ALLOWED_MODULES

        # 创建安全导入器实例，只传递必要的参数
        self.importer = RestrictedImporter(allowed_modules=self.allowed_modules)

        # 创建安全的内置函数环境
        self.safe_builtins = SAFE_BUILTINS.copy()

        # 添加安全的导入函数
        self.safe_builtins['__import__'] = self.safe_import

    @staticmethod
    def _make_blocked_builtin(name: str):
        """创建一个被阻止的内置函数的替代品，调用时抛出安全异常"""
        def _blocked(*args, **kwargs):
            raise PermissionError(
                f"Security restriction: built-in function '{name}' is not allowed in CodeNode. "
                f"This function has been blocked to prevent unauthorized access to system resources."
            )
        _blocked.__name__ = name
        _blocked.__qualname__ = f"blocked_{name}"
        return _blocked

    def safe_import(self, name, *args, **kwargs):
        """安全的导入函数，替代内置的__import__"""
        # 允许调试器相关模块通过
        if name.startswith('_pydevd') or name.startswith('debugpy') or name == 'threading':
            # 使用原始的__import__函数导入调试器模块
            return __import__(name, *args, **kwargs)
        return self.importer.import_module(name)

    def runcode(self, code, debug_capture=None):
        """在安全环境中运行代码

        Args:
            code: 要执行的代码
            debug_capture: 可选的调试输出捕获器，用于捕获调试信息
        """
        # 保存原始的内置函数和递归限制
        original_builtins = dict(builtins.__dict__)
        original_recursion_limit = sys.getrecursionlimit()

        # 定义调试打印函数
        def debug_print(message):
            if debug_capture is not None:
                debug_capture.write(f"[DEBUG] {message}\n")
            else:
                print(f"[DEBUG] {message}")

        # 记录开始执行
        debug_print("RestrictedInterpreter.runcode: Starting code execution")

        try:
            # ★ Phase 0 安全加固 (方案 A — 最小改动)
            # 不替换整个 builtins（之前导致崩溃），而是只针对性地移除/替换危险函数。
            # 这些修改仅影响 user code 的执行线程，finally 块中会恢复。
            removed_builtins = {}
            for name in self.BLOCKED_BUILTINS:
                if name in builtins.__dict__:
                    removed_builtins[name] = builtins.__dict__[name]
                    # 替换为一个会抛出错误的占位函数
                    builtins.__dict__[name] = self._make_blocked_builtin(name)
            debug_print(f"Blocked {len(removed_builtins)} dangerous builtins: {list(removed_builtins.keys())}")

            # 替换 __import__ 为安全版本（允许白名单模块）
            original_import = builtins.__dict__.get('__import__')
            builtins.__dict__['__import__'] = self.safe_import
            debug_print("Replaced __import__ with safe_import")

            # 执行代码
            debug_print("Safe environment setup complete, executing code")
            super().runcode(code)
            debug_print("Code execution completed successfully")
        except Exception as e:
            # 捕获并记录异常
            debug_print(f"Exception during code execution: {type(e).__name__}: {str(e)}")
            # 重新抛出异常，确保finally块执行
            raise e
        finally:
            debug_print("In finally block, restoring original environment")
            try:
                # 恢复原始递归限制
                sys.setrecursionlimit(original_recursion_limit)
                debug_print(f"Restored recursion limit to {original_recursion_limit}")

                # ★ Phase 0 安全加固: 恢复被移除的 builtins
                for name, func in removed_builtins.items():
                    builtins.__dict__[name] = func
                debug_print(f"Restored {len(removed_builtins)} blocked builtins")

                # 恢复原始 __import__
                if original_import is not None:
                    builtins.__dict__['__import__'] = original_import
                debug_print("Restored original __import__")

                # 恢复原始的内置函数 - 使用更安全的方式
                # 直接更新__dict__而不是先清空
                builtins.__dict__.update(original_builtins)
                debug_print("Original builtins restored")
            except Exception as e:
                # 记录恢复过程中的错误
                debug_print(f"Error during environment restoration: {type(e).__name__}: {str(e)}")
            debug_print("RestrictedInterpreter.runcode: Completed")
