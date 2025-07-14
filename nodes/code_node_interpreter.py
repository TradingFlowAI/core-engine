
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

            # @CL 2025 May 13, 以下安全部分的东西先全部注释掉了，这些一替换整个应用就崩了
            #
            # # 设置递归限制
            # if hasattr(self, 'max_recursion'):
            #     sys.setrecursionlimit(self.max_recursion)
            #     print(f"[DEBUG] Set recursion limit to {self.max_recursion}")

            # # 创建安全的内置函数环境
            # print("[DEBUG] Setting up safe builtins environment")

            # # 直接替换整个__dict__而不是逐个删除和添加
            # # 这样可以避免可能的死循环和其他问题
            # safe_dict = {}
            # # 保留所有以下划线开头的特殊属性
            # for key, value in original_builtins.items():
            #     if key.startswith('_'):
            #         safe_dict[key] = value

            # # 添加安全的内置函数
            # for key, value in self.safe_builtins.items():
            #     safe_dict[key] = value

            # # 一次性替换全部内置函数
            # original_builtins_copy = dict(builtins.__dict__)  # 先保存一个副本
            # builtins.__dict__.update(safe_dict)  # 更新（覆盖）内置函数
            #

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

                # 恢复原始的内置函数 - 使用更安全的方式
                # 直接更新__dict__而不是先清空
                builtins.__dict__.update(original_builtins)
                debug_print("Original builtins restored")
            except Exception as e:
                # 记录恢复过程中的错误
                debug_print(f"Error during environment restoration: {type(e).__name__}: {str(e)}")
            debug_print("RestrictedInterpreter.runcode: Completed")
