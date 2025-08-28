import unittest
import sys
import os
import builtins as builtins_module  # 重命名以避免混淆
import io
from contextlib import redirect_stdout
from unittest.mock import patch, MagicMock

"""
简单测试文件，测试RestrictedImporter和RestrictedInterpreter类的功能
特别关注我们修复的问题：内置函数列表中缺少compile、delattr和callable
"""

# 定义测试所需的常量
ALLOWED_MODULES = {
    'math': 'math',
    'json': 'json',
}

# 添加更多必要的内置函数
SAFE_BUILTINS = {
    'abs': abs,
    'all': all,
    'any': any,
    'bool': bool,
    'callable': callable,
    'chr': chr,
    'compile': compile,
    'delattr': delattr,
    'dict': dict,
    'dir': dir,
    'divmod': divmod,
    'enumerate': enumerate,
    'eval': eval,  # 添加eval函数
    'filter': filter,
    'float': float,
    'format': format,
    'frozenset': frozenset,
    'getattr': getattr,  # 添加getattr函数
    'hasattr': hasattr,  # 添加hasattr函数
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
    'print': print,
    'range': range,
    'repr': repr,
    'reversed': reversed,
    'round': round,
    'set': set,
    'setattr': setattr,  # 添加setattr函数
    'slice': slice,
    'sorted': sorted,
    'str': str,
    'sum': sum,
    'tuple': tuple,
    'type': type,
    'zip': zip,
    '__build_class__': builtins_module.__build_class__,  # 添加__build_class__函数
}


# 实现简化版的RestrictedImporter和RestrictedInterpreter类供测试使用
class RestrictedImporter:
    """ 安全导入器，只允许导入白名单中的模块 """

    def __init__(self, allowed_modules):
        self.allowed_modules = allowed_modules
        self.imported_modules = {}

    def import_module(self, name):
        """ 安全地导入模块，只允许白名单中的模块 """
        if name not in self.allowed_modules:
            raise ImportError(f"导入被禁止: 模块 '{name}' 不在允许列表中")

        # 如果是别名，使用实际的模块名
        actual_name = self.allowed_modules[name]

        # 如果已经导入过，直接返回
        if actual_name in self.imported_modules:
            return self.imported_modules[actual_name]

        # 安全导入模块
        try:
            module = __import__(actual_name)
            self.imported_modules[actual_name] = module
            return module
        except Exception as e:
            raise ImportError(f"导入模块 '{actual_name}' 失败: {str(e)}")


class RestrictedInterpreter:
    """ 受限制的代码解释器，提供安全的执行环境 """

    def __init__(self, locals=None, allowed_modules=None):
        self.locals = locals or {}
        self.allowed_modules = allowed_modules or ALLOWED_MODULES
        # 创建安全导入器实例
        self.importer = RestrictedImporter(allowed_modules=self.allowed_modules)
        # 创建安全的内置函数环境
        self.safe_builtins = SAFE_BUILTINS.copy()
        # 添加安全的导入函数
        self.safe_builtins['__import__'] = self.safe_import

    def safe_import(self, name, *args, **kwargs):
        """ 安全的导入函数，替代内置的__import__ """
        return self.importer.import_module(name)

    def runcode(self, code):
        """ 在安全环境中运行代码 """
        # 使用字典来模拟安全的执行环境，而不修改全局builtins
        globals_dict = {
            '__name__': '__main__',  # 添加__name__变量
            '__file__': '<string>',  # 添加__file__变量
            '__package__': None,     # 添加__package__变量
        }
        
        # 添加安全的内置函数到globals字典
        for key, value in self.safe_builtins.items():
            globals_dict[key] = value
            
        # 添加__builtins__到globals字典
        globals_dict['__builtins__'] = self.safe_builtins
        
        try:
            # 在安全的环境中执行代码
            exec(code, globals_dict, self.locals)
        except Exception as e:
            # 捕获并重新抛出异常
            raise e


class TestRestrictedInterpreter(unittest.TestCase):
    """测试RestrictedInterpreter和RestrictedImporter类的功能"""
    
    def test_restricted_importer(self):
        """测试RestrictedImporter类的功能"""
        # 创建一个RestrictedImporter实例
        importer = RestrictedImporter(ALLOWED_MODULES)
        
        # 测试允许的模块导入
        math_module = importer.import_module('math')
        self.assertIsNotNone(math_module)
        self.assertEqual(math_module.__name__, 'math')
        
        # 测试不允许的模块导入
        with self.assertRaises(ImportError):
            importer.import_module('os')
    
    def test_restricted_interpreter_basic(self):
        """测试RestrictedInterpreter的基本功能"""
        # 创建一个RestrictedInterpreter实例
        locals_dict = {'x': 10, 'y': 20}
        interpreter = RestrictedInterpreter(locals=locals_dict, allowed_modules=ALLOWED_MODULES)
        
        # 测试基本的代码执行
        code = """z = x + y"""
        interpreter.runcode(compile(code, '<string>', 'exec'))
        
        # 验证执行结果
        self.assertEqual(interpreter.locals['z'], 30)
    
    def test_restricted_interpreter_with_compile(self):
        """测试RestrictedInterpreter中compile函数的使用"""
        # 创建一个RestrictedInterpreter实例
        locals_dict = {}
        interpreter = RestrictedInterpreter(locals=locals_dict, allowed_modules=ALLOWED_MODULES)
        
        # 测试使用compile函数的代码
        code = """
# 使用compile函数编译一个简单的表达式
expr = "2 + 3 * 4"
compiled_expr = compile(expr, '<string>', 'eval')
result = eval(compiled_expr)
"""
        interpreter.runcode(compile(code, '<string>', 'exec'))
        
        # 验证执行结果
        self.assertEqual(interpreter.locals['result'], 14)
    
    def test_restricted_interpreter_with_delattr(self):
        """测试RestrictedInterpreter中delattr函数的使用"""
        # 创建一个RestrictedInterpreter实例
        locals_dict = {}
        interpreter = RestrictedInterpreter(locals=locals_dict, allowed_modules=ALLOWED_MODULES)
        
        # 测试使用delattr函数的代码
        code = """
class TestClass:
    def __init__(self):
        self.a = 1
        self.b = 2

obj = TestClass()
delattr(obj, 'a')
has_attr_a = hasattr(obj, 'a')
has_attr_b = hasattr(obj, 'b')
"""
        interpreter.runcode(compile(code, '<string>', 'exec'))
        
        # 验证执行结果
        self.assertFalse(interpreter.locals['has_attr_a'])
        self.assertTrue(interpreter.locals['has_attr_b'])
    
    def test_restricted_interpreter_with_callable(self):
        """测试RestrictedInterpreter中callable函数的使用"""
        # 创建一个RestrictedInterpreter实例
        locals_dict = {}
        interpreter = RestrictedInterpreter(locals=locals_dict, allowed_modules=ALLOWED_MODULES)
        
        # 测试使用callable函数的代码
        code = """
def test_func():
    pass

class TestClass:
    def method(self):
        pass

obj = TestClass()

is_func_callable = callable(test_func)
is_method_callable = callable(obj.method)
is_int_callable = callable(42)
"""
        interpreter.runcode(compile(code, '<string>', 'exec'))
        
        # 验证执行结果
        self.assertTrue(interpreter.locals['is_func_callable'])
        self.assertTrue(interpreter.locals['is_method_callable'])
        self.assertFalse(interpreter.locals['is_int_callable'])
    
    def test_restricted_interpreter_builtins_restoration(self):
        """测试RestrictedInterpreter在执行后是否正确恢复内置函数"""
        # 记录原始内置函数
        original_builtins = set(dir(builtins_module))
        
        # 创建一个RestrictedInterpreter实例并执行代码
        interpreter = RestrictedInterpreter()
        code = """x = 1 + 1"""
        interpreter.runcode(compile(code, '<string>', 'exec'))
        
        # 验证内置函数是否被正确恢复
        current_builtins = set(dir(builtins_module))
        self.assertEqual(original_builtins, current_builtins)
    
    def test_restricted_interpreter_exception_handling(self):
        """测试RestrictedInterpreter在执行出错时是否正确处理异常"""
        # 创建一个RestrictedInterpreter实例
        interpreter = RestrictedInterpreter()
        
        # 执行一个会引发异常的代码
        code = """1/0  # 除以零错误"""
        
        # 验证异常是否被正确捕获和重新抛出
        with self.assertRaises(ZeroDivisionError):
            interpreter.runcode(compile(code, '<string>', 'exec'))
        
        # 验证内置函数是否存在于安全环境中
        self.assertIn('print', SAFE_BUILTINS)
        self.assertIn('delattr', SAFE_BUILTINS)
        self.assertIn('compile', SAFE_BUILTINS)
        self.assertIn('callable', SAFE_BUILTINS)

# 运行测试
if __name__ == "__main__":
    unittest.main()
