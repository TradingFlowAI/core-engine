"""
Test Suite: RestrictedInterpreter Security (Phase 0 安全加固)

测试策略说明：
- safe_import 替换 builtins.__import__ 后，importlib.import_module 的内部子
  依赖也会被拦截。生产环境中标准库在 Station 启动时已缓存到 sys.modules，
  用户代码的 `import math` 实际从缓存返回，不触发子依赖 import。
- 因此测试分为两层：
  1. 单元测试：直接测试 safe_import / BLOCKED_BUILTINS / ALLOWED_MODULES 的逻辑
  2. 行为测试：通过 exec 直接测试 blocked builtins 的运行时行为

测试场景：
1. ALLOWED_MODULES 包含标准库和 R&D Mode 模块
2. safe_import 对白名单内模块放行
3. safe_import 对白名单外模块拦截
4. BLOCKED_BUILTINS 包含所有危险函数
5. _make_blocked_builtin 产生的替代函数抛 PermissionError
6. runcode 执行后 builtins 完全恢复
7. 被屏蔽 builtins 在运行时确实被阻止
"""

import builtins
import io
import sys
import pytest
from nodes.code_node_interpreter import (
    RestrictedInterpreter,
    RestrictedImporter,
    ALLOWED_MODULES,
    SAFE_BUILTINS,
)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 1. ALLOWED_MODULES 白名单完整性
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class TestAllowedModules:
    """验证白名单包含预期模块"""

    @pytest.mark.parametrize("module", [
        "math", "json", "datetime", "re", "collections",
        "itertools", "functools", "random", "csv", "time",
        "pandas", "numpy", "requests", "bs4",
    ])
    def test_standard_modules_allowed(self, module):
        assert module in ALLOWED_MODULES, f"'{module}' should be in ALLOWED_MODULES"

    @pytest.mark.parametrize("module", [
        "ccxt", "web3", "eth_account", "eth_abi", "eth_utils",
    ])
    def test_rd_mode_modules_allowed(self, module):
        """R&D Mode 新增模块应在白名单中"""
        assert module in ALLOWED_MODULES

    @pytest.mark.parametrize("module", [
        "os", "subprocess", "sys", "pathlib", "socket",
        "ctypes", "multiprocessing", "pickle", "shutil",
    ])
    def test_dangerous_modules_not_in_whitelist(self, module):
        """危险模块不应在白名单中"""
        assert module not in ALLOWED_MODULES

    def test_aliases_resolve_correctly(self):
        """模块别名应正确映射"""
        assert ALLOWED_MODULES.get("pd") == "pandas"
        assert ALLOWED_MODULES.get("np") == "numpy"


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 2. RestrictedImporter 单元测试
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class TestRestrictedImporter:
    """直接测试 RestrictedImporter 的 import 逻辑"""

    def test_allowed_module_importable(self):
        """白名单模块应能成功导入"""
        importer = RestrictedImporter(allowed_modules=ALLOWED_MODULES)
        # json 是纯 Python 标准库，无复杂子依赖
        module = importer.import_module("json")
        assert hasattr(module, "loads")
        assert hasattr(module, "dumps")

    def test_blocked_module_raises_import_error(self):
        """白名单外模块应抛 ImportError"""
        importer = RestrictedImporter(allowed_modules=ALLOWED_MODULES)
        with pytest.raises(ImportError, match="不在允许列表"):
            importer.import_module("os")

    def test_blocked_subprocess_raises(self):
        importer = RestrictedImporter(allowed_modules=ALLOWED_MODULES)
        with pytest.raises(ImportError, match="不在允许列表"):
            importer.import_module("subprocess")

    def test_blocked_sys_raises(self):
        importer = RestrictedImporter(allowed_modules=ALLOWED_MODULES)
        with pytest.raises(ImportError, match="不在允许列表"):
            importer.import_module("sys")

    def test_cached_module_returned_directly(self):
        """同一模块第二次 import 应从缓存返回"""
        importer = RestrictedImporter(allowed_modules=ALLOWED_MODULES)
        m1 = importer.import_module("json")
        m2 = importer.import_module("json")
        assert m1 is m2


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 3. BLOCKED_BUILTINS 完整性
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class TestBlockedBuiltins:
    """验证 BLOCKED_BUILTINS 包含所有危险函数"""

    @pytest.mark.parametrize("name", [
        "eval", "exec", "compile", "open",
        "globals", "locals", "vars",
        "breakpoint", "exit", "quit",
    ])
    def test_dangerous_builtin_blocked(self, name):
        assert name in RestrictedInterpreter.BLOCKED_BUILTINS

    def test_safe_builtins_not_blocked(self):
        """SAFE_BUILTINS 中的函数不应在 BLOCKED 列表中（除了 compile）"""
        # compile 既在 SAFE_BUILTINS（遗留）又在 BLOCKED（Phase 0 加固）
        # 在运行时 BLOCKED 优先
        for name in SAFE_BUILTINS:
            if name == "compile":
                continue  # compile 是特例
            assert name not in RestrictedInterpreter.BLOCKED_BUILTINS, \
                f"'{name}' in SAFE_BUILTINS but also in BLOCKED_BUILTINS"


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 4. _make_blocked_builtin 行为
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class TestMakeBlockedBuiltin:
    """验证生成的 blocked 替代函数抛出正确异常"""

    def test_blocked_raises_permission_error(self):
        blocked_fn = RestrictedInterpreter._make_blocked_builtin("eval")
        with pytest.raises(PermissionError, match="Security restriction"):
            blocked_fn("1+1")

    def test_blocked_includes_function_name(self):
        blocked_fn = RestrictedInterpreter._make_blocked_builtin("open")
        with pytest.raises(PermissionError, match="open"):
            blocked_fn("/etc/passwd")

    def test_blocked_function_name_attribute(self):
        blocked_fn = RestrictedInterpreter._make_blocked_builtin("exec")
        assert blocked_fn.__name__ == "exec"
        assert "blocked" in blocked_fn.__qualname__


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 5. 运行时 Blocked Builtins 拦截（通过手动模拟 runcode 环境）
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class TestRuntimeBlockedBuiltins:
    """
    手动复现 runcode 的 builtins 替换逻辑，
    然后在替换环境下 exec 用户代码验证拦截。
    """

    def _exec_with_blocked_builtins(self, code: str, ns: dict = None):
        """模拟 runcode 中的 builtins 替换，执行代码"""
        interpreter = RestrictedInterpreter(
            locals={"output_data": None},
            allowed_modules=ALLOWED_MODULES,
        )

        # 先编译代码，并保存真实的 exec（在 builtins 被替换之前）
        compiled = compile(code, "<test>", "exec")
        real_exec = builtins.exec

        # 然后替换 builtins（和 runcode 中逻辑一致）
        saved = {}
        for name in interpreter.BLOCKED_BUILTINS:
            if name in builtins.__dict__:
                saved[name] = builtins.__dict__[name]
                builtins.__dict__[name] = interpreter._make_blocked_builtin(name)

        try:
            # 使用保存的真实 exec 执行用户代码
            if ns is not None:
                real_exec(compiled, ns)
            else:
                real_exec(compiled)
        finally:
            # 恢复原始 builtins
            for name, func in saved.items():
                builtins.__dict__[name] = func

    def test_eval_blocked_at_runtime(self):
        with pytest.raises(PermissionError, match="eval"):
            self._exec_with_blocked_builtins("eval('1+1')")

    def test_exec_blocked_at_runtime(self):
        with pytest.raises(PermissionError, match="exec"):
            self._exec_with_blocked_builtins("exec('x=1')")

    def test_open_blocked_at_runtime(self):
        with pytest.raises(PermissionError, match="open"):
            self._exec_with_blocked_builtins("open('/etc/passwd')")

    def test_globals_blocked_at_runtime(self):
        with pytest.raises(PermissionError, match="globals"):
            self._exec_with_blocked_builtins("globals()")

    def test_breakpoint_blocked_at_runtime(self):
        with pytest.raises(PermissionError, match="breakpoint"):
            self._exec_with_blocked_builtins("breakpoint()")

    def test_safe_builtins_still_work(self):
        """在 blocked 环境中，安全的 builtins 仍然可用"""
        ns = {"output_data": None}

        self._exec_with_blocked_builtins("output_data = len([1, 2, 3])", ns)
        assert ns["output_data"] == 3

        self._exec_with_blocked_builtins("output_data = sorted([3, 1, 2])", ns)
        assert ns["output_data"] == [1, 2, 3]

        self._exec_with_blocked_builtins("output_data = list(range(5))", ns)
        assert ns["output_data"] == [0, 1, 2, 3, 4]

        self._exec_with_blocked_builtins("output_data = isinstance(42, int)", ns)
        assert ns["output_data"] is True


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 6. 环境恢复
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class TestEnvironmentRestore:
    def test_builtins_restored_after_runcode(self):
        """runcode 执行后 builtins 应完全恢复"""
        original_eval = builtins.eval
        original_open = builtins.open
        original_import = builtins.__import__

        interpreter = RestrictedInterpreter(
            locals={"output_data": None},
            allowed_modules=ALLOWED_MODULES,
        )
        # 运行一段简单代码（可能因 os import 报错，但 finally 应恢复）
        try:
            interpreter.runcode(compile("x = 1", "<test>", "exec"))
        except Exception:
            pass  # 可能因 import chain 报错

        # 验证恢复
        assert builtins.eval is original_eval
        assert builtins.open is original_open
        assert builtins.__import__ is original_import

    def test_builtins_restored_after_error(self):
        """即使用户代码报错，builtins 也应恢复"""
        original_eval = builtins.eval

        interpreter = RestrictedInterpreter(
            locals={"output_data": None},
            allowed_modules=ALLOWED_MODULES,
        )
        try:
            interpreter.runcode(compile("raise ValueError('test')", "<test>", "exec"))
        except Exception:
            pass

        assert builtins.eval is original_eval


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 7. safe_import 函数行为
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class TestSafeImportFunction:
    def test_debugger_modules_passthrough(self):
        """调试器模块应透传到原始 __import__"""
        interpreter = RestrictedInterpreter(
            locals={}, allowed_modules=ALLOWED_MODULES,
        )
        # threading 应透传
        mod = interpreter.safe_import("threading")
        import threading
        assert mod is threading

    def test_allowed_module_uses_importer(self):
        """白名单模块应通过 RestrictedImporter 导入"""
        interpreter = RestrictedInterpreter(
            locals={}, allowed_modules=ALLOWED_MODULES,
        )
        # json 是白名单模块
        mod = interpreter.safe_import("json")
        import json
        assert mod is json

    def test_blocked_module_raises(self):
        """非白名单模块应抛 ImportError"""
        interpreter = RestrictedInterpreter(
            locals={}, allowed_modules=ALLOWED_MODULES,
        )
        with pytest.raises(ImportError, match="不在允许列表"):
            interpreter.safe_import("os")
