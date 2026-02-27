"""
Test Suite: LogSanitizer (R&D Mode / Phase 0 安全加固)

测试场景：
1. 基本字符串脱敏
2. 短字符串不脱敏（<= 4 chars 防误伤）
3. 动态注册 register_sensitive_value
4. 多个敏感值同时脱敏
5. sanitize_dict 递归脱敏
6. 空输入边界情况
7. 部分匹配（敏感值嵌入在长字符串中）
"""

import os
import pytest
from common.log_sanitizer import (
    sanitize,
    sanitize_dict,
    register_sensitive_value,
    init_sensitive_values,
    _SENSITIVE_VALUES,
)


@pytest.fixture(autouse=True)
def reset_sensitive_values():
    """每个测试前清空全局敏感值列表"""
    import common.log_sanitizer as mod
    mod._SENSITIVE_VALUES = []
    yield
    mod._SENSITIVE_VALUES = []


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 1. 基本字符串脱敏
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class TestBasicSanitize:
    def test_registered_value_is_masked(self):
        """已注册的敏感值应被脱敏，保留最后 2 位"""
        register_sensitive_value("0xdeadbeef1234567890")
        result = sanitize("My key is 0xdeadbeef1234567890 and it works")
        assert "0xdeadbeef1234567890" not in result
        assert "90" in result  # 最后 2 位保留
        assert "****" in result or "**" in result

    def test_no_registered_value_no_change(self):
        """没有注册敏感值时，消息不变"""
        msg = "Hello world, nothing sensitive here"
        assert sanitize(msg) == msg

    def test_multiple_occurrences_all_masked(self):
        """同一敏感值多次出现，全部脱敏"""
        register_sensitive_value("supersecretkey123")
        msg = "key=supersecretkey123, again=supersecretkey123"
        result = sanitize(msg)
        assert result.count("supersecretkey123") == 0


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 2. 短字符串不脱敏（防误伤）
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class TestShortValueProtection:
    def test_very_short_value_not_registered(self):
        """长度 < 4 的值不应被注册"""
        register_sensitive_value("abc")  # 长度 3
        assert sanitize("abc is here") == "abc is here"  # 不脱敏

    def test_exactly_4_chars_registered(self):
        """长度恰好为 4 的值应被注册"""
        register_sensitive_value("abcd")
        # 但 sanitize 内部检查 len > 4，所以 4 字符不会被脱敏
        # 这是预期行为：防止误伤
        result = sanitize("abcd is here")
        assert "abcd" in result  # 4 chars too short for sanitize

    def test_5_chars_value_is_sanitized(self):
        """长度为 5 的值应被脱敏"""
        register_sensitive_value("abcde")
        result = sanitize("abcde is here")
        assert "abcde" not in result
        assert "de" in result  # 最后 2 位保留


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 3. 动态注册
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class TestDynamicRegistration:
    def test_register_then_sanitize(self):
        """先 register 再 sanitize 应生效"""
        msg = "key=my_api_key_12345"
        assert sanitize(msg) == msg  # 还没注册

        register_sensitive_value("my_api_key_12345")
        result = sanitize(msg)
        assert "my_api_key_12345" not in result

    def test_duplicate_register_no_double_entry(self):
        """重复注册同一值不应产生重复条目"""
        import common.log_sanitizer as mod
        register_sensitive_value("unique_secret_123")
        register_sensitive_value("unique_secret_123")
        count = mod._SENSITIVE_VALUES.count("unique_secret_123")
        assert count == 1

    def test_extra_secrets_parameter(self):
        """sanitize 的 extra_secrets 参数应一次性脱敏"""
        msg = "temporary secret is temp_secret_abc"
        result = sanitize(msg, extra_secrets=["temp_secret_abc"])
        assert "temp_secret_abc" not in result

    def test_extra_secrets_not_persisted(self):
        """extra_secrets 不应持久化到全局列表"""
        import common.log_sanitizer as mod
        sanitize("msg", extra_secrets=["ephemeral_secret_99"])
        assert "ephemeral_secret_99" not in mod._SENSITIVE_VALUES


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 4. 多个敏感值同时脱敏
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class TestMultipleSecrets:
    def test_multiple_secrets_all_masked(self):
        """多个不同敏感值应全部脱敏"""
        register_sensitive_value("private_key_aaaa")
        register_sensitive_value("api_secret_bbbb")
        register_sensitive_value("passphrase_cccc")

        msg = "pk=private_key_aaaa, secret=api_secret_bbbb, pass=passphrase_cccc"
        result = sanitize(msg)
        assert "private_key_aaaa" not in result
        assert "api_secret_bbbb" not in result
        assert "passphrase_cccc" not in result


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 5. sanitize_dict 递归脱敏
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class TestSanitizeDict:
    def test_dict_string_values_sanitized(self):
        """dict 中的字符串值应被脱敏"""
        register_sensitive_value("hidden_value_xyz")
        data = {"key": "the hidden_value_xyz is here", "number": 42}
        result = sanitize_dict(data)
        assert "hidden_value_xyz" not in result["key"]
        assert result["number"] == 42  # 非字符串不变

    def test_nested_dict_sanitized(self):
        """嵌套 dict 应递归脱敏"""
        register_sensitive_value("nested_secret_99")
        data = {"outer": {"inner": "nested_secret_99 found"}}
        result = sanitize_dict(data)
        assert "nested_secret_99" not in result["outer"]["inner"]

    def test_list_values_sanitized(self):
        """list 中的字符串应被脱敏"""
        register_sensitive_value("list_secret_abc")
        data = ["list_secret_abc is exposed", 123, None]
        result = sanitize_dict(data)
        assert "list_secret_abc" not in result[0]
        assert result[1] == 123
        assert result[2] is None

    def test_non_string_passthrough(self):
        """非字符串/dict/list 类型应原样返回"""
        assert sanitize_dict(42) == 42
        assert sanitize_dict(None) is None
        assert sanitize_dict(True) is True


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 6. 空输入边界
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class TestEdgeCases:
    def test_sanitize_empty_string(self):
        assert sanitize("") == ""

    def test_sanitize_none(self):
        assert sanitize(None) is None

    def test_sanitize_dict_empty_dict(self):
        assert sanitize_dict({}) == {}

    def test_sanitize_dict_empty_list(self):
        assert sanitize_dict([]) == []


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 7. 环境变量收集 (init_sensitive_values)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class TestInitFromEnv:
    def test_init_collects_matching_env_vars(self):
        """init_sensitive_values 应从环境变量收集匹配模式的值"""
        import common.log_sanitizer as mod
        # 设置测试环境变量
        os.environ["TEST_API_KEY"] = "test_key_123456"
        os.environ["TEST_API_SECRET"] = "test_secret_abcdef"
        os.environ["TEST_REGULAR_VAR"] = "not_sensitive"

        init_sensitive_values()

        # 检查敏感值被收集
        msg = "key=test_key_123456, secret=test_secret_abcdef, var=not_sensitive"
        result = sanitize(msg)
        assert "test_key_123456" not in result
        assert "test_secret_abcdef" not in result
        assert "not_sensitive" in result  # 非敏感名称不应被收集

        # 清理
        del os.environ["TEST_API_KEY"]
        del os.environ["TEST_API_SECRET"]
        del os.environ["TEST_REGULAR_VAR"]
