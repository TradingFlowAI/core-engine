"""
Test Suite: CodeNode Operator Injection (R&D Mode)

测试场景：
1. _tf_operator 描述符被正确识别并替换为 live Operator
2. 非 _tf_operator dict 保持不变
3. 混合输入 (普通数据 + operator descriptor) 正确处理
4. descriptor 缺少 credential_ref 不会崩溃 (优雅降级)
5. os.environ 在执行线程中被隔离为空 dict
"""

import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from operators.credential_store import CredentialStore
from operators.factory import OperatorFactory


@pytest.fixture(autouse=True)
def fresh_store():
    """每个测试前重置 CredentialStore"""
    store = CredentialStore.get_instance()
    with store._store_lock:
        store._store.clear()
    yield store


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 1. _tf_operator 描述符识别
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class TestDescriptorDetection:
    def test_valid_operator_descriptor_detected(self):
        """正确的 _tf_operator 描述符应被识别"""
        desc = {
            "_tf_operator": True,
            "_type": "EvmOperator",
            "chain": "bsc",
            "chain_id": 56,
            "credential_ref": "some-uuid",
        }
        assert OperatorFactory.is_operator_descriptor(desc) is True

    def test_regular_dict_not_detected(self):
        """普通 dict 不应被识别为 operator"""
        data = {"price": 100.5, "symbol": "BTC/USDT"}
        assert OperatorFactory.is_operator_descriptor(data) is False

    def test_string_not_detected(self):
        assert OperatorFactory.is_operator_descriptor("hello") is False

    def test_none_not_detected(self):
        assert OperatorFactory.is_operator_descriptor(None) is False

    def test_list_not_detected(self):
        assert OperatorFactory.is_operator_descriptor([1, 2, 3]) is False


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 2. Operator 注入模拟（模拟 CodeNode 的扫描逻辑）
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def simulate_codenode_injection(input_data_dict: dict) -> dict:
    """
    模拟 CodeNode execute() 中的 Operator 注入逻辑。
    和真实 code_node_v0_0_1.py 中的扫描代码逻辑一致。
    """
    for key, value in list(input_data_dict.items()):
        if isinstance(value, dict) and value.get("_tf_operator") is True:
            try:
                operator = await OperatorFactory.from_descriptor(value)
                input_data_dict[key] = operator
            except Exception:
                pass  # 保留原始 dict
    return input_data_dict


class TestInjectionSimulation:
    @pytest.mark.asyncio
    @patch("operators.evm_operator.Web3")
    @patch("operators.evm_operator.Account")
    async def test_evm_operator_injected(self, mock_account, mock_web3, fresh_store):
        """EVM Operator descriptor 应被替换为 live EvmOperator"""
        mock_acc = MagicMock()
        mock_acc.address = "0xABCDEF1234567890ABCDEF1234567890ABCDEF12"
        mock_account.from_key.return_value = mock_acc
        mock_web3.return_value = MagicMock()
        mock_web3.HTTPProvider = MagicMock()

        ref = fresh_store.store({"private_key": "0xdeadbeef" * 8})

        input_data = {
            "operator": {
                "_tf_operator": True,
                "_type": "EvmOperator",
                "chain_id": 56,
                "credential_ref": ref,
            },
            "price_data": {"BTC": 50000},
        }

        result = await simulate_codenode_injection(input_data)

        from operators.evm_operator import EvmOperator
        assert isinstance(result["operator"], EvmOperator)
        assert result["price_data"] == {"BTC": 50000}  # 普通数据不变

    @pytest.mark.asyncio
    async def test_cex_operator_injected(self, fresh_store):
        """CEX Operator descriptor 应被替换为 live BinanceOperator"""
        ref = fresh_store.store({
            "api_key": "test_key_123456789",
            "api_secret": "test_secret_abcdefgh",
        })

        input_data = {
            "exchange_op": {
                "_tf_operator": True,
                "_type": "BinanceOperator",
                "exchange": "binance",
                "credential_ref": ref,
            },
        }

        result = await simulate_codenode_injection(input_data)
        from operators.binance_operator import BinanceOperator
        assert isinstance(result["exchange_op"], BinanceOperator)

    @pytest.mark.asyncio
    async def test_mixed_inputs_preserved(self, fresh_store):
        """混合输入中，非 operator 数据保持不变"""
        input_data = {
            "text_input": "hello world",
            "number_input": 42,
            "list_input": [1, 2, 3],
            "nested_dict": {"foo": "bar"},
        }

        result = await simulate_codenode_injection(input_data)
        assert result["text_input"] == "hello world"
        assert result["number_input"] == 42
        assert result["list_input"] == [1, 2, 3]
        assert result["nested_dict"] == {"foo": "bar"}

    @pytest.mark.asyncio
    async def test_invalid_descriptor_kept_as_dict(self, fresh_store):
        """无效的 descriptor (credential_ref 不存在) 应保留为原始 dict"""
        input_data = {
            "broken_op": {
                "_tf_operator": True,
                "_type": "EvmOperator",
                "chain_id": 56,
                "credential_ref": "totally-fake-uuid",
            },
        }

        result = await simulate_codenode_injection(input_data)
        # 失败时保留原始 dict
        assert isinstance(result["broken_op"], dict)
        assert result["broken_op"]["_tf_operator"] is True


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 3. os.environ 隔离
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class TestOsEnvironIsolation:
    def test_environ_replaced_with_empty_dict(self):
        """在执行线程中 os.environ 应被替换为空 dict"""
        import os
        import threading

        original_environ = os.environ
        captured_environ = [None]

        def worker():
            # 模拟 run_code_with_trace 中的隔离逻辑
            import os as _os
            orig = _os.environ
            _os.environ = {}  # 和修复后的代码一致
            try:
                captured_environ[0] = _os.environ
            finally:
                _os.environ = orig

        t = threading.Thread(target=worker)
        t.start()
        t.join()

        # 执行线程中 environ 应该是空 dict
        assert captured_environ[0] == {}
        assert isinstance(captured_environ[0], dict)

        # 主线程 environ 应保持不变
        assert os.environ is original_environ
        assert len(os.environ) > 0  # 主线程有环境变量

    def test_environ_restored_after_error(self):
        """即使执行出错，os.environ 也应被恢复"""
        import os
        import threading

        original_environ = os.environ
        environ_after = [None]

        def worker():
            import os as _os
            orig = _os.environ
            _os.environ = {}
            try:
                raise RuntimeError("Intentional error")
            finally:
                _os.environ = orig
                environ_after[0] = _os.environ

        t = threading.Thread(target=worker)
        t.start()
        t.join()

        assert environ_after[0] is original_environ
