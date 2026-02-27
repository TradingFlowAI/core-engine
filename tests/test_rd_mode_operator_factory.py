"""
Test Suite: OperatorFactory (R&D Mode)

测试场景：
1. is_operator_descriptor 判断
2. 缺少 _tf_operator 标记报错
3. 缺少 credential_ref 报错
4. credential_ref 过期/不存在报错
5. EVM operator 成功创建 (需 mock web3)
6. CEX operator 成功创建 (需 mock ccxt)
7. 未知 _type 报错
"""

import pytest
from unittest.mock import patch, MagicMock
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
# 1. is_operator_descriptor 判断
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class TestIsOperatorDescriptor:
    def test_valid_descriptor(self):
        assert OperatorFactory.is_operator_descriptor({
            "_tf_operator": True,
            "_type": "EvmOperator",
        }) is True

    def test_missing_flag(self):
        assert OperatorFactory.is_operator_descriptor({
            "_type": "EvmOperator",
        }) is False

    def test_flag_is_false(self):
        assert OperatorFactory.is_operator_descriptor({
            "_tf_operator": False,
        }) is False

    def test_not_a_dict(self):
        assert OperatorFactory.is_operator_descriptor("string") is False
        assert OperatorFactory.is_operator_descriptor(42) is False
        assert OperatorFactory.is_operator_descriptor(None) is False
        assert OperatorFactory.is_operator_descriptor([]) is False


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 2 & 3. 缺少标记 / 缺少 credential_ref
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class TestDescriptorValidation:
    @pytest.mark.asyncio
    async def test_missing_tf_operator_flag(self):
        """缺少 _tf_operator 标记应报 ValueError"""
        with pytest.raises(ValueError, match="missing _tf_operator"):
            await OperatorFactory.from_descriptor({"_type": "EvmOperator"})

    @pytest.mark.asyncio
    async def test_missing_credential_ref(self):
        """缺少 credential_ref 应报 ValueError"""
        with pytest.raises(ValueError, match="credential_ref"):
            await OperatorFactory.from_descriptor({
                "_tf_operator": True,
                "_type": "EvmOperator",
            })


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 4. credential_ref 过期/不存在
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class TestCredentialRetrieval:
    @pytest.mark.asyncio
    async def test_nonexistent_credential_ref(self):
        """不存在的 credential_ref 应报 RuntimeError"""
        with pytest.raises(RuntimeError, match="not found or expired"):
            await OperatorFactory.from_descriptor({
                "_tf_operator": True,
                "_type": "EvmOperator",
                "credential_ref": "nonexistent-uuid-1234",
                "chain_id": 56,
            })


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 5. EVM Operator 成功创建
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class TestEvmOperatorCreation:
    @pytest.mark.asyncio
    @patch("operators.evm_operator.Web3")
    @patch("operators.evm_operator.Account")
    async def test_evm_operator_from_descriptor(self, mock_account_cls, mock_web3_cls, fresh_store):
        """有效的 EVM descriptor 应创建 EvmOperator"""
        # Mock Account.from_key
        mock_account = MagicMock()
        mock_account.address = "0x1234567890abcdef1234567890abcdef12345678"
        mock_account_cls.from_key.return_value = mock_account

        # Mock Web3
        mock_w3_instance = MagicMock()
        mock_web3_cls.return_value = mock_w3_instance
        mock_web3_cls.HTTPProvider = MagicMock()

        # 存储凭证
        ref = fresh_store.store({"private_key": "0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"})

        descriptor = {
            "_tf_operator": True,
            "_type": "EvmOperator",
            "chain": "bsc",
            "chain_id": 56,
            "network_type": "evm",
            "credential_ref": ref,
        }

        operator = await OperatorFactory.from_descriptor(descriptor)

        from operators.evm_operator import EvmOperator
        assert isinstance(operator, EvmOperator)
        assert operator.chain_id == 56

    @pytest.mark.asyncio
    async def test_evm_missing_private_key(self, fresh_store):
        """EVM 凭证缺少 private_key 应报 ValueError"""
        ref = fresh_store.store({"wrong_field": "value"})
        with pytest.raises(ValueError, match="private_key"):
            await OperatorFactory.from_descriptor({
                "_tf_operator": True,
                "_type": "EvmOperator",
                "chain_id": 56,
                "credential_ref": ref,
            })

    @pytest.mark.asyncio
    async def test_evm_missing_chain_id(self, fresh_store):
        """EVM descriptor 缺少 chain_id 应报 ValueError"""
        ref = fresh_store.store({"private_key": "0xabc"})
        with pytest.raises(ValueError, match="chain_id"):
            await OperatorFactory.from_descriptor({
                "_tf_operator": True,
                "_type": "EvmOperator",
                "credential_ref": ref,
                # 没有 chain_id
            })


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 6. CEX Operator 成功创建
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class TestCexOperatorCreation:
    @pytest.mark.asyncio
    async def test_binance_operator_from_descriptor(self, fresh_store):
        """有效的 Binance descriptor 应创建 BinanceOperator"""
        ref = fresh_store.store({
            "api_key": "test_api_key_123456",
            "api_secret": "test_api_secret_abcdef",
        })

        descriptor = {
            "_tf_operator": True,
            "_type": "BinanceOperator",
            "exchange": "binance",
            "credential_ref": ref,
        }

        operator = await OperatorFactory.from_descriptor(descriptor)

        from operators.binance_operator import BinanceOperator
        assert isinstance(operator, BinanceOperator)

    @pytest.mark.asyncio
    async def test_okx_operator_from_descriptor(self, fresh_store):
        """OKX descriptor 应创建 BinanceOperator (使用 ccxt okx)"""
        ref = fresh_store.store({
            "api_key": "okx_key_123456",
            "api_secret": "okx_secret_abcdef",
            "passphrase": "okx_passphrase_xyz",
        })

        descriptor = {
            "_tf_operator": True,
            "_type": "OkxOperator",
            "exchange": "okx",
            "credential_ref": ref,
        }

        operator = await OperatorFactory.from_descriptor(descriptor)
        from operators.binance_operator import BinanceOperator
        assert isinstance(operator, BinanceOperator)

    @pytest.mark.asyncio
    async def test_cex_missing_api_key(self, fresh_store):
        """CEX 凭证缺少 api_key 应报 ValueError"""
        ref = fresh_store.store({"api_secret": "only_secret"})
        with pytest.raises(ValueError, match="api_key"):
            await OperatorFactory.from_descriptor({
                "_tf_operator": True,
                "_type": "BinanceOperator",
                "exchange": "binance",
                "credential_ref": ref,
            })


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 7. 未知 _type
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class TestUnknownOperatorType:
    @pytest.mark.asyncio
    async def test_unknown_type_raises(self, fresh_store):
        """未知的 _type 应报 ValueError"""
        ref = fresh_store.store({"key": "val"})
        with pytest.raises(ValueError, match="Unknown operator type"):
            await OperatorFactory.from_descriptor({
                "_tf_operator": True,
                "_type": "UnknownOperator",
                "credential_ref": ref,
            })
