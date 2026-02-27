"""
Test Suite: CredentialStore (R&D Mode)

测试场景：
1. 基本存储和检索
2. TTL 过期自动失效
3. 消费模式（单次使用后删除）
4. 非消费模式（可重复读取）
5. 不存在的 ref 返回 None
6. 多线程并发安全
7. cleanup_expired 批量清理
8. 单例模式验证
"""

import time
import threading
import pytest
from operators.credential_store import CredentialStore


@pytest.fixture(autouse=True)
def fresh_store():
    """每个测试前重置 CredentialStore 内部存储"""
    store = CredentialStore.get_instance()
    # 清空存储
    with store._store_lock:
        store._store.clear()
    yield store


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 1. 基本存储和检索
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class TestBasicOperations:
    def test_store_returns_uuid_string(self, fresh_store):
        """store() 应返回 UUID 字符串"""
        ref = fresh_store.store({"private_key": "0xabc123"})
        assert isinstance(ref, str)
        assert len(ref) == 36  # UUID4 格式: 8-4-4-4-12

    def test_retrieve_returns_stored_credentials(self, fresh_store):
        """retrieve() 应返回存储的原始凭证"""
        creds = {"api_key": "key123", "api_secret": "secret456"}
        ref = fresh_store.store(creds, ttl=60)
        result = fresh_store.retrieve(ref, consume=False)
        assert result == creds

    def test_store_multiple_independent(self, fresh_store):
        """多次存储应返回不同的 ref，内容独立"""
        ref1 = fresh_store.store({"key": "val1"})
        ref2 = fresh_store.store({"key": "val2"})
        assert ref1 != ref2
        assert fresh_store.retrieve(ref1, consume=False)["key"] == "val1"
        assert fresh_store.retrieve(ref2, consume=False)["key"] == "val2"


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 2. TTL 过期自动失效
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class TestTTL:
    def test_retrieve_within_ttl_works(self, fresh_store):
        """TTL 内检索应成功"""
        ref = fresh_store.store({"key": "value"}, ttl=10)
        result = fresh_store.retrieve(ref, consume=False)
        assert result is not None
        assert result["key"] == "value"

    def test_retrieve_after_ttl_returns_none(self, fresh_store):
        """TTL 过期后检索应返回 None"""
        ref = fresh_store.store({"key": "value"}, ttl=0.1)
        time.sleep(0.2)
        result = fresh_store.retrieve(ref)
        assert result is None

    def test_expired_entry_is_deleted(self, fresh_store):
        """TTL 过期后条目应被自动清理"""
        ref = fresh_store.store({"key": "value"}, ttl=0.1)
        assert fresh_store.size() == 1
        time.sleep(0.2)
        fresh_store.retrieve(ref)  # 触发删除
        assert fresh_store.size() == 0


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 3. 消费模式（单次使用后删除）
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class TestConsumeMode:
    def test_consume_true_deletes_after_first_retrieve(self, fresh_store):
        """consume=True 第一次检索后应删除条目"""
        ref = fresh_store.store({"pk": "0xdeadbeef"}, ttl=60)
        first = fresh_store.retrieve(ref, consume=True)
        assert first is not None
        assert first["pk"] == "0xdeadbeef"

        # 第二次检索应返回 None
        second = fresh_store.retrieve(ref, consume=True)
        assert second is None

    def test_consume_false_allows_multiple_reads(self, fresh_store):
        """consume=False 应允许多次检索"""
        ref = fresh_store.store({"pk": "0xdeadbeef"}, ttl=60)
        for _ in range(5):
            result = fresh_store.retrieve(ref, consume=False)
            assert result is not None
            assert result["pk"] == "0xdeadbeef"


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 4. 不存在的 ref
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class TestMissingRef:
    def test_retrieve_nonexistent_ref_returns_none(self, fresh_store):
        """不存在的 ref 应返回 None"""
        result = fresh_store.retrieve("nonexistent-uuid")
        assert result is None

    def test_retrieve_empty_string_ref_returns_none(self, fresh_store):
        """空字符串 ref 应返回 None"""
        result = fresh_store.retrieve("")
        assert result is None


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 5. 多线程并发安全
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class TestConcurrency:
    def test_concurrent_store_and_retrieve(self, fresh_store):
        """多线程同时 store + retrieve 不应 crash"""
        results = []
        errors = []

        def worker(i):
            try:
                ref = fresh_store.store({"thread": i}, ttl=10)
                time.sleep(0.01)
                cred = fresh_store.retrieve(ref, consume=True)
                results.append((i, cred))
            except Exception as e:
                errors.append((i, e))

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(20)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0, f"Errors: {errors}"
        assert len(results) == 20
        for i, cred in results:
            assert cred is not None
            assert cred["thread"] == i

    def test_concurrent_consume_only_one_succeeds(self, fresh_store):
        """多线程 consume 同一 ref，只有一个应成功"""
        ref = fresh_store.store({"secret": "unique"}, ttl=10)
        successes = []
        failures = []

        def consumer():
            result = fresh_store.retrieve(ref, consume=True)
            if result is not None:
                successes.append(result)
            else:
                failures.append(True)

        threads = [threading.Thread(target=consumer) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(successes) == 1  # 只有一个线程拿到凭证
        assert successes[0]["secret"] == "unique"
        assert len(failures) == 9


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 6. cleanup_expired 批量清理
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class TestCleanup:
    def test_cleanup_removes_expired_only(self, fresh_store):
        """cleanup 只删除过期条目，保留有效的"""
        ref_alive = fresh_store.store({"alive": True}, ttl=60)
        fresh_store.store({"expired1": True}, ttl=0.05)
        fresh_store.store({"expired2": True}, ttl=0.05)

        assert fresh_store.size() == 3
        time.sleep(0.1)

        removed = fresh_store.cleanup_expired()
        assert removed == 2
        assert fresh_store.size() == 1
        assert fresh_store.retrieve(ref_alive, consume=False)["alive"] is True


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 7. 单例模式验证
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class TestSingleton:
    def test_get_instance_returns_same_object(self):
        """get_instance() 应始终返回同一对象"""
        s1 = CredentialStore.get_instance()
        s2 = CredentialStore.get_instance()
        assert s1 is s2

    def test_constructor_also_returns_singleton(self):
        """直接构造也应返回同一对象"""
        s1 = CredentialStore()
        s2 = CredentialStore()
        assert s1 is s2
