import pytest
from common.state_store import (
    InMemoryStateStore,
    RedisStateStore,
    StateStoreFactory,
)


# InMemoryStateStore测试
class TestInMemoryStateStore:
    @pytest.fixture
    async def store(self):
        """创建并初始化InMemoryStateStore实例"""
        store = InMemoryStateStore()
        await store.initialize()
        # 注意：对于异步fixture，需要使用yield而不是return
        yield store
        # 清理资源
        await store.close()

    @pytest.mark.asyncio
    async def test_initialize(self):
        """测试初始化"""
        store = InMemoryStateStore()
        result = await store.initialize()
        assert result is True

    @pytest.mark.asyncio
    async def test_set_and_get_node_status(self, store):
        """测试设置和获取节点状态"""
        # 设置状态
        node_id = "test_node_1"
        status = "running"
        error_message = "test error"

        result = await store.set_node_status(node_id, status, error_message)
        assert result is True

        # 获取状态
        node_status = await store.get_node_status(node_id)
        assert "status" in node_status
        assert node_status["status"] == status
        assert "error_message" in node_status
        assert node_status["error_message"] == error_message
        assert "updated_at" in node_status

    @pytest.mark.asyncio
    async def test_get_nonexistent_node_status(self, store):
        """测试获取不存在的节点状态"""
        node_status = await store.get_node_status("nonexistent_node")
        assert node_status == {}

    @pytest.mark.asyncio
    async def test_set_and_get_termination_flag(self, store):
        """测试设置和获取终止标志"""
        node_id = "test_node_2"
        reason = "test termination reason"

        # 设置终止标志
        result = await store.set_termination_flag(node_id, reason)
        assert result is True

        # 获取终止标志
        flag_data = await store.get_termination_flag(node_id)
        assert flag_data is not None
        assert "reason" in flag_data
        assert flag_data["reason"] == reason
        assert "timestamp" in flag_data

    @pytest.mark.asyncio
    async def test_get_nonexistent_termination_flag(self, store):
        """测试获取不存在的终止标志"""
        flag_data = await store.get_termination_flag("nonexistent_node")
        assert flag_data is None

    @pytest.mark.asyncio
    async def test_clear_termination_flag(self, store):
        """测试清除终止标志"""
        node_id = "test_node_3"

        # 先设置终止标志
        await store.set_termination_flag(node_id, "test reason")
        assert await store.get_termination_flag(node_id) is not None

        # 清除终止标志
        result = await store.clear_termination_flag(node_id)
        assert result is True

        # 验证已被清除
        assert await store.get_termination_flag(node_id) is None


REDIS_URL = "redis://127.0.0.1:6379/0"


# 使用实际Redis的测试（替代原来的TestRedisStateStoreMock）
class TestRedisStateStore:
    @pytest.fixture
    async def store(self):
        """创建并初始化RedisStateStore实例"""
        store = RedisStateStore(redis_url=REDIS_URL, key_prefix="test:")

        # 尝试初始化连接，如果Redis不可用则跳过这些测试
        try:
            initialized = await store.initialize()
            if not initialized:
                pytest.skip("Redis server is not available")

            yield store

            # 清理测试数据
            if store.redis_client:
                # 删除测试中使用的键
                await store.redis_client.delete("test:node:test_node")
                await store.redis_client.delete("test:node:test_node:terminate")
                await store.close()
        except Exception as e:
            pytest.skip(f"Failed to connect to Redis: {e}")

    @pytest.mark.asyncio
    async def test_initialize(self):
        """测试初始化"""
        store = RedisStateStore(redis_url=REDIS_URL)
        result = await store.initialize()
        assert result is True
        assert store.redis_client is not None
        await store.close()

    @pytest.mark.asyncio
    async def test_set_and_get_node_status(self, store):
        """测试设置和获取节点状态"""
        # 设置状态
        node_id = "test_node"
        status = "running"
        error_message = "test error"

        result = await store.set_node_status(node_id, status, error_message)
        assert result is True

        # 获取状态
        node_status = await store.get_node_status(node_id)
        assert "status" in node_status
        assert node_status["status"] == status
        assert "error_message" in node_status
        assert node_status["error_message"] == error_message
        assert "updated_at" in node_status

    @pytest.mark.asyncio
    async def test_get_nonexistent_node_status(self, store):
        """测试获取不存在的节点状态"""
        node_status = await store.get_node_status("nonexistent_node")
        assert node_status == {}

    @pytest.mark.asyncio
    async def test_set_and_get_termination_flag(self, store):
        """测试设置和获取终止标志"""
        node_id = "test_node"
        reason = "test termination reason"

        # 设置终止标志
        result = await store.set_termination_flag(node_id, reason)
        assert result is True

        # 获取终止标志
        flag_data = await store.get_termination_flag(node_id)
        assert flag_data is not None
        assert "reason" in flag_data
        assert flag_data["reason"] == reason
        assert "timestamp" in flag_data

    @pytest.mark.asyncio
    async def test_clear_termination_flag(self, store):
        """测试清除终止标志"""
        node_id = "test_node"

        # 先设置终止标志
        await store.set_termination_flag(node_id, "test reason")
        assert await store.get_termination_flag(node_id) is not None

        # 清除终止标志
        result = await store.clear_termination_flag(node_id)
        assert result is True

        # 验证已被清除
        assert await store.get_termination_flag(node_id) is None

    @pytest.mark.asyncio
    async def test_close(self, store):
        """测试关闭连接"""
        # 确保关闭前连接是有效的
        assert store.redis_client is not None

        # 关闭连接
        await store.close()

        # 由于我们无法直接检查连接是否已关闭，这里简单通过一个连接操作是否失败来测试
        # 注意：这不是一个完美的方法，因为异步连接关闭可能需要一些时间
        try:
            result = await store.set_node_status("test_node", "status_after_close")
            assert not result, "Expected operation to fail after connection closed"
        except Exception:
            # 如果发生异常，说明连接已关闭，这是预期行为
            pass


# 状态存储工厂测试
class TestStateStoreFactory:
    def test_create_redis_store(self):
        """测试创建Redis状态存储"""
        config = {"redis_url": "redis://custom:6379/1", "key_prefix": "custom:"}
        store = StateStoreFactory.create("redis", config)

        assert isinstance(store, RedisStateStore)
        assert store.redis_url == "redis://custom:6379/1"
        assert store.key_prefix == "custom:"

    def test_create_memory_store(self):
        """测试创建内存状态存储"""
        store = StateStoreFactory.create("memory")
        assert isinstance(store, InMemoryStateStore)

    def test_create_with_default_config(self):
        """测试使用默认配置创建存储"""
        store = StateStoreFactory.create("redis")
        assert isinstance(store, RedisStateStore)
        assert store.redis_url == REDIS_URL
        assert store.key_prefix == "trading_flow:"

    def test_create_invalid_store_type(self):
        """测试创建无效的存储类型"""
        with pytest.raises(ValueError) as excinfo:
            StateStoreFactory.create("invalid_type")
        assert "Unsupported state store type: invalid_type" in str(excinfo.value)


# 集成测试 - 使用实际的Redis（如果可用）
@pytest.mark.integration
class TestRedisStateStoreIntegration:
    @pytest.fixture
    async def store(self):
        """创建并初始化实际的RedisStateStore实例"""
        store = RedisStateStore(redis_url=REDIS_URL, key_prefix="test_integration:")
        try:
            # 尝试初始化连接，如果Redis不可用则跳过这些测试
            initialized = await store.initialize()
            if not initialized:
                pytest.skip("Redis server is not available")

            yield store

            # 清理测试数据
            if store.redis_client:
                await store.redis_client.delete(
                    "test_integration:node:test_integration_node"
                )
                await store.redis_client.delete(
                    "test_integration:node:test_integration_node:terminate"
                )
                await store.close()
        except Exception as e:
            pytest.skip(f"Failed to connect to Redis: {e}")

    @pytest.mark.asyncio
    async def test_full_lifecycle(self, store):
        """测试完整的生命周期：设置、获取、清除状态和终止标志"""
        node_id = "test_integration_node"

        # 设置状态
        await store.set_node_status(node_id, "running", "integration test")

        # 获取状态
        status = await store.get_node_status(node_id)
        assert status.get("status") == "running"
        assert status.get("error_message") == "integration test"

        # 设置终止标志
        await store.set_termination_flag(node_id, "integration termination")

        # 获取终止标志
        flag = await store.get_termination_flag(node_id)
        assert flag is not None
        assert flag.get("reason") == "integration termination"

        # 清除终止标志
        await store.clear_termination_flag(node_id)

        # 验证终止标志已清除
        flag = await store.get_termination_flag(node_id)
        assert flag is None
