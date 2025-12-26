"""
Infrastructure Connection Tests

Tests for Redis, PostgreSQL, and RabbitMQ connections.
Run with: python -m pytest tests/test_infra_connections.py -v
Or directly: python tests/test_infra_connections.py
"""

import asyncio
import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def test_config_loading():
    """Test that configuration loads correctly."""
    print("\n=== Test 1: Configuration Loading ===")
    try:
        from infra.config import CONFIG, get_station_config
        
        config = get_station_config()
        
        # Check essential config keys
        assert "REDIS_URL" in config, "REDIS_URL not in config"
        assert "POSTGRES_URL" in config, "POSTGRES_URL not in config"
        assert "RABBITMQ_URL" in config, "RABBITMQ_URL not in config"
        
        print(f"✓ REDIS_URL: {config['REDIS_URL'][:30]}...")
        print(f"✓ POSTGRES_HOST: {config.get('POSTGRES_HOST', 'not set')}")
        print(f"✓ RABBITMQ_HOST: {config.get('RABBITMQ_HOST', 'not set')}")
        print("✓ Configuration loaded successfully!")
        return True
    except Exception as e:
        print(f"✗ Configuration loading failed: {e}")
        return False


def test_redis_connection():
    """Test Redis connection."""
    print("\n=== Test 2: Redis Connection ===")
    try:
        from infra.utils.redis_manager import RedisManager, get_redis_client
        
        # Get Redis client
        client = get_redis_client()
        
        # Test ping
        result = client.ping()
        assert result is True, "Redis ping failed"
        print("✓ Redis ping successful")
        
        # Test set/get
        test_key = "infra_test_key"
        test_value = "hello_from_infra"
        client.set(test_key, test_value, ex=60)
        retrieved = client.get(test_key)
        assert retrieved == test_value, f"Expected {test_value}, got {retrieved}"
        print(f"✓ Redis set/get successful: {test_key}={retrieved}")
        
        # Cleanup
        client.delete(test_key)
        print("✓ Redis connection test passed!")
        return True
    except Exception as e:
        print(f"✗ Redis connection failed: {e}")
        return False


def test_postgres_connection():
    """Test PostgreSQL connection."""
    print("\n=== Test 3: PostgreSQL Connection ===")
    try:
        from infra.db.base import get_engine, db_session
        from sqlalchemy import text
        
        # Test engine creation
        engine = get_engine()
        print(f"✓ Engine created: {engine.url}")
        
        # Test connection with a simple query
        with db_session() as session:
            result = session.execute(text("SELECT 1 as test"))
            row = result.fetchone()
            assert row[0] == 1, "Query result mismatch"
            print("✓ PostgreSQL query successful: SELECT 1 = 1")
        
        # Test table listing
        with db_session() as session:
            result = session.execute(text("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                LIMIT 5
            """))
            tables = [row[0] for row in result.fetchall()]
            print(f"✓ Found {len(tables)} tables: {tables[:3]}...")
        
        print("✓ PostgreSQL connection test passed!")
        return True
    except Exception as e:
        print(f"✗ PostgreSQL connection failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_rabbitmq_connection():
    """Test RabbitMQ connection."""
    print("\n=== Test 4: RabbitMQ Connection ===")
    
    async def _test_rabbitmq():
        try:
            from infra.mq.connection_pool import connection_pool
            from infra.config import CONFIG
            
            rabbitmq_url = CONFIG.get("RABBITMQ_URL")
            print(f"✓ RabbitMQ URL: {rabbitmq_url[:30]}...")
            
            # Get connection from pool
            connection = await connection_pool.get_connection(rabbitmq_url)
            assert not connection.is_closed, "Connection is closed"
            print("✓ RabbitMQ connection established")
            
            # Get channel
            channel = await connection_pool.get_channel(rabbitmq_url)
            assert not channel.is_closed, "Channel is closed"
            print("✓ RabbitMQ channel created")
            
            # Test exchange declaration
            exchange = await channel.declare_exchange(
                "test_infra_exchange", 
                "topic",
                auto_delete=True
            )
            print(f"✓ Exchange declared: {exchange.name}")
            
            # Cleanup
            await channel.close()
            print("✓ RabbitMQ connection test passed!")
            return True
            
        except Exception as e:
            print(f"✗ RabbitMQ connection failed: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    return asyncio.run(_test_rabbitmq())


def test_model_imports():
    """Test that all models can be imported."""
    print("\n=== Test 5: Model Imports ===")
    try:
        from infra.db.models import (
            FlowExecutionLog,
            FlowExecutionSignal,
            MonitoredToken,
            TokenPriceHistory,
            VaultOperationHistory,
            OperationType,
        )
        
        print("✓ FlowExecutionLog imported")
        print("✓ FlowExecutionSignal imported")
        print("✓ MonitoredToken imported")
        print("✓ TokenPriceHistory imported")
        print("✓ VaultOperationHistory imported")
        print("✓ OperationType imported")
        
        # Verify table names
        assert FlowExecutionLog.__tablename__ == "flow_execution_logs"
        assert FlowExecutionSignal.__tablename__ == "flow_execution_signals"
        assert MonitoredToken.__tablename__ == "monitored_tokens"
        
        print("✓ All model imports successful!")
        return True
    except Exception as e:
        print(f"✗ Model import failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_service_imports():
    """Test that all services can be imported."""
    print("\n=== Test 6: Service Imports ===")
    try:
        from infra.db.services import (
            FlowExecutionLogService,
            FlowExecutionSignalService,
            MonitoredTokenService,
            TokenPriceHistoryService,
            VaultOperationHistoryService,
        )
        
        print("✓ FlowExecutionLogService imported")
        print("✓ FlowExecutionSignalService imported")
        print("✓ MonitoredTokenService imported")
        print("✓ TokenPriceHistoryService imported")
        print("✓ VaultOperationHistoryService imported")
        
        # Test service instantiation
        log_service = FlowExecutionLogService()
        print("✓ FlowExecutionLogService instantiated")
        
        print("✓ All service imports successful!")
        return True
    except Exception as e:
        print(f"✗ Service import failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def run_all_tests():
    """Run all infrastructure tests."""
    print("=" * 60)
    print("TradingFlow Infrastructure Connection Tests")
    print("=" * 60)
    
    results = {
        "config": test_config_loading(),
        "redis": test_redis_connection(),
        "postgres": test_postgres_connection(),
        "rabbitmq": test_rabbitmq_connection(),
        "models": test_model_imports(),
        "services": test_service_imports(),
    }
    
    print("\n" + "=" * 60)
    print("Test Results Summary")
    print("=" * 60)
    
    passed = sum(1 for v in results.values() if v)
    total = len(results)
    
    for name, result in results.items():
        status = "✓ PASSED" if result else "✗ FAILED"
        print(f"  {name.upper():12} {status}")
    
    print("-" * 60)
    print(f"Total: {passed}/{total} tests passed")
    print("=" * 60)
    
    return all(results.values())


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)
