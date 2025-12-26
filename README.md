<div align="center">

# ⚡ TradingFlow Station

### The High-Performance Workflow Execution Engine for DeFi

[![License](https://img.shields.io/badge/License-Sustainable%20Use-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/Python-3.11+-green.svg)](https://python.org)
[![Build](https://img.shields.io/badge/Build-Passing-brightgreen.svg)]()

**Build transparent, composable, and lightning-fast trading workflows for the decentralized world.**

[Getting Started](#-quick-start) • [Documentation](#-documentation) • [Node Catalog](#-built-in-nodes) • [Contributing](#-contributing)

</div>

---

## 🌟 Why TradingFlow Station?

In the rapidly evolving crypto landscape, traders need tools that are **fast**, **transparent**, and **composable**. TradingFlow Station is the execution backbone of the TradingFlow ecosystem—a DAG-based workflow engine designed specifically for DeFi operations.

### ✨ Key Features

| Feature | Description |
|---------|-------------|
| 🚀 **High Performance** | Async-first architecture with sub-second node execution latency |
| 🔗 **Multi-Chain Native** | Built-in support for Aptos, Flow EVM, and EVM-compatible chains |
| 🧩 **Composable Nodes** | Mix and match 15+ node types to build complex trading strategies |
| 📊 **Real-Time Signals** | Redis-powered pub/sub for instant signal propagation between nodes |
| 🔒 **Vault Integration** | Seamless connection to on-chain vault smart contracts |
| 🌐 **Transparent Execution** | Every action logged, every trade traceable |

---

## 🏗️ Architecture

```
                           ┌─────────────────────────────────────┐
                           │         TradingFlow Station         │
                           └─────────────────────────────────────┘
                                            │
           ┌────────────────────────────────┼────────────────────────────────┐
           │                                │                                │
           ▼                                ▼                                ▼
    ┌─────────────┐                 ┌─────────────┐                 ┌─────────────┐
    │ Price Nodes │                 │  AI Nodes   │                 │ Trade Nodes │
    │  (Binance,  │    ────────►    │  (Models,   │    ────────►    │   (Swap,    │
    │ GeckoTerm.) │     Signals     │   Code)     │     Signals     │    Vault)   │
    └─────────────┘                 └─────────────┘                 └─────────────┘
           │                                │                                │
           └────────────────────────────────┼────────────────────────────────┘
                                            │
                           ┌────────────────┴────────────────┐
                           │        Message Queue            │
                           │    (RabbitMQ / Redis Pub/Sub)   │
                           └─────────────────────────────────┘
```

### Core Components

| Component | Purpose |
|-----------|---------|
| **Flow Scheduler** | Orchestrates DAG execution with cycle management and recovery |
| **Node Executor** | Manages node lifecycle, timeout handling, and signal routing |
| **Signal System** | Type-safe signal propagation with Redis persistence |
| **Vault Services** | Chain-specific integration for Aptos, Flow EVM trading |
| **Task Manager** | Multi-process task coordination with state persistence |

---

## 🚀 Quick Start

### Prerequisites

- Python 3.11+
- Redis 6+
- RabbitMQ 3.x (optional, for distributed mode)
- PostgreSQL 14+ (for log persistence)

### Installation

```bash
# Clone the repository
git clone https://github.com/tradingflow/station.git
cd station

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### Start Infrastructure

```bash
# Start Redis
docker run -d --name redis -p 6379:6379 redis:alpine

# Start RabbitMQ (optional)
docker run -d --name rabbitmq \
  -p 5672:5672 -p 15672:15672 \
  -e RABBITMQ_DEFAULT_USER=guest \
  -e RABBITMQ_DEFAULT_PASS=guest \
  rabbitmq:3-management
```

### Run the Station

```bash
python server.py
```

The Station will be available at `http://localhost:7002`.

---

## 📦 Built-in Nodes

TradingFlow Station ships with a rich collection of production-ready nodes:

### 📈 Data Nodes
| Node | Description |
|------|-------------|
| `binance_price_node` | Fetch real-time and historical price data from Binance |
| `price_node` | Multi-source price aggregation with caching |
| `rootdata_node` | On-chain data fetching from RootData |
| `gsheet_node` | Google Sheets integration for data I/O |
| `dataset_node` | Load and process custom datasets |

### 🤖 Processing Nodes
| Node | Description |
|------|-------------|
| `ai_model_node` | Run AI/ML models for signal generation |
| `code_node` | Execute custom Python code with sandboxing |
| `x_listener_node` | Monitor Twitter/X for social signals |
| `rsshub_node` | RSS feed aggregation and filtering |

### 💱 Trading Nodes
| Node | Description |
|------|-------------|
| `swap_node` | DEX swaps on Aptos, Flow EVM with slippage protection |
| `buy_node` | Simplified buy operations with base token specification |
| `sell_node` | Simplified sell operations with target token specification |
| `vault_node` | Direct vault contract interactions |

### 📢 Output Nodes
| Node | Description |
|------|-------------|
| `telegram_sender_node` | Send notifications to Telegram channels |

---

## 🔌 API Reference

### Flow Management

```bash
# Execute a new flow
curl -X POST http://localhost:7002/flows/execute \
  -H "Content-Type: application/json" \
  -d '{
    "flow_id": "my_strategy",
    "cycle_interval": "5m",
    "flow_json": {
      "nodes": [...],
      "edges": [...]
    }
  }'

# Get flow status
curl http://localhost:7002/flows/my_strategy/status

# Stop a running flow
curl -X POST http://localhost:7002/flows/my_strategy/stop
```

### Node Execution

```bash
# Execute a single node
curl -X POST http://localhost:7002/nodes/execute \
  -H "Content-Type: application/json" \
  -d '{
    "flow_id": "test_flow",
    "node_id": "price_check",
    "node_type": "binance_price_node",
    "config": {
      "symbol": "BTCUSDT",
      "interval": "1h"
    }
  }'

# Check node status
curl http://localhost:7002/nodes/price_check/status
```

### Health Check

```bash
curl http://localhost:7002/health
```

---

## 🛠️ Extending Station

### Creating Custom Nodes

```python
from nodes.node_base import NodeBase

class MyCustomNode(NodeBase):
    """
    A custom node that processes data in your unique way.
    """
    
    # Define input/output handles
    INPUTS = {
        "data_in": {"type": "any", "description": "Input data"}
    }
    
    OUTPUTS = {
        "result_out": {"type": "any", "description": "Processed result"}
    }
    
    async def execute(self) -> dict:
        # Get input from connected nodes
        input_data = await self.get_input("data_in")
        
        # Your custom logic here
        result = self.process(input_data)
        
        # Emit output signal
        await self.emit_signal("result_out", result)
        
        return {"status": "success", "result": result}
```

### Registering Your Node

```python
# In core/init_builtin_nodes.py
from nodes.my_custom_node import MyCustomNode

def init_builtin_nodes():
    # ... existing nodes ...
    NodeRegistry.register("my_custom_node", MyCustomNode)
```

---

## 📊 Performance

| Metric | Value |
|--------|-------|
| Node Execution Latency | < 50ms (p99) |
| Signal Propagation | < 10ms |
| Concurrent Flows | 100+ |
| Memory Footprint | ~256MB base |

---

## 🧪 Testing

```bash
# Run all tests
pytest tests/

# Run specific test module
pytest tests/test_node_base_status.py -v

# Run with coverage
pytest tests/ --cov=. --cov-report=html
```

---

## 📖 Documentation

- [Node Development Guide](docs/)
- [Signal System Design](nodes/docs/SIGNAL_SYSTEM_DESIGN.md)
- [Swap Node Documentation](nodes/docs/swap_node_doc.md)
- [Credits Integration](docs/CREDITS_INTEGRATION.md)
- [Quest Integration](docs/QUEST_INTEGRATION_STATION.md)

---

## 🤝 Contributing

We welcome contributions from the community! Here's how you can help:

1. **Fork** the repository
2. **Create** a feature branch (`git checkout -b feature/amazing-node`)
3. **Commit** your changes (`git commit -m 'Add amazing node'`)
4. **Push** to the branch (`git push origin feature/amazing-node`)
5. **Open** a Pull Request

Please read our [Contributing Guidelines](CONTRIBUTING.md) before submitting.

---

## 🗺️ Roadmap

- [ ] WebSocket real-time updates
- [ ] More DEX integrations (Uniswap V4, Curve, Balancer)
- [ ] Strategy backtesting framework
- [ ] Visual flow builder integration
- [ ] Plugin marketplace for community nodes

---

## 📜 License

This project is licensed under the **TradingFlow Sustainable Use License** - see the [LICENSE](LICENSE) file for details.

### License Summary

| Use Case | License Required? |
|----------|-------------------|
| 👤 Individual / Personal use | ❌ **Free** |
| 🎓 Non-profit / Educational | ❌ **Free** |
| 🏢 Small business (<$1M revenue/funding), internal use only | ❌ **Free** |
| 🏛️ Business with ≥$1M revenue or funding | ✅ Commercial License |
| 🌐 Providing services to third parties (any size) | ✅ Commercial License |
| ☁️ SaaS / Hosted service offering | ✅ Commercial License |

**TL;DR**: Free for individuals and small teams. Enterprises and service providers need a commercial license.

For commercial licensing, contact us at [license@tradingflow.xyz](mailto:license@tradingflow.xyz).

---

## 🔗 Links

- 🌐 [TradingFlow Website](https://tradingflow.xyz)
- 📚 [Documentation](https://docs.tradingflow.xyz)
- 💬 [Discord Community](https://discord.gg/tradingflow)
- 🐦 [Twitter](https://twitter.com/tradingflow)

---

<div align="center">

**Built with ❤️ by the TradingFlow Team**

*Empowering transparent trading in the decentralized world*

</div>
