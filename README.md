<div align="center">

# ⚡ TradingFlow

### The Open-Source Workflow Engine for DeFi Trading

[![License](https://img.shields.io/badge/License-Sustainable%20Use-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/Python-3.11+-green.svg)](https://python.org)
[![Build](https://img.shields.io/badge/Build-Passing-brightgreen.svg)]()

**Build transparent, composable, and high-performance trading workflows for the decentralized world.**

[Getting Started](#-quick-start) • [Architecture](#-architecture) • [Node Catalog](#-built-in-nodes) • [Contributing](#-contributing)

</div>

---

## 🌟 Why TradingFlow?

In the rapidly evolving crypto landscape, traders and developers need tools that are **transparent**, **scalable**, and **maintainable**. TradingFlow is an open-source DAG-based workflow engine designed specifically for DeFi operations—giving you full control and visibility over your trading logic.

### ✨ Core Principles

| Principle | Description |
|-----------|-------------|
| 🔍 **Transparent** | Every node execution is logged and traceable. No black boxes—see exactly what happens at each step |
| 📈 **Horizontally Scalable** | Stateless workers with Redis coordination. Scale from 1 to 100+ instances seamlessly |
| ⚡ **High Performance** | Async-first architecture with sub-50ms node execution latency (p99) |
| 🛠️ **Easy to Maintain** | Clean separation of concerns. Modular nodes. Comprehensive logging and monitoring |
| 🔗 **Multi-Chain Native** | Built-in support for Aptos, Flow EVM, and EVM-compatible chains |
| 🧩 **Composable** | Mix and match 15+ node types to build complex trading strategies |

---

## 🏗️ Architecture

```
                                    ┌─────────────────────────────────────┐
                                    │           TradingFlow               │
                                    │         (Core Engine)               │
                                    └─────────────────────────────────────┘
                                                    │
                    ┌───────────────────────────────┼───────────────────────────────┐
                    │                               │                               │
                    ▼                               ▼                               ▼
            ┌─────────────┐                 ┌─────────────┐                 ┌─────────────┐
            │ Data Nodes  │                 │Process Nodes│                 │ Trade Nodes │
            │  (Binance,  │   ──Signals──►  │  (AI, Code, │   ──Signals──►  │   (Swap,    │
            │ GeckoTerm.) │                 │   Models)   │                 │   Vault)    │
            └─────────────┘                 └─────────────┘                 └─────────────┘
                    │                               │                               │
                    └───────────────────────────────┼───────────────────────────────┘
                                                    │
        ┌───────────────────────────────────────────┼───────────────────────────────────────────┐
        │                                           │                                           │
        ▼                                           ▼                                           ▼
┌───────────────┐                         ┌─────────────────┐                         ┌─────────────────┐
│     Redis     │                         │    RabbitMQ     │                         │   PostgreSQL    │
│ ─────────────  │                         │  ─────────────  │                         │  ─────────────  │
│ • State Store │                         │ • Task Queue   │                         │ • Execution    │
│ • Pub/Sub     │                         │ • Event Bus    │                         │   Logs         │
│ • Signal Cache│                         │ • Distributed  │                         │ • Flow History │
│ • Coordination│                         │   Messaging    │                         │ • Analytics    │
└───────────────┘                         └─────────────────┘                         └─────────────────┘
        │                                           │                                           │
        └───────────────────────────────────────────┼───────────────────────────────────────────┘
                                                    │
                                    ┌───────────────┴───────────────┐
                                    │                               │
                                    ▼                               ▼
                            ┌─────────────┐                 ┌─────────────┐
                            │  Worker 1   │      ...        │  Worker N   │
                            │ (Stateless) │                 │ (Stateless) │
                            └─────────────┘                 └─────────────┘

                            ▲ Horizontally Scalable - Add workers as needed ▲
```

### Why This Architecture?

| Component | Role | Scalability Benefit |
|-----------|------|---------------------|
| **Redis** | Coordination hub for distributed state, pub/sub signals, and caching | Enables stateless workers; supports Redis Cluster for HA |
| **RabbitMQ** | Reliable task distribution and event messaging | Decouples producers/consumers; handles backpressure |
| **PostgreSQL** | Persistent storage for execution logs and analytics | Query historical data; compliance and auditing |
| **Workers** | Stateless execution units that process nodes | Scale horizontally; zero-downtime deployments |

### Core Components

| Component | Purpose |
|-----------|---------|
| **Flow Scheduler** | Orchestrates DAG execution with cycle management, recovery, and multi-instance coordination |
| **Node Executor** | Manages node lifecycle, timeout handling, and signal routing |
| **Signal System** | Type-safe signal propagation with Redis persistence |
| **Vault Services** | Chain-specific integration for Aptos, Flow EVM trading |
| **Task Manager** | Multi-process task coordination with distributed state |

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
git clone https://github.com/TradingFlowAI/core-engine.git
cd core-engine

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

# Start PostgreSQL
docker run -d --name postgres \
  -p 5432:5432 \
  -e POSTGRES_USER=tradingflow \
  -e POSTGRES_PASSWORD=tradingflow \
  -e POSTGRES_DB=tradingflow \
  postgres:15-alpine

# Start RabbitMQ (optional, for distributed mode)
docker run -d --name rabbitmq \
  -p 5672:5672 -p 15672:15672 \
  -e RABBITMQ_DEFAULT_USER=guest \
  -e RABBITMQ_DEFAULT_PASS=guest \
  rabbitmq:3-management
```

### Run TradingFlow

```bash
python server.py
```

The engine will be available at `http://localhost:7002`.

---

## 📦 Built-in Nodes

TradingFlow ships with a rich collection of production-ready nodes:

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

## 📊 Performance & Scalability

| Metric | Value |
|--------|-------|
| Node Execution Latency | < 50ms (p99) |
| Signal Propagation | < 10ms |
| Concurrent Flows | 100+ per worker |
| Horizontal Scaling | Unlimited workers |
| Memory Footprint | ~256MB per worker |

### Scaling Guide

```bash
# Single instance (development)
python server.py

# Multiple workers (production)
# Worker 1
WORKER_ID=worker-1 python server.py --port 7002

# Worker 2
WORKER_ID=worker-2 python server.py --port 7003

# Worker N...
WORKER_ID=worker-n python server.py --port 700X
```

All workers share state via Redis and receive tasks from RabbitMQ, enabling true horizontal scaling.

---

## 🛠️ Extending TradingFlow

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
- [ ] Kubernetes Helm charts for production deployment

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

For commercial licensing, contact us at [cl@tradingflows.ai](mailto:cl@tradingflows.ai).

---

## 🔗 Links

- 🌐 [Website](https://tradingflows.ai)
- 📚 [Documentation](https://docs.tradingflows.ai)
- 💬 [Telegram](https://t.me/tradingflowai)
- 🐦 [Twitter](https://twitter.com/TradingFlowAI)
- 📧 [Contact](mailto:cl@tradingflows.ai)
- 🐙 [GitHub](https://github.com/TradingFlowAI)

---

## 👥 Credits

- **[@Morboz](https://github.com/Morboz)** - Core author. Designed and implemented the initial architecture.
- **[CL](https://github.com/TheCleopatra)** - Maintained and finalized the project over 6+ months of development.

---

<div align="center">

**Built with ❤️ by the TradingFlow Team**

*Empowering transparent and scalable trading in the decentralized world*

</div>
