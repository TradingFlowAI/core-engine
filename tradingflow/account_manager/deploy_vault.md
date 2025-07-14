2025-05-26 15:49:49,693 - python.account_manager.utils.token_price_util - DEBUG - 获取到代币价格 - 地址: 0x7314aeec874a25a1131f49da9679d05f8d931175, USD 价格: 0.99
2025-05-26 15:49:49,693 - python.account_manager.utils.token_price_util - DEBUG - 获取到代币价格 - 地址: 0xd604c06206f6dedd82d42f90d1f5bb34a2e7c5dd, USD 价格: 1.59

```
# cmd1
# 1. 启动本地的hardhat节点
npx hardhat node

```

```
# cmd2
# 2. 初始化token合约, 记录部署的合约地址
python scripts/manage_pool_and_swap_by_vault.py
# 3. 使用vault cli 转账TKA到投资者地址
# python scripts/vault_cli.py transfer {token} {recip} {amount}
python scripts/vault_cli.py transfer 0x7314AEeC874A25A1131F49dA9679D05f8d931175 0x70997970C51812dc3A010C7d01b50e0d17dc79C8 1000

```

```
# cmd3

./start_services.sh
```

```
# cmd6
# 7. 创建vault
curl -X POST http://localhost:8001/evm/vaults/create \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_AUTH_TOKEN" \
  -d '{
    "asset_address": "0x7314AEeC874A25A1131F49dA9679D05f8d931175",
    "investor_address": "0x70997970C51812dc3A010C7d01b50e0d17dc79C8",
    "chain_id": 31337
  }'

curl -X GET http://localhost:8001/evm/vaults/tasks/YOUR_TASK_ID \
  -H "Authorization: Bearer YOUR_AUTH_TOKEN"

curl -X GET http://localhost:8001/evm/vaults/31337/0xcb65A63B74F009A1C724AC221601d7DDD90f67C2
```

```
# cmd2
# 8. 使用vault cli deposit
python scripts/vault_cli.py --vault 0xcb65A63B74F009A1C724AC221601d7DDD90f67C2 --account 0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d  deposit 100
# 10. 设置交易币对 TKB/TKA
python scripts/vault_cli.py --vault 0xcb65A63B74F009A1C724AC221601d7DDD90f67C2  --account 0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d pair 0xD604C06206f6DeDd82d42F90D1F5bB34a2E7c5dd  --max 100%
# 10.4 启动策略
python scripts/vault_cli.py --vault 0xcb65A63B74F009A1C724AC221601d7DDD90f67C2  --account 0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d  strategy --enabled true

```

```
# cmd7
# 11 启动worker node
cd python
python -m py_worker.server

```

```
# cmd6
# 12 执行一个buy node

curl -X POST http://localhost:7000/nodes/execute \
  -H "Content-Type: application/json" \
  -d '{
    "flow_id": "trading_flow",
    "component_id": 2,
    "cycle": 1,
    "node_id": "uniswap_trade_node1",
    "node_type": "dex_trade_node",
    "input_edges": [],
    "output_edges": [
      {
        "source": "uniswap_trade_node1",
        "source_handle": "output",
        "target": "trade_notification_node",
        "target_handle": "tx_hash"
      },
      {
        "source": "uniswap_trade_node1",
        "source_handle": "output",
        "target": "tx_explorer_node",
        "target_handle": "tx_hash"
      }
    ],
    "config": {
      "node_class_type": "dex_trade_node",
      "chain_id": 31337,
      "dex_name": "uniswap",
      "vault_address": "0xcb65A63B74F009A1C724AC221601d7DDD90f67C2",
      "action": "buy",
      "output_token_address": "0xD604C06206f6DeDd82d42F90D1F5bB34a2E7c5dd",
      "amount_in": "1.5",
      "min_amount_out": "0",
      "slippage_tolerance": 0.5,
      "signal_timeout": 10
    }
  }'
# 'component.2.cycle.1.from.uniswap_trade_node1.handle.output.to.tx_explorer_node.handle.*'
# 'component.2.cycle.1.from.uniswap_trade_node1.handle.output.to.trade_notification_node.handle.*'
# 执行一个flow

curl -X POST http://localhost:7000/flows/execute \
  -H "Content-Type: application/json" \
  -d '{
    "flow_id": "trading_decision_flow",
    "cycle_interval": "5m",
    "flow_json": {
      "nodes": [
        {
          "id": "price_data",
          "type": "binance_price_node",
          "config": {
            "node_class_type": "binance_price_node",
            "symbol": "BTCUSDT",
            "interval": "1h",
            "limit": 24
          }
        },
        {
          "id": "ai_analysis",
          "type": "ai_model_node",
          "config": {
            "node_class_type": "ai_model_node",
            "operation": "predictive_signal",
            "window": 8,
            "threshold": 0.65
          }
        },
        {
          "id": "trade_execution",
          "type": "dex_trade_node",
          "config": {
            "node_class_type": "dex_trade_node",
            "chain_id": 31337,
            "dex_name": "uniswap",
            "vault_address": "0xcb65A63B74F009A1C724AC221601d7DDD90f67C2",
            "action": "buy",
            "output_token_address": "0xD604C06206f6DeDd82d42F90D1F5bB34a2E7c5dd",
            "amount_in": "1.5",
            "min_amount_out": "0",
            "slippage_tolerance": 0.5,
            "signal_timeout": 10
          }
        }
      ],
      "edges": [
        {
          "source": "price_data",
          "source_handle": "price_data",
          "target": "ai_analysis",
          "target_handle": "input_data"
        },
        {
          "source": "ai_analysis",
          "source_handle": "signal",
          "target": "trade_execution",
          "target_handle": "trading_signal"
        }
      ]
    }
  }'

# 两个节点的flow

curl -X POST http://localhost:7000/flows/execute \
  -H "Content-Type: application/json" \
  -d '{
    "flow_id": "trading_decision_flow",
    "cycle_interval": "30m",
    "flow_json": {
      "nodes": [
        {
          "id": "ai_analysis",
          "type": "ai_model_node",
          "config": {
            "node_class_type": "ai_model_node",
            "operation": "predictive_signal",
            "window": 8,
            "threshold": 0.65
          }
        },
        {
          "id": "trade_execution",
          "type": "dex_trade_node",
          "config": {
            "node_class_type": "dex_trade_node",
            "chain_id": 31337,
            "dex_name": "uniswap",
            "vault_address": "0xcb65A63B74F009A1C724AC221601d7DDD90f67C2",
            "action": "buy",
            "output_token_address": "0xD604C06206f6DeDd82d42F90D1F5bB34a2E7c5dd",
            "amount_in": "1.5",
            "min_amount_out": "0",
            "slippage_tolerance": 0.5,
            "signal_timeout": 10
          }
        }
      ],
      "edges": [
        {
          "source": "ai_analysis",
          "source_handle": "signal",
          "target": "trade_execution",
          "target_handle": "trading_signal"
        }
      ]
    }
  }'



```

---

```

curl -X POST http://localhost:8001/price/tokens \
  -H "Content-Type: application/json" \
  -d '[
  {"chain_id": 1, "token_address": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"},
  {"chain_id": 1, "token_address": "0x6B175474E89094C44Da98b954EedeAC495271d0F"},
  {"chain_id": 1, "token_address": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"}
]'

curl -X POST http://localhost:8001/price/tokens \
  -H "Content-Type: application/json" \
  -d '{"chain_id": 1, "token_address": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"}'


[2025-05-21 12:40:10,058: INFO/ForkPoolWorker-8] 发送请求: [URL] http://0.0.0.0:8001/price/tokens, [Headers] {'Content-Type': 'application/json'}, [Data] {"chain_id": 31337, "token_address": "0xd604c06206f6dedd82d42f90d1f5bb34a2e7c5dd", "is_active": true}

curl -X POST http://localhost:8001/price/tokens \
  -H "Content-Type: application/json" \
  -d '{"chain_id": 31337, "token_address": "0xd604c06206f6dedd82d42f90d1f5bb34a2e7c5dd", "is_active": true}'



curl -X POST http://localhost:7000/flows/execute \
  -H "Content-Type: application/json" \
  -d '{
    "flow_id": "trading_decision_flow",
    "cycle_interval": "1m",
    "flow_json": {
      "nodes": [
        {
          "id": "ai_analysis",
          "type": "ai_model_node",
          "config": {
            "node_class_type": "ai_model_node",
            "operation": "predictive_signal",
            "window": 8,
            "threshold": 0.65
          }
        },
        {
          "id": "trade_execution",
          "type": "dex_trade_node",
          "config": {
            "node_class_type": "dex_trade_node",
            "chain_id": 31337,
            "dex_name": "uniswap",
            "vault_address": "0xcb65A63B74F009A1C724AC221601d7DDD90f67C2",
            "action": "buy",
            "output_token_address": "0xD604C06206f6DeDd82d42F90D1F5bB34a2E7c5dd",
            "amount_in": "1.5",
            "min_amount_out": "0",
            "slippage_tolerance": 0.5,
            "signal_timeout": 10
          }
        }
      ],
      "edges": [
        {
          "source": "ai_analysis",
          "source_handle": "signal",
          "target": "trade_execution",
          "target_handle": "amount_in_handle"
        }
      ]
    }
  }'

curl -X GET http://localhost:7000/flows/trading_decision_flow/status





curl -X POST http://localhost:7000/flows/execute \
  -H "Content-Type: application/json" \
  -d '{
    "flow_id": "trading_decision_flow_stop_test",
    "cycle_interval": "1m",
    "flow_json": {
      "nodes": [
        {
          "id": "ai_analysis",
          "type": "ai_model_node",
          "config": {
            "node_class_type": "ai_model_node",
            "operation": "predictive_signal",
            "window": 8,
            "threshold": 0.65
          }
        },
        {
          "id": "trade_execution",
          "type": "dex_trade_node",
          "config": {
            "node_class_type": "dex_trade_node",
            "chain_id": 31337,
            "dex_name": "uniswap",
            "vault_address": "0xcb65A63B74F009A1C724AC221601d7DDD90f67C2",
            "action": "buy",
            "output_token_address": "0xD604C06206f6DeDd82d42F90D1F5bB34a2E7c5dd",
            "amount_in": "1.5",
            "min_amount_out": "0",
            "slippage_tolerance": 0.5,
            "signal_timeout": 10
          }
        }
      ],
      "edges": [
        {
          "source": "ai_analysis",
          "source_handle": "signal",
          "target": "trade_execution",
          "target_handle": "amount_in_handle"
        }
      ]
    }
  }'

curl -X GET http://localhost:7000/flows/trading_decision_flow_stop_test/cycles

```

```

% curl -X POST http://localhost:8001/price/aptos/0xa/closest \
  -H "Content-Type: application/json" \
  -d '{
    "target_timestamp": "2025-06-03T15:56:00+08:00",
    "max_time_diff_minutes": 1000
}'

% curl -X POST http://localhost:8001/price/aptos/batch/closest \
  -H "Content-Type: application/json" \
  -d '{
    "token_addresses": [
      "0xa",
      "0x357b0b74bc833e95a115ad22604854d6b0fca151cecd94111770e5d6ffc9dc2b",
      "0x81214a80d82035a190fcb76b6ff3c0145161c3a9f33d137f2bbaee4cfec8a387",
      "0xbae207659db88bea0cbead6da0ed00aac12edcdda169e591cd41c94180b46f3b"
    ],
    "target_timestamp": "2025-06-03T15:53:55+08:00",
    "max_time_diff_minutes": 1000
}'



curl -X POST https://stg-api.tradingflow.pro/companion/aptos/api/tf_vault/trade-signal \
  -H "Content-Type: application/json" \
  -d '{
  "userAddress": "0x6a1a233e8034ad0cf8d68951864a5a49819b3e9751da4b9fe34618dd41ea9d0d",
  "fromTokenMetadataId": "0xbae207659db88bea0cbead6da0ed00aac12edcdda169e591cd41c94180b46f3b",
  "toTokenMetadataId": "0x000000000000000000000000000000000000000000000000000000000000000a",
  "feeTier": 2,
  "amountIn": 2000,
  "amountOutMin": 0,
  "sqrtPriceLimit": "0",
  "deadline": 1749050916000000
}'


1748591382614460
1749050916000000


curl -X POST http://localhost:7000/nodes/execute \
  -H "Content-Type: application/json" \
  -d '{
    "flow_id": "trading_flow",
    "component_id": 2,
    "cycle": 1,
    "node_id": "hyperion_dex_trade_node_1",
    "node_type": "hyperion_dex_trade_node",
    "input_edges": [],
    "output_edges": [
      {
        "source": "hyperion_dex_trade_node_1",
        "source_handle": "output",
        "target": "trade_notification_node",
        "target_handle": "tx_hash"
      },
      {
        "source": "hyperion_dex_trade_node_1",
        "source_handle": "output",
        "target": "tx_explorer_node",
        "target_handle": "tx_hash"
      }
    ],
    "config": {
      "node_class_type": "hyperion_dex_trade_node",
      "vault_address": "0x6a1a233e8034ad0cf8d68951864a5a49819b3e9751da4b9fe34618dd41ea9d0d",
      "input_token_address": "0xbae207659db88bea0cbead6da0ed00aac12edcdda169e591cd41c94180b46f3b",
      "output_token_address": "0xa",
      "amount_in": "2000",
      "slippage_tolerance": 1.0
    }
  }'

# 余额不够的场景
curl -X POST http://localhost:7000/nodes/execute \
  -H "Content-Type: application/json" \
  -d '{
    "flow_id": "trading_flow",
    "component_id": 2,
    "cycle": 1,
    "node_id": "hyperion_dex_trade_node_1",
    "node_type": "hyperion_dex_trade_node",
    "input_edges": [],
    "output_edges": [
      {
        "source": "hyperion_dex_trade_node_1",
        "source_handle": "output",
        "target": "trade_notification_node",
        "target_handle": "tx_hash"
      },
      {
        "source": "hyperion_dex_trade_node_1",
        "source_handle": "output",
        "target": "tx_explorer_node",
        "target_handle": "tx_hash"
      }
    ],
    "config": {
      "node_class_type": "hyperion_dex_trade_node",
      "vault_address": "0x6a1a233e8034ad0cf8d68951864a5a49819b3e9751da4b9fe34618dd41ea9d0d",
      "input_token_address": "0xbae207659db88bea0cbead6da0ed00aac12edcdda169e591cd41c94180b46f3b",
      "output_token_address": "0xa",
      "amount_in_human_readable": 2000.0,
      "slippage_tolerance": 1.0
    }
  }'

# percentage
curl -X POST http://localhost:7000/nodes/execute \
  -H "Content-Type: application/json" \
  -d '{
    "flow_id": "trading_flow",
    "component_id": 2,
    "cycle": 1,
    "node_id": "hyperion_dex_trade_node_1",
    "node_type": "hyperion_dex_trade_node",
    "input_edges": [],
    "output_edges": [
      {
        "source": "hyperion_dex_trade_node_1",
        "source_handle": "output",
        "target": "trade_notification_node",
        "target_handle": "tx_hash"
      },
      {
        "source": "hyperion_dex_trade_node_1",
        "source_handle": "output",
        "target": "tx_explorer_node",
        "target_handle": "tx_hash"
      }
    ],
    "config": {
      "node_class_type": "hyperion_dex_trade_node",
      "vault_address": "0x6a1a233e8034ad0cf8d68951864a5a49819b3e9751da4b9fe34618dd41ea9d0d",
      "input_token_address": "0xbae207659db88bea0cbead6da0ed00aac12edcdda169e591cd41c94180b46f3b",
      "output_token_address": "0xa",
      "amount_in_percentage": 10.0,
      "slippage_tolerance": 1.0
    }
  }'
# vault node
curl -X POST http://localhost:7000/nodes/execute \
  -H "Content-Type: application/json" \
  -d '{
    "flow_id": "trading_flow",
    "component_id": 2,
    "cycle": 1,
    "node_id": "vault_node_1",
    "node_type": "vault_node",
    "input_edges": [],
    "output_edges": [
      {
        "source": "vault_node_1",
        "source_handle": "vault_info",
        "target": "code_node_1",
        "target_handle": "input_data"
      }, {
        "source": "vault_node_1",
        "source_handle": "network",
        "target": "code_node_1",
        "target_handle": "input_data"
      }, {
        "source": "vault_node_1",
        "source_handle": "user_address",
        "target": "code_node_1",
        "target_handle": "input_data"
      }
    ],
    "config": {
      "node_class_type": "vault_node",
      "network": "aptos",
      "user_address": "0x6a1a233e8034ad0cf8d68951864a5a49819b3e9751da4b9fe34618dd41ea9d0d"
    }
  }'


curl -X POST http://localhost:7000/nodes/execute \
  -H "Content-Type: application/json" \
  -d @code_node_only.json


# 两个节点 vault node -> code node
curl -X POST http://localhost:7000/flows/execute \
  -H "Content-Type: application/json" \
  -d @code_test.json

```
