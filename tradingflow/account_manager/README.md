## 如何使用

"""
python -m python.account_manager.server

celery -A python.account_manager.celery_worker beat --loglevel=debug  # 调度定时任务
celery -A python.account_manager.celery_worker worker --loglevel=debug  # worker


# token 地址校验下
curl -X POST http://localhost:8001/evm/vaults/create \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_AUTH_TOKEN" \
  -d '{
    "asset_address": "0x787c6666213624D788522d516847978D7F348902",
    "investor_address": "0x70997970C51812dc3A010C7d01b50e0d17dc79C8",
    "chain_id": 31337
  }'

curl -X GET http://localhost:8001/evm/vaults/tasks/YOUR_TASK_ID \
  -H "Authorization: Bearer YOUR_AUTH_TOKEN"

curl -X GET http://localhost:8001/evm/vaults/31337/0x1a223f93131cd7d898c28ee0b905c39db474fa08
# 根据投资者查询vaults
curl -X GET http://localhost:8001/evm/vaults/investor/31337/0x70997970c51812dc3a010c7d01b50e0d17dc79c8


curl -X POST "http://localhost:8001/token/" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_ACCESS_TOKEN" \
  -d '{
    "chain_id": 1,
    "token_address": "0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9",
    "name": "Aave",
    "symbol": "AAVE",
    "decimals": 18,
    "primary_source": "geckoterminal",
    "description": "Aave是一个去中心化金融借贷协议",
    "logo_url": "https://example.com/aave-logo.png"
  }'


curl -X POST "http://localhost:8001/token/batch" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_ACCESS_TOKEN" \
  -d '{
    "tokens": [
      {
        "chain_id": 1,
        "token_address": "0x1f9840a85d5af5bf1d1762f925bdaddc4201f984",
        "name": "Uniswap",
        "symbol": "UNI",
        "decimals": 18
      },
      {
        "chain_id": 56,
        "token_address": "0x0e09fabb73bd3ade0a17ecc321fd13a19e81ce82",
        "name": "PancakeSwap",
        "symbol": "CAKE",
        "decimals": 18
      }
    ]
  }'
"""
