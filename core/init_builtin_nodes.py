"""
Initialize Built-in Nodes with Versioning
为所有内置节点注册版本信息
"""

from core.node_registry import register_node

# 这个文件用于为所有内置节点添加版本信息
# 所有内置节点从 0.0.1 开始

# 使用方式：
# 1. 在每个节点类定义前添加 @register_node 装饰器
# 2. 或者在这里统一导入和注册

def init_all_builtin_nodes():
    """
    初始化所有内置节点
    为每个节点注册到版本控制系统
    """

    # 注册所有内置节点（v0.0.1）
    node_registrations = [
        {
            'node_type': 'vault_node',
            'version': '0.0.1',
            'module': 'nodes.vault_node',
            'class': 'VaultNode',
            'metadata': {
                'display_name': 'Vault Node',
                'category': 'defi',
                'description': 'Query user holdings in TradingFlow Vault'
            }
        },
        {
            'node_type': 'swap_node',
            'version': '0.0.1',
            'module': 'nodes.swap_node',
            'class': 'SwapNode',
            'metadata': {
                'display_name': 'Swap Node',
                'category': 'defi',
                'description': 'Execute token swaps'
            }
        },
        {
            'node_type': 'ai_model_node',
            'version': '0.0.1',
            'module': 'nodes.ai_model_node',
            'class': 'AIModelNode',
            'metadata': {
                'display_name': 'AI Model Node',
                'category': 'ai',
                'description': 'Execute AI model predictions'
            }
        },
        {
            'node_type': 'code_node',
            'version': '0.0.1',
            'module': 'nodes.code_node',
            'class': 'CodeNode',
            'metadata': {
                'display_name': 'Code Node',
                'category': 'utility',
                'description': 'Execute custom Python code'
            }
        },
        {
            'node_type': 'dataset_node',
            'version': '0.0.1',
            'module': 'nodes.dataset_node',
            'class': 'DatasetNode',
            'metadata': {
                'display_name': 'Dataset Node',
                'category': 'data',
                'description': 'Read/write data from Google Sheets'
            }
        },
        {
            'node_type': 'price_node',
            'version': '0.0.1',
            'module': 'nodes.price_node',
            'class': 'PriceNode',
            'metadata': {
                'display_name': 'Price Node',
                'category': 'data',
                'description': 'Fetch cryptocurrency prices from CoinGecko'
            }
        },
        {
            'node_type': 'hyperion_dex_trade_node',
            'version': '0.0.1',
            'module': 'nodes.hyperion_dex_trade_node',
            'class': 'HyperionDexTradeNode',
            'metadata': {
                'display_name': 'Hyperion DEX Trade Node',
                'category': 'defi',
                'description': 'Execute trades on Hyperion DEX'
            }
        },
        {
            'node_type': 'uniswap_dex_trade_node',
            'version': '0.0.1',
            'module': 'nodes.uniswap_dex_trade_node',
            'class': 'UniswapDexTradeNode',
            'metadata': {
                'display_name': 'Uniswap DEX Trade Node',
                'category': 'defi',
                'description': 'Execute trades on Uniswap'
            }
        },
        {
            'node_type': 'telegram_sender_node',
            'version': '0.0.1',
            'module': 'nodes.telegram_sender_node',
            'class': 'TelegramSenderNode',
            'metadata': {
                'display_name': 'Telegram Sender Node',
                'category': 'notification',
                'description': 'Send messages via Telegram'
            }
        },
        {
            'node_type': 'rsshub_node',
            'version': '0.0.1',
            'module': 'nodes.rsshub_node',
            'class': 'RSSHubNode',
            'metadata': {
                'display_name': 'RSS Hub Node',
                'category': 'data',
                'description': 'Fetch RSS feeds'
            }
        },
        {
            'node_type': 'x_listener_node',
            'version': '0.0.1',
            'module': 'nodes.x_listener_node',
            'class': 'XListenerNode',
            'metadata': {
                'display_name': 'X (Twitter) Listener Node',
                'category': 'data',
                'description': 'Listen to X (Twitter) updates'
            }
        }
    ]

    # 动态导入和注册
    for registration in node_registrations:
        try:
            # 动态导入模块
            module = __import__(registration['module'], fromlist=[registration['class']])
            node_class = getattr(module, registration['class'])

            # 使用装饰器注册
            decorated_class = register_node(
                registration['node_type'],
                version=registration['version'],
                **registration['metadata']
            )(node_class)

            print(f"✓ Registered: {registration['node_type']} v{registration['version']}")

        except Exception as e:
            print(f"✗ Failed to register {registration['node_type']}: {e}")

    print(f"\nTotal nodes registered: {len(node_registrations)}")


if __name__ == '__main__':
    # 运行注册
    init_all_builtin_nodes()

    # 打印注册信息
    from core.node_registry import NodeRegistry

    print("\n=== Registered Nodes ===")
    for node_type in NodeRegistry.get_all_node_types():
        versions = NodeRegistry.get_all_versions(node_type)
        latest = NodeRegistry.get_latest_version(node_type)
        print(f"{node_type}: {versions} (latest: {latest})")
