"""
Initialize Built-in Nodes with Versioning

This file registers version information for all built-in nodes.
All built-in nodes start from version 0.0.1.

Usage:
1. Add @register_node decorator before each node class definition
2. Or import and register here uniformly
"""

from core.node_registry import register_node


def init_all_builtin_nodes():
    """
    Initialize all built-in nodes.
    Register each node to the version control system.
    """

    # Register all built-in nodes (using their current versions)
    node_registrations = [
        {
            'node_type': 'vault_node',
            'version': '0.0.1',
            'module': 'nodes.vault_node_v0_0_1',
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
            'module': 'nodes.swap_node_v0_0_1',
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
            'module': 'nodes.ai_model_node_v0_0_1',
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
            'module': 'nodes.code_node_v0_0_1',
            'class': 'CodeNode',
            'metadata': {
                'display_name': 'Code Node',
                'category': 'utility',
                'description': 'Execute custom Python code'
            }
        },
        {
            'node_type': 'dataset_node',
            'version': '0.0.2',
            'module': 'nodes.dataset_node_v0_0_2',
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
            'module': 'nodes.price_node_v0_0_1',
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
            'module': 'nodes.telegram_sender_node_v0_0_1',
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
            'module': 'nodes.rsshub_node_v0_0_1',
            'class': 'RSSHubNode',
            'metadata': {
                'display_name': 'RSS Hub Node',
                'category': 'data',
                'description': 'Fetch RSS feeds'
            }
        },
        {
            'node_type': 'rootdata_node',
            'version': '0.0.1',
            'module': 'nodes.rootdata_node_v0_0_1',
            'class': 'RootDataNode',
            'metadata': {
                'display_name': 'RootData Node',
                'category': 'data',
                'description': 'Query RootData APIs with optional Redis cache'
            }
        },
        {
            'node_type': 'x_listener_node',
            'version': '0.0.1',
            'module': 'nodes.x_listener_node_v0_0_1',
            'class': 'XListenerNode',
            'metadata': {
                'display_name': 'X (Twitter) Listener Node',
                'category': 'data',
                'description': 'Listen to X (Twitter) updates'
            }
        }
    ]

    # Dynamic import and registration
    for registration in node_registrations:
        try:
            # Dynamically import module
            module = __import__(registration['module'], fromlist=[registration['class']])
            node_class = getattr(module, registration['class'])

            # Register using decorator
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
    # Run registration
    init_all_builtin_nodes()

    # Print registration info
    from core.node_registry import NodeRegistry

    print("\n=== Registered Nodes ===")
    for node_type in NodeRegistry.get_all_node_types():
        versions = NodeRegistry.get_all_versions(node_type)
        latest = NodeRegistry.get_latest_version(node_type)
        print(f"{node_type}: {versions} (latest: {latest})")
