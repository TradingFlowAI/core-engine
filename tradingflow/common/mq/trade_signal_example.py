import argparse
import asyncio
import logging

from web3 import Web3

from tradingflow.common.mq.dex_trade_signal_consumer import DexTradeSignalConsumer
from tradingflow.common.mq.dex_trade_signal_publisher import DexTradeSignalPublisher

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# 配置常量
CHAIN_ID = "31337"  # Hardhat本地网络
DEX_NAME = "uniswap"
VAULT_ADDRESS = "0xfDD930c22708c7572278cf74D64f3721Eedc18Ad"  # 示例金库地址
TOKEN_ADDRESS = "0x88D3CAaD49fC2e8E38C812c5f4ACdd0a8B065F66"  # 示例代币地址
RPC_URL = "http://localhost:8545"


async def run_publisher_example():
    """发布者示例 - 发送交易信号"""
    # 初始化发布者
    publisher = DexTradeSignalPublisher(
        chain_id=CHAIN_ID,
        dex_name=DEX_NAME,
        vault_address=VAULT_ADDRESS,
    )

    try:
        # 连接到消息队列
        await publisher.connect()
        logger.info("发布者已连接到消息队列")

        # 1. 发布买入信号（异步模式，不等待结果）
        buy_result = await publisher.publish_buy_signal(
            token_address=TOKEN_ADDRESS,
            amount_in="1000000000000000000",  # 1 ETH (18位小数)
            min_amount_out="100000000",  # 最小接收数量
            metadata={"source": "trading_bot", "strategy": "dip_buying"},
        )
        logger.info(f"买入信号发送结果: {buy_result}")

        # 等待一段时间，确保消息被处理
        await asyncio.sleep(2)

        # 2. 发布买入信号并等待结果（RPC模式）
        logger.info("发送买入信号并等待结果...")
        buy_rpc_result = await publisher.publish_buy_signal_and_wait(
            token_address=TOKEN_ADDRESS,
            amount_in="2000000000000000000",  # 2 ETH
            min_amount_out="150000000",
            metadata={"source": "trading_bot", "strategy": "momentum"},
        )
        logger.info(f"买入信号RPC执行结果: {buy_rpc_result}")

        # 3. 发布卖出信号（异步模式）
        sell_result = await publisher.publish_sell_signal(
            token_address=TOKEN_ADDRESS,
            # 不指定amount_in，表示卖出全部持仓
            min_amount_out="2500000000000000000",  # 最小获得2.5 ETH
            metadata={"source": "trading_bot", "strategy": "take_profit"},
        )
        logger.info(f"卖出信号发送结果: {sell_result}")

        # 等待一段时间，确保消息被处理
        await asyncio.sleep(2)

        # 4. 发布卖出信号并等待结果（RPC模式）
        logger.info("发送卖出信号并等待结果...")
        sell_rpc_result = await publisher.publish_sell_signal_and_wait(
            token_address=TOKEN_ADDRESS,
            amount_in="5000000",  # 部分卖出
            min_amount_out="100000000000000000",  # 最小获得0.1 ETH
            metadata={"source": "trading_bot", "strategy": "partial_profit"},
        )
        logger.info(f"卖出信号RPC执行结果: {sell_rpc_result}")

    except Exception as e:
        logger.exception(f"发布者示例运行错误: {str(e)}")
    finally:
        # 关闭连接
        await publisher.close()
        logger.info("发布者已断开连接")


async def run_consumer_example():
    """消费者示例 - 接收并处理交易信号"""
    # 初始化Web3连接
    w3 = Web3(Web3.HTTPProvider(RPC_URL))
    if not w3.is_connected():
        logger.error(f"无法连接到区块链节点: {RPC_URL}")
        return

    # 初始化信号消费者
    consumer = DexTradeSignalConsumer(
        chain_id=CHAIN_ID,
        dex_name=DEX_NAME,
        rpc_url=RPC_URL,
    )

    try:
        # 启动消费者
        await consumer.start()
        logger.info("消费者已启动，等待信号消息...")

        # 保持消费者运行一段时间
        await asyncio.sleep(600)  # 运行60秒

    except Exception as e:
        logger.exception(f"消费者示例运行错误: {str(e)}")
    finally:
        # 停止消费者
        await consumer.stop()
        logger.info("消费者已停止")


async def main():
    """主函数，选择要运行的示例"""
    parser = argparse.ArgumentParser(description="DEX交易信号发布与消费示例")
    parser.add_argument(
        "--mode",
        "-m",
        type=int,
        default=1,
        help="运行模式 (1=运行发布者, 2=运行消费者, 3=运行模拟交易, 4=模拟交易带持仓信息)",
    )

    # 解析命令行参数
    args = parser.parse_args()

    example_choice = args.mode

    try:
        if example_choice == 1:
            await run_publisher_example()
        elif example_choice == 2:
            await run_consumer_example()
        else:
            logger.error(f"无效的示例选择: {example_choice}")
    except Exception as e:
        logger.exception(f"运行示例时出错: {str(e)}")


if __name__ == "__main__":
    asyncio.run(main())
