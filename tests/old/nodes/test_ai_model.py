import asyncio
import logging

from nodes.ai_model_node import AIModelNode

# 配置日志
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(funcName)s() - %(message)s",
)


def test_sintegration_runn():
    ai_node = AIModelNode(
        flow_id="flow_1",
        component_id=1,
        cycle=1,
        node_id="ai_model_node_1",
        name="交易信号生成器",
        model_name="gpt-3.5-turbo",
        output_signal_type="dex_trade",  # 输出交易信号
        system_prompt="你是一个专业的交易顾问，负责分析市场数据并提出交易建议。",
        prompt="请分析以下市场数据，并给出具体的交易建议，包括交易对、操作类型、数量和理由:",
        output_format_prompt=True,  # 启用格式提示，告诉AI要怎么输出
    )

    asyncio.run(ai_node.execute())

    if __name__ == "__main__":
        # 运行集成测试
        test_sintegration_runn()
