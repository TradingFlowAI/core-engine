FROM python:3.10-slim

WORKDIR /app

# 安装系统依赖
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# 复制项目文件
COPY tradingflow/depot /app/tradingflow/depot
COPY tradingflow/station /app/tradingflow/station

# 安装 Python 依赖
RUN pip install --no-cache-dir \
    sanic \
    aiohttp \
    redis \
    pydantic \
    aio-pika \
    numpy \
    pandas \
    scikit-learn \
    python-dotenv \
    prometheus-client

# 设置环境变量
ENV PYTHONPATH=/app

# 暴露端口
EXPOSE 7001

# 复制并设置启动脚本权限
COPY tradingflow/station/docker-entrypoint.sh /app/
RUN chmod +x /app/docker-entrypoint.sh

# 启动服务
ENTRYPOINT ["/app/docker-entrypoint.sh"]
