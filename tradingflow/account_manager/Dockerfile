FROM python:3.10-slim

WORKDIR /app

# 安装系统依赖
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# 复制项目文件
COPY python/common /app/python/common
COPY python/account_manager /app/python/account_manager
COPY python/py_worker/mq /app/python/py_worker/mq

# 安装 Python 依赖
RUN pip install --no-cache-dir \
    sanic \
    aiohttp \
    redis \
    celery \
    pydantic \
    aio-pika \
    web3 \
    python-dotenv \
    prometheus-client

# 设置环境变量
ENV PYTHONPATH=/app

# 暴露端口
EXPOSE 8001

# 启动服务
CMD ["python", "python/account_manager/server.py"]
