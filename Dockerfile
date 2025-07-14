FROM python:3.10-slim

WORKDIR /app

# 安装系统依赖
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# 复制项目文件
COPY python/common /app/python/common
COPY python/py_worker /app/python/py_worker

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
EXPOSE 8002

# 启动服务
CMD ["python", "python/py_worker/server.py"]
