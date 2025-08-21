FROM python:3.11-slim

WORKDIR /app

# 安装系统依赖
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# 复制 requirements 文件
COPY requirements.txt /app/requirements.txt

# 安装 Python 依赖
RUN pip install --no-cache-dir -r /app/requirements.txt

# 复制项目文件
COPY tradingflow/depot /app/tradingflow/depot
COPY tradingflow/station /app/tradingflow/station

# 接收 Google 凭证作为构建参数
ARG GOOGLE_CREDENTIALS_JSON
RUN mkdir -p /app/tradingflow/station/config
# 使用 printf 而不是 echo 来避免换行符问题，并确保 JSON 格式正确
RUN printf '%s' "$GOOGLE_CREDENTIALS_JSON" > /app/tradingflow/station/config/google_credentials.json
# 验证 JSON 格式是否正确
RUN python3 -m json.tool /app/tradingflow/station/config/google_credentials.json > /dev/null || (echo "Invalid JSON format in Google credentials" && exit 1)

# 设置环境变量
ENV PYTHONPATH=/app

# 暴露端口
EXPOSE 7002

# 复制并设置启动脚本权限
COPY tradingflow/station/docker-entrypoint.sh /app/
RUN chmod +x /app/docker-entrypoint.sh

# 启动服务
ENTRYPOINT ["/app/docker-entrypoint.sh"]
