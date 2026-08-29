# NetAtlas 一体化镜像（Linux 容器内使用 masscan/scapy 原生后端）
# 构建:  docker build -t netatlas .
# 运行:  docker compose up -d   （推荐，见 docker-compose.yml）
FROM python:3.13-slim AS base

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    NETATLAS_PATHS__ROOT=/app

# 系统依赖：libpcap（scapy/masscan 发包）、masscan（Debian 仓库）、构建工具
RUN apt-get update && apt-get install -y --no-install-recommends \
        libpcap0.8 masscan ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt ./
RUN pip install -r requirements.txt

COPY . .

# zgrab2 为可选增强：镜像内不含 Go 工具链，运行时检测到 bin/zgrab2 则自动启用，
# 否则 L7 自动降级到内置 asyncio 引擎（功能完备，协议覆盖略少）。
RUN mkdir -p data/raw data/parquet data/meta data/geoip data/seeds logs \
    && python -m compileall -q orchestrator modules storage webui scripts

EXPOSE 8000-9000

# 默认 dry-run 自检启动；生产扫描：docker compose run netatlas --targets x.x.x.x/24
ENTRYPOINT ["python", "scripts/start.py"]
CMD ["--dry-run"]
