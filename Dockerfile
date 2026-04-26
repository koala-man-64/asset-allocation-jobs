# syntax=docker/dockerfile:1.7
# Tasks-only image for Azure Container Apps Jobs.
FROM python:3.14-slim-bookworm

WORKDIR /app

ARG CONTRACTS_VERSION=1.1.0
ARG RUNTIME_COMMON_VERSION=2.0.0

COPY asset-allocation-jobs/requirements.lock.txt ./
RUN apt-get update && apt-get install -y postgresql-client && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir -r requirements.lock.txt

RUN --mount=type=secret,id=pipconfig,target=/etc/pip.conf,required=false \
    pip install --no-cache-dir \
    "asset-allocation-contracts==${CONTRACTS_VERSION}" \
    "asset-allocation-runtime-common==${RUNTIME_COMMON_VERSION}"

COPY asset-allocation-jobs/pyproject.toml asset-allocation-jobs/README.md ./
COPY asset-allocation-jobs/alpaca/ alpaca/
COPY asset-allocation-jobs/alpha_vantage/ alpha_vantage/
COPY asset-allocation-jobs/core/ core/
COPY asset-allocation-jobs/massive_provider/ massive_provider/
COPY asset-allocation-jobs/monitoring/ monitoring/
COPY asset-allocation-jobs/tasks/ tasks/
RUN pip install --no-cache-dir .

CMD ["python", "-c", "print('asset-allocation task image: specify a job command (e.g., python -m tasks.market_data.bronze_market_data)')"]
