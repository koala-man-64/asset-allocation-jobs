# syntax=docker/dockerfile:1.7
# Tasks-only image for Azure Container Apps Jobs.
FROM python:3.14-slim-bookworm

WORKDIR /app

ARG CONTRACTS_VERSION=3.11.0
ARG RUNTIME_COMMON_VERSION=3.4.7

COPY requirements.lock.txt ./
RUN pip install --no-cache-dir -r requirements.lock.txt

RUN --mount=type=secret,id=pipconfig,target=/etc/pip.conf,required=false \
    pip install --no-cache-dir \
    "asset-allocation-contracts==${CONTRACTS_VERSION}" \
    "asset-allocation-runtime-common==${RUNTIME_COMMON_VERSION}"

COPY pyproject.toml README.md ./
COPY alpaca/ alpaca/
COPY alpha_vantage/ alpha_vantage/
COPY core/ core/
COPY massive_provider/ massive_provider/
COPY monitoring/ monitoring/
COPY tasks/ tasks/
RUN pip install --no-cache-dir .

CMD ["python", "-c", "print('asset-allocation task image: specify a job command (e.g., python -m tasks.market_data.bronze_market_data)')"]
