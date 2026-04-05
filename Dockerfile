# Tasks-only image for Azure Container Apps Jobs.
# This Dockerfile is built from the shared workspace root so it can vendor the sibling
# contracts repo during GitHub Actions release builds.
FROM python:3.14-slim-bookworm

WORKDIR /app

COPY asset-allocation-jobs/requirements.lock.txt ./
RUN apt-get update && apt-get install -y postgresql-client && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir -r requirements.lock.txt

COPY asset-allocation-contracts/python/ /tmp/asset-allocation-contracts/
RUN pip install --no-cache-dir /tmp/asset-allocation-contracts

COPY asset-allocation-jobs/pyproject.toml asset-allocation-jobs/README.md ./
COPY asset-allocation-jobs/alpaca/ alpaca/
COPY asset-allocation-jobs/alpha_vantage/ alpha_vantage/
COPY asset-allocation-jobs/core/ core/
COPY asset-allocation-jobs/massive_provider/ massive_provider/
COPY asset-allocation-jobs/monitoring/ monitoring/
COPY asset-allocation-jobs/tasks/ tasks/
RUN pip install --no-cache-dir .

CMD ["python", "-c", "print('asset-allocation task image: specify a job command (e.g., python -m tasks.market_data.bronze_market_data)')"]
