# syntax=docker/dockerfile:1
FROM python:3.14-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

COPY libs/contracts /src/libs/contracts
COPY services/consumer /src/services/consumer
RUN pip install /src/libs/contracts /src/services/consumer \
    && useradd --system --uid 10001 --no-create-home app

USER app

ENTRYPOINT ["product-consumer"]
