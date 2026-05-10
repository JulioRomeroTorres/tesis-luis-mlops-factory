# -------------------- builder --------------------
FROM python:3.12-slim AS builder

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

COPY pyproject.toml ./
COPY app ./app

RUN ls -a

RUN pip install -U pip \
    && pip install .

# -------------------- runtime --------------------
FROM python:3.12-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/opt/venv/bin:$PATH"

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN useradd -m -u 10001 appuser

RUN mkdir -p /app/uploads /app/images /app/tmp

RUN mkdir -p /app/telemetry_output && \
    chmod -R 777 /app/telemetry_output

COPY --from=builder /opt/venv /opt/venv
COPY app ./app
COPY gcp-sa-key.json ./

RUN chown -R appuser:appuser /app && \
    chmod -R 755 /app

EXPOSE 8080
USER appuser

CMD ["python", "-m", "app.main"]