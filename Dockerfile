# One image for all Python services (simulator, alerts, live-map); compose picks the command.
FROM python:3.12-slim

COPY --from=ghcr.io/astral-sh/uv:0.13.7 /uv /usr/local/bin/uv

ENV PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    UV_PROJECT_ENVIRONMENT=/app/.venv \
    PATH="/app/.venv/bin:$PATH"

WORKDIR /app

# 1) Dependencies only. This layer is cached until pyproject.toml / uv.lock change,
#    so editing code doesn't re-download every package.
COPY pyproject.toml uv.lock .python-version ./
RUN uv sync --frozen --no-dev --no-install-project

# 2) Our code.
COPY README.md ./
COPY src ./src
RUN uv sync --frozen --no-dev

# Don't run as root inside the container.
RUN useradd --system --uid 1001 app
USER app

CMD ["python", "-m", "delivery_pipeline.simulator"]
