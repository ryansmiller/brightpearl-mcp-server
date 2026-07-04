FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim

WORKDIR /app
COPY pyproject.toml uv.lock README.md ./
COPY brightpearl_client ./brightpearl_client
COPY sync ./sync
COPY mcp_server ./mcp_server
# Frozen: install exactly what uv.lock pins, no dev deps, as a real package
RUN uv sync --frozen --no-dev --no-editable

ENV PATH="/app/.venv/bin:$PATH" PYTHONUNBUFFERED=1
RUN useradd --create-home app && chown -R app /app
USER app

# Default: MCP server. The sync-ingest service overrides with:
#   python -m sync.webhooks
CMD ["python", "-m", "mcp_server.server"]
