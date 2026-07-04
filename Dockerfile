FROM python:3.13-slim

WORKDIR /app
COPY pyproject.toml README.md ./
COPY brightpearl_client ./brightpearl_client
COPY sync ./sync
COPY mcp_server ./mcp_server
RUN pip install --no-cache-dir .

ENV PYTHONUNBUFFERED=1
# Default: MCP server. The sync-ingest service overrides with:
#   python -m sync.webhooks
CMD ["python", "-m", "mcp_server.server"]
