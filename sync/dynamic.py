"""Dynamic sync engines: search dumps and reference-GET tables.

Search dumps generate their BigQuery schema from the resource-search's own
column metadata (name + reportDataType), so new searchable resources are pure
config in sync/resources.py.
"""

import json
import logging
import re
from datetime import datetime, timezone
from typing import Any

from google.cloud import bigquery

from brightpearl_client import BrightpearlClient, BrightpearlError

from .bq import BigQueryWriter
from .pipeline import SWEEP_OVERLAP
from .resources import REFERENCE_GETS, SEARCH_DUMPS

logger = logging.getLogger(__name__)

LOAD_CHUNK = 25_000  # rows buffered before each staging load + MERGE

# Brightpearl reportDataType → BigQuery type. Money comes through as STRING
# ("5652.10"); semantic views CAST as needed.
TYPE_MAP = {
    "INTEGER": "INT64",
    "IDSET": "INT64",
    "BOOLEAN": "BOOL",
    "PERIOD": "TIMESTAMP",
    "DATE": "TIMESTAMP",
    "DATETIME": "TIMESTAMP",
    "MONEY": "NUMERIC",
    "STRING": "STRING",
    "SEARCH_STRING": "STRING",
}


def snake(name: str) -> str:
    return re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()


def _coerce(value: Any, bq_type: str) -> Any:
    if value is None or value == "":
        return None
    if bq_type == "INT64":
        if isinstance(value, list):  # IDSET can surface a list
            return int(value[0]) if len(value) == 1 else None
        return int(value)
    if bq_type == "NUMERIC":
        # BigQuery parses NUMERIC from strings exactly — never round-trip
        # money through float
        return value if isinstance(value, (int, float, str)) else str(value)
    if bq_type == "BOOL":
        if isinstance(value, str):
            return value.strip().lower() in ("true", "1", "yes")
        return bool(value)
    if bq_type == "STRING" and not isinstance(value, str):
        return json.dumps(value)
    return value


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class SearchDumpSyncer:
    """Sync any searchable resource into a typed BigQuery table."""

    def __init__(self, bp: BrightpearlClient, bq: BigQueryWriter):
        self.bp = bp
        self.bq = bq

    def _schema_for(self, columns: list[dict]) -> list[bigquery.SchemaField]:
        fields = [
            bigquery.SchemaField(snake(c["name"]), TYPE_MAP.get(c["reportDataType"], "STRING"))
            for c in columns
        ]
        fields.append(bigquery.SchemaField("when_upserted", "TIMESTAMP", mode="REQUIRED"))
        return fields

    def _to_row(self, api_row: dict, columns: list[dict]) -> dict:
        out = {}
        for c in columns:
            bq_type = TYPE_MAP.get(c["reportDataType"], "STRING")
            out[snake(c["name"])] = _coerce(api_row.get(c["name"]), bq_type)
        out["when_upserted"] = _now()
        return out

    def _watermarks(self, table: str) -> tuple[int | None, datetime | None]:
        rows = self.bq.query(
            f"SELECT watermark_id, watermark_updated_on "
            f"FROM `{self.bq._table_ref('sync_state')}` WHERE resource = '{table}'"
        )
        if not rows:
            return None, None
        return rows[0]["watermark_id"], rows[0]["watermark_updated_on"]

    async def sync(self, table: str, *, full: bool = False) -> int:
        spec = SEARCH_DUMPS[table]
        service, resource = spec["search"]
        mode, col = spec["incremental"]
        key = snake(spec["key"])

        filters: dict[str, Any] = {}
        last_id, last_ts = (None, None) if full else self._watermarks(table)
        if not full:
            if mode == "id" and last_id is not None:
                filters[col] = f"{last_id + 1}/"
            elif mode == "updated" and last_ts is not None:
                # overlap guards against late indexing / clock skew; upserts
                # make re-reading the overlap free
                since = last_ts.astimezone(timezone.utc) - SWEEP_OVERLAP
                filters[col] = f"{since.strftime('%Y-%m-%dT%H:%M:%S.000Z')}/"

        schema: list[bigquery.SchemaField] | None = None
        buffer: list[dict] = []
        total = 0
        max_id, max_ts = last_id, last_ts
        truncate_mode = mode == "full"
        all_rows: list[dict] = []

        first = 1
        while True:
            try:
                page = await self.bp.search(service, resource, filters=filters, first_result=first)
            except BrightpearlError as e:
                if filters and first == 1:
                    logger.warning("%s: filter %s rejected (%s); falling back to full scan",
                                   table, filters, e)
                    filters, truncate_mode = {}, False
                    continue
                raise
            if schema is None:
                schema = self._schema_for(page.columns)
                self.bq.ensure_table(table, schema)
            watermark_col = snake(col) if col else None
            for api_row in page.results:
                row = self._to_row(api_row, page.columns)
                if truncate_mode:
                    all_rows.append(row)
                else:
                    buffer.append(row)
                if row.get(key) is not None and isinstance(row[key], int):
                    max_id = max(max_id or 0, row[key])
                if mode == "updated" and watermark_col and row.get(watermark_col):
                    ts = datetime.fromisoformat(str(row[watermark_col]))
                    if max_ts is None or ts > max_ts:
                        max_ts = ts
            if not truncate_mode and len(buffer) >= LOAD_CHUNK:
                total += self.bq.upsert(table, buffer, schema, key)
                logger.info("%s: %d rows merged (through result %d/%d)",
                            table, total, page.last_result, page.results_available)
                buffer = []
            if not page.has_more:
                break
            first = page.next_first_result

        if truncate_mode:
            total = self.bq.truncate_load(table, all_rows, schema or [])
        elif buffer:
            total += self.bq.upsert(table, buffer, schema, key)

        self._record(table, "full" if (full or truncate_mode) else mode, total, max_id, max_ts)
        logger.info("%s: done, %d rows", table, total)
        return total

    def _record(self, table, kind, rows, max_id, max_ts):
        self.bq.upsert(
            "sync_state",
            [{
                "resource": table,
                "watermark_id": max_id,
                "watermark_updated_on": max_ts.isoformat() if max_ts else None,
                "last_run_at": datetime.now(timezone.utc).isoformat(),
                "last_run_kind": kind,
                "last_run_rows": rows,
            }],
        )


REFERENCE_SCHEMA = [
    bigquery.SchemaField("id", "STRING"),
    bigquery.SchemaField("name", "STRING"),
    bigquery.SchemaField("raw_payload", "JSON"),
    bigquery.SchemaField("when_upserted", "TIMESTAMP", mode="REQUIRED"),
]

_ID_CANDIDATES = ("id",)
_NAME_CANDIDATES = ("name", "code", "description", "title")


def _pluck_id(item: dict) -> str | None:
    for k in item:
        if k in _ID_CANDIDATES or k.lower().endswith("id"):
            return str(item[k])
    return None


def _pluck_name(item: dict) -> str | None:
    for k in _NAME_CANDIDATES:
        if isinstance(item.get(k), str) and item[k]:
            return item[k]
    return None


class ReferenceSyncer:
    """Truncate-and-reload tiny GET-collection endpoints (statuses, types...)."""

    def __init__(self, bp: BrightpearlClient, bq: BigQueryWriter):
        self.bp = bp
        self.bq = bq

    async def sync(self, table: str) -> int:
        path = REFERENCE_GETS[table]
        payload = await self.bp.get(path)
        items = payload if isinstance(payload, list) else [payload]
        now = _now()
        rows = [
            {
                "id": _pluck_id(item),
                "name": _pluck_name(item),
                "raw_payload": item,
                "when_upserted": now,
            }
            for item in items
            if isinstance(item, dict)
        ]
        n = self.bq.truncate_load(table, rows, REFERENCE_SCHEMA)
        self.bq.upsert(
            "sync_state",
            [{
                "resource": table,
                "last_run_at": now,
                "last_run_kind": "reference",
                "last_run_rows": n,
            }],
        )
        logger.info("%s: %d reference rows", table, n)
        return n
