"""BigQuery writer: staging loads + MERGE upserts, child-row replacement."""

import logging
import os
from typing import Any

from google.cloud import bigquery

from .schema import TABLES

logger = logging.getLogger(__name__)


class BigQueryWriter:
    def __init__(self, project: str | None = None, dataset: str | None = None):
        self.project = project or os.environ["GCP_PROJECT_ID"]
        self.dataset = dataset or os.environ.get("BQ_DATASET", "brightpearl")
        self.client = bigquery.Client(project=self.project)

    def _table_ref(self, name: str) -> str:
        return f"{self.project}.{self.dataset}.{name}"

    def ensure_tables(self) -> None:
        for name, spec in TABLES.items():
            table = bigquery.Table(self._table_ref(name), schema=spec["schema"])
            self.client.create_table(table, exists_ok=True)

    def _load_staging(self, name: str, rows: list[dict[str, Any]]) -> str:
        staging = self._table_ref(f"_stg_{name}")
        job = self.client.load_table_from_json(
            rows,
            staging,
            job_config=bigquery.LoadJobConfig(
                schema=TABLES[name]["schema"],
                write_disposition="WRITE_TRUNCATE",
            ),
        )
        job.result()
        return staging

    def upsert(self, name: str, rows: list[dict[str, Any]]) -> int:
        """MERGE rows into the target table keyed on the table's key_field.

        Staging is deduped on the key (keeping the freshest when_upserted) so
        overlapping pages or duplicate webhook deliveries stay idempotent.
        """
        if not rows:
            return 0
        spec = TABLES[name]
        key = spec["key_field"]
        staging = self._load_staging(name, rows)
        target = self._table_ref(name)
        cols = [f.name for f in spec["schema"]]
        updates = ", ".join(f"T.{c} = S.{c}" for c in cols if c != key)
        sql = f"""
        MERGE `{target}` T
        USING (
          SELECT * EXCEPT(_rn) FROM (
            SELECT *, ROW_NUMBER() OVER (
              PARTITION BY {key} ORDER BY when_upserted DESC
            ) AS _rn
            FROM `{staging}`
          ) WHERE _rn = 1
        ) S
        ON T.{key} = S.{key}
        WHEN MATCHED THEN UPDATE SET {updates}
        WHEN NOT MATCHED THEN INSERT ROW
        """
        if name == "sync_state":
            sql = sql.replace("ORDER BY when_upserted DESC", "ORDER BY last_run_at DESC")
        self.client.query_and_wait(sql)
        return len(rows)

    def replace_children(
        self, name: str, parent_field: str, parent_ids: list[int], rows: list[dict[str, Any]]
    ) -> int:
        """Replace all child rows for the given parents (handles deleted lines)."""
        if not parent_ids:
            return 0
        target = self._table_ref(name)
        if rows:
            staging = self._load_staging(name, rows)
            insert_sql = f"INSERT INTO `{target}` SELECT * FROM `{staging}`"
        else:
            insert_sql = None
        ids = ", ".join(str(i) for i in parent_ids)
        self.client.query_and_wait(
            f"DELETE FROM `{target}` WHERE {parent_field} IN ({ids})"
        )
        if insert_sql:
            self.client.query_and_wait(insert_sql)
        return len(rows)

    def query(self, sql: str) -> list[dict[str, Any]]:
        return [dict(r) for r in self.client.query_and_wait(sql)]
