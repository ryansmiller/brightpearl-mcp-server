"""SearchDumpSyncer id-mode incremental sync + BigQuery concurrent-abort retry."""

from google.api_core.exceptions import BadRequest

from brightpearl_client import BrightpearlError
from brightpearl_client.client import SearchPage
from sync.bq import BigQueryWriter
from sync.dynamic import SearchDumpSyncer

JOURNAL_COLUMNS = [{"name": "journalRowId", "reportDataType": "INTEGER"}]
PAYMENT_COLUMNS = [{"name": "paymentId", "reportDataType": "IDSET"}]


def _page(ids: list[int], first: int, available: int, columns=JOURNAL_COLUMNS) -> SearchPage:
    return SearchPage(
        results=[{c["name"]: i for c in columns} for i in ids],
        results_available=available,
        first_result=first,
        last_result=first + len(ids) - 1,
        columns=columns,
    )


class FakeBP:
    """Serves pre-built pages; records the filters each search used."""

    def __init__(self, pages: list[SearchPage], fail_filtered_with: BrightpearlError | None = None):
        self.pages = pages
        self.fail_filtered_with = fail_filtered_with
        self.calls: list[dict] = []

    async def search(self, service, resource, *, filters=None, first_result=1):
        self.calls.append(dict(filters or {}))
        if self.fail_filtered_with and filters:
            raise self.fail_filtered_with
        # pages are keyed by first_result order
        for page in self.pages:
            if page.first_result == first_result:
                return page
        raise AssertionError(f"no page for first_result={first_result}")


class FakeBQ:
    def __init__(self, watermark_id=None):
        self.watermark_id = watermark_id
        self.upserts: list[tuple[str, list[dict]]] = []

    def _table_ref(self, name):
        return f"p.d.{name}"

    def query(self, sql):
        if self.watermark_id is None:
            return []
        return [{"watermark_id": self.watermark_id, "watermark_updated_on": None}]

    def ensure_table(self, name, schema, cluster_fields=None):
        pass

    def upsert(self, name, rows, schema=None, key=None):
        self.upserts.append((name, list(rows)))
        return len(rows)

    def truncate_load(self, name, rows, schema, cluster_fields=None):
        self.upserts.append((name, list(rows)))
        return len(rows)

    def synced_ids(self, table):
        return {
            r["journal_row_id"] for name, rows in self.upserts if name == table for r in rows
        }

    def recorded_state(self):
        return next(rows[0] for name, rows in self.upserts if name == "sync_state")


async def test_id_mode_pages_descending_and_stops_at_watermark():
    # watermark 100; ids 105..96 live upstream, newest first across two pages
    bq = FakeBQ(watermark_id=100)
    bp = FakeBP([
        _page([105, 104, 103], first=1, available=200),
        _page([102, 101, 100, 99], first=4, available=200),
    ])
    syncer = SearchDumpSyncer(bp, bq)

    total = await syncer.sync("journal_rows")

    assert bp.calls[0] == {"sort": "journalRowId.DESC"}
    assert bq.synced_ids("journal_rows") == {105, 104, 103, 102, 101}
    assert total == 5
    assert bq.recorded_state()["watermark_id"] == 105
    # stopped mid-page-2: never requested a third page
    assert len(bp.calls) == 2


async def test_id_mode_first_sync_scans_everything_unfiltered():
    bq = FakeBQ(watermark_id=None)
    bp = FakeBP([_page([1, 2, 3], first=1, available=3)])
    syncer = SearchDumpSyncer(bp, bq)

    total = await syncer.sync("journal_rows")

    assert bp.calls == [{}]
    assert total == 3
    assert bq.recorded_state()["watermark_id"] == 3


async def test_rejected_sort_falls_back_to_full_scan():
    bq = FakeBQ(watermark_id=100)
    bp = FakeBP(
        [_page([101, 102], first=1, available=2)],
        fail_filtered_with=BrightpearlError("CMNC-018 cannot be parsed as: INTEGER", 400),
    )
    syncer = SearchDumpSyncer(bp, bq)

    total = await syncer.sync("journal_rows")

    # first call filtered (rejected), second unfiltered; early-stop disabled
    assert bp.calls == [{"sort": "journalRowId.DESC"}, {}]
    assert total == 2


async def test_transient_errors_do_not_trigger_full_scan():
    # a 503 (throttle exhaustion) must fail fast, not burn the request
    # budget on an unfiltered scan
    bq = FakeBQ(watermark_id=100)
    bp = FakeBP(
        [_page([101, 102], first=1, available=2)],
        fail_filtered_with=BrightpearlError("service unavailable", 503),
    )
    syncer = SearchDumpSyncer(bp, bq)

    try:
        await syncer.sync("journal_rows")
        raise AssertionError("expected BrightpearlError")
    except BrightpearlError:
        pass
    assert bp.calls == [{"sort": "journalRowId.DESC"}]  # no unfiltered retry
    assert bq.upserts == []  # nothing written, table untouched


async def test_page_past_available_ignores_misreported_total():
    # supplier-payment-search always claims resultsAvailable=500; full pages
    # must keep paging until a short page arrives
    bq = FakeBQ(watermark_id=None)
    page1 = _page(list(range(1, 501)), first=1, available=500, columns=PAYMENT_COLUMNS)
    page2 = _page(list(range(501, 521)), first=501, available=500, columns=PAYMENT_COLUMNS)
    bp = FakeBP([page1, page2])
    syncer = SearchDumpSyncer(bp, bq)

    total = await syncer.sync("supplier_payments")

    assert total == 520
    assert len(bp.calls) == 2  # short page 2 ended the scan
    assert bq.recorded_state()["watermark_id"] == 520


def _writer_with_fake_client(fake_client) -> BigQueryWriter:
    writer = object.__new__(BigQueryWriter)
    writer.client = fake_client
    return writer


def test_dml_retries_concurrent_update_aborts(monkeypatch):
    monkeypatch.setattr("sync.bq.time.sleep", lambda s: None)
    attempts = []

    class FlakyClient:
        def query_and_wait(self, sql):
            attempts.append(sql)
            if len(attempts) < 3:
                raise BadRequest("Transaction is aborted due to concurrent update against table t")
            return "ok"

    writer = _writer_with_fake_client(FlakyClient())
    assert writer._dml("MERGE ...") == "ok"
    assert len(attempts) == 3


def test_dml_does_not_retry_other_bad_requests():
    class BrokenClient:
        def query_and_wait(self, sql):
            raise BadRequest("Syntax error at [2:1]")

    writer = _writer_with_fake_client(BrokenClient())
    try:
        writer._dml("MERGE ...")
        raise AssertionError("expected BadRequest")
    except BadRequest as e:
        assert "Syntax error" in str(e)
