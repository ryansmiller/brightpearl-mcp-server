import httpx
import pytest
import respx

from brightpearl_client import (
    BrightpearlAuthError,
    BrightpearlClient,
    BrightpearlConfig,
    RateLimiter,
)

BASE = "https://use1.brightpearlconnect.com/public-api/testacct"


def make_client() -> BrightpearlClient:
    config = BrightpearlConfig(
        account="testacct",
        datacenter="use1",
        app_ref="testref",
        account_token="testtoken",
        _env_file=None,
    )
    return BrightpearlClient(config, RateLimiter(6000, 0.0))


SEARCH_BODY = {
    "response": {
        "metaData": {
            "resultsAvailable": 3,
            "resultsReturned": 2,
            "firstResult": 1,
            "lastResult": 2,
            "columns": [{"name": "orderId"}, {"name": "reference"}],
        },
        "results": [[101, "SO-101"], [102, "SO-102"]],
    }
}


@respx.mock
async def test_search_sends_auth_headers_and_parses_rows():
    route = respx.get(f"{BASE}/order-service/order-search").mock(
        return_value=httpx.Response(200, json=SEARCH_BODY)
    )
    async with make_client() as bp:
        page = await bp.orders.search()

    sent = route.calls.last.request
    assert sent.headers["brightpearl-app-ref"] == "testref"
    assert sent.headers["brightpearl-account-token"] == "testtoken"
    assert page.results == [
        {"orderId": 101, "reference": "SO-101"},
        {"orderId": 102, "reference": "SO-102"},
    ]
    assert page.results_available == 3
    assert page.has_more and page.next_first_result == 3


@respx.mock
async def test_retries_after_503_throttle():
    route = respx.get(f"{BASE}/order-service/order-search")
    route.side_effect = [
        httpx.Response(503, headers={"brightpearl-next-throttle-period": "100"}),
        httpx.Response(200, json=SEARCH_BODY),
    ]
    async with make_client() as bp:
        page = await bp.orders.search()
    assert route.call_count == 2
    assert len(page.results) == 2


@respx.mock
async def test_auth_failure_raises():
    respx.get(f"{BASE}/order-service/order-search").mock(
        return_value=httpx.Response(401, text="bad token")
    )
    async with make_client() as bp:
        with pytest.raises(BrightpearlAuthError):
            await bp.orders.search()


@respx.mock
async def test_multi_id_get_chunks_and_flattens():
    ids = list(range(1, 151))  # forces two chunks of <=100
    def responder(request):
        requested = request.url.path.rsplit("/", 1)[-1].split(",")
        return httpx.Response(
            200, json={"response": [{"id": int(i)} for i in requested]}
        )

    route = respx.get(url__regex=rf"{BASE}/order-service/order/.*").mock(
        side_effect=responder
    )
    async with make_client() as bp:
        out = await bp.orders.get(ids)
    assert route.call_count == 2
    assert [o["id"] for o in out] == ids
