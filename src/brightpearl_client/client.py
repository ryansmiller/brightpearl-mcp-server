import asyncio
import logging
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any

import httpx

from .config import BrightpearlConfig
from .exceptions import (
    BrightpearlAuthError,
    BrightpearlError,
    BrightpearlNotFound,
    BrightpearlThrottled,
)
from .rate_limiter import RateLimiter

logger = logging.getLogger(__name__)

# Multi-ID GETs are joined into the URL path; keep chunks well under URI limits.
MULTI_ID_CHUNK = 100


@dataclass
class SearchPage:
    """One page of a Brightpearl resource-search response, rows keyed by column name."""

    results: list[dict[str, Any]]
    results_available: int
    first_result: int
    last_result: int

    @property
    def has_more(self) -> bool:
        return self.last_result < self.results_available

    @property
    def next_first_result(self) -> int:
        return self.last_result + 1


class ResourceAPI:
    """Thin accessor for one searchable resource, e.g. order-service/order."""

    def __init__(self, client: "BrightpearlClient", service: str, resource: str):
        self._client = client
        self.service = service
        self.resource = resource

    async def search(
        self,
        filters: dict[str, Any] | None = None,
        columns: list[str] | None = None,
        first_result: int = 1,
        *,
        priority: bool = False,
    ) -> SearchPage:
        return await self._client.search(
            self.service,
            self.resource,
            filters=filters,
            columns=columns,
            first_result=first_result,
            priority=priority,
        )

    def search_all(
        self,
        filters: dict[str, Any] | None = None,
        columns: list[str] | None = None,
        *,
        priority: bool = False,
    ) -> AsyncIterator[dict[str, Any]]:
        return self._client.search_all(
            self.service, self.resource, filters=filters, columns=columns, priority=priority
        )

    async def get(self, ids: list[int], *, priority: bool = False) -> list[dict[str, Any]]:
        return await self._client.get_resources(
            self.service, self.resource, ids, priority=priority
        )


class BrightpearlClient:
    """Async Brightpearl API client.

    All requests flow through one RateLimiter sharing the account's 200 req/min
    budget. Pass priority=True for webhook follow-up fetches and live MCP
    lookups; leave it False for background sweeps.
    """

    def __init__(
        self,
        config: BrightpearlConfig | None = None,
        rate_limiter: RateLimiter | None = None,
    ):
        self.config = config or BrightpearlConfig()
        self.rate_limiter = rate_limiter or RateLimiter(
            self.config.requests_per_minute, self.config.reserve_fraction
        )
        self._http = httpx.AsyncClient(
            base_url=self.config.base_url,
            headers=self.config.auth_headers,
            timeout=self.config.timeout_seconds,
        )

        self.orders = ResourceAPI(self, "order-service", "order")
        self.products = ResourceAPI(self, "product-service", "product")
        self.contacts = ResourceAPI(self, "contact-service", "contact")
        self.goods_out_notes = ResourceAPI(self, "warehouse-service", "goods-note/goods-out")

    async def __aenter__(self) -> "BrightpearlClient":
        return self

    async def __aexit__(self, *exc_info) -> None:
        await self.close()

    async def close(self) -> None:
        await self._http.aclose()

    async def get(
        self, path: str, params: dict[str, Any] | None = None, *, priority: bool = False
    ) -> Any:
        """GET a public-api path (e.g. 'order-service/order/123') → the 'response' payload."""
        last_throttle = 0.0
        for attempt in range(self.config.max_retries + 1):
            await self.rate_limiter.acquire(priority=priority)
            try:
                resp = await self._http.get(path, params=params)
            except httpx.TransportError as exc:
                if attempt == self.config.max_retries:
                    raise BrightpearlError(f"transport failure for {path}: {exc}") from exc
                await asyncio.sleep(min(2**attempt, 30))
                continue

            remaining = resp.headers.get("brightpearl-requests-remaining")
            if remaining and remaining.isdigit():
                self.rate_limiter.sync_remaining(int(remaining))

            if resp.status_code == 503:
                period_ms = resp.headers.get("brightpearl-next-throttle-period", "60000")
                last_throttle = int(period_ms) / 1000 if period_ms.isdigit() else 60.0
                self.rate_limiter.note_throttled(last_throttle)
                logger.warning("throttled by Brightpearl; pausing %.1fs", last_throttle)
                continue
            if resp.status_code in (401, 403):
                raise BrightpearlAuthError(resp.text, resp.status_code)
            if resp.status_code == 404:
                raise BrightpearlNotFound(path, 404)
            if resp.status_code >= 500:
                if attempt == self.config.max_retries:
                    raise BrightpearlError(resp.text, resp.status_code)
                await asyncio.sleep(min(2**attempt, 30))
                continue
            if resp.status_code >= 400:
                raise BrightpearlError(resp.text, resp.status_code)

            body = resp.json()
            if body.get("errors"):
                raise BrightpearlError(str(body["errors"]), resp.status_code)
            return body.get("response", body)

        raise BrightpearlThrottled(
            f"still throttled after {self.config.max_retries} retries", 503
        )

    async def search(
        self,
        service: str,
        resource: str,
        filters: dict[str, Any] | None = None,
        columns: list[str] | None = None,
        first_result: int = 1,
        *,
        priority: bool = False,
    ) -> SearchPage:
        """Run one page of a resource-search, returning rows keyed by column name.

        `filters` pass through as query params — e.g. {"updatedOn": "2026-07-01T00:00:00Z/"}
        for an open-ended updatedOn range.
        """
        params: dict[str, Any] = dict(filters or {})
        if columns:
            params["columns"] = ",".join(columns)
        if first_result > 1:
            params["firstResult"] = first_result

        payload = await self.get(f"{service}/{resource}-search", params, priority=priority)
        meta = payload["metaData"]
        names = [c["name"] for c in meta["columns"]]
        rows = [dict(zip(names, row)) for row in payload.get("results", [])]
        return SearchPage(
            results=rows,
            results_available=meta.get("resultsAvailable", len(rows)),
            first_result=meta.get("firstResult", first_result),
            last_result=meta.get("lastResult", first_result + len(rows) - 1),
        )

    async def search_all(
        self,
        service: str,
        resource: str,
        filters: dict[str, Any] | None = None,
        columns: list[str] | None = None,
        *,
        priority: bool = False,
    ) -> AsyncIterator[dict[str, Any]]:
        """Iterate every row of a resource-search, paginating transparently."""
        first = 1
        while True:
            page = await self.search(
                service, resource, filters=filters, columns=columns,
                first_result=first, priority=priority,
            )
            for row in page.results:
                yield row
            if not page.has_more or not page.results:
                return
            first = page.next_first_result

    async def get_resources(
        self, service: str, resource: str, ids: list[int], *, priority: bool = False
    ) -> list[dict[str, Any]]:
        """Fetch full resources by ID using multi-ID GETs (one request per chunk)."""
        out: list[dict[str, Any]] = []
        for i in range(0, len(ids), MULTI_ID_CHUNK):
            chunk = ids[i : i + MULTI_ID_CHUNK]
            id_set = ",".join(str(x) for x in chunk)
            payload = await self.get(f"{service}/{resource}/{id_set}", priority=priority)
            out.extend(payload if isinstance(payload, list) else [payload])
        return out
