import asyncio
import time


class RateLimiter:
    """Token bucket for Brightpearl's per-account request budget (200/min).

    A configurable reserve fraction is held back for priority calls — webhook
    follow-up fetches and live MCP lookups — so background sweeps can never
    starve them. Priority callers may dip into the reserve; background callers
    block once only the reserve remains.

    The server's own view of the budget wins when it is stricter than ours:
    `sync_remaining()` folds in the `brightpearl-requests-remaining` response
    header, and `note_throttled()` pauses everything for the duration given by
    `brightpearl-next-throttle-period` after a 503.
    """

    def __init__(self, requests_per_minute: int = 200, reserve_fraction: float = 0.25):
        self._capacity = float(requests_per_minute)
        self._tokens = float(requests_per_minute)
        self._reserve = self._capacity * reserve_fraction
        self._rate = requests_per_minute / 60.0
        self._updated = time.monotonic()
        self._throttled_until = 0.0
        self._lock = asyncio.Lock()

    def _refill(self) -> None:
        now = time.monotonic()
        self._tokens = min(self._capacity, self._tokens + (now - self._updated) * self._rate)
        self._updated = now

    async def acquire(self, *, priority: bool = False) -> None:
        while True:
            async with self._lock:
                self._refill()
                now = time.monotonic()
                floor = 0.0 if priority else self._reserve
                if now >= self._throttled_until and self._tokens >= 1.0 + floor:
                    self._tokens -= 1.0
                    return
                wait = max(
                    self._throttled_until - now,
                    (1.0 + floor - self._tokens) / self._rate,
                )
            await asyncio.sleep(min(wait, 5.0))

    def note_throttled(self, retry_after_seconds: float) -> None:
        self._throttled_until = max(
            self._throttled_until, time.monotonic() + retry_after_seconds
        )

    def sync_remaining(self, server_remaining: int) -> None:
        self._refill()
        if server_remaining < self._tokens:
            self._tokens = float(server_remaining)
