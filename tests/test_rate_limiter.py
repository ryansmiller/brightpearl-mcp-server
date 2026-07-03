import asyncio

import pytest

from brightpearl_client.rate_limiter import RateLimiter


async def test_background_callers_cannot_touch_reserve():
    # 4-token bucket with a 50% reserve: background gets 2, priority gets the rest
    rl = RateLimiter(requests_per_minute=4, reserve_fraction=0.5)

    await rl.acquire()
    await rl.acquire()
    # Third background acquire would dip into the reserve — it must block
    with pytest.raises(TimeoutError):
        await asyncio.wait_for(rl.acquire(), timeout=0.1)

    # Priority calls may use the reserve
    await asyncio.wait_for(rl.acquire(priority=True), timeout=0.1)


async def test_note_throttled_pauses_even_priority():
    rl = RateLimiter(requests_per_minute=600, reserve_fraction=0.0)
    rl.note_throttled(0.3)
    with pytest.raises(TimeoutError):
        await asyncio.wait_for(rl.acquire(priority=True), timeout=0.1)
    # After the throttle window passes, acquisition succeeds
    await asyncio.wait_for(rl.acquire(priority=True), timeout=1.0)


async def test_sync_remaining_lowers_local_budget():
    rl = RateLimiter(requests_per_minute=200, reserve_fraction=0.0)
    rl.sync_remaining(1)
    await asyncio.wait_for(rl.acquire(), timeout=0.1)
    # Bucket now nearly empty; next acquire must wait for refill
    with pytest.raises(TimeoutError):
        await asyncio.wait_for(rl.acquire(), timeout=0.05)
