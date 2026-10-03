"""Фасад: фильтр крупных ордеров, дедупликация, связка поиска со слежкой."""

import pytest

from hl_twap_watcher import TwapWatcher, WatcherConfig
from hl_twap_watcher.types import TwapEvent

from .conftest import make_state


def make_watcher(*, free_slots: bool = True) -> tuple[TwapWatcher, list[tuple[str, int]]]:
    """Наблюдатель без соединений: BTC по $100, порог $1000, слежка подменена."""
    watcher = TwapWatcher(on_event=lambda event: None, config=WatcherConfig(min_notional_usd=1000))
    watcher._markets.apply_meta({"universe": [{"name": "BTC"}, {"name": "ETH"}]})
    watcher._markets.apply_mids({"BTC": "100"})

    tracked: list[tuple[str, int]] = []

    async def track(wallet: str, twap_id: int, *, since_ms: int) -> bool:
        if free_slots:
            tracked.append((wallet, twap_id))
        return free_slots

    watcher._tracker.track = track  # type: ignore[method-assign]
    return watcher, tracked


def drain(watcher: TwapWatcher) -> list[TwapEvent]:
    """Забирает события из очереди диспетчера без запуска воркера."""
    queue = watcher._dispatcher._queue
    return [queue.get_nowait() for _ in range(queue.qsize())]


def test_requires_callback() -> None:
    with pytest.raises(ValueError, match="callback"):
        TwapWatcher()


async def test_large_twap_is_tracked_and_reported_once() -> None:
    watcher, tracked = make_watcher()

    await watcher._on_twap_state(42, make_state(sz="20"))  # $2000
    await watcher._on_twap_state(42, make_state(sz="20"))  # повтор из twapStates

    [event] = drain(watcher)
    assert event["type"] == "created"
    assert event["notional_usd"] == 2000.0
    assert event["tracked"] is True
    assert len(tracked) == 1


async def test_small_twap_is_ignored() -> None:
    watcher, tracked = make_watcher()

    await watcher._on_twap_state(42, make_state(sz="5"))  # $500

    assert drain(watcher) == []
    assert tracked == []


async def test_non_perp_is_ignored() -> None:
    watcher, _ = make_watcher()

    await watcher._on_twap_state(1, make_state(coin="@107", sz="1000"))
    await watcher._on_twap_state(2, make_state(coin="xyz:SP500", sz="1000"))

    assert drain(watcher) == []


async def test_twap_without_price_is_retried_later() -> None:
    watcher, _ = make_watcher()

    await watcher._on_twap_state(1, make_state(coin="ETH", sz="1"))
    assert drain(watcher) == []

    watcher._markets.apply_mids({"ETH": "5000"})
    await watcher._on_twap_state(1, make_state(coin="ETH", sz="1"))
    assert [event["twap_id"] for event in drain(watcher)] == [1]


async def test_full_slots_still_emit_created() -> None:
    watcher, _ = make_watcher(free_slots=False)

    await watcher._on_twap_state(42, make_state(sz="20"))

    [event] = drain(watcher)
    assert event["tracked"] is False
    assert watcher.stats()["tracking_full"] == 1
