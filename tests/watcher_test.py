"""Фасад: сборка событий из потоков и проверка аргументов."""

import pytest

from hl_twap_watcher import TwapWatcher
from hl_twap_watcher.types import TwapEvent

from .conftest import OTHER, WALLET, make_state, make_trade


def make_watcher() -> tuple[TwapWatcher, list[TwapEvent]]:
    """Наблюдатель без соединений: события копятся в очереди диспетчера."""
    events: list[TwapEvent] = []
    watcher = TwapWatcher(on_event=events.append)
    watcher._markets.apply_meta({"universe": [{"name": "BTC"}]})
    watcher._markets.apply_mids({"BTC": "100"})
    return watcher, events


def drain(watcher: TwapWatcher) -> list[TwapEvent]:
    """Забирает события из очереди диспетчера без запуска воркера."""
    queue = watcher._dispatcher._queue
    return [queue.get_nowait() for _ in range(queue.qsize())]


def test_requires_callback() -> None:
    with pytest.raises(ValueError, match="callback"):
        TwapWatcher()


def test_twap_state_becomes_created_event_and_is_tracked() -> None:
    watcher, _ = make_watcher()

    watcher._on_twap_state(42, make_state(sz="3"))

    [event] = drain(watcher)
    assert event["type"] == "created"
    assert event["notional_usd"] == 300.0
    assert watcher.stats()["tracked_twaps"] == 1


def test_slice_matched_by_side() -> None:
    watcher, _ = make_watcher()
    watcher._on_twap_state(1, make_state(user=WALLET, side="B"))
    watcher._on_twap_state(2, make_state(user=OTHER, side="A"))
    drain(watcher)

    # WALLET покупает, OTHER продает — сделка между ними слайс для обоих ордеров.
    watcher._on_zero_hash_trade(make_trade(buyer=WALLET, seller=OTHER))

    slices = drain(watcher)
    assert [(e["type"], e["twap_id"], e["side"]) for e in slices] == [
        ("slice", 1, "BUY"),
        ("slice", 2, "SELL"),
    ]


def test_trade_on_wrong_side_is_not_a_slice() -> None:
    watcher, _ = make_watcher()
    watcher._on_twap_state(1, make_state(user=WALLET, side="B"))
    drain(watcher)

    # WALLET в этой сделке продавец, а его ордер на покупку.
    watcher._on_zero_hash_trade(make_trade(buyer=OTHER, seller=WALLET))

    assert drain(watcher) == []
