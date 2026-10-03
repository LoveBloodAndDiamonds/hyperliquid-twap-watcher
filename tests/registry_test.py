"""Регистр отслеживаемых ордеров: сопоставление слайсов и расписание проверок."""

from hl_twap_watcher._registry import TwapRegistry
from hl_twap_watcher.config import WatcherConfig
from hl_twap_watcher.types import TwapCreatedEvent

from .conftest import OTHER, WALLET, make_created

CONFIG = WatcherConfig(slice_silence_seconds=90, check_cooldown_seconds=60, check_max_backoff_seconds=900)


def test_match_slice_by_wallet_coin_side(created: TwapCreatedEvent) -> None:
    registry = TwapRegistry(CONFIG)
    registry.add(created, now=0)

    assert registry.match_slice(WALLET, "BTC", "BUY", now=1) == [100]
    assert registry.match_slice(WALLET, "BTC", "SELL", now=1) == []
    assert registry.match_slice(WALLET, "ETH", "BUY", now=1) == []
    assert registry.match_slice(OTHER, "BTC", "BUY", now=1) == []


def test_match_slice_ambiguous_returns_all_candidates() -> None:
    registry = TwapRegistry(CONFIG)
    registry.add(make_created(101), now=0)
    registry.add(make_created(100), now=0)

    assert registry.match_slice(WALLET, "BTC", "BUY", now=1) == [100, 101]


def test_remove_cleans_index(created: TwapCreatedEvent) -> None:
    registry = TwapRegistry(CONFIG)
    registry.add(created, now=0)

    registry.remove(WALLET, 100)
    registry.remove(WALLET, 100)  # повторное снятие безопасно

    assert len(registry) == 0
    assert registry.match_slice(WALLET, "BTC", "BUY", now=1) == []


def test_due_after_silence(created: TwapCreatedEvent) -> None:
    registry = TwapRegistry(CONFIG)
    registry.add(created, now=1000)
    twap = registry.for_wallet(WALLET)[0]

    assert not registry.is_due(twap, 1050)
    assert registry.is_due(twap, 1091)

    # Слайс сбрасывает тишину.
    registry.match_slice(WALLET, "BTC", "BUY", now=1080)
    assert not registry.is_due(twap, 1091)


def test_due_after_planned_end_even_with_slices() -> None:
    registry = TwapRegistry(CONFIG)
    # Ордер на 1 минуту, созданный 10 минут назад: срок давно вышел.
    event = make_created(minutes=1, timestamp=0)
    registry.add(event, now=600)
    twap = registry.for_wallet(WALLET)[0]

    registry.match_slice(WALLET, "BTC", "BUY", now=600)

    assert registry.is_due(twap, 601)


def test_postpone_backoff_grows_and_caps(created: TwapCreatedEvent) -> None:
    registry = TwapRegistry(CONFIG)
    registry.add(created, now=0)
    twap = registry.for_wallet(WALLET)[0]

    delays = []
    for _ in range(8):
        registry.postpone(twap, 1000)
        delays.append(twap.next_check_at - 1000)

    assert delays == [60, 120, 240, 480, 900, 900, 900, 900]
    assert not registry.is_due(twap, 1500)

    # Слайс отменяет паузу: следующая тишина проверяется сразу.
    registry.match_slice(WALLET, "BTC", "BUY", now=1000)
    assert twap.failed_checks == 0
    assert twap.next_check_at == 0


def test_next_due_wallet_prefers_longest_waiting() -> None:
    registry = TwapRegistry(CONFIG)
    registry.add(make_created(1, user=WALLET), now=100)
    registry.add(make_created(2, user=OTHER), now=50)

    assert registry.next_due_wallet(now=120) is None
    assert registry.next_due_wallet(now=500) == OTHER
