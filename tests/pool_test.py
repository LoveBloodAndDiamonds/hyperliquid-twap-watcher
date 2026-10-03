"""Пул `twapStates`: учет подписок, отказы биржи, отсев и дедупликация ордеров."""

import asyncio
from typing import Any

from hl_twap_watcher._markets import Markets
from hl_twap_watcher._pool import TwapStatesPool, _SlotState
from hl_twap_watcher.config import WatcherConfig

from .conftest import WALLET, make_state


def make_pool() -> tuple[TwapStatesPool, asyncio.Queue[str], list[tuple[int, dict[str, Any]]]]:
    """Собирает пул со справочником из двух перпов и списком найденных ордеров."""
    markets = Markets()
    markets.apply_meta({"universe": [{"name": "BTC"}, {"name": "ETH"}]})
    queue: asyncio.Queue[str] = asyncio.Queue()
    found: list[tuple[int, dict[str, Any]]] = []
    pool = TwapStatesPool(WatcherConfig(), markets, queue, lambda i, s: found.append((i, s)))
    return pool, queue, found


def states_message(*states: tuple[int, dict[str, Any]]) -> dict[str, Any]:
    """Сообщение канала `twapStates`."""
    return {
        "channel": "twapStates",
        "data": {"dex": "", "user": WALLET, "states": [[i, s] for i, s in states]},
    }


def test_subscription_confirm_moves_pending_to_tracked() -> None:
    pool, _, _ = make_pool()
    state = _SlotState()
    state.pending.append((WALLET, 0.0))

    pool._handle_message(
        state,
        {
            "channel": "subscriptionResponse",
            "data": {"method": "subscribe", "subscription": {"type": "twapStates", "user": WALLET}},
        },
    )

    assert not state.pending
    assert WALLET in state.tracked
    assert pool.checked == 1


def test_limit_error_requeues_oldest_pending() -> None:
    pool, queue, _ = make_pool()
    state = _SlotState()
    state.pending.append((WALLET, 0.0))

    pool._handle_message(state, {"channel": "error", "data": "Cannot track more than 15 total users"})

    assert not state.pending
    assert queue.get_nowait() == WALLET
    assert pool.rejected == 1
    assert state.blocked_until > 0


def test_other_errors_are_ignored() -> None:
    pool, queue, _ = make_pool()
    state = _SlotState()
    state.pending.append((WALLET, 0.0))

    pool._handle_message(state, {"channel": "error", "data": "Already unsubscribed"})

    assert len(state.pending) == 1
    assert queue.empty()


def test_twap_states_dedup_and_filter() -> None:
    pool, _, found = make_pool()
    state = _SlotState()

    pool._handle_message(
        state,
        states_message(
            (1, make_state(coin="BTC")),
            (2, make_state(coin="@107")),  # спот
            (3, make_state(coin="xyz:SP500")),  # builder-dex
        ),
    )
    # Пока кошелек в подписке, биржа повторяет состояния — второй раз не отдаем.
    pool._handle_message(state, states_message((1, make_state(coin="BTC"))))

    assert [twap_id for twap_id, _ in found] == [1]


def test_unknown_perp_is_not_remembered() -> None:
    """Новый листинг, которого еще нет в справочнике, найдется после его обновления."""
    pool, _, found = make_pool()
    state = _SlotState()

    pool._handle_message(state, states_message((1, make_state(coin="NEW"))))
    pool._markets.apply_meta({"universe": [{"name": "BTC"}, {"name": "NEW"}]})
    pool._handle_message(state, states_message((1, make_state(coin="NEW"))))

    assert [twap_id for twap_id, _ in found] == [1]
