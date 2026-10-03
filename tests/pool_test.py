"""Пул `twapStates`: учет подписок, отказы биржи, отсев и дедупликация ордеров."""

import asyncio
import time
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


def test_reject_cooldown_grows_and_resets() -> None:
    pool, _, _ = make_pool()
    state = _SlotState()
    limit_error = {"channel": "error", "data": "Cannot track more than 15 total users"}

    cooldowns = []
    for _ in range(6):
        state.pending.append((WALLET, 0.0))
        before = time.time()
        pool._handle_message(state, limit_error)
        cooldowns.append(round(state.blocked_until - before))

    assert cooldowns == [1, 2, 4, 8, 16, 16]

    # Успешная подписка сбрасывает серию.
    state.pending.append((WALLET, 0.0))
    pool._handle_message(
        state,
        {
            "channel": "subscriptionResponse",
            "data": {"method": "subscribe", "subscription": {"type": "twapStates", "user": WALLET}},
        },
    )
    assert state.reject_streak == 0


class FakeWebsocket:
    """Запоминает запросы на пересоздание соединения."""

    def __init__(self) -> None:
        self.reconnects = 0

    async def reconnect(self) -> None:
        self.reconnects += 1


async def test_crowded_node_triggers_reconnect_once_per_interval() -> None:
    from hl_twap_watcher._pool import _Slot

    pool, queue, _ = make_pool()
    websocket = FakeWebsocket()
    slot = _Slot(websocket=websocket)  # type: ignore[arg-type]
    limit_error = {"channel": "error", "data": "Cannot track more than 15 total users"}

    # Соединение почти пустое, а биржа отказывает — ноду занял кто-то еще.
    for _ in range(5):
        slot.state.pending.append((WALLET, 0.0))
        pool._handle_message(slot.state, limit_error)
    slot.state.pending.append(("0xpending", 0.0))

    await pool._leave_crowded_node(slot)
    await pool._leave_crowded_node(slot)  # второй раз раньше интервала — нет

    assert websocket.reconnects == 1
    assert pool.reconnects == 1
    # Непроверенный кандидат вернулся в очередь.
    assert "0xpending" in [queue.get_nowait() for _ in range(queue.qsize())]


def test_rejects_on_full_connection_are_not_crowded() -> None:
    pool, _, _ = make_pool()
    state = _SlotState()
    for index in range(13):
        state.tracked[f"0x{index}"] = 0.0
    state.pending.append((WALLET, 0.0))

    pool._handle_message(state, {"channel": "error", "data": "Cannot track more than 15 total users"})

    assert state.crowded_rejects == 0
