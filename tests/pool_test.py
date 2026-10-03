"""Поиск: учет подписок `twapStates`, отказы биржи, уход с занятой ноды."""

import asyncio
import time
from typing import Any

from hl_twap_watcher._pool import DiscoveryPool, _Slot, _SlotState
from hl_twap_watcher.config import WatcherConfig

from .conftest import WALLET, make_state

LIMIT_ERROR = {"channel": "error", "data": "Cannot track more than 15 total users"}


def make_pool() -> tuple[DiscoveryPool, asyncio.Queue[str], list[tuple[int, dict[str, Any]]]]:
    """Собирает пул без соединений и список переданных состояний TWAP."""
    queue: asyncio.Queue[str] = asyncio.Queue()
    found: list[tuple[int, dict[str, Any]]] = []

    async def on_twap(twap_id: int, state: dict[str, Any]) -> None:
        found.append((twap_id, state))

    return DiscoveryPool(WatcherConfig(), queue, on_twap), queue, found


def confirm_message(user: str = WALLET) -> dict[str, Any]:
    """Подтверждение подписки `twapStates`."""
    return {
        "channel": "subscriptionResponse",
        "data": {"method": "subscribe", "subscription": {"type": "twapStates", "user": user}},
    }


async def test_subscription_confirm_moves_pending_to_tracked() -> None:
    pool, _, _ = make_pool()
    state = _SlotState()
    state.pending.append((WALLET, 0.0))

    await pool._handle_message(state, confirm_message())

    assert not state.pending
    assert WALLET in state.tracked
    assert pool.checked == 1


async def test_twap_states_are_forwarded() -> None:
    pool, _, found = make_pool()

    await pool._handle_message(
        _SlotState(),
        {
            "channel": "twapStates",
            "data": {"dex": "", "user": WALLET, "states": [[1, make_state()], [2, make_state()]]},
        },
    )

    assert [twap_id for twap_id, _ in found] == [1, 2]


async def test_limit_error_requeues_oldest_pending() -> None:
    pool, queue, _ = make_pool()
    state = _SlotState()
    state.pending.append((WALLET, 0.0))

    await pool._handle_message(state, LIMIT_ERROR)

    assert not state.pending
    assert queue.get_nowait() == WALLET
    assert pool.rejected == 1
    assert state.blocked_until > 0


async def test_other_errors_are_ignored() -> None:
    pool, queue, _ = make_pool()
    state = _SlotState()
    state.pending.append((WALLET, 0.0))

    await pool._handle_message(state, {"channel": "error", "data": "Already unsubscribed"})

    assert len(state.pending) == 1
    assert queue.empty()


async def test_reject_cooldown_grows_and_resets() -> None:
    pool, _, _ = make_pool()
    state = _SlotState()

    cooldowns = []
    for _ in range(6):
        state.pending.append((WALLET, 0.0))
        before = time.time()
        await pool._handle_message(state, LIMIT_ERROR)
        cooldowns.append(round(state.blocked_until - before))

    assert cooldowns == [1, 2, 4, 8, 16, 16]

    # Успешная подписка сбрасывает серию.
    state.pending.append((WALLET, 0.0))
    await pool._handle_message(state, confirm_message())
    assert state.reject_streak == 0


class FakeWebsocket:
    """Запоминает запросы на пересоздание соединения."""

    def __init__(self) -> None:
        self.reconnects = 0

    async def reconnect(self) -> None:
        self.reconnects += 1


async def test_crowded_node_triggers_reconnect_once_per_interval() -> None:
    pool, queue, _ = make_pool()
    websocket = FakeWebsocket()
    slot = _Slot(websocket=websocket)  # type: ignore[arg-type]

    # Соединение почти пустое, а биржа отказывает — ноду занял кто-то еще.
    for _ in range(5):
        slot.state.pending.append((WALLET, 0.0))
        await pool._handle_message(slot.state, LIMIT_ERROR)
    slot.state.pending.append(("0xpending", 0.0))

    await pool._leave_crowded_node(slot)
    await pool._leave_crowded_node(slot)  # второй раз раньше интервала — нет

    assert websocket.reconnects == 1
    assert pool.reconnects == 1
    # Непроверенный кандидат вернулся в очередь.
    assert "0xpending" in [queue.get_nowait() for _ in range(queue.qsize())]


async def test_rejects_on_full_connection_are_not_crowded() -> None:
    pool, _, _ = make_pool()
    state = _SlotState()
    for index in range(13):
        state.tracked[f"0x{index}"] = 0.0
    state.pending.append((WALLET, 0.0))

    await pool._handle_message(state, LIMIT_ERROR)

    assert state.crowded_rejects == 0
