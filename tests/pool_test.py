"""Поиск: учет подписок `twapStates`, отказы биржи, уход с занятой ноды."""

import asyncio
import time
from typing import Any

from hl_twap_watcher._pool import DiscoveryPool, _Slot
from hl_twap_watcher.config import WatcherConfig

from .conftest import WALLET, make_state

LIMIT_ERROR = {"channel": "error", "data": "Cannot track more than 15 total users"}


class FakeWebsocket:
    """Запоминает запросы на пересоздание соединения."""

    def __init__(self) -> None:
        self.reconnects = 0

    async def reconnect(self) -> None:
        self.reconnects += 1


def make_pool() -> tuple[DiscoveryPool, asyncio.Queue[str], list[tuple[int, dict[str, Any]]]]:
    """Собирает пул без соединений и список переданных состояний TWAP."""
    queue: asyncio.Queue[str] = asyncio.Queue()
    found: list[tuple[int, dict[str, Any]]] = []

    async def on_twap(twap_id: int, state: dict[str, Any]) -> None:
        found.append((twap_id, state))

    return DiscoveryPool(WatcherConfig(), queue, on_twap), queue, found


def make_slot() -> tuple[_Slot, FakeWebsocket]:
    """Соединение пула с фейковым вебсокетом."""
    websocket = FakeWebsocket()
    return _Slot(websocket=websocket), websocket  # type: ignore[arg-type]


def confirm_message(user: str = WALLET) -> dict[str, Any]:
    """Подтверждение подписки `twapStates`."""
    return {
        "channel": "subscriptionResponse",
        "data": {"method": "subscribe", "subscription": {"type": "twapStates", "user": user}},
    }


async def test_subscription_confirm_moves_pending_to_tracked() -> None:
    pool, _, _ = make_pool()
    slot, _ = make_slot()
    slot.state.pending.append((WALLET, 0.0))

    await pool._handle_message(slot, confirm_message())

    assert not slot.state.pending
    assert WALLET in slot.state.tracked
    assert pool.checked == 1


async def test_twap_states_are_forwarded() -> None:
    pool, _, found = make_pool()
    slot, _ = make_slot()

    await pool._handle_message(
        slot,
        {
            "channel": "twapStates",
            "data": {"dex": "", "user": WALLET, "states": [[1, make_state()], [2, make_state()]]},
        },
    )

    assert [twap_id for twap_id, _ in found] == [1, 2]


async def test_limit_error_requeues_oldest_pending() -> None:
    pool, queue, _ = make_pool()
    slot, _ = make_slot()
    slot.state.pending.append((WALLET, 0.0))

    await pool._handle_message(slot, LIMIT_ERROR)

    assert not slot.state.pending
    assert queue.get_nowait() == WALLET
    assert pool.rejected == 1
    assert slot.state.blocked_until > 0


async def test_other_errors_are_ignored() -> None:
    pool, queue, _ = make_pool()
    slot, _ = make_slot()
    slot.state.pending.append((WALLET, 0.0))

    await pool._handle_message(slot, {"channel": "error", "data": "Already unsubscribed"})

    assert len(slot.state.pending) == 1
    assert queue.empty()
    assert not slot.backoff.crowded


async def test_reject_cooldown_grows_and_resets() -> None:
    pool, _, _ = make_pool()
    slot, _ = make_slot()

    cooldowns = []
    for _ in range(6):
        slot.state.pending.append((WALLET, 0.0))
        before = time.time()
        await pool._handle_message(slot, LIMIT_ERROR)
        cooldowns.append(round(slot.state.blocked_until - before))

    assert cooldowns == [1, 2, 4, 8, 16, 16]

    # Успешная подписка сбрасывает серию.
    slot.state.pending.append((WALLET, 0.0))
    await pool._handle_message(slot, confirm_message())
    assert slot.state.reject_streak == 0


async def test_any_limit_reject_marks_node_crowded() -> None:
    """Даже почти полное соединение уходит с ноды: оно работает не в полную силу."""
    pool, _, _ = make_pool()
    slot, _ = make_slot()
    for index in range(13):
        slot.state.tracked[f"0x{index}"] = 0.0
    slot.state.pending.append((WALLET, 0.0))

    await pool._handle_message(slot, LIMIT_ERROR)

    assert slot.backoff.crowded


async def test_crowded_node_reconnects_and_requeues_pending() -> None:
    pool, queue, _ = make_pool()
    slot, websocket = make_slot()

    slot.state.pending.append((WALLET, 0.0))
    await pool._handle_message(slot, LIMIT_ERROR)
    slot.state.pending.append(("0xpending", 0.0))

    await pool._leave_crowded_node(slot)  # первый уход — сразу
    assert websocket.reconnects == 1
    assert pool.reconnects == 1
    # Непроверенный кандидат вернулся в очередь.
    assert "0xpending" in [queue.get_nowait() for _ in range(queue.qsize())]

    # Новая нода тоже занята: повтор только после паузы.
    slot.backoff.crowded = True
    await pool._leave_crowded_node(slot)
    assert websocket.reconnects == 1
