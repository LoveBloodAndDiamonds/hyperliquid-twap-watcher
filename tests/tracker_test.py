"""Слежка: слоты кошельков, слайсы, завершения, новые ордера и отказы биржи."""

from typing import Any

from hl_twap_watcher._tracker import CHANNELS, TwapTracker, _Connection, subscription
from hl_twap_watcher.config import MAX_USERS_PER_WS, WatcherConfig
from hl_twap_watcher.types import TwapEvent

from .conftest import OTHER, WALLET, make_fill, make_record, make_state

LIMIT_ERROR = {"channel": "error", "data": "Cannot track more than 15 total users."}


class FakeWebsocket:
    """Подменяет Websocket: запоминает подписки, отписки и переподключения."""

    def __init__(self) -> None:
        self._subscriptions: list[dict[str, Any]] = []
        self.unsubscribed: list[dict[str, Any]] = []
        self.reconnects = 0
        self.connected = True

    @property
    def subscriptions(self) -> list[dict[str, Any]]:
        return list(self._subscriptions)

    async def add_subscription(self, message: dict[str, Any]) -> bool:
        self._subscriptions.append(message)
        return True

    async def remove_subscription(self, message: dict[str, Any]) -> None:
        self._subscriptions.remove(message)
        self.unsubscribed.append(message)

    async def reconnect(self) -> None:
        self.reconnects += 1


def make_tracker(
    connections: int = 1,
) -> tuple[TwapTracker, list[FakeWebsocket], list[TwapEvent], list[tuple[int, dict[str, Any]]]]:
    """Трекер с фейковыми соединениями, пойманными событиями и новыми ордерами."""
    events: list[TwapEvent] = []
    new_twaps: list[tuple[int, dict[str, Any]]] = []

    async def on_new_twap(twap_id: int, state: dict[str, Any]) -> None:
        new_twaps.append((twap_id, state))

    tracker = TwapTracker(WatcherConfig(), events.append, on_new_twap)
    sockets = [FakeWebsocket() for _ in range(connections)]
    tracker._connections = [_Connection(websocket=ws) for ws in sockets]  # type: ignore[arg-type]
    return tracker, sockets, events, new_twaps


def fills_message(*items: dict[str, Any], snapshot: bool = False) -> dict[str, Any]:
    data: dict[str, Any] = {"user": WALLET, "twapSliceFills": list(items)}
    if snapshot:
        data["isSnapshot"] = True
    return {"channel": "userTwapSliceFills", "data": data}


def history_message(*records: dict[str, Any], snapshot: bool = False) -> dict[str, Any]:
    data: dict[str, Any] = {"user": WALLET, "history": list(records)}
    if snapshot:
        data["isSnapshot"] = True
    return {"channel": "userTwapHistory", "data": data}


async def test_track_subscribes_wallet_once() -> None:
    tracker, [ws], _, _ = make_tracker()

    assert await tracker.track(WALLET, 1, since_ms=0)
    assert await tracker.track(WALLET, 2, since_ms=0)  # второй ордер того же кошелька

    assert ws.subscriptions == [subscription(channel, WALLET) for channel in CHANNELS]
    assert tracker.wallets_count == 1
    assert tracker.twaps_count == 2
    assert tracker.is_tracked(WALLET)
    assert tracker.is_twap_tracked(WALLET, 2)


async def test_track_returns_false_when_slots_full() -> None:
    tracker, _, _, _ = make_tracker(connections=1)
    for index in range(MAX_USERS_PER_WS):
        assert await tracker.track(f"0x{index:040x}", index, since_ms=0)

    assert not await tracker.track(WALLET, 999, since_ms=0)
    assert not tracker.is_tracked(WALLET)


async def test_wallets_spread_across_connections() -> None:
    tracker, sockets, _, _ = make_tracker(connections=2)

    await tracker.track(WALLET, 1, since_ms=0)
    await tracker.track(OTHER, 2, since_ms=0)

    assert [len(ws.subscriptions) for ws in sockets] == [2, 2]


async def test_fills_become_slices_with_exact_twap_id() -> None:
    tracker, [ws], events, _ = make_tracker()
    await tracker.track(WALLET, 1, since_ms=1000)
    connection = tracker._connections[0]

    await tracker._handle_message(
        connection,
        fills_message(
            make_fill(1, time_ms=2000, tid=11),
            make_fill(1, time_ms=2000, tid=12),  # тот же слайс, второй уровень стакана
            make_fill(77, time_ms=2000, tid=13),  # ордер, который не отслеживается
        ),
    )

    assert [(e["type"], e["twap_id"], e["trade_id"]) for e in events] == [
        ("slice", 1, 11),
        ("slice", 1, 12),
    ]


async def test_snapshot_backfills_only_new_fills() -> None:
    tracker, _, events, _ = make_tracker()
    await tracker.track(WALLET, 1, since_ms=1000)
    connection = tracker._connections[0]
    await tracker._handle_message(connection, fills_message(make_fill(1, time_ms=2000, tid=11)))

    # После реконнекта снапшот повторяет старое и приносит пропущенное.
    await tracker._handle_message(
        connection,
        fills_message(
            make_fill(1, time_ms=3000, tid=13),
            make_fill(1, time_ms=2000, tid=11),
            make_fill(1, time_ms=500, tid=10),  # раньше обнаружения
            snapshot=True,
        ),
    )

    assert [e["trade_id"] for e in events] == [11, 13]


async def test_finished_statuses_and_wallet_release() -> None:
    tracker, [ws], events, _ = make_tracker()
    for twap_id in (1, 2, 3, 4):
        await tracker.track(WALLET, twap_id, since_ms=0)
    connection = tracker._connections[0]

    await tracker._handle_message(
        connection,
        history_message(
            make_record(1, "finished"),
            make_record(2, "terminated"),
            make_record(3, "stopped"),
        ),
    )
    assert tracker.wallets_count == 1  # ордер 4 еще жив — слот держим
    assert ws.unsubscribed == []

    await tracker._handle_message(connection, history_message(make_record(4, "error")))

    reasons = {e["twap_id"]: e["reason"] for e in events if e["type"] == "finished"}
    assert reasons == {1: "completed", 2: "cancelled", 3: "stopped", 4: "error"}
    assert tracker.wallets_count == 0
    assert ws.unsubscribed == [subscription(channel, WALLET) for channel in CHANNELS]
    assert ws.subscriptions == []


async def test_activated_record_reports_new_twap() -> None:
    tracker, _, events, new_twaps = make_tracker()
    await tracker.track(WALLET, 1, since_ms=0)
    connection = tracker._connections[0]

    await tracker._handle_message(
        connection,
        history_message(
            make_record(1, "activated"),
            make_record(2, "activated", state=make_state(coin="ETH")),
            make_record(3, "finished"),  # чужой завершенный ордер — не интересен
            # Старый ордер, финальная запись которого обрезана снапшотом.
            make_record(4, "activated", state=make_state(timestamp=0, minutes=60)),
            snapshot=True,
        ),
    )

    assert [twap_id for twap_id, _ in new_twaps] == [2]
    assert events == []


async def test_limit_error_reconnects_with_growing_pause() -> None:
    tracker, [ws], _, _ = make_tracker()
    await tracker.track(WALLET, 1, since_ms=0)
    connection = tracker._connections[0]

    await tracker._handle_message(connection, LIMIT_ERROR)
    await tracker._leave_crowded_node(connection)  # первый уход — сразу
    assert ws.reconnects == 1

    # Новая нода тоже занята: повтор только после паузы.
    connection.crowded = True
    await tracker._leave_crowded_node(connection)
    assert ws.reconnects == 1

    connection.last_reconnect_at -= 6
    await tracker._leave_crowded_node(connection)
    assert ws.reconnects == 2
    assert connection.reconnect_streak == 2

    assert tracker.rejected == 1
    # Кошелек остается за соединением: подписка повторится после реконнекта.
    assert tracker.is_tracked(WALLET)


async def test_reconnect_streak_resets_when_stable() -> None:
    tracker, _, _, _ = make_tracker()
    connection = tracker._connections[0]
    connection.reconnect_streak = 3
    connection.last_reconnect_at -= 121

    await tracker._leave_crowded_node(connection)

    assert connection.reconnect_streak == 0


async def test_subscription_confirm_clears_pending() -> None:
    tracker, _, _, _ = make_tracker()
    await tracker.track(WALLET, 1, since_ms=0)
    connection = tracker._connections[0]
    assert len(connection.pending) == 2

    for channel in CHANNELS:
        await tracker._handle_message(
            connection,
            {"channel": "subscriptionResponse", "data": subscription(channel, WALLET)},
        )

    assert not connection.pending
    # Без ожидающих подписок ошибка лимита ни к кому не относится.
    await tracker._handle_message(connection, LIMIT_ERROR)
    assert tracker.rejected == 0
