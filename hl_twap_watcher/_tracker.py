"""Слежка за крупными TWAP: постоянные подписки на историю и слайсы кошелька."""

__all__ = ["TwapTracker"]

import asyncio
import time
from collections import deque
from collections.abc import Awaitable, Callable
from contextlib import suppress
from dataclasses import dataclass, field
from typing import Any

from loguru import logger as _logger

from ._events import FINISH_REASONS, build_finished, build_slice, latest_records, record_status
from ._node_backoff import NodeBackoff
from ._websocket import Websocket
from .config import MAX_USERS_PER_WS, WatcherConfig
from .types import LoggerLike, TwapEvent

type EventHandler = Callable[[TwapEvent], None]
"""Приемник готовых событий (диспетчер)."""

type NewTwapHandler = Callable[[int, dict[str, Any]], Awaitable[None]]
"""Обработчик нового TWAP, замеченного в истории отслеживаемого кошелька."""

CHANNELS = ("userTwapHistory", "userTwapSliceFills")
"""Подписки слежки на кошелек. Биржа считает их одним пользователем в лимите ноды."""

SUBSCRIBE_LIMIT_ERROR = "Cannot track more than"
"""Начало текста ошибки, которой биржа отвечает на подписку сверх лимита."""


def subscription(channel: str, wallet: str) -> dict[str, Any]:
    """Собирает сообщение подписки на канал кошелька."""
    return {"method": "subscribe", "subscription": {"type": channel, "user": wallet}}


@dataclass
class _TrackedTwap:
    """Ордер под слежкой: помнит последний отданный слайс для дедупликации."""

    last_fill_ms: int
    """Время последнего отданного слайса, миллисекунды. До первого — время
    обнаружения: слайсы раньше него уже учтены в `executed_size` события `created`."""

    last_fill_tids: set[int] = field(default_factory=set)
    """Сделки с временем `last_fill_ms`: один слайс — несколько сделок с одним временем."""

    def accept(self, fill: dict[str, Any]) -> bool:
        """Проверяет, что слайс новый, и запоминает его."""
        fill_ms, tid = int(fill["time"]), int(fill["tid"])

        # После реконнекта снапшот повторяет уже отданные слайсы.
        if fill_ms < self.last_fill_ms:
            return False
        if fill_ms == self.last_fill_ms and tid in self.last_fill_tids:
            return False

        if fill_ms > self.last_fill_ms:
            self.last_fill_ms = fill_ms
            self.last_fill_tids = set()
        self.last_fill_tids.add(tid)
        return True


@dataclass
class _Connection:
    """Одно соединение слежки и учет его подписок."""

    websocket: Websocket
    wallets: set[str] = field(default_factory=set)
    """Кошельки, закрепленные за соединением (подтвержденные и ожидающие)."""

    pending: deque[tuple[str, str, float]] = field(default_factory=deque)
    """Подписки «кошелек, канал, время отправки», на которые биржа еще не ответила."""

    backoff: NodeBackoff = field(default_factory=NodeBackoff)
    """Уход с занятой ноды: биржа отказала в подписке — ноду делит кто-то еще."""

    tasks: list[asyncio.Task] = field(default_factory=list)


class TwapTracker:
    """Держит постоянные подписки на кошельки с крупными TWAP.

    На кошелек — две подписки, `userTwapHistory` и `userTwapSliceFills`, и один слот
    лимита ноды. Из них приходят слайсы с точным идентификатором ордера и смены
    статуса: событие о завершении — через доли секунды, без REST-запросов.

    На каждом подключении биржа присылает снапшот истории и слайсов, поэтому
    пропущенное во время реконнекта восстанавливается само.
    """

    _MAINTENANCE_INTERVAL = 1.0
    """Как часто проверяются зависшие подписки и занятые ноды, секунды."""

    _PENDING_TIMEOUT = 5.0
    """Сколько ждать ответ биржи на подписку, прежде чем забыть о ней."""

    def __init__(
        self,
        config: WatcherConfig,
        on_event: EventHandler,
        on_new_twap: NewTwapHandler,
        *,
        logger: LoggerLike | None = None,
    ) -> None:
        """Инициализирует слежку.

        :param config: Настройки наблюдателя.
        :param on_event: Приемник событий `slice` и `finished`.
        :param on_new_twap: Обработчик нового TWAP отслеживаемого кошелька: решает,
            крупный ли он, и отдает `created`.
        :param logger: Логгер. По умолчанию — loguru.
        """
        self._config = config
        self._on_event = on_event
        self._on_new_twap = on_new_twap
        self._logger = logger or _logger

        self._connections: list[_Connection] = []
        self._maintenance_task: asyncio.Task | None = None

        # Кошелек -> его ордера под слежкой и соединение, за которым он закреплен.
        self._wallets: dict[str, dict[int, _TrackedTwap]] = {}
        self._wallet_connection: dict[str, _Connection] = {}

        self.rejected = 0
        """Сколько подписок слежки биржа отклонила по лимиту."""

        self.reconnects = 0
        """Сколько раз соединение пересоздавалось, чтобы уйти с занятой ноды."""

    @property
    def wallets_count(self) -> int:
        """Сколько кошельков под слежкой."""
        return len(self._wallets)

    @property
    def twaps_count(self) -> int:
        """Сколько ордеров под слежкой."""
        return sum(len(twaps) for twaps in self._wallets.values())

    def is_tracked(self, wallet: str) -> bool:
        """Проверяет, что кошелек уже под слежкой: искать его TWAP заново не нужно.

        :param wallet: Адрес в нижнем регистре.
        """
        return wallet in self._wallets

    def is_twap_tracked(self, wallet: str, twap_id: int) -> bool:
        """Проверяет, что ордер под слежкой."""
        return twap_id in self._wallets.get(wallet, {})

    async def start(self) -> None:
        """Поднимает соединения слежки."""
        for index in range(self._config.tracking_connections):
            connection = self._create_connection(index)
            connection.tasks.append(asyncio.create_task(connection.websocket.start()))
            self._connections.append(connection)

        self._maintenance_task = asyncio.create_task(self._maintenance_loop())

        self._logger.info(
            f"Twap tracker started: {self._config.tracking_connections} connections, "
            f"{self._config.tracking_capacity} wallet slots"
        )

    async def stop(self) -> None:
        """Останавливает соединения и забывает ордера под слежкой."""
        if self._maintenance_task is not None:
            self._maintenance_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._maintenance_task
            self._maintenance_task = None

        for connection in self._connections:
            await connection.websocket.stop()
        for connection in self._connections:
            for task in connection.tasks:
                task.cancel()
                with suppress(asyncio.CancelledError):
                    await task

        self._connections.clear()
        self._wallets.clear()
        self._wallet_connection.clear()

    async def track(self, wallet: str, twap_id: int, *, since_ms: int) -> bool:
        """Берет ордер под слежку.

        :param wallet: Кошелек-владелец в нижнем регистре.
        :param twap_id: Идентификатор ордера.
        :param since_ms: Время обнаружения: слайсы раньше него не отдаются.
        :return: False, если свободных слотов нет и ордер не отслеживается.
        """
        twaps = self._wallets.get(wallet)

        # Кошелек уже держит слот: подписки по нему отдают все его ордера.
        if twaps is not None:
            twaps.setdefault(twap_id, _TrackedTwap(last_fill_ms=since_ms))
            return True

        free = [c for c in self._connections if len(c.wallets) < MAX_USERS_PER_WS]
        if not free:
            return False

        # Самое свободное соединение: нагрузка и риск отказа распределяются ровнее.
        connection = min(free, key=lambda c: len(c.wallets))
        self._wallets[wallet] = {twap_id: _TrackedTwap(last_fill_ms=since_ms)}
        self._wallet_connection[wallet] = connection
        connection.wallets.add(wallet)

        for channel in CHANNELS:
            if await connection.websocket.add_subscription(subscription(channel, wallet)):
                connection.pending.append((wallet, channel, time.time()))

        return True

    def _create_connection(self, index: int) -> _Connection:
        """Создает соединение слежки с обработчиками, привязанными к нему."""
        connection: _Connection

        async def on_connect() -> None:
            """Ожидает ответы на все подписки: Websocket отправит их сразу после хука."""
            now = time.time()
            connection.pending = deque(
                (sub["subscription"]["user"], sub["subscription"]["type"], now)
                for sub in connection.websocket.subscriptions
            )
            connection.backoff.crowded = False

        async def on_message(msg: dict[str, Any]) -> None:
            """Разбирает сообщение этого соединения."""
            await self._handle_message(connection, msg)

        connection = _Connection(
            websocket=Websocket(
                self._config.ws_url,
                on_message,
                on_connect=on_connect,
                name=f"twap-tracker-{index}",
                logger=self._logger,
            )
        )
        return connection

    async def _handle_message(self, connection: _Connection, msg: dict[str, Any]) -> None:
        """Раскладывает сообщение по обработчикам каналов."""
        channel = msg.get("channel")
        data = msg.get("data")

        if channel == "subscriptionResponse":
            self._confirm_subscription(connection, data or {})
        elif channel == "error":
            self._handle_error(connection, str(data or ""))
        elif channel == "userTwapSliceFills" and isinstance(data, dict):
            self._handle_fills(data)
        elif channel == "userTwapHistory" and isinstance(data, dict):
            await self._handle_history(data)

    def _confirm_subscription(self, connection: _Connection, data: dict[str, Any]) -> None:
        """Снимает подтвержденную подписку из ожидания."""
        # Ответы приходят и на отписки — они в ожидании не числятся.
        if data.get("method") != "subscribe":
            return

        sub = data.get("subscription", {})
        key = (str(sub.get("user", "")).lower(), sub.get("type"))
        item = next((p for p in connection.pending if (p[0], p[1]) == key), None)
        if item is not None:
            connection.pending.remove(item)

    def _handle_error(self, connection: _Connection, text: str) -> None:
        """Отмечает соединение занятым, если биржа отказала в подписке по лимиту."""
        if not text.startswith(SUBSCRIBE_LIMIT_ERROR) or not connection.pending:
            # Остальные ошибки безобидны: например, отписка от уже снятой подписки.
            self._logger.debug(f"Twap tracker received error message: {text}")
            return

        # Отказ приходит без адреса, а биржа отвечает в порядке запросов.
        wallet, channel, _ = connection.pending.popleft()
        self.rejected += 1

        # Соединение держит не больше 14 кошельков при лимите ноды 15: отказ значит,
        # что ноду делит кто-то еще. Кошелек остается за соединением — после
        # переподключения подписка повторится, а снапшот вернет пропущенное.
        connection.backoff.crowded = True
        self._logger.warning(
            f"Tracking subscription {channel} for {wallet} rejected by exchange node limit"
        )

    def _handle_fills(self, data: dict[str, Any]) -> None:
        """Отдает новые слайсы отслеживаемых ордеров."""
        wallet = str(data.get("user", "")).lower()
        twaps = self._wallets.get(wallet)
        if twaps is None:
            return

        # Снапшот идет от свежих к старым, а события должны идти по времени.
        items = sorted(data.get("twapSliceFills", []), key=lambda item: item["fill"]["time"])

        for item in items:
            twap = twaps.get(int(item.get("twapId") or 0))
            if twap is None or not twap.accept(item["fill"]):
                continue

            self._on_event(build_slice(item, wallet=wallet))

    async def _handle_history(self, data: dict[str, Any]) -> None:
        """Отдает завершения отслеживаемых ордеров и замечает новые ордера кошелька."""
        wallet = str(data.get("user", "")).lower()
        twaps = self._wallets.get(wallet)
        if twaps is None:
            return

        now = time.time()

        for twap_id, record in sorted(latest_records(data.get("history", [])).items()):
            status = record_status(record)
            reason = FINISH_REASONS.get(status)

            if twap_id in twaps:
                if reason is not None:
                    del twaps[twap_id]
                    self._on_event(build_finished(record, reason=reason, now=now))
                continue

            # Новый ордер отслеживаемого кошелька: поиск его не увидит — кошелек
            # уже не попадает в кандидаты. Крупный ли он, решает обработчик.
            if status == "activated" and not _is_expired(record["state"], now):
                await self._on_new_twap(twap_id, record["state"])

        if not self._wallets.get(wallet):
            await self._release_wallet(wallet)

    async def _release_wallet(self, wallet: str) -> None:
        """Освобождает слот кошелька, у которого не осталось ордеров под слежкой."""
        self._wallets.pop(wallet, None)
        connection = self._wallet_connection.pop(wallet, None)
        if connection is None:
            return

        connection.wallets.discard(wallet)
        for channel in CHANNELS:
            await connection.websocket.remove_subscription(subscription(channel, wallet))

    async def _maintenance_loop(self) -> None:
        """Забывает зависшие подписки и уводит соединения с занятых нод."""
        while True:
            await asyncio.sleep(self._MAINTENANCE_INTERVAL)

            for connection in self._connections:
                try:
                    self._drop_stale_pending(connection)
                    await self._leave_crowded_node(connection)
                except Exception as exc:
                    self._logger.warning(f"Twap tracker maintenance failed: {exc!r}")

    def _drop_stale_pending(self, connection: _Connection) -> None:
        """Забывает подписки без ответа: иначе отказы относились бы не к тем кошелькам."""
        now = time.time()
        while connection.pending and now - connection.pending[0][2] > self._PENDING_TIMEOUT:
            wallet, channel, _ = connection.pending.popleft()
            self._logger.debug(f"No subscription response for {channel} {wallet}")

    async def _leave_crowded_node(self, connection: _Connection) -> None:
        """Пересоздает соединение, если его нода занята: подписки повторятся на новой."""
        now = time.time()
        if not connection.backoff.should_reconnect(now) or not connection.websocket.connected:
            return

        connection.backoff.reconnecting(now)
        self.reconnects += 1
        self._logger.info(
            f"Twap tracker connection shares exchange node limit "
            f"({len(connection.wallets)} wallets), reconnecting"
        )
        await connection.websocket.reconnect()


def _is_expired(state: dict[str, Any], now: float) -> bool:
    """Проверяет, что расчетный срок ордера давно вышел.

    Снапшот истории бывает обрезан: у старого ордера может не оказаться финальной
    записи, и последней останется `activated`. Живым такой ордер не считаем.
    """
    planned_end = int(state["timestamp"]) / 1000 + int(state["minutes"]) * 60
    return now > planned_end + _EXPIRED_GRACE_SECONDS


_EXPIRED_GRACE_SECONDS = 600
"""Допуск к расчетному концу ордера: последний слайс плавает относительно срока."""
