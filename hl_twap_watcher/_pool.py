"""Поиск: пул WS-соединений, который проверяет кошельки-кандидаты и находит их TWAP."""

__all__ = ["DiscoveryPool"]

import asyncio
import time
from collections import OrderedDict, deque
from collections.abc import Awaitable, Callable
from contextlib import suppress
from dataclasses import dataclass, field
from typing import Any

from loguru import logger as _logger

from ._node_backoff import NodeBackoff
from ._tracker import SUBSCRIBE_LIMIT_ERROR
from ._websocket import Websocket
from .config import MAX_USERS_PER_WS, WatcherConfig
from .types import LoggerLike

type TwapStateHandler = Callable[[int, dict[str, Any]], Awaitable[None]]
"""Обработчик состояния TWAP из `twapStates`: `(twap_id, состояние с биржи)`.
Биржа повторяет состояния, пока кошелек в подписке, — дедупликация за обработчиком."""


@dataclass
class _SlotState:
    """Состояние подписок одного WS-соединения. Сбрасывается на каждом реконнекте."""

    tracked: OrderedDict[str, float] = field(default_factory=OrderedDict)
    """Кошелек -> время подписки, в порядке добавления."""

    pending: deque[tuple[str, float]] = field(default_factory=deque)
    """Пары «кошелек, время отправки» для подписок, на которые биржа не ответила."""

    blocked_until: float = 0.0
    """До этого момента новые подписки не отправляем: биржа только что отказала."""

    reject_streak: int = 0
    """Сколько отказов подряд получило соединение: от этого растет пауза."""


@dataclass
class _Slot:
    """Одно соединение пула: вебсокет, его состояние и фоновые задачи."""

    websocket: Websocket
    state: _SlotState = field(default_factory=_SlotState)
    tasks: list[asyncio.Task] = field(default_factory=list)
    backoff: NodeBackoff = field(default_factory=NodeBackoff)
    """Уход с занятой ноды. Живет дольше состояния: переживает реконнекты."""


class DiscoveryPool:
    """Держит соединения поиска и ротирует по ним подписки `twapStates`.

    Подписка на `twapStates` возможна только по конкретному кошельку, а на одно
    соединение их влезает 14. Поэтому кандидат занимает слот на `watch_ttl` секунд —
    за это время успевает прийти состояние всех его TWAP — и уступает место следующему.

    Лимит "15 total users" биржа считает не на соединение и не на IP, а на
    серверную ноду за балансировщиком (их около трех). Два соединения, попавшие
    на одну ноду, делят 15 слотов на двоих. Такое соединение пересоздается, чтобы
    попасть на другую ноду.
    """

    _POLL_INTERVAL = 0.3
    """Как часто соединение проверяет TTL подписок и добирает кандидатов, секунды."""

    _REJECT_COOLDOWN = 1.0
    """Пауза соединения после первого отказа биржи в подписке, секунды."""

    _MAX_REJECT_COOLDOWN = 16.0
    """Предел паузы при серии отказов, секунды."""

    _PENDING_TIMEOUT = 5.0
    """Сколько ждать ответ биржи на подписку, прежде чем считать слот свободным."""

    def __init__(
        self,
        config: WatcherConfig,
        queue: asyncio.Queue[str],
        on_twap: TwapStateHandler,
        *,
        logger: LoggerLike | None = None,
    ) -> None:
        """Инициализирует пул.

        :param config: Настройки наблюдателя.
        :param queue: Общая с потоком сделок очередь адресов на проверку.
        :param on_twap: Обработчик каждого состояния TWAP из `twapStates`.
        :param logger: Логгер. По умолчанию — loguru.
        """
        self._config = config
        self._queue = queue
        self._on_twap = on_twap
        self._logger = logger or _logger

        self._slots: list[_Slot] = []

        self.checked = 0
        """Сколько кошельков биржа реально приняла в подписку `twapStates`."""

        self.rejected = 0
        """Сколько подписок биржа отклонила по лимиту (кошелек вернулся в очередь)."""

        self.queue_max = 0
        """Пиковая глубина очереди."""

        self.reconnects = 0
        """Сколько раз соединение пересоздавалось, чтобы уйти с занятой ноды."""

    async def start(self) -> None:
        """Поднимает `discovery_connections` независимых соединений."""
        for index in range(self._config.discovery_connections):
            slot = self._create_slot(index)
            slot.tasks = [
                asyncio.create_task(slot.websocket.start()),
                asyncio.create_task(self._slot_loop(slot)),
            ]
            self._slots.append(slot)

        self._logger.info(
            f"Discovery pool started: {self._config.discovery_connections} connections, "
            f"{self._config.discovery_capacity} slots, ttl={self._config.watch_ttl}s"
        )

    async def stop(self) -> None:
        """Останавливает все соединения пула."""
        for slot in self._slots:
            await slot.websocket.stop()

        tasks = [task for slot in self._slots for task in slot.tasks]
        for task in tasks:
            task.cancel()
        for task in tasks:
            with suppress(asyncio.CancelledError):
                await task

        self._slots.clear()

    def _create_slot(self, index: int) -> _Slot:
        """Создает соединение пула с обработчиками, привязанными к его состоянию."""
        slot: _Slot

        async def on_connect() -> None:
            """Сбрасывает состояние: подписки прошлого соединения умерли вместе с ним."""
            slot.state = _SlotState()
            slot.backoff.crowded = False

        async def on_message(msg: dict[str, Any]) -> None:
            """Передает сообщение разбору с состоянием этого соединения."""
            await self._handle_message(slot, msg)

        slot = _Slot(
            websocket=Websocket(
                self._config.ws_url,
                on_message,
                on_connect=on_connect,
                name=f"twap-discovery-{index}",
                logger=self._logger,
            )
        )
        return slot

    async def _slot_loop(self, slot: _Slot) -> None:
        """Ротирует подписки соединения: снимает истекшие, добирает новых кандидатов."""
        while True:
            try:
                if slot.websocket.connected:
                    state = slot.state
                    self._drop_stale_pending(state)
                    await self._release_expired(slot.websocket, state)
                    await self._fill_free_slots(slot.websocket, state)
                    await self._leave_crowded_node(slot)

                self.queue_max = max(self.queue_max, self._queue.qsize())

            except asyncio.CancelledError:
                raise
            except Exception as exc:
                # Обычно это отправка в соединение, которое только что умерло:
                # вебсокет переподключится сам, а состояние сбросит on_connect.
                self._logger.debug(f"Discovery slot loop error: {exc!r}")

            await asyncio.sleep(self._POLL_INTERVAL)

    async def _leave_crowded_node(self, slot: _Slot) -> None:
        """Пересоздает соединение, если его нода занята кем-то еще."""
        state = slot.state
        now = time.time()

        if not slot.backoff.should_reconnect(now):
            return

        slot.backoff.reconnecting(now)
        self.reconnects += 1
        self._logger.info(
            f"Discovery connection shares exchange node limit "
            f"({len(state.tracked)} own slots), reconnecting"
        )

        # Кандидаты без ответа биржи еще не проверены — не теряем их.
        for user, _ in state.pending:
            self._queue.put_nowait(user)
        state.pending.clear()

        await slot.websocket.reconnect()

    def _drop_stale_pending(self, state: _SlotState) -> None:
        """Освобождает слоты подписок, ответ по которым так и не пришел."""
        now = time.time()

        while state.pending and now - state.pending[0][1] > self._PENDING_TIMEOUT:
            user, _ = state.pending.popleft()
            self._logger.warning(f"No subscription response for {user}, slot released")

    async def _release_expired(self, websocket: Websocket, state: _SlotState) -> None:
        """Снимает подписки с истекшим TTL, освобождая слоты под новых кандидатов."""
        now = time.time()

        for user, added_at in list(state.tracked.items()):
            if now - added_at <= self._config.watch_ttl:
                continue

            state.tracked.pop(user)
            await websocket.send(
                {"method": "unsubscribe", "subscription": {"type": "twapStates", "user": user}}
            )

    async def _fill_free_slots(self, websocket: Websocket, state: _SlotState) -> None:
        """Занимает свободные слоты соединения кандидатами из очереди."""
        # Слот занимает и подписка без ответа биржи: иначе легко уйти за лимит.
        while len(state.tracked) + len(state.pending) < MAX_USERS_PER_WS:
            if time.time() < state.blocked_until:
                return

            try:
                # Таймаут нужен, чтобы вернуться к проверке TTL, даже если очередь пуста.
                user = await asyncio.wait_for(self._queue.get(), timeout=1.0)
            except TimeoutError:
                return

            if user in state.tracked or any(user == pending for pending, _ in state.pending):
                continue

            try:
                await websocket.send(
                    {"method": "subscribe", "subscription": {"type": "twapStates", "user": user}}
                )
            except Exception:
                # Соединение умерло между проверкой и отправкой: кандидат не должен потеряться.
                self._queue.put_nowait(user)
                raise

            state.pending.append((user, time.time()))

    async def _handle_message(self, slot: _Slot, msg: dict[str, Any]) -> None:
        """Разбирает ответ соединения: подтверждение подписки, отказ или состояния TWAP."""
        channel = msg.get("channel")

        if channel == "subscriptionResponse":
            self._confirm_subscription(slot.state, msg["data"])
            return

        if channel == "error":
            self._handle_error(slot, str(msg.get("data", "")))
            return

        if channel != "twapStates":
            return

        for twap_id, state_data in msg["data"].get("states", []):
            await self._on_twap(int(twap_id), state_data)

    def _confirm_subscription(self, state: _SlotState, data: dict[str, Any]) -> None:
        """Переводит подтвержденную биржей подписку из ожидания в занятые слоты."""
        # Ответы приходят и на отписки — они слоты не занимают.
        if data.get("method") != "subscribe":
            return

        user = str(data.get("subscription", {}).get("user", "")).lower()
        item = next((pending for pending in state.pending if pending[0] == user), None)
        if item is None:
            return

        state.pending.remove(item)
        state.tracked[user] = time.time()
        state.reject_streak = 0
        self.checked += 1

    def _handle_error(self, slot: _Slot, text: str) -> None:
        """Освобождает слот, если биржа отказала в подписке по лимиту."""
        state = slot.state

        # Отказ приходит без адреса, поэтому относим его к самой старой подписке
        # без ответа: биржа отвечает в порядке запросов.
        if not text.startswith(SUBSCRIBE_LIMIT_ERROR) or not state.pending:
            # Остальные ошибки безобидны: например, отписка от уже снятой подписки.
            self._logger.debug(f"Discovery pool received error message: {text}")
            return

        user, _ = state.pending.popleft()
        self.rejected += 1

        # Нода вмещает 15 подписок, а соединение занимает не больше 14: отказ значит,
        # что слоты ноды заняты кем-то еще. Даже если своих слотов много, соединение
        # работает не в полную силу — лучше перейти на свободную ноду.
        slot.backoff.crowded = True

        # Лимит общий на все соединения, поэтому упереться в него можно и с
        # пустыми слотами. Даем бирже паузу и возвращаем кошелек в очередь. Пауза
        # растет с серией отказов: долбить биржу подписками — тратить лимит
        # сообщений на IP, а слоты все равно не освободятся раньше.
        cooldown = self._REJECT_COOLDOWN * 2 ** min(state.reject_streak, 4)
        state.blocked_until = time.time() + min(cooldown, self._MAX_REJECT_COOLDOWN)
        state.reject_streak += 1
        self._queue.put_nowait(user)
