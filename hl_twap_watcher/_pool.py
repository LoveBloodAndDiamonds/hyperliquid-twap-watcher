"""Пул WS-соединений, который проверяет кошельки-кандидаты и находит активные TWAP."""

__all__ = ["TwapStatesPool"]

import asyncio
import time
from collections import OrderedDict, deque
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass, field
from typing import Any

from loguru import logger as _logger

from ._markets import Markets
from ._websocket import Websocket
from .config import MAX_USERS_PER_WS, WatcherConfig
from .types import LoggerLike

type TwapStateHandler = Callable[[int, dict[str, Any]], None]
"""Обработчик впервые увиденного перпового TWAP: `(twap_id, состояние с биржи)`."""

_SUBSCRIBE_LIMIT_ERROR = "Cannot track more than"
"""Начало текста ошибки, которой биржа отвечает на подписку сверх лимита."""


@dataclass
class _SlotState:
    """Состояние подписок одного WS-соединения. Сбрасывается на каждом реконнекте."""

    tracked: OrderedDict[str, float] = field(default_factory=OrderedDict)
    """Кошелек -> время подписки, в порядке добавления."""

    pending: deque[tuple[str, float]] = field(default_factory=deque)
    """Пары «кошелек, время отправки» для подписок, на которые биржа не ответила."""

    blocked_until: float = 0.0
    """До этого момента новые подписки не отправляем: биржа только что отказала."""


@dataclass
class _Slot:
    """Одно соединение пула: вебсокет, его состояние и фоновые задачи."""

    websocket: Websocket
    state: _SlotState = field(default_factory=_SlotState)
    tasks: list[asyncio.Task] = field(default_factory=list)


class TwapStatesPool:
    """Держит несколько WS-соединений и ротирует по ним подписки `twapStates`.

    Подписка на `twapStates` возможна только по конкретному кошельку, а на одно
    соединение их влезает 14. Поэтому кандидат занимает слот на `watch_ttl` секунд —
    за это время успевает прийти состояние всех его TWAP — и уступает место следующему.
    """

    _POLL_INTERVAL = 0.3
    """Как часто соединение проверяет TTL подписок и добирает кандидатов, секунды."""

    _REJECT_COOLDOWN = 1.0
    """Пауза соединения после отказа биржи в подписке, секунды."""

    _PENDING_TIMEOUT = 5.0
    """Сколько ждать ответ биржи на подписку, прежде чем считать слот свободным."""

    def __init__(
        self,
        config: WatcherConfig,
        markets: Markets,
        queue: asyncio.Queue[str],
        on_twap: TwapStateHandler,
        *,
        logger: LoggerLike | None = None,
    ) -> None:
        """Инициализирует пул.

        :param config: Настройки наблюдателя.
        :param markets: Справочник рынков: отсев спотовых и builder-dex ордеров.
        :param queue: Общая с потоком сделок очередь адресов на проверку.
        :param on_twap: Обработчик впервые увиденного перпового TWAP.
        :param logger: Логгер. По умолчанию — loguru.
        """
        self._config = config
        self._markets = markets
        self._queue = queue
        self._on_twap = on_twap
        self._logger = logger or _logger

        self._slots: list[_Slot] = []

        # ID ордера уникален только в паре с кошельком. OrderedDict вместо set:
        # нужно вытеснять самые старые записи.
        self._seen: OrderedDict[tuple[str, int], None] = OrderedDict()

        self.checked = 0
        """Сколько кошельков биржа реально приняла в подписку `twapStates`."""

        self.rejected = 0
        """Сколько подписок биржа отклонила по лимиту (кошелек вернулся в очередь)."""

        self.queue_max = 0
        """Пиковая глубина очереди."""

    async def start(self) -> None:
        """Поднимает `watchers_count` независимых соединений."""
        for index in range(self._config.watchers_count):
            slot = self._create_slot(index)
            slot.tasks = [
                asyncio.create_task(slot.websocket.start()),
                asyncio.create_task(self._slot_loop(slot)),
            ]
            self._slots.append(slot)

        self._logger.info(
            f"Twap states pool started: {self._config.watchers_count} connections, "
            f"{self._config.pool_capacity} slots, ttl={self._config.watch_ttl}s"
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

        async def on_message(msg: dict[str, Any]) -> None:
            """Передает сообщение разбору с состоянием этого соединения."""
            self._handle_message(slot.state, msg)

        slot = _Slot(
            websocket=Websocket(
                self._config.ws_url,
                on_message,
                on_connect=on_connect,
                name=f"twap-states-{index}",
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

                self.queue_max = max(self.queue_max, self._queue.qsize())

            except asyncio.CancelledError:
                raise
            except Exception as exc:
                # Обычно это отправка в соединение, которое только что умерло:
                # вебсокет переподключится сам, а состояние сбросит on_connect.
                self._logger.debug(f"Twap states slot loop error: {exc!r}")

            await asyncio.sleep(self._POLL_INTERVAL)

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

    def _handle_message(self, state: _SlotState, msg: dict[str, Any]) -> None:
        """Разбирает ответ соединения: подтверждение подписки, отказ или состояния TWAP."""
        channel = msg.get("channel")

        if channel == "subscriptionResponse":
            self._confirm_subscription(state, msg["data"])
            return

        if channel == "error":
            self._handle_error(state, str(msg.get("data", "")))
            return

        if channel != "twapStates":
            return

        for twap_id, state_data in msg["data"].get("states", []):
            self._handle_twap_state(int(twap_id), state_data)

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
        self.checked += 1

    def _handle_error(self, state: _SlotState, text: str) -> None:
        """Освобождает слот, если биржа отказала в подписке по лимиту."""
        # Отказ приходит без адреса, поэтому относим его к самой старой подписке
        # без ответа: биржа отвечает в порядке запросов.
        if not text.startswith(_SUBSCRIBE_LIMIT_ERROR) or not state.pending:
            # Остальные ошибки безобидны: например, отписка от уже снятой подписки.
            self._logger.debug(f"Twap states pool received error message: {text}")
            return

        user, _ = state.pending.popleft()
        self.rejected += 1

        # Лимит общий на все соединения, поэтому упереться в него можно и с
        # пустыми слотами. Даем бирже паузу и возвращаем кошелек в очередь.
        state.blocked_until = time.time() + self._REJECT_COOLDOWN
        self._queue.put_nowait(user)

    def _handle_twap_state(self, twap_id: int, data: dict[str, Any]) -> None:
        """Отсеивает не-перпы и повторы, а впервые увиденный ордер отдает дальше."""
        # twapStates отдает все ордера кошелька: спот (`@107`) и builder-dex
        # (`xyz:SP500`) вне scope. Проверка до дедупликации: перп, листинг которого
        # справочник еще не подхватил, не должен навсегда застрять в памяти повторов.
        if not self._markets.is_perp(data["coin"]):
            return

        key = (data["user"].lower(), twap_id)
        if key in self._seen:
            return

        self._seen[key] = None
        while len(self._seen) > self._config.max_seen_twaps:
            self._seen.popitem(last=False)

        self._on_twap(twap_id, data)
