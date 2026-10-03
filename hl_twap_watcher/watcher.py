"""Публичный фасад библиотеки: наблюдатель за TWAP-ордерами Hyperliquid."""

__all__ = ["TwapWatcher"]

import asyncio
import time
from contextlib import suppress
from typing import Any, Self

import aiohttp
from loguru import logger as _logger

from ._dispatcher import Dispatcher
from ._events import build_created, build_slice
from ._http import InfoClient
from ._liveness import TwapLiveness
from ._market_stream import MarketStream
from ._markets import Markets
from ._pool import TwapStatesPool
from ._registry import TwapRegistry
from .config import WatcherConfig
from .types import (
    Callback,
    LoggerLike,
    TwapCreatedEvent,
    TwapEvent,
    TwapFinishedEvent,
    TwapSide,
    TwapSliceEvent,
    WatcherStats,
)

_MIDS_WAIT_SECONDS = 10.0
"""Сколько при старте ждать первых mid-цен, секунды."""


class TwapWatcher:
    """Находит TWAP-ордера Hyperliquid и сообщает о них через callback'и.

    Три события:

    - `created` — найден активный TWAP-ордер на перпе;
    - `slice` — исполнился слайс найденного ордера;
    - `finished` — ордер завершился (доработал срок, отменен, остановлен).

    Callback'и принимают обычные функции и корутины. Можно передать общий
    `on_event` (тип различается по `event["type"]`), специализированные или оба
    сразу — тогда общий вызывается первым.

    Пример::

        async def on_created(event: TwapCreatedEvent) -> None:
            print(event["coin"], event["side"], event["notional_usd"])


        async with TwapWatcher(on_created=on_created):
            await asyncio.Event().wait()
    """

    def __init__(
        self,
        *,
        on_event: Callback[TwapEvent] | None = None,
        on_created: Callback[TwapCreatedEvent] | None = None,
        on_slice: Callback[TwapSliceEvent] | None = None,
        on_finished: Callback[TwapFinishedEvent] | None = None,
        config: WatcherConfig | None = None,
        session: aiohttp.ClientSession | None = None,
        logger: LoggerLike | None = None,
    ) -> None:
        """Создает наблюдатель. Соединения открываются в `start()`.

        :param on_event: Общий обработчик всех событий.
        :param on_created: Обработчик найденных ордеров.
        :param on_slice: Обработчик слайсов.
        :param on_finished: Обработчик завершенных ордеров.
        :param config: Технические параметры. По умолчанию — `WatcherConfig()`.
        :param session: Внешняя сессия aiohttp для REST-запросов. Если не передана,
            наблюдатель создаст и закроет свою.
        :param logger: Логгер. По умолчанию — loguru.
        :raises ValueError: Если не передан ни один callback.
        """
        if not any((on_event, on_created, on_slice, on_finished)):
            raise ValueError("At least one callback is required")

        self._config = config or WatcherConfig()
        self._logger = logger or _logger

        self._client = InfoClient(self._config.info_url, session=session, logger=self._logger)
        self._markets = Markets()
        self._registry = TwapRegistry(self._config)
        self._dispatcher = Dispatcher(
            on_event=on_event,
            on_created=on_created,
            on_slice=on_slice,
            on_finished=on_finished,
            logger=self._logger,
        )

        # Очередь кошельков-кандидатов: поток сделок кладет, пул `twapStates` забирает.
        self._candidates: asyncio.Queue[str] = asyncio.Queue()

        self._stream = MarketStream(
            self._config,
            self._markets,
            self._candidates,
            self._on_zero_hash_trade,
            logger=self._logger,
        )
        self._pool = TwapStatesPool(
            self._config,
            self._markets,
            self._candidates,
            self._on_twap_state,
            logger=self._logger,
        )
        self._liveness = TwapLiveness(
            self._config,
            self._client,
            self._registry,
            self._dispatcher.emit,
            logger=self._logger,
        )

        self._refresh_task: asyncio.Task | None = None
        self._running = False

    @property
    def running(self) -> bool:
        """True между `start()` и `stop()`."""
        return self._running

    async def start(self) -> None:
        """Загружает список перпов и запускает все потоки.

        :raises RuntimeError: Если наблюдатель уже запущен.
        :raises ConnectionError: Если не удалось загрузить список перпов.
        """
        if self._running:
            raise RuntimeError("TwapWatcher is already running")

        # Без списка перпов не на что подписываться — ошибку отдаем пользователю.
        # Сессию закрываем сами: при ошибке в `async with` до __aexit__ дело не дойдет.
        try:
            self._markets.apply_meta(await self._client.perp_meta())
        except Exception:
            await self._client.close()
            raise

        await self._dispatcher.start()
        await self._stream.start()

        # Пул ждет первых цен: иначе ордера, найденные в первую секунду, придут без
        # долларовой оценки. Ждем недолго — цены не повод не стартовать.
        with suppress(TimeoutError):
            await asyncio.wait_for(self._stream.mids_ready.wait(), timeout=_MIDS_WAIT_SECONDS)

        await self._pool.start()
        await self._liveness.start()
        self._refresh_task = asyncio.create_task(self._refresh_markets_loop())

        self._running = True
        self._logger.info("TwapWatcher started")

    async def stop(self) -> None:
        """Останавливает потоки, доставляет накопленные события и закрывает соединения."""
        if self._refresh_task is not None:
            self._refresh_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._refresh_task
            self._refresh_task = None

        # Сначала источники событий, потом доставка: иначе события теряются.
        await self._liveness.stop()
        await self._pool.stop()
        await self._stream.stop()
        await self._dispatcher.stop()
        await self._client.close()

        if self._running:
            self._running = False
            self._logger.info("TwapWatcher stopped")

    async def __aenter__(self) -> Self:
        """Запускает наблюдатель при входе в `async with`."""
        await self.start()
        return self

    async def __aexit__(self, *_: object) -> None:
        """Останавливает наблюдатель при выходе из `async with`."""
        await self.stop()

    def stats(self) -> WatcherStats:
        """Возвращает счетчики работы с момента запуска."""
        return WatcherStats(
            trades=self._stream.trades,
            zero_hash_trades=self._stream.zero_hash_trades,
            candidates_queued=self._stream.queued,
            candidates_dropped=self._stream.dropped,
            queue_size=self._candidates.qsize(),
            queue_max=self._pool.queue_max,
            wallets_checked=self._pool.checked,
            subscriptions_rejected=self._pool.rejected,
            pool_reconnects=self._pool.reconnects,
            tracked_twaps=len(self._registry),
            liveness_checks=self._liveness.checks,
            events_created=self._dispatcher.created,
            events_slice=self._dispatcher.slices,
            events_finished=self._dispatcher.finished,
            callback_errors=self._dispatcher.errors,
        )

    def _on_twap_state(self, twap_id: int, state: dict[str, Any]) -> None:
        """Превращает впервые увиденный ордер в событие и ставит его под наблюдение."""
        now = time.time()
        event = build_created(twap_id, state, mid_price=self._markets.mid(state["coin"]), now=now)

        self._registry.add(event, now=now)
        self._dispatcher.emit(event)

    def _on_zero_hash_trade(self, trade: dict[str, Any]) -> None:
        """Сверяет сделку движка с отслеживаемыми ордерами и отдает слайсы."""
        now = time.time()
        buyer, seller = trade["users"]

        # Порядок участников в сделке фиксирован: сначала покупатель, затем продавец.
        participants: tuple[tuple[str, TwapSide], ...] = ((buyer, "BUY"), (seller, "SELL"))

        for user, side in participants:
            wallet = user.lower()
            twap_ids = self._registry.match_slice(wallet, trade["coin"], side, now=now)
            if twap_ids:
                self._dispatcher.emit(
                    build_slice(trade, wallet=wallet, side=side, twap_ids=twap_ids)
                )

    async def _refresh_markets_loop(self) -> None:
        """Периодически перечитывает список перпов и подписывается на новые листинги."""
        while True:
            await asyncio.sleep(self._config.markets_refresh_seconds)

            try:
                added = self._markets.apply_meta(await self._client.perp_meta())
            except Exception as exc:
                self._logger.warning(f"Markets refresh failed: {exc!r}")
                continue

            if added:
                await self._stream.add_coins(added)
