"""Публичный фасад библиотеки: наблюдатель за TWAP-ордерами Hyperliquid."""

__all__ = ["TwapWatcher"]

import asyncio
import time
from collections import OrderedDict
from contextlib import suppress
from typing import Any, Self

import aiohttp
from loguru import logger as _logger

from ._dispatcher import Dispatcher
from ._events import build_created
from ._http import InfoClient
from ._market_stream import MarketStream
from ._markets import Markets
from ._pool import DiscoveryPool
from ._tracker import TwapTracker
from .config import WatcherConfig
from .types import (
    Callback,
    LoggerLike,
    TwapCreatedEvent,
    TwapEvent,
    TwapFinishedEvent,
    TwapSliceEvent,
    WatcherStats,
)

_MIDS_WAIT_SECONDS = 10.0
"""Сколько при старте ждать первых mid-цен, секунды."""


class TwapWatcher:
    """Находит крупные TWAP-ордера Hyperliquid и сообщает о них через callback'и.

    Три события:

    - `created` — найден активный TWAP на перпе не меньше `min_notional_usd`;
    - `slice` — исполнился слайс отслеживаемого ордера;
    - `finished` — отслеживаемый ордер завершился (доработал срок, отменен, остановлен).

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
        :param session: Внешняя сессия aiohttp для REST-запроса списка перпов. Если
            не передана, наблюдатель создаст и закроет свою.
        :param logger: Логгер. По умолчанию — loguru.
        :raises ValueError: Если не передан ни один callback.
        """
        if not any((on_event, on_created, on_slice, on_finished)):
            raise ValueError("At least one callback is required")

        self._config = config or WatcherConfig()
        self._logger = logger or _logger

        self._client = InfoClient(self._config.info_url, session=session, logger=self._logger)
        self._markets = Markets()
        self._dispatcher = Dispatcher(
            on_event=on_event,
            on_created=on_created,
            on_slice=on_slice,
            on_finished=on_finished,
            logger=self._logger,
        )
        self._tracker = TwapTracker(
            self._config,
            self._dispatcher.emit,
            self._on_twap_state,
            logger=self._logger,
        )

        # Очередь кошельков-кандидатов: поток сделок кладет, поиск забирает.
        self._candidates: asyncio.Queue[str] = asyncio.Queue()

        self._stream = MarketStream(
            self._config,
            self._markets,
            self._candidates,
            self._tracker.is_tracked,
            logger=self._logger,
        )
        self._pool = DiscoveryPool(
            self._config,
            self._candidates,
            self._on_twap_state,
            logger=self._logger,
        )

        # Пары «кошелек, ID ордера», которые уже разобраны: крупные отданы событием,
        # мелкие отброшены. OrderedDict вместо set: нужно вытеснять самые старые.
        self._seen: OrderedDict[tuple[str, int], None] = OrderedDict()
        self._tracking_full = 0

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

        # Без цен поиск не отличит крупный ордер от мелкого — ждем первые mid-цены.
        # Недолго: ордер без цены просто проверится при следующем появлении.
        with suppress(TimeoutError):
            await asyncio.wait_for(self._stream.mids_ready.wait(), timeout=_MIDS_WAIT_SECONDS)

        await self._tracker.start()
        await self._pool.start()
        self._refresh_task = asyncio.create_task(self._refresh_markets_loop())

        self._running = True
        self._logger.info(
            f"TwapWatcher started: min notional ${self._config.min_notional_usd:,.0f}"
        )

    async def stop(self) -> None:
        """Останавливает потоки, доставляет накопленные события и закрывает соединения."""
        if self._refresh_task is not None:
            self._refresh_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._refresh_task
            self._refresh_task = None

        # Сначала источники событий, потом доставка: иначе события теряются.
        await self._pool.stop()
        await self._stream.stop()
        await self._tracker.stop()
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
            subscriptions_rejected=self._pool.rejected + self._tracker.rejected,
            node_reconnects=self._pool.reconnects + self._tracker.reconnects,
            tracked_twaps=self._tracker.twaps_count,
            tracked_wallets=self._tracker.wallets_count,
            tracking_capacity=self._config.tracking_capacity,
            tracking_full=self._tracking_full,
            events_created=self._dispatcher.created,
            events_slice=self._dispatcher.slices,
            events_finished=self._dispatcher.finished,
            callback_errors=self._dispatcher.errors,
        )

    async def _on_twap_state(self, twap_id: int, state: dict[str, Any]) -> None:
        """Разбирает активный TWAP из поиска или истории отслеживаемого кошелька.

        Крупный ордер, увиденный впервые, берется под слежку и отдается событием
        `created`. Остальное отбрасывается.
        """
        coin = state["coin"]

        # Биржа отдает все ордера кошелька: спот (`@107`) и builder-dex (`xyz:SP500`)
        # вне scope. Проверка до дедупликации: перп, листинг которого справочник
        # еще не подхватил, не должен навсегда застрять в памяти повторов.
        if not self._markets.is_perp(coin):
            return

        wallet = state["user"].lower()
        key = (wallet, twap_id)
        if key in self._seen or self._tracker.is_twap_tracked(wallet, twap_id):
            return

        # Без цены размер в долларах неизвестен. Не запоминаем ордер: биржа повторит
        # его при следующей проверке кошелька, а цена к тому времени появится.
        mid = self._markets.mid(coin)
        if mid is None:
            self._logger.debug(f"No mid price for {coin}, twap {twap_id} postponed")
            return

        self._remember(key)

        notional = float(state["sz"]) * mid
        if notional < self._config.min_notional_usd:
            return

        now = time.time()
        tracked = await self._tracker.track(wallet, twap_id, since_ms=int(now * 1000))
        if not tracked:
            self._tracking_full += 1
            self._logger.warning(
                f"Tracking slots are full ({self._config.tracking_capacity} wallets): "
                f"twap {twap_id} ({coin} ${notional:,.0f}, {wallet}) will not be tracked"
            )

        self._dispatcher.emit(
            build_created(twap_id, state, mid_price=mid, tracked=tracked, now=now)
        )

    def _remember(self, key: tuple[str, int]) -> None:
        """Запоминает разобранный ордер, вытесняя самые старые записи."""
        self._seen[key] = None
        while len(self._seen) > self._config.max_seen_twaps:
            self._seen.popitem(last=False)

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
