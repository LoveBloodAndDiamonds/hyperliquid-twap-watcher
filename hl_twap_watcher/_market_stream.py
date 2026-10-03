"""Глобальный поток сделок и mid-цен: поиск кошельков-кандидатов и слайсов TWAP."""

__all__ = ["ZERO_HASH", "MarketStream"]

import asyncio
import time
from collections import OrderedDict
from collections.abc import Callable
from contextlib import suppress
from typing import Any

from loguru import logger as _logger

from ._markets import Markets
from ._websocket import Websocket
from .config import WatcherConfig
from .types import LoggerLike

ZERO_HASH = "0x" + "0" * 64
"""Хеш сделки, исполненной движком биржи, а не транзакцией пользователя."""

type ZeroHashTradeHandler = Callable[[dict[str, Any]], None]
"""Обработчик сделки с нулевым хешом: сверяет ее с отслеживаемыми TWAP."""


class MarketStream:
    """Слушает сделки по всем перпам и mid-цены в одном WS-соединении.

    Глобального стрима TWAP у Hyperliquid нет, поэтому кандидаты ищутся по сделкам
    с нулевым хешом: так исполняются слайсы TWAP, ликвидации и ADL. Отличить одно
    от другого умеет только пул `twapStates` — по подписке на кошелек.

    Те же сделки — источник слайсов уже найденных ордеров: их сверяет с регистром
    обработчик `on_zero_hash_trade`.
    """

    def __init__(
        self,
        config: WatcherConfig,
        markets: Markets,
        queue: asyncio.Queue[str],
        on_zero_hash_trade: ZeroHashTradeHandler,
        *,
        logger: LoggerLike | None = None,
    ) -> None:
        """Инициализирует поток.

        :param config: Настройки наблюдателя.
        :param markets: Справочник рынков: список монет для подписки и приемник цен.
        :param queue: Общая с пулом `twapStates` очередь адресов на проверку.
        :param on_zero_hash_trade: Обработчик каждой сделки с нулевым хешом.
        :param logger: Логгер. По умолчанию — loguru.
        """
        self._config = config
        self._markets = markets
        self._queue = queue
        self._on_zero_hash_trade = on_zero_hash_trade
        self._logger = logger or _logger

        self._dedup_ttl = config.watch_ttl * config.wallet_dedup_factor
        self._queue_limit = config.queue_limit

        self._websocket: Websocket | None = None
        self._task: asyncio.Task | None = None

        # Кошелек -> время последней постановки в очередь. OrderedDict нужен,
        # чтобы вытеснять самые старые записи.
        self._seen_wallets: OrderedDict[str, float] = OrderedDict()

        self.mids_ready = asyncio.Event()
        """Взводится с первым сообщением `allMids`: до него долларовые оценки недоступны."""

        self.trades = 0
        """Сколько сделок пришло из потока."""

        self.zero_hash_trades = 0
        """Сколько из них исполнено движком биржи."""

        self.queued = 0
        """Сколько кошельков поставлено в очередь после дедупликации."""

        self.dropped = 0
        """Сколько кандидатов вытеснено из переполненной очереди."""

    async def start(self) -> None:
        """Поднимает WS-подписку на `allMids` и сделки по всем перпам справочника."""
        subscriptions = [{"method": "subscribe", "subscription": {"type": "allMids"}}]
        subscriptions += [self._trades_subscription(coin) for coin in self._markets.perps]

        self._websocket = Websocket(
            self._config.ws_url,
            self._on_message,
            subscription_messages=subscriptions,
            name="market-stream",
            logger=self._logger,
        )

        # start() не возвращает управление, пока вебсокет работает, поэтому
        # держим его в отдельной задаче. Реконнект — внутри Websocket.
        self._task = asyncio.create_task(self._websocket.start())

        self._logger.info(
            f"Market stream subscribed to {len(self._markets.perps)} perps and allMids"
        )

    async def stop(self) -> None:
        """Закрывает соединение и останавливает фоновую задачу."""
        if self._websocket is not None:
            await self._websocket.stop()
            self._websocket = None

        if self._task is not None:
            self._task.cancel()
            with suppress(asyncio.CancelledError):
                await self._task
            self._task = None

    async def add_coins(self, coins: list[str]) -> None:
        """Подписывается на сделки новых листингов без переподключения.

        :param coins: Новые перпы.
        """
        if self._websocket is None:
            return

        for coin in coins:
            await self._websocket.add_subscription(self._trades_subscription(coin))

        self._logger.info(f"Market stream subscribed to new perps: {coins}")

    @staticmethod
    def _trades_subscription(coin: str) -> dict[str, Any]:
        """Собирает сообщение подписки на сделки монеты."""
        return {"method": "subscribe", "subscription": {"type": "trades", "coin": coin}}

    async def _on_message(self, msg: dict[str, Any]) -> None:
        """Разбирает сообщение: обновляет цены или ищет кандидатов в сделках."""
        channel = msg.get("channel")

        if channel == "allMids":
            self._markets.apply_mids(msg["data"]["mids"])
            self.mids_ready.set()
            return

        # Кроме сделок и цен приходят ответы на подписку — их пропускаем.
        if channel != "trades":
            return

        for trade in msg["data"]:
            self.trades += 1

            if trade["hash"] != ZERO_HASH:
                continue

            self.zero_hash_trades += 1

            # В сделке всегда два участника: TWAP-ом может оказаться любой.
            for user in trade["users"]:
                self._enqueue(user.lower())

            self._on_zero_hash_trade(trade)

    def _enqueue(self, user: str) -> None:
        """Ставит кошелек в очередь на проверку, если он давно не проверялся."""
        now = time.time()
        last = self._seen_wallets.get(user)

        # Слайсы одного TWAP идут каждые 30 секунд от того же кошелька: без окна
        # дедупликации очередь забьется повторами.
        if last is not None and now - last < self._dedup_ttl:
            return

        self._seen_wallets[user] = now
        self._seen_wallets.move_to_end(user)
        while len(self._seen_wallets) > self._config.max_seen_wallets:
            self._seen_wallets.popitem(last=False)

        self.queued += 1

        # Кандидатов приходит больше, чем пул успевает проверить (нулевой хеш есть
        # и у ликвидаций с ADL). Без предела очередь растет часами и адрес доходит
        # до подписки, когда ордер давно не новый: старых выгоднее выбросить.
        while self._queue.qsize() >= self._queue_limit:
            try:
                self._queue.get_nowait()
            except asyncio.QueueEmpty:
                break

            self.dropped += 1

        self._queue.put_nowait(user)
