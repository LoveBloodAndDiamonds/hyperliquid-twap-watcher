"""Публичные модели библиотеки: события TWAP и типы callback-функций."""

__all__ = [
    "TwapSide",
    "TwapFinishReason",
    "TwapCreatedEvent",
    "TwapSliceEvent",
    "TwapFinishedEvent",
    "TwapEvent",
    "WatcherStats",
    "Callback",
    "LoggerLike",
]

import logging
from collections.abc import Awaitable, Callable
from typing import Literal, TypedDict

import loguru

type TwapSide = Literal["BUY", "SELL"]
"""Сторона ордера: покупка или продажа."""

type TwapFinishReason = Literal["completed", "cancelled", "stopped", "error"]
"""Причина завершения TWAP-ордера.

- `completed` — ордер доработал свой срок (статус биржи `finished`);
- `cancelled` — ордер сняли досрочно (статус биржи `terminated`);
- `stopped` — сработал стоп по цене `stopPx` (статус биржи `stopped`);
- `error` — биржа остановила ордер с ошибкой (статус биржи `error`).
"""


class TwapCreatedEvent(TypedDict):
    """Найден активный TWAP-ордер не меньше `WatcherConfig.min_notional_usd`.

    Биржа отдает все активные ордера кошелька, поэтому событие приходит и по
    ордерам, созданным давно: свежесть видна по `age_sec`. После рестарта
    наблюдателя событие по еще живым ордерам придет повторно.
    """

    type: Literal["created"]
    """Тип события — для сопоставления в общем callback."""

    twap_id: int
    """Идентификатор ордера на бирже. Уникален только в паре с кошельком."""

    wallet: str
    """Адрес кошелька-владельца в нижнем регистре."""

    coin: str
    """Тикер перпа как его отдает биржа: `BTC`, `kPEPE`."""

    side: TwapSide
    """Сторона ордера."""

    size: float
    """Полный размер ордера в монете."""

    executed_size: float
    """Сколько уже исполнено к моменту обнаружения, в монете."""

    executed_notional: float
    """Сколько уже исполнено к моменту обнаружения, в долларах."""

    minutes: int
    """Длительность исполнения ордера в минутах."""

    reduce_only: bool
    """True, если ордер только сокращает позицию."""

    randomize: bool
    """True, если биржа рандомизирует размер слайсов."""

    created_at_ms: int
    """Время создания ордера на бирже, миллисекунды."""

    detected_at_ms: int
    """Время обнаружения ордера наблюдателем, миллисекунды."""

    age_sec: float
    """Возраст ордера в момент обнаружения, секунды."""

    mid_price: float
    """Mid-цена монеты в момент обнаружения."""

    notional_usd: float
    """Полный размер ордера в долларах по mid-цене: `size × mid_price`."""

    tracked: bool
    """True — ордер под слежкой: по нему придут `slice` и `finished`. False — слоты
    слежки заняты, об ордере известно только это событие."""


class TwapSliceEvent(TypedDict):
    """Исполнение слайса отслеживаемого TWAP-ордера.

    Одна сделка — одно событие: слайс, съевший несколько уровней стакана,
    придет несколькими событиями с одинаковым `time_ms`.
    """

    type: Literal["slice"]
    """Тип события — для сопоставления в общем callback."""

    twap_id: int
    """Ордер, к которому относится слайс."""

    wallet: str
    """Адрес кошелька-владельца в нижнем регистре."""

    coin: str
    """Тикер перпа."""

    side: TwapSide
    """Сторона слайса — совпадает со стороной ордера."""

    price: float
    """Цена исполнения."""

    size: float
    """Исполненный размер в монете."""

    notional_usd: float
    """Исполненный размер в долларах: `price × size`."""

    time_ms: int
    """Время сделки на бирже, миллисекунды."""

    trade_id: int
    """Идентификатор сделки на бирже (`tid`)."""


class TwapFinishedEvent(TypedDict):
    """Отслеживаемый TWAP-ордер завершен: доработал срок, отменен, остановлен по
    стопу или с ошибкой. Приходит из подписки на историю ордеров кошелька — через
    доли секунды после смены статуса на бирже.
    """

    type: Literal["finished"]
    """Тип события — для сопоставления в общем callback."""

    twap_id: int
    """Идентификатор ордера на бирже."""

    wallet: str
    """Адрес кошелька-владельца в нижнем регистре."""

    coin: str
    """Тикер перпа."""

    side: TwapSide
    """Сторона ордера."""

    reason: TwapFinishReason
    """Причина завершения."""

    size: float
    """Полный (запланированный) размер ордера в монете."""

    executed_size: float
    """Итоговый исполненный размер в монете."""

    executed_notional: float
    """Итоговый исполненный размер в долларах."""

    average_price: float | None
    """Средняя цена исполнения. None — ничего не исполнилось."""

    minutes: int
    """Запланированная длительность ордера в минутах."""

    created_at_ms: int
    """Время создания ордера на бирже, миллисекунды."""

    finished_at_ms: int
    """Время завершения ордера по данным биржи, миллисекунды (точность — секунда)."""

    detected_at_ms: int
    """Время, когда наблюдатель узнал о завершении, миллисекунды."""


type TwapEvent = TwapCreatedEvent | TwapSliceEvent | TwapFinishedEvent
"""Любое событие наблюдателя. Конкретный тип различается по полю `type`."""


class WatcherStats(TypedDict):
    """Счетчики работы наблюдателя с момента запуска."""

    trades: int
    """Сколько сделок пришло из потока."""

    zero_hash_trades: int
    """Сколько из них исполнено движком биржи — источник кошельков-кандидатов."""

    candidates_queued: int
    """Сколько кошельков поставлено в очередь на проверку после дедупликации."""

    candidates_dropped: int
    """Сколько кандидатов вытеснено из переполненной очереди."""

    queue_size: int
    """Текущая глубина очереди кандидатов."""

    queue_max: int
    """Пиковая глубина очереди: показывает, успевает ли пул за потоком сделок."""

    wallets_checked: int
    """Сколько кошельков биржа приняла в подписку `twapStates`."""

    subscriptions_rejected: int
    """Сколько подписок (поиска и слежки) биржа отклонила по лимиту."""

    node_reconnects: int
    """Сколько раз соединение пересоздавалось, чтобы уйти с занятой ноды биржи."""

    tracked_twaps: int
    """Сколько ордеров сейчас под слежкой."""

    tracked_wallets: int
    """Сколько кошельков занимают слоты слежки."""

    tracking_capacity: int
    """Сколько всего слотов слежки (кошельков)."""

    tracking_full: int
    """Сколько найденных ордеров не взяли под слежку: слоты были заняты."""

    events_created: int
    """Сколько событий `created` отправлено."""

    events_slice: int
    """Сколько событий `slice` отправлено."""

    events_finished: int
    """Сколько событий `finished` отправлено."""

    callback_errors: int
    """Сколько раз callback пользователя выбросил исключение."""


type Callback[E] = Callable[[E], Awaitable[None] | None]
"""Обработчик события: обычная функция или корутина."""

type LoggerLike = logging.Logger | loguru.Logger
"""Логгер: loguru или стандартный `logging.Logger`."""
