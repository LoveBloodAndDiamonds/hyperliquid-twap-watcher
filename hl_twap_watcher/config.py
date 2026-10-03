"""Настройки наблюдателя. Значения по умолчанию подобраны под лимиты Hyperliquid."""

__all__ = ["MAX_USERS_PER_WS", "WatcherConfig"]

from dataclasses import dataclass

MAX_USERS_PER_WS = 14
"""Сколько кошельков одно WS-соединение держит в подписках.

Биржа отвечает "Cannot track more than 15 total users". Лимит считается на серверную
ноду за балансировщиком, а не на соединение: соединения одного IP, попавшие на одну
ноду, делят 15 слотов. Нод около трех, то есть на IP выходит около 45 кошельков.
Разные подписки на один кошелек (`twapStates`, `userTwapHistory`,
`userTwapSliceFills`) занимают один слот. 14 на соединение оставляет запас.
"""


@dataclass(frozen=True, slots=True)
class WatcherConfig:
    """Технические параметры наблюдателя.

    Значения по умолчанию укладываются в лимиты Hyperliquid на один IP: одно
    соединение поиска и два соединения слежки — по одному на ноду биржи.
    """

    ws_url: str = "wss://api.hyperliquid.xyz/ws"
    """Адрес WebSocket API."""

    info_url: str = "https://api.hyperliquid.xyz/info"
    """Адрес REST info-эндпоинта."""

    min_notional_usd: float = 100_000.0
    """Минимальный полный размер ордера в долларах (`size × mid`). Ордера меньше
    игнорируются: о них не приходит ни одного события."""

    discovery_connections: int = 1
    """Соединения поиска: кошелек-кандидат подписывается на `twapStates` на
    `watch_ttl` секунд и уступает слот следующему."""

    tracking_connections: int = 2
    """Соединения слежки: кошелек с крупным TWAP держит слот, пока у него есть
    отслеживаемые ордера. Емкость — `tracking_connections × 14` кошельков."""

    watch_ttl: float = 2.0
    """Сколько секунд кандидат занимает слот поиска. Биржа присылает состояние
    TWAP сразу после подтверждения подписки."""

    queue_wait_seconds: float = 30.0
    """Сколько кандидат может ждать проверки. Из этого считается предел очереди:
    более старые кандидаты выбрасываются — их ордера уже не новые."""

    wallet_recheck_seconds: float = 20.0
    """Раньше этого кошелек повторно в поиск не попадает: слайсы одного TWAP идут
    каждые 30 секунд, без паузы очередь забьется повторами."""

    markets_refresh_seconds: float = 600.0
    """Как часто перечитывать список перпов: подхватывает новые листинги."""

    max_seen_twaps: int = 200_000
    """Размер памяти о найденных ордерах для дедупликации."""

    max_seen_wallets: int = 20_000
    """Размер памяти о проверенных кошельках для дедупликации кандидатов."""

    @property
    def discovery_capacity(self) -> int:
        """Сколько кошельков поиск проверяет одновременно."""
        return self.discovery_connections * MAX_USERS_PER_WS

    @property
    def tracking_capacity(self) -> int:
        """Сколько кошельков слежка держит одновременно."""
        return self.tracking_connections * MAX_USERS_PER_WS

    @property
    def queue_limit(self) -> int:
        """Предельная глубина очереди кандидатов.

        Слот поиска освобождается каждые `watch_ttl` секунд. Очередь глубже той,
        что поиск разберет за `queue_wait_seconds`, состоит из протухших кандидатов.
        """
        return max(1, int(self.discovery_capacity * self.queue_wait_seconds / self.watch_ttl))
