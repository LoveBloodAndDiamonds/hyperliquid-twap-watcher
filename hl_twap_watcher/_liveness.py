"""Детект завершения TWAP-ордеров: тишина в слайсах плюс контрольный запрос истории."""

__all__ = ["TwapLiveness"]

import asyncio
import time
from collections.abc import Callable
from contextlib import suppress
from typing import Any

from loguru import logger as _logger

from ._events import build_finished
from ._http import InfoClient
from ._registry import TrackedTwap, TwapRegistry
from .config import WatcherConfig
from .types import LoggerLike, TwapFinishedEvent, TwapFinishReason

type FinishedHandler = Callable[[TwapFinishedEvent], None]
"""Обработчик события о завершении ордера."""

_FINISH_REASONS: dict[str, TwapFinishReason] = {
    "finished": "completed",
    "terminated": "cancelled",
    "stopped": "stopped",
    "error": "error",
}
"""Финальные статусы `twapHistory` и соответствующие причины завершения.
Все прочие статусы (`activated` и неизвестные) считаются живым ордером."""

_IDLE_SLEEP = 1.0
"""Пауза цикла, когда проверять некого, секунды."""


class TwapLiveness:
    """Определяет, какие отслеживаемые ордера завершились.

    События «ордер завершен» у биржи нет. Пока ордер исполняется, его слайсы идут
    через поток сделок примерно раз в 30 секунд, поэтому затянувшаяся тишина —
    повод спросить биржу историю ордеров кошелька. Закрывает ордер только финальный
    статус в истории: слайс мог просто потеряться на реконнекте потока.
    """

    def __init__(
        self,
        config: WatcherConfig,
        client: InfoClient,
        registry: TwapRegistry,
        on_finished: FinishedHandler,
        *,
        logger: LoggerLike | None = None,
    ) -> None:
        """Инициализирует детект завершения.

        :param config: Настройки наблюдателя: лимит запросов и паузы проверок.
        :param client: REST-клиент info-эндпоинта.
        :param registry: Регистр ордеров под наблюдением.
        :param on_finished: Обработчик события о завершении.
        :param logger: Логгер. По умолчанию — loguru.
        """
        self._config = config
        self._client = client
        self._registry = registry
        self._on_finished = on_finished
        self._logger = logger or _logger

        self._task: asyncio.Task | None = None

        self.checks = 0
        """Сколько контрольных запросов истории сделано."""

    async def start(self) -> None:
        """Запускает фоновый цикл проверок."""
        self._task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        """Останавливает цикл проверок."""
        if self._task is None:
            return

        self._task.cancel()
        with suppress(asyncio.CancelledError):
            await self._task
        self._task = None

    async def _loop(self) -> None:
        """Проверяет по одному кошельку за раз, не превышая лимит запросов."""
        while True:
            wallet = self._registry.next_due_wallet(time.time())
            if wallet is None:
                await asyncio.sleep(_IDLE_SLEEP)
                continue

            try:
                await self.check_wallet(wallet)
            except Exception as exc:
                # Ошибка разбора не должна останавливать детект для остальных ордеров,
                # а пауза не дает зациклиться на одном кошельке.
                self._logger.exception(f"Liveness check failed for {wallet}: {exc!r}")
                self._postpone_due(wallet)

            await asyncio.sleep(self._config.rest_interval)

    async def check_wallet(self, wallet: str) -> None:
        """Сверяет ордера кошелька с историей биржи и закрывает завершенные.

        :param wallet: Адрес кошелька в нижнем регистре.
        """
        try:
            history = await self._client.twap_history(wallet)

        except Exception as exc:
            # Сбой биржи — не повод закрывать ордера: проверку повторим позже.
            self._logger.warning(f"Twap history request failed for {wallet}: {exc!r}")
            self._postpone_due(wallet)
            return

        self.checks += 1
        now = time.time()
        latest = self._latest_records(history)

        for twap in self._registry.for_wallet(wallet):
            record = latest.get(twap.twap_id)
            reason = _FINISH_REASONS.get(_status(record)) if record else None

            # История пришла по всему кошельку: закрываем все завершенные ордера,
            # а не только тот, что стал поводом для проверки.
            if record is not None and reason is not None:
                self._registry.remove(twap.wallet, twap.twap_id)
                self._on_finished(build_finished(record, reason=reason, now=now))
                continue

            if not self._registry.is_due(twap, now):
                continue

            if record is None and self._is_abandoned(twap, now):
                # Биржа не отдает ордер в истории (у активных ботов она обрезана),
                # а срок давно вышел: держать его вечно нельзя.
                self._registry.remove(twap.wallet, twap.twap_id)
                self._logger.warning(
                    f"Twap {twap.twap_id} ({twap.coin}, {twap.wallet}) not found in history "
                    f"after planned end, dropped without finished event"
                )
                continue

            self._registry.postpone(twap, now)

    def _postpone_due(self, wallet: str) -> None:
        """Откладывает проверку ордеров кошелька, которые ее ждали."""
        now = time.time()
        for twap in self._registry.for_wallet(wallet):
            if self._registry.is_due(twap, now):
                self._registry.postpone(twap, now)

    def _is_abandoned(self, twap: TrackedTwap, now: float) -> bool:
        """Проверяет, что расчетный конец ордера прошел с большим запасом."""
        return now > twap.planned_end + self._config.finished_grace_seconds

    @staticmethod
    def _latest_records(history: list[dict[str, Any]]) -> dict[int, dict[str, Any]]:
        """Возвращает последнюю запись истории по каждому ордеру."""
        latest: dict[int, dict[str, Any]] = {}

        for record in history:
            # Старые записи (до 2025 года) биржа отдает без идентификатора ордера.
            if "twapId" not in record:
                continue

            twap_id = int(record["twapId"])
            current = latest.get(twap_id)

            # Биржа отдает историю от свежих к старым, но порядок не обещан. При
            # равном времени финальный статус важнее `activated`: ордер мог
            # завершиться в ту же секунду, что и запуститься.
            if current is None or (record["time"], _is_final(record)) > (
                current["time"],
                _is_final(current),
            ):
                latest[twap_id] = record

        return latest


def _status(record: dict[str, Any]) -> str:
    """Возвращает статус записи истории: `activated`, `finished` и т.д."""
    return str(record.get("status", {}).get("status", ""))


def _is_final(record: dict[str, Any]) -> bool:
    """Проверяет, что запись истории — финальный статус ордера."""
    return _status(record) in _FINISH_REASONS
