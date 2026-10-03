"""Доставка событий в callback-функции пользователя."""

__all__ = ["Dispatcher"]

import asyncio
import inspect
from contextlib import suppress

from loguru import logger as _logger

from .types import (
    Callback,
    LoggerLike,
    TwapCreatedEvent,
    TwapEvent,
    TwapFinishedEvent,
    TwapSliceEvent,
)


class Dispatcher:
    """Очередь событий и воркер, который передает их в callback'и.

    События рождаются в обработчиках вебсокетов, а callback пользователя может быть
    медленным. Очередь развязывает одно с другим: чтение потоков не ждет
    пользовательский код. Порядок событий сохраняется.
    """

    _QUEUE_WARNING_STEP = 10_000
    """Шаг глубины очереди, на котором пишется предупреждение о медленном callback."""

    _DRAIN_TIMEOUT = 5.0
    """Сколько при остановке ждать доставки накопленных событий, секунды."""

    def __init__(
        self,
        *,
        on_event: Callback[TwapEvent] | None = None,
        on_created: Callback[TwapCreatedEvent] | None = None,
        on_slice: Callback[TwapSliceEvent] | None = None,
        on_finished: Callback[TwapFinishedEvent] | None = None,
        logger: LoggerLike | None = None,
    ) -> None:
        """Сохраняет callback'и.

        :param on_event: Общий обработчик всех событий. Вызывается первым.
        :param on_created: Обработчик найденных ордеров.
        :param on_slice: Обработчик слайсов.
        :param on_finished: Обработчик завершенных ордеров.
        :param logger: Логгер. По умолчанию — loguru.
        """
        self._on_event = on_event
        self._on_created = on_created
        self._on_slice = on_slice
        self._on_finished = on_finished
        self._logger = logger or _logger

        self._queue: asyncio.Queue[TwapEvent] = asyncio.Queue()
        self._task: asyncio.Task | None = None
        self._next_warning = self._QUEUE_WARNING_STEP

        self.created = 0
        """Сколько событий `created` поставлено в доставку."""

        self.slices = 0
        """Сколько событий `slice` поставлено в доставку."""

        self.finished = 0
        """Сколько событий `finished` поставлено в доставку."""

        self.errors = 0
        """Сколько раз callback выбросил исключение."""

    async def start(self) -> None:
        """Запускает воркер доставки."""
        self._task = asyncio.create_task(self._worker())

    async def stop(self) -> None:
        """Доставляет накопленные события (с таймаутом) и останавливает воркер."""
        if self._task is None:
            return

        with suppress(TimeoutError):
            await asyncio.wait_for(self._queue.join(), timeout=self._DRAIN_TIMEOUT)

        self._task.cancel()
        with suppress(asyncio.CancelledError):
            await self._task
        self._task = None

    def emit(self, event: TwapEvent) -> None:
        """Ставит событие в очередь доставки.

        :param event: Событие наблюдателя.
        """
        match event["type"]:
            case "created":
                self.created += 1
            case "slice":
                self.slices += 1
            case "finished":
                self.finished += 1

        self._queue.put_nowait(event)

        # Очередь без предела: терять события хуже, чем занять память. Но растущая
        # очередь — признак медленного callback, о нем стоит знать.
        size = self._queue.qsize()
        if size >= self._next_warning:
            self._logger.warning(f"Event queue size is {size}: callbacks are too slow")
            self._next_warning += self._QUEUE_WARNING_STEP
        elif size < self._QUEUE_WARNING_STEP:
            self._next_warning = self._QUEUE_WARNING_STEP

    async def _worker(self) -> None:
        """Передает события в общий и специализированный callback по очереди."""
        while True:
            event = await self._queue.get()
            try:
                await self._call(self._on_event, event)

                # Сужение типа по полю `type` для специализированных callback'ов.
                match event["type"]:
                    case "created":
                        await self._call(self._on_created, event)
                    case "slice":
                        await self._call(self._on_slice, event)
                    case "finished":
                        await self._call(self._on_finished, event)
            finally:
                self._queue.task_done()

    async def _call[E](self, callback: Callback[E] | None, event: E) -> None:
        """Вызывает callback (функцию или корутину) и логирует его ошибки."""
        if callback is None:
            return

        try:
            result = callback(event)
            if inspect.isawaitable(result):
                await result
        except Exception as exc:
            # Ошибка пользователя не должна останавливать доставку остальных событий.
            self.errors += 1
            self._logger.exception(f"Callback {_name(callback)} failed: {exc!r}")


def _name(callback: object) -> str:
    """Возвращает читаемое имя callback для логов."""
    return getattr(callback, "__qualname__", None) or repr(callback)
