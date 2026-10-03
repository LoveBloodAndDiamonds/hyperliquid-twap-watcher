"""Самовосстанавливающийся WebSocket: реконнект, ping, сторож тишины и очередь сообщений.

Механизм позаимствован из `unicex._base.websocket` и упрощен под нужды библиотеки:
весь цикл жизни соединения живет в одной задаче, поэтому реконнект не может
оставить висеть второе соединение.
"""

__all__ = ["Websocket"]

import asyncio
import json
from collections.abc import Awaitable, Callable
from contextlib import suppress
from typing import Any

import websockets
from loguru import logger as _logger
from websockets.asyncio.client import ClientConnection

from .types import LoggerLike

type MessageCallback = Callable[[dict[str, Any]], Awaitable[None]]
"""Обработчик декодированного сообщения."""

type ConnectCallback = Callable[[], Awaitable[None]]
"""Хук, вызываемый на каждом новом соединении до отправки подписок."""

_HYPERLIQUID_PING = json.dumps({"method": "ping"})
"""Ping Hyperliquid: без сообщений от клиента биржа закрывает соединение через минуту."""


class Websocket:
    """Асинхронный WebSocket-клиент с автоматическим переподключением.

    Чтение и обработка разнесены: чтение кладет сообщения в очередь, а отдельный
    воркер передает их в callback. Медленный обработчик не тормозит чтение, а при
    переполнении очередь сбрасывается — свежие данные важнее старых.
    """

    def __init__(
        self,
        url: str,
        callback: MessageCallback,
        *,
        subscription_messages: list[dict[str, Any]] | None = None,
        on_connect: ConnectCallback | None = None,
        ping_interval: float = 30.0,
        ping_message: str | None = _HYPERLIQUID_PING,
        no_message_reconnect_timeout: float | None = 60.0,
        reconnect_timeout: float = 5.0,
        max_queue_size: int = 5000,
        name: str = "ws",
        logger: LoggerLike | None = None,
    ) -> None:
        """Инициализирует вебсокет.

        :param url: Адрес вебсокета.
        :param callback: Обработчик декодированных сообщений.
        :param subscription_messages: Сообщения подписки, отправляемые на каждом подключении.
        :param on_connect: Хук нового соединения: вызывается до отправки подписок.
        :param ping_interval: Интервал ping (и протокольного, и `ping_message`), секунды.
        :param ping_message: Текстовый ping. None — только протокольные ping-фреймы.
        :param no_message_reconnect_timeout: Тишина, после которой соединение
            пересоздается, секунды. None — не следить.
        :param reconnect_timeout: Пауза перед переподключением, секунды.
        :param max_queue_size: Предел очереди сообщений: при переполнении она сбрасывается.
        :param name: Имя соединения для логов.
        :param logger: Логгер. По умолчанию — loguru.
        """
        self._url = url
        self._callback = callback
        self._subscription_messages = list(subscription_messages or [])
        self._on_connect = on_connect
        self._ping_interval = ping_interval
        self._ping_message = ping_message
        self._no_message_reconnect_timeout = no_message_reconnect_timeout
        self._reconnect_timeout = reconnect_timeout
        self._max_queue_size = max_queue_size
        self._name = name
        self._logger = logger or _logger

        self._conn: ClientConnection | None = None
        self._queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self._running = False
        self._reconnect_requested = False

    @property
    def running(self) -> bool:
        """True, пока вебсокет не остановлен."""
        return self._running

    @property
    def connected(self) -> bool:
        """True, если соединение установлено и подписки отправлены."""
        return self._conn is not None

    async def start(self) -> None:
        """Держит соединение до вызова `stop()`. Возвращает управление только после остановки."""
        if self._running:
            raise RuntimeError(f"Websocket {self._name} is already running")
        self._running = True

        try:
            while self._running:
                try:
                    await self._run_connection()
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    # Закрытие по stop() и reconnect() тоже приходит исключением —
                    # это не ошибка.
                    if self._reconnect_requested:
                        self._logger.info(f"Websocket {self._name} reconnecting on request")
                    elif self._running:
                        self._logger.error(
                            f"Websocket {self._name} error: {exc!r}, "
                            f"reconnecting in {self._reconnect_timeout}s"
                        )

                self._reconnect_requested = False
                if self._running:
                    await asyncio.sleep(self._reconnect_timeout)
        finally:
            self._running = False

    async def stop(self) -> None:
        """Останавливает вебсокет: закрывает соединение и выходит из цикла `start()`."""
        self._running = False

        conn = self._conn
        if conn is not None:
            with suppress(Exception):
                await conn.close()

    async def reconnect(self) -> None:
        """Пересоздает соединение, не останавливая вебсокет: подписки повторятся сами."""
        conn = self._conn
        if conn is None:
            return

        self._reconnect_requested = True
        with suppress(Exception):
            await conn.close()

    async def send(self, message: dict[str, Any]) -> None:
        """Отправляет сообщение в текущее соединение.

        :param message: Сообщение, сериализуется в JSON.
        :raises ConnectionError: Если соединения сейчас нет (идет реконнект).
        """
        conn = self._conn
        if conn is None:
            raise ConnectionError(f"Websocket {self._name} is not connected")

        await conn.send(json.dumps(message))

    async def add_subscription(self, message: dict[str, Any]) -> None:
        """Добавляет подписку: отправляет ее сейчас и повторяет на каждом реконнекте.

        :param message: Сообщение подписки.
        """
        self._subscription_messages.append(message)

        # Без соединения подписка уйдет сама при следующем подключении.
        if self._conn is not None:
            with suppress(Exception):
                await self.send(message)

    async def _run_connection(self) -> None:
        """Проживает одно соединение: от подключения до первой ошибки или остановки."""
        self._logger.debug(f"Websocket {self._name} connecting to {self._url}")

        async with websockets.connect(
            self._url, max_size=None, ping_interval=self._ping_interval
        ) as conn:
            # Очередь пересоздается: сообщения прошлого соединения относятся к
            # прошлому состоянию подписок и только запутают обработчик.
            self._queue = asyncio.Queue()
            helpers: list[asyncio.Task] = []

            try:
                if self._on_connect is not None:
                    await self._on_connect()

                helpers.append(asyncio.create_task(self._worker(self._queue)))
                if self._ping_message:
                    helpers.append(asyncio.create_task(self._ping_task(conn)))

                for message in self._subscription_messages:
                    await conn.send(json.dumps(message))

                # Соединение публикуется только после подписок: иначе send() из
                # внешнего кода мог бы обогнать их.
                self._conn = conn
                self._logger.info(f"Websocket {self._name} connected to {self._url}")

                await self._receive_loop(conn)
            finally:
                self._conn = None
                for task in helpers:
                    task.cancel()
                await asyncio.gather(*helpers, return_exceptions=True)

    async def _receive_loop(self, conn: ClientConnection) -> None:
        """Читает сообщения, пока вебсокет запущен."""
        while self._running:
            try:
                # Сторож тишины: подвисшее соединение без единого сообщения не
                # закрывается само, его надо пересоздать.
                raw = await asyncio.wait_for(
                    conn.recv(), timeout=self._no_message_reconnect_timeout
                )
            except TimeoutError:
                raise ConnectionError(
                    f"No messages in {self._no_message_reconnect_timeout} seconds"
                ) from None

            self._handle_message(raw)

    def _handle_message(self, raw: str | bytes) -> None:
        """Декодирует сообщение и кладет его в очередь обработки."""
        try:
            message = json.loads(raw)
        except json.JSONDecodeError:
            self._logger.warning(f"Websocket {self._name} got non-JSON message: {raw!r:.200}")
            return

        # Ответы на наш ping обработчику не нужны.
        if not isinstance(message, dict) or message.get("channel") == "pong":
            return

        if self._queue.qsize() >= self._max_queue_size:
            cleared = self._clear_queue()
            self._logger.error(f"Websocket {self._name} queue overflow, dropped {cleared} messages")

        self._queue.put_nowait(message)

    def _clear_queue(self) -> int:
        """Очищает очередь сообщений и возвращает число выброшенных."""
        cleared = 0
        while True:
            try:
                self._queue.get_nowait()
            except asyncio.QueueEmpty:
                return cleared
            cleared += 1

    async def _worker(self, queue: asyncio.Queue[dict[str, Any]]) -> None:
        """Передает сообщения из очереди в callback по одному, сохраняя порядок."""
        while True:
            message = await queue.get()
            try:
                await self._callback(message)
            except Exception as exc:
                self._logger.exception(
                    f"Websocket {self._name} callback failed on message: {exc!r}"
                )

    async def _ping_task(self, conn: ClientConnection) -> None:
        """Периодически отправляет текстовый ping."""
        while True:
            await asyncio.sleep(self._ping_interval)
            try:
                await conn.send(self._ping_message or "")
            except Exception as exc:
                # Соединение умерло: цикл чтения заметит это сам и переподключится.
                self._logger.debug(f"Websocket {self._name} ping failed: {exc!r}")
                return

    def __repr__(self) -> str:
        """Репрезентация вебсокета."""
        return f"<Websocket(name={self._name}, url={self._url})>"
