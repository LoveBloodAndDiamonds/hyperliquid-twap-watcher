"""Минимальный REST-клиент info-эндпоинта Hyperliquid с повторами при сбоях."""

__all__ = ["InfoClient", "ResponseError"]

import asyncio
import json
from typing import Any

import aiohttp
from loguru import logger as _logger

from .types import LoggerLike


class ResponseError(Exception):
    """Биржа вернула ошибку или неразборчивый ответ."""

    def __init__(self, message: str, status_code: int) -> None:
        """Сохраняет HTTP-статус ответа.

        :param message: Текст ошибки.
        :param status_code: HTTP-статус ответа.
        """
        super().__init__(message)
        self.status_code = status_code


class InfoClient:
    """Клиент `POST /info`: только те запросы, что нужны наблюдателю.

    Повторяет запрос при таймаутах, сетевых ошибках, 429 и 5xx — логика
    позаимствована из `unicex._base.client`.
    """

    _RETRY_STATUSES = frozenset({429, 500, 502, 503, 504})
    """HTTP-статусы, после которых запрос имеет смысл повторить."""

    def __init__(
        self,
        url: str,
        *,
        session: aiohttp.ClientSession | None = None,
        max_retries: int = 3,
        retry_delay: float = 0.5,
        timeout: float = 10.0,
        logger: LoggerLike | None = None,
    ) -> None:
        """Инициализирует клиент.

        :param url: Адрес info-эндпоинта.
        :param session: Внешняя сессия aiohttp. Если не передана, клиент создаст и
            закроет свою.
        :param max_retries: Сколько всего попыток на запрос.
        :param retry_delay: Базовая пауза между попытками, удваивается с каждой, секунды.
        :param timeout: Таймаут одного запроса, секунды.
        :param logger: Логгер. По умолчанию — loguru.
        """
        self._url = url
        self._session = session
        self._owns_session = session is None
        self._max_retries = max(1, max_retries)
        self._retry_delay = max(0.0, retry_delay)
        self._timeout = aiohttp.ClientTimeout(total=timeout)
        self._logger = logger or _logger

    async def close(self) -> None:
        """Закрывает сессию, если клиент создавал ее сам."""
        if self._owns_session and self._session is not None:
            await self._session.close()
            self._session = None

    async def perp_meta(self) -> dict[str, Any]:
        """Возвращает метаданные перпов основного dex: `{"universe": [...]}`."""
        return await self._post({"type": "meta"})

    async def twap_history(self, user: str) -> list[dict[str, Any]]:
        """Возвращает историю TWAP-ордеров кошелька, от свежих записей к старым.

        Каждая запись — смена статуса ордера: `activated`, затем один из
        `finished`, `terminated`, `stopped`, `error`.

        :param user: Адрес кошелька.
        """
        return await self._post({"type": "twapHistory", "user": user})

    async def _post(self, payload: dict[str, Any]) -> Any:
        """Выполняет запрос с повторами и возвращает разобранный JSON."""
        if self._session is None:
            self._session = aiohttp.ClientSession()

        last_error: Exception | None = None
        for attempt in range(1, self._max_retries + 1):
            try:
                async with self._session.post(
                    self._url, json=payload, timeout=self._timeout
                ) as response:
                    return await self._handle_response(response)

            except ResponseError as exc:
                if exc.status_code not in self._RETRY_STATUSES:
                    raise
                last_error = exc

            except (TimeoutError, aiohttp.ClientConnectionError) as exc:
                last_error = exc

            self._logger.debug(
                f"Info request {payload.get('type')} attempt {attempt}/{self._max_retries} "
                f"failed: {last_error!r}"
            )
            if attempt < self._max_retries:
                await asyncio.sleep(self._retry_delay * 2 ** (attempt - 1))

        raise ConnectionError(
            f"Info request {payload.get('type')} failed after {self._max_retries} attempts: "
            f"{last_error!r}"
        ) from last_error

    @staticmethod
    async def _handle_response(response: aiohttp.ClientResponse) -> Any:
        """Проверяет статус ответа и разбирает JSON."""
        text = await response.text()

        if response.status != 200:
            raise ResponseError(
                f"HTTP {response.status}: {text[:500]}", status_code=response.status
            )

        try:
            return json.loads(text)
        except json.JSONDecodeError:
            raise ResponseError(
                f"Invalid JSON: {text[:500]}", status_code=response.status
            ) from None
