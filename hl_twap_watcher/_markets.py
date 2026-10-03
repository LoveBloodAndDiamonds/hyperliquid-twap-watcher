"""Справочник рынков: список перпов основного dex и их mid-цены."""

__all__ = ["Markets"]

from typing import Any


class Markets:
    """Хранит список торгуемых перпов и последние mid-цены.

    Список перпов нужен для подписки на сделки и для отсева спотовых ордеров и
    ордеров builder-dex'ов (`xyz:SP500`): `twapStates` отдает все ордера кошелька.
    Mid-цены приходят из WS-канала `allMids` и переводят размер ордера в доллары.
    """

    def __init__(self) -> None:
        """Создает пустой справочник: наполняется `apply_meta` и `apply_mids`."""
        self._perps: set[str] = set()
        self._mids: dict[str, float] = {}

    @property
    def perps(self) -> list[str]:
        """Торгуемые перпы в алфавитном порядке."""
        return sorted(self._perps)

    def apply_meta(self, meta: dict[str, Any]) -> list[str]:
        """Обновляет список перпов из ответа `meta`.

        :param meta: Ответ info-запроса `{"type": "meta"}`.
        :return: Перпы, которых не было в справочнике (новые листинги).
        """
        # Делистнутые рынки остаются в справочнике индексов, но торгов по ним нет.
        perps = {asset["name"] for asset in meta["universe"] if not asset.get("isDelisted")}

        added = sorted(perps - self._perps)
        self._perps = perps
        return added

    def apply_mids(self, mids: dict[str, str]) -> None:
        """Обновляет mid-цены из сообщения канала `allMids`.

        :param mids: Словарь `монета -> цена строкой`.
        """
        # Обновляем, а не заменяем: монета, пропавшая из одного сообщения, не теряет цену.
        for coin, price in mids.items():
            self._mids[coin] = float(price)

    def is_perp(self, coin: str) -> bool:
        """Проверяет, что монета — торгуемый перп основного dex."""
        return coin in self._perps

    def mid(self, coin: str) -> float | None:
        """Возвращает mid-цену монеты или None, если цены еще нет."""
        return self._mids.get(coin)
