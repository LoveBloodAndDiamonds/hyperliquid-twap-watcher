"""Регистр отслеживаемых TWAP-ордеров: сопоставление слайсов и расписание проверок."""

__all__ = ["TrackedTwap", "TwapRegistry"]

from dataclasses import dataclass

from .config import WatcherConfig
from .types import TwapCreatedEvent, TwapSide

type _SliceKey = tuple[str, str, TwapSide]
"""Ключ сопоставления слайса: кошелек, монета, сторона."""


@dataclass(slots=True)
class TrackedTwap:
    """Ордер под наблюдением: ждет слайсов и проверяется на завершение."""

    twap_id: int
    wallet: str
    coin: str
    side: TwapSide
    minutes: int
    created_at_ms: int

    last_slice_at: float
    """Время последнего слайса, секунды. До первого слайса — время обнаружения:
    иначе ордер ушел бы на проверку сразу после находки."""

    next_check_at: float = 0.0
    """Раньше этого момента ордер не проверяется, секунды."""

    failed_checks: int = 0
    """Сколько проверок подряд показали, что ордер еще жив."""

    @property
    def planned_end(self) -> float:
        """Расчетный конец ордера, секунды."""
        return self.created_at_ms / 1000 + self.minutes * 60


class TwapRegistry:
    """Хранит ордера под наблюдением и решает, какие из них пора проверить.

    В сделке нет идентификатора TWAP, поэтому слайс относится к ордеру по
    кошельку, монете и стороне. Два живых ордера с одинаковым ключом — редкость,
    но возможны: тогда слайс достается обоим кандидатам.
    """

    _PLANNED_END_TOLERANCE = 60.0
    """Допуск к расчетному концу ордера, секунды: последний слайс плавает."""

    def __init__(self, config: WatcherConfig) -> None:
        """Создает пустой регистр.

        :param config: Настройки наблюдателя: порог тишины и паузы проверок.
        """
        self._config = config
        self._twaps: dict[tuple[str, int], TrackedTwap] = {}
        self._by_key: dict[_SliceKey, set[int]] = {}

    def __len__(self) -> int:
        """Сколько ордеров под наблюдением."""
        return len(self._twaps)

    def add(self, event: TwapCreatedEvent, *, now: float) -> None:
        """Ставит найденный ордер под наблюдение.

        :param event: Событие о найденном ордере.
        :param now: Текущее время, секунды.
        """
        twap = TrackedTwap(
            twap_id=event["twap_id"],
            wallet=event["wallet"],
            coin=event["coin"],
            side=event["side"],
            minutes=event["minutes"],
            created_at_ms=event["created_at_ms"],
            last_slice_at=now,
        )

        self._twaps[(twap.wallet, twap.twap_id)] = twap
        self._by_key.setdefault((twap.wallet, twap.coin, twap.side), set()).add(twap.twap_id)

    def remove(self, wallet: str, twap_id: int) -> None:
        """Снимает ордер с наблюдения.

        :param wallet: Кошелек-владелец в нижнем регистре.
        :param twap_id: Идентификатор ордера.
        """
        twap = self._twaps.pop((wallet, twap_id), None)
        if twap is None:
            return

        key = (twap.wallet, twap.coin, twap.side)
        ids = self._by_key.get(key)
        if ids is not None:
            ids.discard(twap_id)
            if not ids:
                del self._by_key[key]

    def match_slice(self, wallet: str, coin: str, side: TwapSide, *, now: float) -> list[int]:
        """Находит ордера, к которым относится сделка, и отмечает у них слайс.

        :param wallet: Кошелек участника сделки в нижнем регистре.
        :param coin: Монета сделки.
        :param side: Сторона участника в сделке.
        :param now: Текущее время, секунды.
        :return: Подходящие ордера по возрастанию ID. Пусто — сделка не наша.
        """
        ids = self._by_key.get((wallet, coin, side))
        if not ids:
            return []

        for twap_id in ids:
            twap = self._twaps[(wallet, twap_id)]
            twap.last_slice_at = now
            # Слайс доказывает, что ордер жив: следующая тишина проверяется без паузы.
            twap.failed_checks = 0
            twap.next_check_at = 0.0

        return sorted(ids)

    def for_wallet(self, wallet: str) -> list[TrackedTwap]:
        """Возвращает все ордера кошелька под наблюдением."""
        return [twap for (owner, _), twap in self._twaps.items() if owner == wallet]

    def is_due(self, twap: TrackedTwap, now: float) -> bool:
        """Проверяет, пора ли спросить биржу о судьбе ордера.

        Повод — затянувшаяся тишина в слайсах или пройденный расчетный конец.
        Второе нужно для ордеров с общим ключом: слайсы соседа держат тишину
        короткой, и досрочную отмену одного из них видно только так.
        """
        if now < twap.next_check_at:
            return False

        silent = now - twap.last_slice_at > self._config.slice_silence_seconds
        overdue = now > twap.planned_end + self._PLANNED_END_TOLERANCE
        return silent or overdue

    def next_due_wallet(self, now: float) -> str | None:
        """Выбирает кошелек для следующей проверки.

        Один запрос истории закрывает все ордера кошелька, поэтому проверки
        группируются по кошельку. Первым идет ордер, дольше всех ждущий проверки.

        :param now: Текущее время, секунды.
        :return: Адрес кошелька или None, если проверять некого.
        """
        due = [twap for twap in self._twaps.values() if self.is_due(twap, now)]
        if not due:
            return None

        first = min(due, key=lambda twap: (twap.next_check_at, twap.last_slice_at))
        return first.wallet

    def postpone(self, twap: TrackedTwap, now: float) -> None:
        """Откладывает следующую проверку ордера с растущей паузой.

        :param twap: Ордер, который оказался живым или не проверился.
        :param now: Текущее время, секунды.
        """
        twap.failed_checks += 1
        # Степень ограничена: ордер с триггером может молчать неделями, а дальше
        # предела пауза все равно не растет.
        delay = self._config.check_cooldown_seconds * 2 ** min(twap.failed_checks - 1, 16)
        twap.next_check_at = now + min(delay, self._config.check_max_backoff_seconds)
