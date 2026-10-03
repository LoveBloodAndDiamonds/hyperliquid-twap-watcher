"""Сборка публичных событий из сырых данных биржи."""

__all__ = [
    "FINISH_REASONS",
    "parse_side",
    "record_status",
    "latest_records",
    "build_created",
    "build_slice",
    "build_finished",
]

from typing import Any

from .types import (
    TwapCreatedEvent,
    TwapFinishedEvent,
    TwapFinishReason,
    TwapSide,
    TwapSliceEvent,
)

FINISH_REASONS: dict[str, TwapFinishReason] = {
    "finished": "completed",
    "terminated": "cancelled",
    "stopped": "stopped",
    "error": "error",
}
"""Финальные статусы истории TWAP и соответствующие причины завершения.
Все прочие статусы (`activated` и неизвестные) считаются живым ордером."""


def parse_side(raw: str) -> TwapSide:
    """Переводит сторону из формата биржи (`B` / `A`) в `BUY` / `SELL`."""
    # У Hyperliquid "A" (ask) — продажа, "B" (bid) — покупка.
    return "BUY" if raw == "B" else "SELL"


def record_status(record: dict[str, Any]) -> str:
    """Возвращает статус записи истории TWAP: `activated`, `finished` и т.д."""
    return str(record.get("status", {}).get("status", ""))


def latest_records(history: list[dict[str, Any]]) -> dict[int, dict[str, Any]]:
    """Возвращает последнюю запись истории по каждому ордеру.

    :param history: Записи канала `userTwapHistory`: `{"time", "state", "status", "twapId"}`.
    :return: Словарь `twap_id -> самая свежая запись`.
    """
    latest: dict[int, dict[str, Any]] = {}

    for record in history:
        # Старые записи (до 2025 года) биржа отдает без идентификатора ордера.
        if "twapId" not in record:
            continue

        twap_id = int(record["twapId"])
        current = latest.get(twap_id)

        # Порядок записей биржа не обещает. При равном времени финальный статус
        # важнее `activated`: ордер мог завершиться в ту же секунду, что и запуститься.
        if current is None or _record_key(record) > _record_key(current):
            latest[twap_id] = record

    return latest


def _record_key(record: dict[str, Any]) -> tuple[int, bool]:
    """Ключ сравнения записей истории: время, затем финальность статуса."""
    return int(record["time"]), record_status(record) in FINISH_REASONS


def build_created(
    twap_id: int, state: dict[str, Any], *, mid_price: float, tracked: bool, now: float
) -> TwapCreatedEvent:
    """Собирает событие о найденном ордере из состояния TWAP.

    :param twap_id: Идентификатор ордера.
    :param state: Состояние ордера с биржи (`twapStates` или запись истории).
    :param mid_price: Текущая mid-цена монеты.
    :param tracked: Взят ли ордер под слежку.
    :param now: Текущее время, секунды.
    :return: Событие `created`.
    """
    size = float(state["sz"])
    created_at_ms = int(state["timestamp"])

    return TwapCreatedEvent(
        type="created",
        twap_id=twap_id,
        wallet=state["user"].lower(),
        coin=state["coin"],
        side=parse_side(state["side"]),
        size=size,
        executed_size=float(state["executedSz"]),
        executed_notional=float(state["executedNtl"]),
        minutes=int(state["minutes"]),
        reduce_only=bool(state["reduceOnly"]),
        randomize=bool(state.get("randomize", False)),
        created_at_ms=created_at_ms,
        detected_at_ms=int(now * 1000),
        age_sec=max(0.0, now - created_at_ms / 1000),
        mid_price=mid_price,
        notional_usd=size * mid_price,
        tracked=tracked,
    )


def build_slice(item: dict[str, Any], *, wallet: str) -> TwapSliceEvent:
    """Собирает событие слайса из элемента канала `userTwapSliceFills`.

    :param item: Элемент `{"fill": {...}, "twapId": ...}`.
    :param wallet: Кошелек-владелец в нижнем регистре.
    :return: Событие `slice`.
    """
    fill = item["fill"]
    price = float(fill["px"])
    size = float(fill["sz"])

    return TwapSliceEvent(
        type="slice",
        twap_id=int(item["twapId"]),
        wallet=wallet,
        coin=fill["coin"],
        side=parse_side(fill["side"]),
        price=price,
        size=size,
        notional_usd=price * size,
        time_ms=int(fill["time"]),
        trade_id=int(fill["tid"]),
    )


def build_finished(
    record: dict[str, Any], *, reason: TwapFinishReason, now: float
) -> TwapFinishedEvent:
    """Собирает событие о завершении из записи истории с финальным статусом.

    :param record: Запись истории: `{"time", "state", "status", "twapId"}`.
    :param reason: Причина завершения, уже переведенная из статуса биржи.
    :param now: Текущее время, секунды.
    :return: Событие `finished`.
    """
    state = record["state"]
    executed_size = float(state["executedSz"])
    executed_notional = float(state["executedNtl"])

    return TwapFinishedEvent(
        type="finished",
        twap_id=int(record["twapId"]),
        wallet=state["user"].lower(),
        coin=state["coin"],
        side=parse_side(state["side"]),
        reason=reason,
        size=float(state["sz"]),
        executed_size=executed_size,
        executed_notional=executed_notional,
        average_price=executed_notional / executed_size if executed_size else None,
        minutes=int(state["minutes"]),
        created_at_ms=int(state["timestamp"]),
        # Время записи в истории биржа отдает в секундах.
        finished_at_ms=int(record["time"]) * 1000,
        detected_at_ms=int(now * 1000),
    )
