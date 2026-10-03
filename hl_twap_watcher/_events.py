"""Сборка публичных событий из сырых данных биржи."""

__all__ = ["parse_side", "build_created", "build_slice", "build_finished"]

from typing import Any

from .types import (
    TwapCreatedEvent,
    TwapFinishedEvent,
    TwapFinishReason,
    TwapSide,
    TwapSliceEvent,
)


def parse_side(raw: str) -> TwapSide:
    """Переводит сторону из формата биржи (`B` / `A`) в `BUY` / `SELL`."""
    # У Hyperliquid "A" (ask) — продажа, "B" (bid) — покупка.
    return "BUY" if raw == "B" else "SELL"


def build_created(
    twap_id: int, state: dict[str, Any], *, mid_price: float | None, now: float
) -> TwapCreatedEvent:
    """Собирает событие о найденном ордере из состояния канала `twapStates`.

    :param twap_id: Идентификатор ордера.
    :param state: Состояние ордера с биржи.
    :param mid_price: Текущая mid-цена монеты или None.
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
        notional_usd=size * mid_price if mid_price else None,
    )


def build_slice(
    trade: dict[str, Any], *, wallet: str, side: TwapSide, twap_ids: list[int]
) -> TwapSliceEvent:
    """Собирает событие слайса из сделки потока `trades`.

    :param trade: Сделка с биржи.
    :param wallet: Кошелек-владелец слайса в нижнем регистре.
    :param side: Сторона кошелька в сделке.
    :param twap_ids: Отслеживаемые ордера кошелька с той же монетой и стороной.
    :return: Событие `slice`.
    """
    price = float(trade["px"])
    size = float(trade["sz"])

    return TwapSliceEvent(
        type="slice",
        # Однозначно определить ордер можно, только если кандидат один.
        twap_id=twap_ids[0] if len(twap_ids) == 1 else None,
        candidate_twap_ids=twap_ids,
        wallet=wallet,
        coin=trade["coin"],
        side=side,
        price=price,
        size=size,
        notional_usd=price * size,
        time_ms=int(trade["time"]),
        trade_id=int(trade["tid"]),
    )


def build_finished(
    record: dict[str, Any], *, reason: TwapFinishReason, now: float
) -> TwapFinishedEvent:
    """Собирает событие о завершении из записи `twapHistory` с финальным статусом.

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
