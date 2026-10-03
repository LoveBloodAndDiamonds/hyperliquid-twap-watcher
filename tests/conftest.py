"""Общие фикстуры: сырые данные биржи в том виде, в каком их отдает Hyperliquid."""

import time
from typing import Any

from hl_twap_watcher._events import build_created
from hl_twap_watcher.types import TwapCreatedEvent

WALLET = "0xec1bd5ed185b352863def10a8d7bd2d44031b312"
OTHER = "0xabfae5ef417fec83a63dcdbff5a13f71d09045c5"
ZERO_HASH = "0x" + "0" * 64


def make_state(
    *,
    user: str = WALLET,
    coin: str = "BTC",
    side: str = "B",
    sz: str = "1.0",
    minutes: int = 60,
    timestamp: int | None = None,
) -> dict[str, Any]:
    """Состояние ордера из канала `twapStates`."""
    return {
        "coin": coin,
        "user": user,
        "side": side,
        "sz": sz,
        "executedSz": "0.25",
        "executedNtl": "21000.0",
        "minutes": minutes,
        "reduceOnly": False,
        "randomize": True,
        "timestamp": timestamp if timestamp is not None else int(time.time() * 1000) - 60_000,
        "trigger": None,
        "stopPx": None,
    }


def make_trade(
    *,
    coin: str = "BTC",
    buyer: str = WALLET,
    seller: str = OTHER,
    px: str = "84000.0",
    sz: str = "0.01",
    hash_: str = ZERO_HASH,
    tid: int = 1,
) -> dict[str, Any]:
    """Сделка из канала `trades`."""
    return {
        "coin": coin,
        "side": "B",
        "px": px,
        "sz": sz,
        "time": 1791044743051,
        "hash": hash_,
        "tid": tid,
        "users": [buyer, seller],
    }


def make_record(
    twap_id: int,
    status: str,
    *,
    time_s: int = 1790615681,
    executed_sz: str = "1.0",
    executed_ntl: str = "84000.0",
    state: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Запись истории `twapHistory`."""
    record_state = dict(state or make_state())
    record_state["executedSz"] = executed_sz
    record_state["executedNtl"] = executed_ntl
    return {
        "time": time_s,
        "state": record_state,
        "status": {"status": status},
        "twapId": twap_id,
    }


def make_fill(
    twap_id: int,
    *,
    time_ms: int,
    tid: int = 1,
    coin: str = "BTC",
    side: str = "B",
    px: str = "84000.0",
    sz: str = "0.01",
) -> dict[str, Any]:
    """Элемент канала `userTwapSliceFills`: `{"fill": {...}, "twapId": ...}`."""
    return {
        "fill": {
            "coin": coin,
            "px": px,
            "sz": sz,
            "side": side,
            "time": time_ms,
            "hash": ZERO_HASH,
            "tid": tid,
            "twapId": None,
        },
        "twapId": twap_id,
    }


def make_created(twap_id: int = 100, **state_kwargs: Any) -> TwapCreatedEvent:
    """Событие `created` по BTC-ордеру."""
    return build_created(
        twap_id, make_state(**state_kwargs), mid_price=84000.0, tracked=True, now=time.time()
    )
