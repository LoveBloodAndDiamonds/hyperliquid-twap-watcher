"""Асинхронный наблюдатель за TWAP-ордерами Hyperliquid.

Находит новые TWAP-ордера, их слайсы и завершения и передает их в callback'и.
"""

__all__ = [
    "TwapWatcher",
    "WatcherConfig",
    "MAX_USERS_PER_WS",
    "TwapSide",
    "TwapFinishReason",
    "TwapCreatedEvent",
    "TwapSliceEvent",
    "TwapFinishedEvent",
    "TwapEvent",
    "WatcherStats",
    "Callback",
]

from .config import MAX_USERS_PER_WS, WatcherConfig
from .types import (
    Callback,
    TwapCreatedEvent,
    TwapEvent,
    TwapFinishedEvent,
    TwapFinishReason,
    TwapSide,
    TwapSliceEvent,
    WatcherStats,
)
from .watcher import TwapWatcher
