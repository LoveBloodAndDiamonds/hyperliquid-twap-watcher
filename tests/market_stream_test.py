"""Поток рынка: цены, кандидаты из сделок с нулевым хешом, предел очереди."""

import asyncio

from hl_twap_watcher._market_stream import MarketStream
from hl_twap_watcher._markets import Markets
from hl_twap_watcher.config import WatcherConfig

from .conftest import OTHER, WALLET, make_trade


def make_stream(
    config: WatcherConfig | None = None, tracked: set[str] | None = None
) -> tuple[MarketStream, Markets, asyncio.Queue[str]]:
    """Собирает поток без соединения; `tracked` — кошельки под слежкой."""
    markets = Markets()
    queue: asyncio.Queue[str] = asyncio.Queue()
    tracked = tracked or set()
    stream = MarketStream(config or WatcherConfig(), markets, queue, tracked.__contains__)
    return stream, markets, queue


async def test_all_mids_update_prices() -> None:
    stream, markets, _ = make_stream()

    await stream._on_message({"channel": "allMids", "data": {"mids": {"BTC": "84000.5"}}})

    assert markets.mid("BTC") == 84000.5
    assert stream.mids_ready.is_set()


async def test_zero_hash_trade_enqueues_both_sides() -> None:
    stream, _, queue = make_stream()

    await stream._on_message({"channel": "trades", "data": [make_trade()]})

    assert [queue.get_nowait(), queue.get_nowait()] == [WALLET, OTHER]
    assert stream.zero_hash_trades == 1


async def test_tracked_wallet_is_not_enqueued() -> None:
    stream, _, queue = make_stream(tracked={WALLET})

    await stream._on_message({"channel": "trades", "data": [make_trade()]})

    assert [queue.get_nowait() for _ in range(queue.qsize())] == [OTHER]


async def test_regular_trade_is_ignored() -> None:
    stream, _, queue = make_stream()

    await stream._on_message({"channel": "trades", "data": [make_trade(hash_="0xabc")]})

    assert queue.empty()
    assert stream.trades == 1


async def test_wallet_dedup_window() -> None:
    stream, _, queue = make_stream()

    await stream._on_message({"channel": "trades", "data": [make_trade(), make_trade(tid=2)]})

    assert queue.qsize() == 2
    assert stream.queued == 2


async def test_queue_limit_drops_oldest() -> None:
    # Предел очереди = слоты × ожидание / TTL: 14 × (5/14) / 5 = 1 кошелек.
    config = WatcherConfig(discovery_connections=1, queue_wait_seconds=5 / 14, watch_ttl=5)
    stream, _, queue = make_stream(config)
    assert config.queue_limit == 1

    await stream._on_message({"channel": "trades", "data": [make_trade()]})

    assert queue.qsize() == 1
    assert queue.get_nowait() == OTHER
    assert stream.dropped == 1
