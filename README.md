# hyperliquid-twap-watcher

Async Python library that watches **large TWAP orders on Hyperliquid** and delivers three kinds of events to your callbacks:

| Event | When |
|---|---|
| `created` | an active perp TWAP of at least `min_notional_usd` ($100k by default) is found |
| `slice` | a slice of a tracked TWAP is filled — within ~0.5 s, with the exact TWAP id |
| `finished` | a tracked TWAP is completed, cancelled, stopped by `stopPx` or failed — within ~1 s |

Hyperliquid has no global TWAP stream, so the library finds TWAPs from public market data and then follows each one through per-wallet WebSocket subscriptions. Only the official API is used, no API keys are needed, and REST is called only to load the perp list.

## Installation

```bash
# From PyPI
pip install hyperliquid-twap-watcher

# Straight from GitHub
pip install "git+https://github.com/LoveBloodAndDiamonds/hyperliquid-twap-watcher.git@main"
uv add "git+https://github.com/LoveBloodAndDiamonds/hyperliquid-twap-watcher.git@main"
```

Requires Python 3.12+.

## Quick start

```python
import asyncio

from hl_twap_watcher import TwapCreatedEvent, TwapFinishedEvent, TwapSliceEvent, TwapWatcher


async def on_created(event: TwapCreatedEvent) -> None:
    print("NEW", event["coin"], event["side"], event["notional_usd"], event["wallet"])


def on_slice(event: TwapSliceEvent) -> None:  # plain functions work too
    print("SLICE", event["twap_id"], event["size"], "@", event["price"])


async def on_finished(event: TwapFinishedEvent) -> None:
    print("DONE", event["twap_id"], event["reason"], event["average_price"])


async def main() -> None:
    async with TwapWatcher(
        on_created=on_created, on_slice=on_slice, on_finished=on_finished
    ) as watcher:
        while True:
            await asyncio.sleep(60)
            print(watcher.stats())


asyncio.run(main())
```

A runnable version lives in [examples/basic.py](examples/basic.py): `uv run python examples/basic.py 300`.

### One callback for everything

Events are `TypedDict`s with a literal `type` field, so a single handler can branch on it and type checkers narrow the type:

```python
from hl_twap_watcher import TwapEvent, TwapWatcher


async def on_event(event: TwapEvent) -> None:
    match event["type"]:
        case "created":
            ...
        case "slice":
            ...
        case "finished":
            ...


watcher = TwapWatcher(on_event=on_event)
```

`on_event` and the specific callbacks can be combined: `on_event` is called first. Callbacks may be sync functions or coroutines. An exception in a callback is logged and does not stop the watcher.

## Events

### `created` — `TwapCreatedEvent`

| Field | Type | Description |
|---|---|---|
| `twap_id` | `int` | Order id (unique per wallet) |
| `wallet` | `str` | Owner address, lowercase |
| `coin` | `str` | Perp ticker: `BTC`, `kPEPE` |
| `side` | `"BUY" \| "SELL"` | Order side |
| `size` | `float` | Full order size in coins |
| `executed_size` / `executed_notional` | `float` | Already executed at detection time |
| `minutes` | `int` | Planned duration |
| `reduce_only`, `randomize` | `bool` | Order flags |
| `created_at_ms` / `detected_at_ms` | `int` | Exchange creation time / detection time |
| `age_sec` | `float` | Order age at detection |
| `mid_price` / `notional_usd` | `float` | Mid price and `size × mid_price` |
| `tracked` | `bool` | `False` if all tracking slots were busy: no `slice`/`finished` will follow |

The exchange reports **all active** orders of a wallet, so `created` also fires for orders placed hours or days ago — use `age_sec` to filter fresh ones. After a restart, still-active orders are reported again.

### `slice` — `TwapSliceEvent`

One event per trade: a slice that hits several price levels comes as several events with the same `time_ms`.

| Field | Type | Description |
|---|---|---|
| `twap_id` | `int` | Owning order |
| `wallet`, `coin`, `side` | | As above |
| `price`, `size`, `notional_usd` | `float` | Fill price, size, `price × size` |
| `time_ms`, `trade_id` | `int` | Trade time and exchange trade id |

### `finished` — `TwapFinishedEvent`

| Field | Type | Description |
|---|---|---|
| `reason` | `"completed" \| "cancelled" \| "stopped" \| "error"` | Exchange status `finished` / `terminated` / `stopped` / `error` |
| `size` | `float` | Planned size |
| `executed_size` / `executed_notional` | `float` | Final executed amount |
| `average_price` | `float \| None` | `executed_notional / executed_size` |
| `finished_at_ms` | `int` | Finish time reported by the exchange (second precision) |
| `detected_at_ms` | `int` | When the watcher learned about it |

plus `twap_id`, `wallet`, `coin`, `side`, `minutes`, `created_at_ms`.

## How it works

```
trades + allMids (all perps)  →  wallets from engine-executed (zero-hash) trades
            ↓
DISCOVERY   1 connection × 14 slots, twapStates for 2 s per wallet
            ↓  TWAP ≥ min_notional_usd → created
TRACKING    2 connections × 14 wallets: userTwapHistory + userTwapSliceFills
            ├─ slice fills with twapId   → slice
            ├─ final status in history   → finished (slot freed when the wallet has no TWAPs left)
            └─ new "activated" TWAP ≥ N  → created
```

1. **Candidates.** Engine-executed trades have a zero hash — that is how TWAP slices (and also liquidations/ADL) look. Both participants become candidates. Wallets that are already tracked are skipped: their new TWAPs arrive through their history subscription.
2. **Discovery.** `twapStates` can only be subscribed per wallet. A connection rotates candidates through its 14 slots, 2 s each (the exchange answers in ~0.5 s).
3. **Tracking.** A wallet with a large TWAP keeps two subscriptions until all of its tracked TWAPs finish. On every (re)connect the exchange sends a snapshot, so fills and statuses missed during a reconnect are recovered.

### Exchange limits

The exchange allows **15 tracked users per backend node per IP**, and there are about 3 nodes behind the load balancer (~44 wallets per IP). All subscriptions to one wallet count as one user. Connections that land on the same node share its 15 slots; a connection that gets rejected while it is mostly empty reconnects (at most once a minute) to land on another node.

Defaults use one connection per node: 14 discovery slots + 28 tracking wallets. Live observations show ~22 wallets with ≥ $100k TWAPs at a time. If tracking is full, `created` still arrives with `tracked=False` and a warning is logged.

## Configuration

All technical knobs live in `WatcherConfig`:

```python
from hl_twap_watcher import TwapWatcher, WatcherConfig

watcher = TwapWatcher(
    on_event=handler,
    config=WatcherConfig(min_notional_usd=250_000),
    logger=my_logger,  # loguru (default) or logging.Logger
)
```

| Option | Default | Meaning |
|---|---|---|
| `min_notional_usd` | `100_000` | Minimal `size × mid` of a reported TWAP |
| `discovery_connections` | `1` | `twapStates` rotation connections (14 slots each) |
| `tracking_connections` | `2` | Tracking connections (14 wallets each) |
| `watch_ttl` | `2.0` | Seconds a candidate holds a discovery slot |
| `queue_wait_seconds` | `30.0` | Max candidate age in the queue |
| `wallet_recheck_seconds` | `20.0` | Min pause before the same wallet is checked again |
| `markets_refresh_seconds` | `600` | Perp list refresh (new listings) |

`watcher.stats()` returns counters: trades seen, candidates queued/dropped, wallets checked, subscription rejects, node reconnects, tracked TWAPs and wallets, tracking capacity, TWAPs skipped because tracking was full, events emitted, callback errors.

## Development

```bash
make install    # uv sync
make format     # ruff fix + format
make check      # ruff, basedpyright, pytest — same as CI
make build      # wheel + sdist into dist/
```

Work happens in `dev`. CI runs lint, type check and tests on every push to `main`/`dev` and on pull requests.

### Release

1. Bump `version` in `pyproject.toml` on `dev`.
2. Open a pull request `dev → main`. The **Version check** job fails if the version is not greater than the one on PyPI.
3. Merge it. **Publish to PyPI** builds and uploads the package via trusted publishing.

## License

BSD 3-Clause, see [LICENSE](LICENSE).
