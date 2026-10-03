# hyperliquid-twap-watcher

Async Python library that watches **TWAP orders on Hyperliquid** and delivers three kinds of events to your callbacks:

| Event | When |
|---|---|
| `created` | an active TWAP order on a perp is found |
| `slice` | a slice of a found TWAP is filled |
| `finished` | a found TWAP is completed, cancelled, stopped by `stopPx` or failed |

Hyperliquid has no global TWAP stream, so the library reconstructs it from public data. No API keys are needed.

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
| `mid_price` / `notional_usd` | `float \| None` | Mid price and `size × mid_price` |

The exchange reports **all active** orders of a wallet, so `created` also fires for orders placed hours or days ago — use `age_sec` to filter fresh ones. After a restart, still-active orders are reported again.

### `slice` — `TwapSliceEvent`

One event per trade: a slice that hits several price levels comes as several events with the same `time_ms`.

| Field | Type | Description |
|---|---|---|
| `twap_id` | `int \| None` | Owning order; `None` if ambiguous (see below) |
| `candidate_twap_ids` | `list[int]` | All tracked orders of the wallet with this coin and side |
| `wallet`, `coin`, `side` | | As above |
| `price`, `size`, `notional_usd` | `float` | Fill price, size, `price × size` |
| `time_ms`, `trade_id` | `int` | Trade time and exchange trade id |

Trades carry no TWAP id, so a slice is matched by wallet + coin + side. If a wallet runs two TWAPs with the same coin and side at once, `twap_id` is `None` and both ids are in `candidate_twap_ids`.

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

1. **Candidates.** One WebSocket listens to `trades` for every perp plus `allMids`. Engine-executed trades have a zero hash — that is how TWAP slices (and also liquidations/ADL) look. Both participants become candidates.
2. **Detection.** `twapStates` can only be subscribed per wallet, at most 14 per connection and ~30 per IP. A pool of connections rotates candidates through these slots (5 s each) and reports new orders.
3. **Slices.** Every zero-hash trade is matched against tracked orders.
4. **Completion.** Slices of a live TWAP arrive every ~30 s. After 90 s of silence the watcher requests the wallet's `twapHistory` and emits `finished` with the exact final status. Live-but-silent orders (e.g. waiting for a trigger) are re-checked with exponential backoff.

Consequences worth knowing:

- Detection is **sampling**, not a firehose: a TWAP is found once one of its slices shows up and its wallet gets a slot. Typical latency is seconds; under heavy load the candidate queue drops stale entries.
- `finished` arrives **≥ 90 s** after the real finish.
- Only perps of the main dex are reported (no spot, no builder-deployed dexes).

## Configuration

All technical knobs live in `WatcherConfig`; defaults fit Hyperliquid's per-IP limits.

```python
from hl_twap_watcher import TwapWatcher, WatcherConfig

watcher = TwapWatcher(
    on_event=handler,
    config=WatcherConfig(slice_silence_seconds=120, rest_requests_per_minute=20),
    logger=my_logger,  # loguru (default) or logging.Logger
)
```

| Option | Default | Meaning |
|---|---|---|
| `watchers_count` | `2` | `twapStates` connections (2 × 14 slots is the per-IP ceiling) |
| `watch_ttl` | `5.0` | Seconds a wallet holds a slot |
| `queue_wait_seconds` | `30.0` | Max candidate age in the queue |
| `slice_silence_seconds` | `90.0` | Silence before a completion check |
| `check_cooldown_seconds` / `check_max_backoff_seconds` | `60` / `900` | Re-check backoff for live orders |
| `finished_grace_seconds` | `3600` | Drop an order missing from history this long after its planned end |
| `rest_requests_per_minute` | `40` | Budget for `twapHistory` requests (weight 20 of 1200/min) |
| `markets_refresh_seconds` | `600` | Perp list refresh (new listings) |

`watcher.stats()` returns counters: trades seen, candidates queued/dropped, wallets checked, subscription rejects, tracked orders, events emitted, callback errors.

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
