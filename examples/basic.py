"""Пример: печатает найденные TWAP-ордера, их слайсы и завершения.

Запуск: `uv run python examples/basic.py [секунды]`. Без аргумента работает до Ctrl+C.
"""

import asyncio
import sys

from loguru import logger

from hl_twap_watcher import (
    TwapCreatedEvent,
    TwapEvent,
    TwapFinishedEvent,
    TwapSliceEvent,
    TwapWatcher,
)


async def on_created(event: TwapCreatedEvent) -> None:
    """Печатает найденный ордер."""
    logger.info(
        f"CREATED  #{event['twap_id']} {event['coin']:<8} {event['side']:<4} "
        f"${event['notional_usd']:>13,.0f} {event['minutes']:>5} min  age {event['age_sec']:>8.0f}s  "
        f"tracked={event['tracked']}  {event['wallet']}"
    )


def on_slice(event: TwapSliceEvent) -> None:
    """Печатает слайс. Callback может быть и обычной функцией."""
    logger.info(
        f"SLICE    #{event['twap_id']} {event['coin']:<8} {event['side']:<4} "
        f"{event['size']} @ {event['price']}  (${event['notional_usd']:,.0f})"
    )


async def on_finished(event: TwapFinishedEvent) -> None:
    """Печатает завершенный ордер и задержку, с которой о нем узнали."""
    # Время завершения биржа отдает с точностью до секунды.
    delay = (event["detected_at_ms"] - event["finished_at_ms"]) / 1000
    logger.info(
        f"FINISHED #{event['twap_id']} {event['coin']:<8} {event['reason']:<9} "
        f"executed {event['executed_size']}/{event['size']} avg {event['average_price']}  "
        f"delay ~{delay:.1f}s"
    )


async def on_event(event: TwapEvent) -> None:
    """Общий callback: тип события различается по полю `type`."""
    if event["type"] == "finished":
        logger.debug(f"Finished event for wallet {event['wallet']}")


async def main() -> None:
    """Запускает наблюдатель и раз в минуту печатает счетчики."""
    duration = float(sys.argv[1]) if len(sys.argv) > 1 else None

    async with TwapWatcher(
        on_event=on_event,
        on_created=on_created,
        on_slice=on_slice,
        on_finished=on_finished,
    ) as watcher:
        loop = asyncio.get_running_loop()
        started = loop.time()

        while duration is None or loop.time() - started < duration:
            await asyncio.sleep(min(60, duration or 60))
            logger.info(f"STATS {watcher.stats()}")


if __name__ == "__main__":
    logger.remove()
    logger.add(sys.stderr, level="INFO")
    asyncio.run(main())
