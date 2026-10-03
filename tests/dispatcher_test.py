"""Доставка событий: sync и async callback'и, порядок вызова, изоляция ошибок."""

import asyncio

from hl_twap_watcher._dispatcher import Dispatcher
from hl_twap_watcher._events import build_finished, build_slice
from hl_twap_watcher.types import TwapEvent

from .conftest import WALLET, make_created, make_record, make_trade


async def deliver(dispatcher: Dispatcher, *events: TwapEvent) -> None:
    """Прогоняет события через воркер и останавливает его."""
    await dispatcher.start()
    for event in events:
        dispatcher.emit(event)
    await dispatcher.stop()


async def test_general_then_specific_callbacks() -> None:
    calls: list[str] = []

    async def on_event(event: TwapEvent) -> None:
        calls.append(f"event:{event['type']}")

    def on_created(event: object) -> None:  # обычная функция тоже подходит
        calls.append("created")

    async def on_slice(event: object) -> None:
        await asyncio.sleep(0)
        calls.append("slice")

    dispatcher = Dispatcher(on_event=on_event, on_created=on_created, on_slice=on_slice)
    await deliver(
        dispatcher,
        make_created(),
        build_slice(make_trade(), wallet=WALLET, side="BUY", twap_ids=[100]),
        build_finished(make_record(100, "finished"), reason="completed", now=0),
    )

    assert calls == ["event:created", "created", "event:slice", "slice", "event:finished"]
    assert (dispatcher.created, dispatcher.slices, dispatcher.finished) == (1, 1, 1)


async def test_callback_error_does_not_stop_delivery() -> None:
    delivered: list[int] = []

    def on_created(event: dict) -> None:
        if event["twap_id"] == 1:
            raise RuntimeError("user bug")
        delivered.append(event["twap_id"])

    dispatcher = Dispatcher(on_created=on_created)
    await deliver(dispatcher, make_created(1), make_created(2))

    assert delivered == [2]
    assert dispatcher.errors == 1


async def test_stop_drains_queue() -> None:
    delivered: list[int] = []

    async def on_created(event: dict) -> None:
        await asyncio.sleep(0.01)
        delivered.append(event["twap_id"])

    dispatcher = Dispatcher(on_created=on_created)
    await deliver(dispatcher, *(make_created(i) for i in range(5)))

    assert delivered == [0, 1, 2, 3, 4]
