"""Детект завершения: разбор истории ордеров и расписание повторных проверок."""

import time
from typing import Any

from hl_twap_watcher._liveness import TwapLiveness
from hl_twap_watcher._registry import TwapRegistry
from hl_twap_watcher.config import WatcherConfig
from hl_twap_watcher.types import TwapFinishedEvent

from .conftest import WALLET, make_created, make_record


class FakeClient:
    """Подменяет InfoClient: отдает заранее заданную историю или ошибку."""

    def __init__(self, history: list[dict[str, Any]] | Exception) -> None:
        self.history = history
        self.calls: list[str] = []

    async def twap_history(self, user: str) -> list[dict[str, Any]]:
        self.calls.append(user)
        if isinstance(self.history, Exception):
            raise self.history
        return self.history


def make_liveness(
    history: list[dict[str, Any]] | Exception, config: WatcherConfig | None = None
) -> tuple[TwapLiveness, TwapRegistry, list[TwapFinishedEvent]]:
    """Собирает детект с фейковым клиентом и списком пойманных событий."""
    config = config or WatcherConfig()
    registry = TwapRegistry(config)
    finished: list[TwapFinishedEvent] = []
    liveness = TwapLiveness(config, FakeClient(history), registry, finished.append)  # type: ignore[arg-type]
    return liveness, registry, finished


def silent_add(registry: TwapRegistry, twap_id: int, **kwargs: Any) -> None:
    """Ставит ордер под наблюдение так, будто слайсов не было уже 10 минут."""
    registry.add(make_created(twap_id, **kwargs), now=time.time() - 600)


async def test_finished_statuses_map_to_reasons() -> None:
    history = [
        make_record(1, "finished"),
        make_record(2, "terminated"),
        make_record(3, "stopped"),
        make_record(4, "error"),
    ]
    liveness, registry, finished = make_liveness(history)
    for twap_id in (1, 2, 3, 4):
        silent_add(registry, twap_id)

    await liveness.check_wallet(WALLET)

    reasons = {event["twap_id"]: event["reason"] for event in finished}
    assert reasons == {1: "completed", 2: "cancelled", 3: "stopped", 4: "error"}
    assert len(registry) == 0
    assert liveness.checks == 1


async def test_latest_record_wins_over_activation() -> None:
    history = [
        make_record(1, "activated", time_s=100),
        make_record(1, "finished", time_s=200),
    ]
    liveness, registry, finished = make_liveness(history)
    silent_add(registry, 1)

    await liveness.check_wallet(WALLET)

    assert [event["reason"] for event in finished] == ["completed"]


async def test_active_twap_is_postponed() -> None:
    liveness, registry, finished = make_liveness([make_record(1, "activated")])
    silent_add(registry, 1)

    await liveness.check_wallet(WALLET)

    twap = registry.for_wallet(WALLET)[0]
    assert finished == []
    assert twap.failed_checks == 1
    assert twap.next_check_at > time.time()


async def test_finished_twap_closed_even_if_not_due() -> None:
    """История пришла по кошельку — закрывается и ордер, который не был поводом."""
    liveness, registry, finished = make_liveness([make_record(2, "terminated")])
    registry.add(make_created(2), now=time.time())  # слайс был только что

    await liveness.check_wallet(WALLET)

    assert [event["twap_id"] for event in finished] == [2]


async def test_records_without_twap_id_are_skipped() -> None:
    legacy = make_record(1, "finished")
    del legacy["twapId"]
    liveness, registry, finished = make_liveness([legacy, make_record(2, "finished")])
    silent_add(registry, 2)

    await liveness.check_wallet(WALLET)

    assert [event["twap_id"] for event in finished] == [2]


async def test_request_error_postpones() -> None:
    liveness, registry, finished = make_liveness(ConnectionError("boom"))
    silent_add(registry, 1)

    await liveness.check_wallet(WALLET)

    assert finished == []
    assert registry.for_wallet(WALLET)[0].failed_checks == 1
    assert liveness.checks == 0


async def test_unknown_twap_dropped_after_grace() -> None:
    config = WatcherConfig(finished_grace_seconds=60)
    liveness, registry, finished = make_liveness([], config)
    # Ордер на минуту, созданный час назад: расчетный конец давно прошел.
    silent_add(registry, 1, minutes=1, timestamp=int((time.time() - 3600) * 1000))

    await liveness.check_wallet(WALLET)

    assert finished == []
    assert len(registry) == 0
