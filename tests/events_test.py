"""Сборка публичных событий и разбор истории TWAP."""

from hl_twap_watcher._events import (
    build_created,
    build_finished,
    build_slice,
    latest_records,
    parse_side,
)

from .conftest import WALLET, make_fill, make_record, make_state


def test_parse_side() -> None:
    assert parse_side("B") == "BUY"
    assert parse_side("A") == "SELL"


def test_build_created() -> None:
    state = make_state(user=WALLET.upper().replace("0X", "0x"), sz="2.0", timestamp=1_000_000)

    event = build_created(7, state, mid_price=100.0, tracked=False, now=1_100.0)

    assert event["type"] == "created"
    assert event["twap_id"] == 7
    assert event["wallet"] == WALLET
    assert event["side"] == "BUY"
    assert event["size"] == 2.0
    assert event["executed_size"] == 0.25
    assert event["notional_usd"] == 200.0
    assert event["age_sec"] == 100.0
    assert event["detected_at_ms"] == 1_100_000
    assert event["randomize"] is True
    assert event["tracked"] is False


def test_build_slice_from_fill() -> None:
    item = make_fill(5, time_ms=123, tid=9, side="A", px="10", sz="3")

    event = build_slice(item, wallet=WALLET)

    assert event["type"] == "slice"
    assert event["twap_id"] == 5
    assert event["side"] == "SELL"
    assert event["notional_usd"] == 30.0
    assert (event["time_ms"], event["trade_id"]) == (123, 9)


def test_build_finished() -> None:
    record = make_record(9, "terminated", executed_sz="0.5", executed_ntl="50.0", time_s=1_000)

    event = build_finished(record, reason="cancelled", now=2_000.0)

    assert event["type"] == "finished"
    assert event["twap_id"] == 9
    assert event["reason"] == "cancelled"
    assert event["average_price"] == 100.0
    assert event["finished_at_ms"] == 1_000_000
    assert event["detected_at_ms"] == 2_000_000


def test_build_finished_nothing_executed() -> None:
    record = make_record(9, "terminated", executed_sz="0.0", executed_ntl="0.0")

    assert build_finished(record, reason="cancelled", now=0)["average_price"] is None


def test_latest_records_prefers_newest_and_final() -> None:
    history = [
        make_record(1, "activated", time_s=100),
        make_record(1, "finished", time_s=200),
        # Завершение в ту же секунду, что и запуск: финальный статус важнее.
        make_record(2, "terminated", time_s=300),
        make_record(2, "activated", time_s=300),
    ]

    latest = latest_records(history)

    assert latest[1]["status"]["status"] == "finished"
    assert latest[2]["status"]["status"] == "terminated"


def test_latest_records_skip_legacy_without_twap_id() -> None:
    legacy = make_record(1, "finished")
    del legacy["twapId"]

    assert latest_records([legacy]) == {}
