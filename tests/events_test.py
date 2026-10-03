"""Сборка публичных событий из сырых данных биржи."""

from hl_twap_watcher._events import build_created, build_finished, build_slice, parse_side

from .conftest import WALLET, make_record, make_state, make_trade


def test_parse_side() -> None:
    assert parse_side("B") == "BUY"
    assert parse_side("A") == "SELL"


def test_build_created() -> None:
    state = make_state(user=WALLET.upper().replace("0X", "0x"), sz="2.0", timestamp=1_000_000)

    event = build_created(7, state, mid_price=100.0, now=1_100.0)

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


def test_build_created_without_price() -> None:
    event = build_created(7, make_state(), mid_price=None, now=1_100.0)

    assert event["mid_price"] is None
    assert event["notional_usd"] is None


def test_build_slice_single_candidate() -> None:
    event = build_slice(make_trade(px="10", sz="3"), wallet=WALLET, side="BUY", twap_ids=[5])

    assert event["twap_id"] == 5
    assert event["candidate_twap_ids"] == [5]
    assert event["notional_usd"] == 30.0
    assert event["trade_id"] == 1


def test_build_slice_ambiguous() -> None:
    event = build_slice(make_trade(), wallet=WALLET, side="BUY", twap_ids=[5, 6])

    assert event["twap_id"] is None
    assert event["candidate_twap_ids"] == [5, 6]


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
