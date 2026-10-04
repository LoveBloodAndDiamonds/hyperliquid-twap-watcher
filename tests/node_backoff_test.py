"""Уход с занятой ноды: когда переподключаться и с какими паузами."""

from hl_twap_watcher._node_backoff import NodeBackoff


def test_free_node_never_reconnects() -> None:
    assert not NodeBackoff().should_reconnect(now=1000)


def test_first_reconnect_is_immediate_then_pauses_grow() -> None:
    backoff = NodeBackoff(crowded=True)
    now = 1000.0

    delays = []
    for _ in range(6):
        assert backoff.should_reconnect(now)
        backoff.reconnecting(now)
        assert not backoff.crowded  # новое соединение начинает с чистого листа

        # Новая нода тоже занята: ждем, пока пауза позволит следующую попытку.
        backoff.crowded = True
        waited = 0
        while not backoff.should_reconnect(now + waited):
            waited += 1
        delays.append(waited)
        now += waited

    assert delays == [5, 10, 20, 40, 60, 60]


def test_streak_resets_after_stable_period() -> None:
    backoff = NodeBackoff(streak=3, last_reconnect_at=1000)

    backoff.should_reconnect(now=1100)
    assert backoff.streak == 3

    backoff.should_reconnect(now=1121)
    assert backoff.streak == 0
