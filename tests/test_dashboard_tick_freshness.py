from dashboard.routes.live_ops import _tick_age_seconds


def test_tick_freshness_uses_local_receive_time_not_future_dhan_ltt():
    now = 1_800_000_000.0
    tick = {
        "timestamp": now + 19_800,
        "receive_timestamp": now - 2.5,
    }

    assert _tick_age_seconds(tick, now) == 2.5


def test_tick_freshness_is_unknown_when_receipt_time_is_missing():
    assert _tick_age_seconds({"timestamp": 1_800_000_000}, 1_800_000_001) is None
