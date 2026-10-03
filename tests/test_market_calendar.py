from datetime import datetime

import pytest

from core.market_status import EngineStatus, IST, MarketState, MarketStatus, EnvMarketStatus


@pytest.mark.parametrize("stamp,expected", [
    ("2026-10-02T11:00:00", MarketState.OVERNIGHT),
    ("2026-10-02T19:00:00", MarketState.OVERNIGHT),
    ("2026-10-03T11:00:00", MarketState.OVERNIGHT),
    ("2026-10-05T11:00:00", MarketState.LIVE_TRADING),
    ("2026-10-20T11:00:00", MarketState.OVERNIGHT),
    ("2026-10-20T17:02:00", MarketState.LIVE_TRADING),
    ("2026-11-08T19:00:00", MarketState.OVERNIGHT),
    ("2026-12-25T19:00:00", MarketState.OVERNIGHT),
    ("2027-01-04T11:00:00", MarketState.OVERNIGHT),
])
def test_verified_sessions_and_calendar_expiry(stamp, expected):
    market = MarketStatus()
    market._now = lambda: datetime.fromisoformat(stamp).replace(tzinfo=IST)
    market.set_engine_status(EngineStatus.TRADING)
    market.mark_rest_data_fresh()
    assert market.state == expected
    assert market.is_trading_allowed == (expected == MarketState.LIVE_TRADING)
    assert market.snapshot()["market_state"] == expected.value
    assert EnvMarketStatus(market).snapshot()["market_state"] == expected.value


def test_unavailable_calendar_stays_closed_and_is_visible():
    market = MarketStatus()
    market._now = lambda: datetime(2026, 10, 5, 11, tzinfo=IST)
    market._calendar = {}
    assert market.snapshot()["market_state"] == "overnight"
    assert market.snapshot()["calendar_reason"] == "calendar_update_required"
