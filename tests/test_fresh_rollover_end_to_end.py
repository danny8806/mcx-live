"""Fresh, independent integration checks for the expiry rollover path.

These tests construct new Dhan-shaped REST responses and a new scrip-master
payload. They do not import fixtures or expected outcomes from older tests.
"""
import csv
import io
from datetime import datetime

import trading_engine as trading_engine_module
from core.rollover import RolloverService
from data.dhan.scrip_master import ScripMasterClient
from execution.live.dhan_transport import DhanRestTransport
from trading_engine import TradingEngine


MASTER_FIELDS = (
    "SEM_EXM_EXCH_ID", "SEM_SEGMENT", "SEM_SMST_SECURITY_ID",
    "SEM_INSTRUMENT_NAME", "SEM_EXPIRY_CODE", "SEM_TRADING_SYMBOL",
    "SEM_LOT_UNITS", "SEM_CUSTOM_SYMBOL", "SEM_EXPIRY_DATE",
    "SEM_STRIKE_PRICE", "SEM_OPTION_TYPE", "SEM_TICK_SIZE",
    "SEM_EXPIRY_FLAG", "SEM_EXCH_INSTRUMENT_TYPE", "SEM_SERIES",
    "SM_SYMBOL_NAME",
)


def _fresh_master():
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=MASTER_FIELDS)
    writer.writeheader()
    # Deliberately omit GOLDM October 2026, matching a retired series. The
    # candidate IDs and expiries come from the current public Dhan master.
    for security_id, symbol, custom, expiry in (
        ("571445", "GOLDM-05Nov2026-FUT", "GOLDM NOV FUT",
         "2026-11-05 23:30:00"),
        ("575011", "GOLDM-04Dec2026-FUT", "GOLDM DEC FUT",
         "2026-12-04 23:30:00"),
    ):
        writer.writerow({
            "SEM_EXM_EXCH_ID": "MCX", "SEM_SEGMENT": "M",
            "SEM_SMST_SECURITY_ID": security_id,
            "SEM_INSTRUMENT_NAME": "FUTCOM", "SEM_EXPIRY_CODE": "0",
            "SEM_TRADING_SYMBOL": symbol, "SEM_LOT_UNITS": "1",
            "SEM_CUSTOM_SYMBOL": custom, "SEM_EXPIRY_DATE": expiry,
            "SEM_STRIKE_PRICE": "0", "SEM_OPTION_TYPE": "XX",
            "SEM_TICK_SIZE": "1", "SEM_EXPIRY_FLAG": "M",
            "SEM_EXCH_INSTRUMENT_TYPE": "FUTCOM", "SEM_SERIES": "2",
            "SM_SYMBOL_NAME": "GOLDM",
        })
    return output.getvalue()


class _Clock(datetime):
    @classmethod
    def now(cls, tz=None):
        return cls(2026, 10, 9, 12, 0, tzinfo=tz)


class _DhanHTTP:
    def __init__(self, positions=None, orders=None):
        self.positions = positions or []
        self.orders = orders or []
        self.get_calls = []

    def _get(self, path):
        self.get_calls.append(path)
        if path == "/positions":
            return self.positions
        if path == "/orders":
            return self.orders
        raise AssertionError(f"unexpected broker endpoint: {path}")


def _engine(tmp_path, monkeypatch, *, positions=None, orders=None):
    monkeypatch.setattr(trading_engine_module, "datetime", _Clock)
    http = _DhanHTTP(positions=positions, orders=orders)
    broker = DhanRestTransport(
        client_id="test-client", http=http,
        instruments={"GOLDM": {
            "security_id": "569003", "exchange_segment": "MCX_COMM",
            "symbol": "MCX:GOLDM202610",
        }},
    )
    client = ScripMasterClient(
        url="", cache_path=str(tmp_path / "fresh-master.csv"),
        refresh_hours=24, fetch=_fresh_master)
    service = RolloverService(
        config={"enabled": True, "trigger_days_before_expiry": 2,
                "block_entries_on_window": True,
                "cancel_resting_on_window": True,
                "force_close_on_last_session": True,
                "calendar_path": str(tmp_path / "no-holidays.json")},
        state_path=str(tmp_path / "fresh-rollover.json"),
        scrip_master=client)
    env = type("LiveEnv", (), {
        "name": "live", "is_live": True, "broker": broker,
        "strategies": {},
    })()
    engine = TradingEngine.__new__(TradingEngine)
    engine.config = {"instruments": {"GOLDM": {
        "symbol": "MCX:GOLDM202610", "security_id": "569003",
        "exchange_segment": "MCX_COMM",
    }}}
    engine._rollover_service = service
    engine._rollover_blocked = {("live", "GOLDM")}
    engine._rollover_watchdogs = set()
    engine._rollover_applied = {}
    engine._env_for = lambda _name=None: env
    engine.publish_event = lambda *args, **kwargs: None
    return engine, http


def test_fresh_end_to_end_missing_expiry_rolls_only_after_broker_clear(
        tmp_path, monkeypatch):
    engine, http = _engine(tmp_path, monkeypatch)

    engine._rollover_tick("live")

    assert http.get_calls.count("/positions") == 1
    assert http.get_calls.count("/orders") >= 2
    assert engine._rollover_blocked == {("live", "GOLDM")}
    override = engine._rollover_service.overrides()["GOLDM"]
    assert override["active_symbol"] == "MCX:GOLDM202611"
    assert override["active_security_id"] == "571445"

    # A fresh engine boot applies the durable override before runtime creation.
    restarted = TradingEngine.__new__(TradingEngine)
    restarted.config = {"instruments": {"GOLDM": {
        "symbol": "MCX:GOLDM202610", "security_id": "569003",
    }}}
    restarted._rollover_service = RolloverService(
        config={"enabled": True},
        state_path=str(tmp_path / "fresh-rollover.json"),
        scrip_master=ScripMasterClient(url="", fetch=_fresh_master))
    restarted._rollover_applied = {}
    restarted.publish_event = lambda *args, **kwargs: None
    restarted._apply_rollover_startup()
    assert restarted.config["instruments"]["GOLDM"]["symbol"] == "MCX:GOLDM202611"
    assert restarted.config["instruments"]["GOLDM"]["security_id"] == "571445"


def test_fresh_end_to_end_never_switches_with_old_position_or_order(
        tmp_path, monkeypatch):
    scenarios = (
        ([{"securityId": "569003", "netQty": 1}], [], "position"),
        ([], [{"securityId": "569003", "orderStatus": "PENDING"}], "order"),
    )
    for positions, orders, label in scenarios:
        scenario_dir = tmp_path / label
        scenario_dir.mkdir()
        engine, _http = _engine(
            scenario_dir, monkeypatch, positions=positions, orders=orders)

        engine._rollover_tick("live")

        assert engine._rollover_service.overrides() == {}, label
        assert ("live", "GOLDM") in engine._rollover_blocked, label


def test_fresh_end_to_end_broker_lookup_failure_keeps_state_and_retries(
        tmp_path, monkeypatch):
    engine, http = _engine(tmp_path, monkeypatch)

    def unavailable(path):
        http.get_calls.append(path)
        raise ConnectionError("simulated Dhan outage")

    http._get = unavailable
    engine._rollover_tick("live")
    assert engine._rollover_service.overrides() == {}
    assert ("live", "GOLDM") in engine._rollover_blocked

    # Recover broker endpoints; the next independent watchdog invocation
    # must resolve and persist the next series without restarting the process.
    http._get = lambda path: []
    engine._rollover_tick("live")
    assert engine._rollover_service.overrides()["GOLDM"]["active_security_id"] == "571445"
