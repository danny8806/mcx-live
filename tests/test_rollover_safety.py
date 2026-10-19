import json
from datetime import date, datetime
from types import SimpleNamespace

from core.rollover import RolloverService, resolve_window
from data.dhan.scrip_master import FutureContract
from application.live_position_flow import LivePositionFlowMixin
from execution.live.dhan_transport import DhanRestTransport
from strategies.types import OrderState
from trading_engine import TradingEngine


class _Master:
    def __init__(self, rows):
        self.rows = rows

    def futures(self, asset):
        return [row for row in self.rows if row.asset == asset]

    def find_symbol(self, asset, symbol):
        return next((row for row in self.futures(asset)
                     if row.config_symbol() == symbol), None)

    def next_contract(self, asset, current, as_of=None):
        return min((row for row in self.futures(asset)
                    if row.expiry_date > current.expiry_date
                    and row.expiry_date >= (as_of or date.today())),
                   key=lambda row: row.expiry, default=None)


def _future(security_id, day):
    expiry = datetime.fromisoformat(f"{day} 23:30:00")
    return FutureContract(
        asset="GOLDM", security_id=security_id,
        trading_symbol=f"GOLDM-{day}", expiry=expiry,
        lot_size=1.0, tick_size=1.0)


def _service(tmp_path, master, **config):
    return RolloverService(
        config={"enabled": True, "trigger_days_before_expiry": 2,
                "block_entries_on_window": True,
                "cancel_resting_on_window": True,
                "force_close_on_last_session": True, **config},
        state_path=str(tmp_path / "rollover.json"),
        scrip_master=master)


def test_rollover_window_starts_two_trading_sessions_before_last_session():
    assert resolve_window(date(2026, 10, 9), trigger_days=2) == (
        date(2026, 10, 7), date(2026, 10, 9))


def test_current_default_rollover_window_is_three_trading_sessions():
    service = RolloverService(
        config={"enabled": True}, state_path="unused-rollover-test.json",
        scrip_master=_Master([]))
    assert service.trigger_days == 3
    assert resolve_window(date(2026, 10, 9), trigger_days=3) == (
        date(2026, 10, 6), date(2026, 10, 9))


def test_rollover_uses_full_mcX_closures_but_not_partial_sessions(tmp_path):
    calendar_path = tmp_path / "mcx_calendar.json"
    calendar_path.write_text(json.dumps({
        "valid_through": "2026-12-31",
        "sessions": {
            "2026-11-04": {"closed": True, "reason": "exchange holiday"},
            "2026-11-03": {"open": "17:00", "reason": "evening session"},
        },
    }), encoding="utf-8")
    service = RolloverService(
        config={"enabled": True, "trigger_days_before_expiry": 2,
                "calendar_path": str(calendar_path),
                "holidays": ["2026-11-02"]},
        state_path=str(tmp_path / "rollover.json"), scrip_master=_Master([]))

    assert date(2026, 11, 4) in service.holidays
    assert date(2026, 11, 2) in service.holidays
    assert date(2026, 11, 3) not in service.holidays
    assert service.status()["calendar_valid_through"] == "2026-12-31"
    assert resolve_window(date(2026, 11, 5), trigger_days=2,
                          holidays=service.holidays) == (
        date(2026, 10, 30), date(2026, 11, 5))


def test_flat_switch_waits_for_verified_resting_order_clear(tmp_path):
    current = _future("old", "2026-10-09")
    next_ = _future("next", "2026-11-06")
    service = _service(tmp_path, _Master([current, next_]))

    actions = service.evaluate(
        "GOLDM", current.config_symbol(), current.security_id,
        date(2026, 10, 7), has_positions=False,
        resting_orders_clear=False)

    assert "block_entries" in actions
    assert "cancel_resting" in actions
    assert "persist_switch" not in actions
    assert service.overrides() == {}

    actions = service.evaluate(
        "GOLDM", current.config_symbol(), current.security_id,
        date(2026, 10, 7), has_positions=False,
        resting_orders_clear=True)

    assert "persist_switch" in actions
    assert service.overrides()["GOLDM"]["active_security_id"] == "next"


def test_broker_confirmation_is_only_required_inside_rollover_window(tmp_path):
    current = _future("old", "2026-10-09")
    service = _service(tmp_path, _Master([current]))

    assert not service.requires_broker_confirmation(
        "GOLDM", current.config_symbol(), current.security_id, date(2026, 10, 6))
    assert service.requires_broker_confirmation(
        "GOLDM", current.config_symbol(), current.security_id, date(2026, 10, 7))


def test_scrip_master_outage_blocks_entries_until_contract_is_verified(tmp_path):
    service = _service(tmp_path, _Master([]))

    actions = service.evaluate(
        "GOLDM", "MCX:GOLDM202610", "old", date(2026, 10, 7),
        has_positions=False)

    assert "unknown" in actions
    assert "block_entries" in actions
    assert "persist_switch" not in actions


def test_scrip_master_outage_keeps_gate_during_previously_known_window(tmp_path):
    service = _service(tmp_path, _Master([]))
    rec = service.record("GOLDM")
    rec.window_open = "2026-10-07"
    rec.last_session = "2026-10-09"

    actions = service.evaluate(
        "GOLDM", "MCX:GOLDM202610", "old", date(2026, 10, 8),
        has_positions=False)

    assert "unknown" in actions
    assert "block_entries" in actions
    assert "persist_switch" not in actions


def test_expired_or_removed_contract_blocks_new_entries(tmp_path):
    next_ = _future("next", "2026-11-06")
    service = _service(tmp_path, _Master([next_]))

    actions = service.evaluate(
        "GOLDM", "MCX:GOLDM202610", "569003", date(2026, 10, 9),
        has_positions=False)

    assert "unknown" in actions
    assert "block_entries" in actions
    assert "persist_switch" not in actions


def test_missing_expired_mapping_recovers_to_nearest_verified_series_when_flat(tmp_path):
    next_ = _future("next", "2026-11-06")
    service = _service(tmp_path, _Master([next_]))
    symbol = "MCX:GOLDM202610"

    assert service.requires_broker_confirmation(
        "GOLDM", symbol, "569003", date(2026, 10, 9))
    actions = service.evaluate(
        "GOLDM", symbol, "569003", date(2026, 10, 9),
        has_positions=False, resting_orders_clear=True)

    assert "persist_switch" in actions
    assert service.overrides()["GOLDM"]["active_symbol"] == next_.config_symbol()
    assert service.overrides()["GOLDM"]["active_security_id"] == "next"


def test_missing_expired_mapping_waits_for_old_position_and_orders_to_clear(tmp_path):
    next_ = _future("next", "2026-11-06")
    service = _service(tmp_path, _Master([next_]))
    symbol = "MCX:GOLDM202610"

    open_position = service.evaluate(
        "GOLDM", symbol, "569003", date(2026, 10, 9),
        has_positions=True, resting_orders_clear=True)
    working_order = service.evaluate(
        "GOLDM", symbol, "569003", date(2026, 10, 9),
        has_positions=False, resting_orders_clear=False)

    assert "block_entries" in open_position
    assert "persist_switch" not in open_position
    assert "persist_switch" not in working_order
    assert service.overrides() == {}


def test_configured_security_id_must_match_scrip_master(tmp_path):
    current = _future("master-id", "2026-11-05")
    service = _service(tmp_path, _Master([current]))

    actions = service.evaluate(
        "GOLDM", current.config_symbol(), "stale-id", date(2026, 10, 9),
        has_positions=False)

    assert "unknown" in actions
    assert "block_entries" in actions
    assert "persist_switch" not in actions


def test_final_session_force_close_is_reissued_until_position_is_flat(tmp_path):
    current = _future("old", "2026-10-09")
    next_ = _future("next", "2026-11-06")
    service = _service(tmp_path, _Master([current, next_]))

    first = service.evaluate(
        "GOLDM", current.config_symbol(), current.security_id,
        date(2026, 10, 9), has_positions=True,
        resting_orders_clear=True)
    second = service.evaluate(
        "GOLDM", current.config_symbol(), current.security_id,
        date(2026, 10, 9), has_positions=True,
        resting_orders_clear=True)

    assert first.count("force_close") == 1
    assert second.count("force_close") == 1


def test_rollover_requires_exact_dhan_contract_flat_confirmation():
    engine = TradingEngine.__new__(TradingEngine)
    alerts = []
    engine._rollover_service = SimpleNamespace(
        alert_once=lambda *args, **kwargs: alerts.append((args, kwargs)))
    env = SimpleNamespace(
        broker=SimpleNamespace(
            instrument_flat=lambda instrument, security_id: False,
            _own_net_positions=lambda: [],
        ),
        strategies={},
    )

    assert engine._rollover_metal_open(env, "GOLDM", "569003") is True
    assert len(alerts) == 1
    assert alerts[0][0][1] == "position_still_open"
    assert alerts[0][1]["security_id"] == "569003"


def test_rollover_position_check_retries_until_dhan_confirms_flat():
    engine = TradingEngine.__new__(TradingEngine)
    broker_state = {"flat": False}
    engine._rollover_service = SimpleNamespace(alert_once=lambda *args, **kwargs: None)
    env = SimpleNamespace(
        broker=SimpleNamespace(
            instrument_flat=lambda instrument, security_id: broker_state["flat"],
            _own_net_positions=lambda: [],
        ),
        strategies={},
    )

    # A watchdog retry while Dhan still sees the old position keeps rollover
    # blocked; a later retry can proceed once Dhan reports that exact SID flat.
    assert engine._rollover_metal_open(env, "GOLDM", "569003") is True
    broker_state["flat"] = True
    assert engine._rollover_metal_open(env, "GOLDM", "569003") is False


def test_rollover_position_api_error_or_missing_capability_fails_closed():
    engine = TradingEngine.__new__(TradingEngine)
    alerts = []
    engine._rollover_service = SimpleNamespace(
        alert_once=lambda *args, **kwargs: alerts.append((args, kwargs)))

    def broken_check(_instrument, _security_id):
        raise RuntimeError("positions unavailable")

    for broker in (SimpleNamespace(instrument_flat=broken_check), SimpleNamespace()):
        env = SimpleNamespace(broker=broker, strategies={})
        assert engine._rollover_metal_open(env, "GOLDM", "569003") is True

    assert len(alerts) == 2


def test_dhan_exact_flat_check_decodes_open_flat_and_unknown_responses():
    class Http:
        body = []
        error = None

        def _get(self, path):
            assert path == "/positions"
            if self.error:
                raise self.error
            return self.body

    http = Http()
    broker = DhanRestTransport(
        client_id="test", http=http,
        instruments={"GOLDM": {"security_id": "569003"}},
    )

    http.body = [{"securityId": "569003", "netQty": 1}]
    assert broker.instrument_flat("GOLDM", "569003") is False
    http.body = [{"securityId": "569003", "netQty": 0}]
    assert broker.instrument_flat("GOLDM", "569003") is True
    http.body = []
    assert broker.instrument_flat("GOLDM", "569003") is True
    http.body = [{"securityId": "569003"}]
    try:
        broker.instrument_flat("GOLDM", "569003")
    except ValueError:
        pass
    else:
        raise AssertionError("missing netQty must be treated as unknown")
    http.error = ConnectionError("Dhan unavailable")
    try:
        broker.instrument_flat("GOLDM", "569003")
    except RuntimeError as exc:
        assert "positions query failed" in str(exc)
    else:
        raise AssertionError("positions API failure must remain unknown")


def test_rollover_order_check_uses_exact_contract_broker_orderbook():
    class Http:
        body = []
        error = None

        def _get(self, path):
            assert path == "/orders"
            if self.error:
                raise self.error
            return self.body

    http = Http()
    broker = DhanRestTransport(
        client_id="test", http=http,
        instruments={"GOLDM": {"security_id": "569003"}},
    )
    http.body = [
        {"securityId": "other", "orderStatus": "PENDING"},
        {"securityId": "569003", "orderStatus": "PART_TRADED"},
    ]
    assert broker.instrument_orders_clear("GOLDM", "569003") is False
    http.body[1]["orderStatus"] = "CANCELLED"
    assert broker.instrument_orders_clear("GOLDM", "569003") is True
    # A manual order is present only in the broker book, not the transport's
    # in-memory system order map; it must still keep the old series unsettled.
    http.body = [{"securityId": "569003", "orderStatus": "PENDING"}]
    assert broker.instrument_orders_clear("GOLDM", "569003") is False
    http.body = [{"securityId": "569003"}]
    try:
        broker.instrument_orders_clear("GOLDM", "569003")
    except ValueError as exc:
        assert "missing security ID or status" in str(exc)
    else:
        raise AssertionError("malformed order row must be treated as unknown")
    http.error = ConnectionError("Dhan unavailable")
    try:
        broker.instrument_orders_clear("GOLDM", "569003")
    except RuntimeError as exc:
        assert "order book query failed" in str(exc)
    else:
        raise AssertionError("Dhan order-book failure must remain unknown")


def test_rollover_retries_after_scrip_master_recovers(tmp_path):
    master = _Master([])
    service = _service(tmp_path, master)
    symbol = "MCX:GOLDM202610"

    first = service.evaluate(
        "GOLDM", symbol, "old", date(2026, 10, 7), has_positions=False,
        resting_orders_clear=True)
    assert "block_entries" in first
    assert "persist_switch" not in first

    current = _future("old", "2026-10-09")
    next_ = _future("next", "2026-11-06")
    master.rows = [current, next_]
    recovered = service.evaluate(
        "GOLDM", symbol, "old", date(2026, 10, 7), has_positions=False,
        resting_orders_clear=True)

    assert "persist_switch" in recovered
    assert service.overrides()["GOLDM"]["active_security_id"] == "next"


def test_verified_expiry_survives_contract_removal_from_active_master(tmp_path):
    current = _future("old", "2026-10-09")
    next_ = _future("next", "2026-11-06")
    master = _Master([current, next_])
    state_path = tmp_path / "rollover.json"
    service = _service(tmp_path, master)
    service.state_path = state_path

    # Save authoritative identity/expiry before Dhan retires the current row.
    assert service.evaluate(
        "GOLDM", current.config_symbol(), current.security_id,
        date(2026, 10, 1), has_positions=False,
        resting_orders_clear=True) == []
    master.rows = [next_]

    assert service.requires_broker_confirmation(
        "GOLDM", current.config_symbol(), current.security_id,
        date(2026, 10, 7))
    actions = service.evaluate(
        "GOLDM", current.config_symbol(), current.security_id,
        date(2026, 10, 7), has_positions=False,
        resting_orders_clear=True)

    assert "persist_switch" in actions
    assert service.overrides()["GOLDM"]["active_security_id"] == "next"
    restarted = RolloverService(
        config={"enabled": True, "trigger_days_before_expiry": 2},
        state_path=str(state_path), scrip_master=_Master([next_]))
    assert restarted.overrides()["GOLDM"]["active_security_id"] == "next"


def test_missing_next_expiry_forces_rate_limited_master_refresh(tmp_path):
    current = _future("old", "2026-10-09")
    next_ = _future("next", "2026-11-06")

    class RefreshableMaster(_Master):
        refreshes = 0

        def refresh_if_due(self):
            self.refreshes += 1
            self.rows.append(next_)
            return True

    master = RefreshableMaster([current])
    service = _service(tmp_path, master)
    actions = service.evaluate(
        "GOLDM", current.config_symbol(), current.security_id,
        date(2026, 10, 7), has_positions=False,
        resting_orders_clear=True)

    assert master.refreshes == 1
    assert "persist_switch" in actions
    assert service.overrides()["GOLDM"]["active_security_id"] == "next"


def test_persisted_rollover_override_survives_service_restart(tmp_path):
    current = _future("old", "2026-10-09")
    next_ = _future("next", "2026-11-06")
    state_path = tmp_path / "rollover.json"
    service = _service(tmp_path, _Master([current, next_]))
    service.state_path = state_path
    actions = service.evaluate(
        "GOLDM", current.config_symbol(), current.security_id,
        date(2026, 10, 7), has_positions=False,
        resting_orders_clear=True)
    assert "persist_switch" in actions

    restored = RolloverService(
        config={"enabled": True, "trigger_days_before_expiry": 2},
        state_path=str(state_path), scrip_master=_Master([current, next_]))
    assert restored.overrides()["GOLDM"]["active_security_id"] == "next"


def _emergency_exit_harness(submission_states):
    class Execution:
        def __init__(self):
            self._orders = {}
            self.states = list(submission_states)
            self.created = 0

        def create_order(self, signal, **_kwargs):
            self.created += 1
            order = SimpleNamespace(
                order_id=f"EXIT-{self.created}",
                state=OrderState.CREATED,
                strategy_id="gold_02", instrument="GOLDM",
                parent_position_id=signal.parent_position_id,
                fill_ids=[], reason=None,
            )
            self._orders[order.order_id] = order
            return order

        def submit_order(self, order):
            state = self.states.pop(0) if self.states else "submitted"
            order.state = OrderState(state.lower())
            if state == "rejected":
                order.reason = "temporary rejection"

        def update_price(self, *_args):
            pass

        def get_fills(self, **_kwargs):
            return []

    position = SimpleNamespace(
        position_id="POS-1", trade_id="TRADE-1", strategy_id="gold_02",
        instrument="GOLDM", is_long=True, quantity=1, multiplier=10,
        current_mark=250000, average_entry=249000, position_generation=1,
        exit_started=False, exit_order_id=None,
    )
    execution = Execution()
    env = SimpleNamespace(
        name="live", is_live=True, execution_engine=execution,
        position_manager=SimpleNamespace(open_positions=[position]),
        runtimes=SimpleNamespace(require=lambda _sid: SimpleNamespace(
            lifecycle=SimpleNamespace(register_order=lambda *_args: None))),
        persistence=None,
        broker_router=SimpleNamespace(route_fill=lambda *_args, **_kwargs: None),
        strategies={}, mode="LIVE",
    )

    class Harness:
        emergency_exit_all = LivePositionFlowMixin.emergency_exit_all

        def _env_for(self, _name):
            return env

        def _persist_order(self, *_args):
            pass

        def _handle_fill(self, *_args, **_kwargs):
            pass

    return Harness(), execution


def test_final_session_force_close_deduplicates_working_exit():
    engine, execution = _emergency_exit_harness(["submitted"])

    first = engine.emergency_exit_all("live", instrument="GOLDM")
    second = engine.emergency_exit_all("live", instrument="GOLDM")

    assert len(first["closed"]) == 1
    assert first["closed"][0]["state"] == "submitted"
    assert second["closed"] == []
    assert second["errors"][0]["error"] == "position_exit_already_working"
    assert execution.created == 1


def test_final_session_force_close_retries_after_definitive_rejection():
    engine, execution = _emergency_exit_harness(["rejected", "submitted"])

    first = engine.emergency_exit_all("live", instrument="GOLDM")
    second = engine.emergency_exit_all("live", instrument="GOLDM")

    assert first["closed"] == []
    assert first["errors"][0]["error"] == "temporary rejection"
    assert len(second["closed"]) == 1
    assert second["closed"][0]["state"] == "submitted"
    assert execution.created == 2
