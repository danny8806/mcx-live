from types import SimpleNamespace

import pytest

from dashboard.routes import strategies


@pytest.mark.parametrize(
    ("is_long", "expected_state", "expected_side"),
    [(True, "long_position", "LONG"),
     (False, "short_position", "SHORT")],
)
def test_reconcile_open_position_uses_position_book(
        monkeypatch, is_long, expected_state, expected_side):
    position = SimpleNamespace(
        is_open=True,
        is_long=is_long,
        snapshot=lambda: {"position_id": "p-1", "is_long": is_long},
    )
    env = SimpleNamespace(
        position_manager=SimpleNamespace(
            get_positions_by_strategy=lambda _strategy_id: [position]),
    )
    monkeypatch.setattr(strategies, "_env_for", lambda _target=None: env)

    result = strategies._reconcile_open_position(
        "silver_01", {"state": "flat", "position_side": None})

    assert result["state"] == expected_state
    assert result["position_side"] == expected_side
    assert result["position"]["position_id"] == "p-1"


def test_reconcile_open_position_leaves_flat_snapshot_unchanged(monkeypatch):
    env = SimpleNamespace(
        position_manager=SimpleNamespace(
            get_positions_by_strategy=lambda _strategy_id: []),
    )
    monkeypatch.setattr(strategies, "_env_for", lambda _target=None: env)
    source = {"state": "flat", "position_side": None}

    result = strategies._reconcile_open_position("gold_02", source)

    assert result == source
    assert result is not source
