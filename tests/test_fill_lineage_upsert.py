from persistence.manager import PersistenceManager


def test_fill_upsert_enriches_broker_lineage_without_losing_first_write(tmp_path):
    persistence = PersistenceManager(
        state_path=str(tmp_path / "state.json"),
        db_path=str(tmp_path / "live.sqlite"), execution_mode="LIVE")
    try:
        persistence.save_signal({
            "signal_id": "S-1", "strategy_id": "silver_01",
            "instrument": "SILVERM", "side": "LONG", "signal_type": "LONG",
            "signal_timestamp": 100,
        })
        persistence.save_trade({
            "trade_id": "T-1", "strategy_id": "silver_01",
            "instrument": "SILVERM", "side": "LONG",
            "entry_signal_id": "S-1", "status": "open",
        })
        persistence.save_order({
            "order_id": "O-1", "trade_id": "T-1", "strategy_id": "silver_01",
            "instrument": "SILVERM", "side": "BUY", "quantity": 1,
            "state": "filled", "filled_quantity": 1,
            "broker_order_id": "D-1",
        })
        persistence.save_fill({
            "fill_id": "F-1", "order_id": "O-1", "trade_id": "T-1",
            "strategy_id": "silver_01", "instrument": "SILVERM",
            "side": "BUY", "quantity": 1, "price": 100,
        })
        # A later poller enrichment must repair missing broker/lifecycle fields
        # on the same fill identity without replacing the original fill data.
        persistence.save_fill({
            "fill_id": "F-1", "order_id": "O-1", "trade_id": "T-1",
            "entry_signal_id": "S-1", "strategy_id": "silver_01",
            "instrument": "SILVERM", "side": "BUY", "quantity": 1,
            "price": 100, "broker_fill_id": "BF-1",
            "broker_order_id": "D-1", "broker_trade_id": "BT-1",
            "cumulative_filled_quantity": 1, "lifecycle_id": "T-1",
            "position_id": "P-1", "position_generation": 2,
        })

        [fill] = persistence.get_fills("F-1")
        assert fill["broker_fill_id"] == "BF-1"
        assert fill["broker_order_id"] == "D-1"
        assert fill["broker_trade_id"] == "BT-1"
        assert fill["entry_signal_id"] == "S-1"
        assert fill["cumulative_filled_quantity"] == 1
        assert fill["position_id"] == "P-1"
        assert fill["position_generation"] == 2
        assert fill["price"] == 100
    finally:
        persistence.close()
