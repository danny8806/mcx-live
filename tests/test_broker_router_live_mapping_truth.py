from execution.broker_router import BrokerEventRouter


class _Persistence:
    def get_broker_order_mappings(self):
        return [
            {
                "broker_order_id": "LIVE-local-only",
                "order_id": "LIVE-local-only",
                "trade_id": "trade-local",
                "strategy_id": "gold_02",
                "instrument": "GOLDM",
                "execution_mode": "LIVE",
            },
            {
                "broker_order_id": "DHAN-123",
                "order_id": "LIVE-real-order",
                "trade_id": "trade-real",
                "strategy_id": "gold_02",
                "instrument": "GOLDM",
                "execution_mode": "LIVE",
            },
        ]


def test_restore_skips_legacy_local_id_alias_but_keeps_dhan_mapping():
    router = BrokerEventRouter(persistence=_Persistence())

    restored = router.restore()

    assert restored == 1
    assert router.resolve("LIVE-local-only") is None
    mapping = router.resolve("DHAN-123")
    assert mapping is not None
    assert mapping.order_id == "LIVE-real-order"
