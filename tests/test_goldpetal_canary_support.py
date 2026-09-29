import json
from pathlib import Path

from data.dhan.scrip_master import parse_scrip_master


def test_goldpetal_future_is_resolved_by_the_rollover_scrip_parser():
    csv_text = """SEM_EXM_EXCH_ID,SEM_SEGMENT,SEM_SMST_SECURITY_ID,SEM_INSTRUMENT_NAME,SEM_EXPIRY_CODE,SEM_TRADING_SYMBOL,SEM_LOT_UNITS,SEM_CUSTOM_SYMBOL,SEM_EXPIRY_DATE,SEM_STRIKE_PRICE,SEM_OPTION_TYPE,SEM_TICK_SIZE,SEM_EXPIRY_FLAG,SEM_EXCH_INSTRUMENT_TYPE,SEM_SERIES,SM_SYMBOL_NAME
MCX,M,571306,FUTCOM,0,GOLDPETAL-30Oct2026-FUT,1.0,GOLDPETAL OCT FUT,2026-10-30 23:30:00,0.00000,XX,100.0000,M,FUTCOM,2,GOLDPETAL
"""

    contracts = parse_scrip_master(csv_text)

    assert len(contracts) == 1
    assert contracts[0].asset == "GOLDPETAL"
    assert contracts[0].security_id == "571306"
    assert contracts[0].config_symbol() == "MCX:GOLDPETAL202610"


def test_goldpetal_is_configured_at_the_confirmed_one_unit_canary_size():
    root = Path(__file__).resolve().parents[1]
    config = json.loads((root / "config" / "live_settings.json").read_text())
    petal = config["instruments"]["GOLDPETAL"]

    assert petal["security_id"] == "571306"
    assert petal["exchange_segment"] == "MCX_COMM"
    assert petal["instrument"] == "FUTCOM"
    assert petal["tick_size"] == 1.0
    assert petal["lot_size"] == 1
    assert config["live_test_order_cycle"]["enabled"] is False
    assert config["live_test_order_cycle"]["quantity"] == 1
