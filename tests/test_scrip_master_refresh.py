import csv
import io
from datetime import date

from data.dhan.scrip_master import ScripMasterClient


_FIELDS = [
    "SEM_EXM_EXCH_ID", "SEM_SEGMENT", "SEM_SMST_SECURITY_ID",
    "SEM_INSTRUMENT_NAME", "SEM_EXPIRY_CODE", "SEM_TRADING_SYMBOL",
    "SEM_LOT_UNITS", "SEM_CUSTOM_SYMBOL", "SEM_EXPIRY_DATE",
    "SEM_STRIKE_PRICE", "SEM_OPTION_TYPE", "SEM_TICK_SIZE",
    "SEM_EXPIRY_FLAG", "SEM_EXCH_INSTRUMENT_TYPE", "SEM_SERIES",
    "SM_SYMBOL_NAME",
]


def _master(*contracts):
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=_FIELDS)
    writer.writeheader()
    for security_id, symbol, custom, expiry in contracts:
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


def test_rollover_refresh_bypasses_fresh_cache_and_is_rate_limited(tmp_path):
    calls = []
    payloads = [
        _master(("old", "GOLDM-09Oct2026-FUT", "GOLDM OCT FUT",
                 "2026-10-09 23:30:00")),
        _master(("old", "GOLDM-09Oct2026-FUT", "GOLDM OCT FUT",
                 "2026-10-09 23:30:00"),
                ("next", "GOLDM-06Nov2026-FUT", "GOLDM NOV FUT",
                 "2026-11-06 23:30:00")),
    ]

    def fetch():
        calls.append(1)
        return payloads[min(len(calls) - 1, len(payloads) - 1)]

    client = ScripMasterClient(
        url="", cache_path=str(tmp_path / "master.csv"),
        refresh_hours=24, fetch=fetch)
    current = client.find_symbol("GOLDM", "MCX:GOLDM202610")
    assert current is not None
    assert client.next_contract("GOLDM", current, as_of=date(2026, 10, 7)) is None

    assert client.refresh_if_due() is True
    current = client.find_symbol("GOLDM", "MCX:GOLDM202610")
    candidate = client.next_contract("GOLDM", current, as_of=date(2026, 10, 7))
    assert candidate is not None
    assert candidate.security_id == "next"
    assert len(calls) == 2

    # Repeated watchdog ticks inside the cooldown do not hammer Dhan's master.
    assert client.refresh_if_due() is True
    assert len(calls) == 2


def test_failed_forced_refresh_keeps_known_contracts_and_waits_for_retry(tmp_path):
    calls = []

    def fetch():
        calls.append(1)
        if len(calls) == 1:
            return _master(("old", "GOLDM-09Oct2026-FUT", "GOLDM OCT FUT",
                            "2026-10-09 23:30:00"))
        raise ConnectionError("temporary scrip-master outage")

    client = ScripMasterClient(
        url="", cache_path=str(tmp_path / "master.csv"),
        refresh_hours=24, fetch=fetch)
    assert client.find_symbol("GOLDM", "MCX:GOLDM202610") is not None

    # Known rows stay available for expiry calculation, but no fabricated
    # candidate is produced and a retry is rate-limited.
    assert client.refresh_if_due() is True
    assert client.find_symbol("GOLDM", "MCX:GOLDM202610") is not None
    assert client.refresh_if_due() is True
    assert len(calls) == 2
