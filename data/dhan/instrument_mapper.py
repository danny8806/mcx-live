"""Instrument mapping for Dhan to Nautilus domain."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional


@dataclass
class InstrumentMeta:
    """Canonical instrument representation."""
    symbol: str
    security_id: str
    exchange_segment: str
    instrument: str
    multiplier: float
    tick_size: float
    lot_size: int
    session_open: str
    session_close: str
    session_minutes: int


# Built-in instrument registry (populated from config)
INSTRUMENTS: dict[str, InstrumentMeta] = {}


def register_instrument(name: str, config: dict[str, Any]) -> InstrumentMeta:
    """Register an instrument from configuration."""
    meta = InstrumentMeta(
        symbol=config["symbol"],
        security_id=config["security_id"],
        exchange_segment=config["exchange_segment"],
        instrument=config["instrument"],
        multiplier=float(config.get("multiplier", 1.0)),
        tick_size=float(config.get("tick_size", 1.0)),
        lot_size=int(config.get("lot_size", 1)),
        session_open=config.get("session_open", "09:00"),
        session_close=config.get("session_close", "23:30"),
        session_minutes=int(config.get("session_minutes", 870)),
    )
    INSTRUMENTS[name] = meta
    INSTRUMENTS[meta.symbol] = meta
    INSTRUMENTS[meta.security_id] = meta
    return meta


def get_instrument(key: str) -> Optional[InstrumentMeta]:
    """Get instrument by name, symbol, or security ID."""
    return INSTRUMENTS.get(key)


def resolve_symbol(raw: str) -> tuple[str, dict[str, str]]:
    """Resolve a raw symbol string to (symbol, metadata dict).

    Handles:
    - Full symbol like "MCX:GOLDM202610"
    - Security ID like "569003"
    - Instrument name like "GOLDM"
    """
    raw = raw.strip()

    # Check if it's a registered instrument name
    meta = INSTRUMENTS.get(raw)
    if meta:
        return meta.symbol, {
            "sid": meta.security_id,
            "exch": meta.exchange_segment,
            "inst": meta.instrument,
        }

    # Check if it's a full symbol
    meta = INSTRUMENTS.get(raw)
    if meta:
        return raw, {
            "sid": meta.security_id,
            "exch": meta.exchange_segment,
            "inst": meta.instrument,
        }

    # Check if it's a security ID
    meta = INSTRUMENTS.get(raw)
    if meta:
        return meta.symbol, {
            "sid": raw,
            "exch": meta.exchange_segment,
            "inst": meta.instrument,
        }

    raise ValueError(f"Cannot resolve instrument: {raw}")
