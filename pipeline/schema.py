"""Trade event schema + validation shared by the producer, tests and reference aggregator."""
from __future__ import annotations

import json
import math
from dataclasses import asdict, dataclass
from typing import Any


@dataclass(frozen=True)
class Trade:
    trade_id: str
    symbol: str
    price: float
    size: float
    ts_ms: int  # event time, epoch milliseconds

    def to_json(self) -> bytes:
        return json.dumps(asdict(self), separators=(",", ":")).encode()


def parse_trade(raw: bytes | str | dict[str, Any]) -> Trade | None:
    """Return a Trade, or None if the payload is malformed or fails sanity checks.

    Mirrors the filter applied in the Spark job so bad data is dropped identically
    in both places.
    """
    try:
        d = json.loads(raw) if isinstance(raw, (bytes, str)) else raw
        t = Trade(
            trade_id=str(d["trade_id"]),
            symbol=str(d["symbol"]).upper(),
            price=float(d["price"]),
            size=float(d["size"]),
            ts_ms=int(d["ts_ms"]),
        )
    except (KeyError, TypeError, ValueError, json.JSONDecodeError):
        return None
    if not t.symbol or not t.trade_id:
        return None
    if not (math.isfinite(t.price) and math.isfinite(t.size)):
        return None
    if t.price <= 0 or t.size <= 0 or t.ts_ms <= 0:
        return None
    return t
