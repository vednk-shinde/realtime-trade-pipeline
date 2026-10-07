"""Pure-Python reference implementation of the 1-minute OHLCV/VWAP aggregation.

The Spark job computes the same thing at scale; this version is used in unit tests
and by `scripts/verify_pipeline.py` to check Spark's output row-for-row.
"""
from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass

from .schema import Trade

WINDOW_MS = 60_000


@dataclass(frozen=True)
class Candle:
    symbol: str
    window_start_ms: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    vwap: float
    trade_count: int


def window_start(ts_ms: int, window_ms: int = WINDOW_MS) -> int:
    return ts_ms - (ts_ms % window_ms)


def aggregate(trades: Iterable[Trade], window_ms: int = WINDOW_MS) -> list[Candle]:
    buckets: dict[tuple[str, int], list[Trade]] = defaultdict(list)
    seen: set[str] = set()
    for t in trades:
        if t.trade_id in seen:  # exactly-once semantics for at-least-once delivery
            continue
        seen.add(t.trade_id)
        buckets[(t.symbol, window_start(t.ts_ms, window_ms))].append(t)

    out = []
    for (sym, ws), ts in sorted(buckets.items(), key=lambda kv: (kv[0][1], kv[0][0])):
        ts.sort(key=lambda t: (t.ts_ms, t.trade_id))
        vol = sum(t.size for t in ts)
        out.append(
            Candle(
                symbol=sym,
                window_start_ms=ws,
                open=ts[0].price,
                high=max(t.price for t in ts),
                low=min(t.price for t in ts),
                close=ts[-1].price,
                volume=vol,
                vwap=sum(t.price * t.size for t in ts) / vol,
                trade_count=len(ts),
            )
        )
    return out
