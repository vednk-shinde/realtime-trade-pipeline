"""Deterministic-able synthetic market-data generator (geometric random walk)."""
from __future__ import annotations

import math
import random
import time
import uuid
from collections.abc import Iterator

from .schema import Trade

DEFAULT_START_PRICES = {"BTCUSDT": 65000.0, "ETHUSDT": 3200.0, "SOLUSDT": 150.0, "AAPL": 230.0}


def trade_stream(
    symbols: dict[str, float] | None = None,
    seed: int | None = None,
    start_ms: int | None = None,
    step_ms: int = 1,
    volatility: float = 0.0005,
) -> Iterator[Trade]:
    """Yield an endless stream of trades. Event time advances `step_ms` per trade,
    so the stream can be generated much faster than real time for backfills/tests."""
    rng = random.Random(seed)
    prices = dict(symbols or DEFAULT_START_PRICES)
    names = list(prices)
    ts = start_ms if start_ms is not None else int(time.time() * 1000)
    n = 0
    while True:
        sym = rng.choice(names)
        prices[sym] *= math.exp(rng.gauss(0, volatility))
        size = round(rng.lognormvariate(-2, 1), 6) or 0.000001
        tid = str(uuid.UUID(int=rng.getrandbits(128), version=4)) if seed is not None else str(uuid.uuid4())
        yield Trade(trade_id=tid, symbol=sym, price=round(prices[sym], 4), size=size, ts_ms=ts)
        n += 1
        ts += step_ms
