import itertools
import json

import pytest

from pipeline.aggregate import WINDOW_MS, aggregate, window_start
from pipeline.generator import trade_stream
from pipeline.schema import Trade, parse_trade


def T(i, sym="BTC", price=100.0, size=1.0, ts=1_000):
    return Trade(trade_id=str(i), symbol=sym, price=price, size=size, ts_ms=ts)


# --- schema / validation ---------------------------------------------------

def test_parse_roundtrip():
    t = T(1, price=12.5, size=0.25, ts=1700000000000)
    assert parse_trade(t.to_json()) == t


@pytest.mark.parametrize(
    "payload",
    [
        b"not json",
        b"{}",
        json.dumps({"trade_id": "1", "symbol": "A", "price": -1, "size": 1, "ts_ms": 1}),
        json.dumps({"trade_id": "1", "symbol": "A", "price": 1, "size": 0, "ts_ms": 1}),
        json.dumps({"trade_id": "1", "symbol": "", "price": 1, "size": 1, "ts_ms": 1}),
        json.dumps({"trade_id": "1", "symbol": "A", "price": "NaN", "size": 1, "ts_ms": 1}),
        json.dumps({"trade_id": "1", "symbol": "A", "price": 1, "size": 1, "ts_ms": "x"}),
    ],
)
def test_parse_rejects_bad_payloads(payload):
    assert parse_trade(payload) is None


def test_parse_normalises_symbol_case():
    assert parse_trade({"trade_id": "1", "symbol": "btcusdt", "price": 1, "size": 1, "ts_ms": 1}).symbol == "BTCUSDT"


# --- generator -------------------------------------------------------------

def test_generator_is_deterministic_with_seed():
    a = list(itertools.islice(trade_stream(seed=7, start_ms=1), 50))
    b = list(itertools.islice(trade_stream(seed=7, start_ms=1), 50))
    assert a == b


def test_generator_output_passes_validation_and_time_is_monotonic():
    trades = list(itertools.islice(trade_stream(seed=1, start_ms=1_000), 2_000))
    assert all(parse_trade(t.to_json()) == t for t in trades)
    assert [t.ts_ms for t in trades] == sorted(t.ts_ms for t in trades)
    assert len({t.trade_id for t in trades}) == len(trades)


# --- aggregation -----------------------------------------------------------

def test_ohlcv_vwap():
    trades = [
        T(1, price=10, size=1, ts=1_000),
        T(2, price=14, size=3, ts=2_000),
        T(3, price=8, size=2, ts=3_000),
        T(4, price=12, size=2, ts=4_000),
    ]
    (c,) = aggregate(trades)
    assert (c.open, c.high, c.low, c.close) == (10, 14, 8, 12)
    assert c.volume == 8 and c.trade_count == 4
    assert c.vwap == pytest.approx((10 + 42 + 16 + 24) / 8)


def test_open_close_use_event_time_not_arrival_order():
    late_first = [T(1, price=99, ts=50_000), T(2, price=1, ts=1_000)]
    (c,) = aggregate(late_first)
    assert (c.open, c.close) == (1, 99)


def test_window_boundaries_and_symbols_split():
    trades = [
        T(1, "A", ts=WINDOW_MS - 1),
        T(2, "A", ts=WINDOW_MS),
        T(3, "B", ts=WINDOW_MS),
    ]
    candles = aggregate(trades)
    assert [(c.symbol, c.window_start_ms) for c in candles] == [("A", 0), ("A", WINDOW_MS), ("B", WINDOW_MS)]


def test_duplicates_are_ignored():
    t = T(1, price=5, size=2)
    (c,) = aggregate([t, t, t])
    assert c.trade_count == 1 and c.volume == 2


def test_window_start():
    assert window_start(125_000) == 120_000


def test_aggregate_handles_large_stream():
    trades = list(itertools.islice(trade_stream(seed=3, start_ms=0, step_ms=5), 100_000))
    candles = aggregate(trades)
    assert sum(c.trade_count for c in candles) == 100_000
    assert all(c.low <= min(c.open, c.close) <= max(c.open, c.close) <= c.high for c in candles)
