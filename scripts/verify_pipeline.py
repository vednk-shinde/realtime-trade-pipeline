"""Correctness check: recompute candles from the raw `trades` table with the pure-Python
reference implementation and compare against what Spark wrote to `candles_1m`.

    python scripts/verify_pipeline.py            # exits non-zero on mismatch
"""
import math
import os
import sys

import psycopg2

from pipeline.aggregate import aggregate
from pipeline.schema import Trade

conn = psycopg2.connect(
    host=os.getenv("PG_HOST", "localhost"), dbname=os.getenv("PG_DB", "trades"),
    user=os.getenv("PG_USER", "pipeline"), password=os.getenv("PG_PASSWORD", "pipeline"),
)
cur = conn.cursor()
# Only compare windows that are safely closed (older than watermark + margin).
cur.execute("SELECT trade_id, symbol, price, size, (extract(epoch FROM event_time)*1000)::bigint "
            "FROM trades WHERE event_time < now() - interval '2 minutes'")
ref = {(c.symbol, c.window_start_ms): c for c in aggregate(Trade(*r) for r in cur.fetchall())}

cur.execute("SELECT symbol, (extract(epoch FROM window_start)*1000)::bigint, open, high, low, close, volume, vwap, trade_count "
            "FROM candles_1m WHERE window_start < now() - interval '2 minutes'")
got = {(r[0], r[1]): r for r in cur.fetchall()}

bad = 0
for key, c in ref.items():
    g = got.get(key)
    if g is None:
        print("MISSING", key); bad += 1; continue
    pairs = [(c.open, g[2]), (c.high, g[3]), (c.low, g[4]), (c.close, g[5]), (c.volume, g[6]), (c.vwap, g[7])]
    if g[8] != c.trade_count or not all(math.isclose(a, b, rel_tol=1e-9, abs_tol=1e-9) for a, b in pairs):
        print("MISMATCH", key, c, g); bad += 1
print(f"checked {len(ref)} candles, {bad} problems")
sys.exit(1 if bad else 0)
