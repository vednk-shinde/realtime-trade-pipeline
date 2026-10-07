# Real-Time Trade Data Pipeline

End-to-end streaming pipeline: **Kafka → Spark Structured Streaming → PostgreSQL → live Streamlit dashboard.**
It ingests a fast-moving trade feed (synthetic by default, or the live public Binance trade stream),
validates and de-duplicates it, builds 1-minute OHLCV + VWAP candles with event-time windows and a watermark,
and serves them to a dashboard that refreshes every 2 seconds.

```
 producer ──► Kafka (6 partitions, keyed by symbol) ──► Spark Structured Streaming ──┬─► trades      (raw, PK trade_id)
 simulated | Binance WS                                  validate · dedupe · window  └─► candles_1m  (idempotent UPSERT)
                                                                                              │
                                                                       Streamlit dashboard ◄──┘  (fragment refresh, 2 s)
```

## Design decisions

| Topic | Decision |
|---|---|
| Delivery semantics | Idempotent, `acks=all` producer; Spark `dropDuplicates(trade_id)` inside the watermark; DB-level `PRIMARY KEY`/`ON CONFLICT` → **effectively-once** end to end. |
| Late data | Event-time windows with a 30 s watermark; candles are UPSERTed so a window updated by late data converges to the right value. |
| Ordering | Kafka key = symbol → per-symbol order within a partition; open/close use `min_by/max_by(event time)` so they don't depend on arrival order. |
| Bad data | Malformed/invalid events are dropped by the same rules in Python (`pipeline/schema.py`) and Spark (`valid()`). |
| Write path | Raw trades: JDBC append into an UNLOGGED staging table, merged with `ON CONFLICT DO NOTHING`. Candles (tiny): collected on the driver and bulk-UPSERTed via `execute_values`. |
| Query performance | `(symbol, event_time DESC)` B-tree for the dashboard's access pattern; BRIN on `event_time` for cheap time-range scans; `(symbol, window_start)` PK on candles. Pre-aggregated candles mean the dashboard never scans raw trades. |
| Correctness | `scripts/verify_pipeline.py` recomputes candles from raw trades with a pure-Python reference and diffs them against Spark's output. |

## Run it

```bash
docker compose up -d --build         # Kafka(KRaft) + Postgres + producer + Spark + dashboard
open http://localhost:8501           # dashboard

SOURCE=binance docker compose up -d producer   # switch to the live Binance trade stream
RATE=20000 docker compose up -d producer       # change synthetic load (events/s)

python scripts/verify_pipeline.py    # Spark output vs. reference implementation (needs psycopg2-binary)
```

## Tests

```bash
pip install -r requirements-dev.txt && pytest -q
```

17 unit tests cover validation, deterministic generation, OHLCV/VWAP maths, window boundaries, out-of-order events,
duplicate suppression and a 100k-event aggregation. The full-stack run is a manual GitHub Actions job (`e2e`, *Run workflow*).

## Status and measured numbers

* The Python logic is unit-tested (above). The Docker stack was **not run in the authoring environment** (no Docker daemon available),
  so Spark/Kafka throughput figures are intentionally **not claimed here**. After running it yourself, record
  e.g. sustained events/s, p95 `lag_seconds` from the `pipeline_lag` view, and dashboard query times in this section.
* A defensible way to quantify the "query time" improvement: benchmark `EXPLAIN ANALYZE` of the dashboard query against `trades`
  with and without the `(symbol, event_time DESC)` index, and against `candles_1m`.

## Layout

```
pipeline/   schema, generator, reference aggregator (pure Python, tested)
producer/   Kafka producer (simulated | Binance)
spark/      Structured Streaming job
sql/        schema, indexes, merge function, lag view
dashboard/  Streamlit app
scripts/    end-to-end verification
docker/     images; docker-compose.yml at root
```

## Next steps

Schema Registry + Avro, Kafka partitions scaled with a Spark cluster instead of `local[*]`, partitioned `trades` table with retention,
Prometheus/Grafana for lag alerts, Snowflake sink.

MIT licensed.
