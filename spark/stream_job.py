"""Spark Structured Streaming job: Kafka -> (validate, dedupe, 1-min OHLCV/VWAP) -> PostgreSQL.

Two sinks per micro-batch:
  * raw trades  -> `trades` (JDBC append, deduplicated on trade_id by the DB)
  * candles     -> `candles_1m` (idempotent UPSERT, so late/updated windows converge)
"""
import os

import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

KAFKA = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("TOPIC", "trades")
PG_HOST = os.getenv("PG_HOST", "postgres")
PG_DB = os.getenv("PG_DB", "trades")
PG_USER = os.getenv("PG_USER", "pipeline")
PG_PASSWORD = os.getenv("PG_PASSWORD", "pipeline")
CHECKPOINT = os.getenv("CHECKPOINT_DIR", "/tmp/checkpoints")
MAX_OFFSETS = os.getenv("MAX_OFFSETS_PER_TRIGGER", "500000")

SCHEMA = StructType(
    [
        StructField("trade_id", StringType()),
        StructField("symbol", StringType()),
        StructField("price", DoubleType()),
        StructField("size", DoubleType()),
        StructField("ts_ms", LongType()),
    ]
)

JDBC_URL = f"jdbc:postgresql://{PG_HOST}:5432/{PG_DB}"
JDBC_PROPS = {"user": PG_USER, "password": PG_PASSWORD, "driver": "org.postgresql.Driver"}

UPSERT_CANDLES = """
INSERT INTO candles_1m (symbol, window_start, open, high, low, close, volume, vwap, trade_count)
VALUES %s
ON CONFLICT (symbol, window_start) DO UPDATE SET
  open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low, close = EXCLUDED.close,
  volume = EXCLUDED.volume, vwap = EXCLUDED.vwap, trade_count = EXCLUDED.trade_count,
  updated_at = now()
"""


def valid(df):
    """Same rules as pipeline.schema.parse_trade."""
    return df.where(
        F.col("trade_id").isNotNull()
        & (F.length("symbol") > 0)
        & (F.col("price") > 0)
        & (F.col("size") > 0)
        & (F.col("ts_ms") > 0)
    )


def write_raw(batch_df, batch_id):
    out = batch_df.select(
        "trade_id", "symbol", "price", "size", (F.col("ts_ms") / 1000).cast("timestamp").alias("event_time")
    )
    # trades_staging is TRUNCATEd by the DB function after each merge, see sql/init.sql
    out.write.jdbc(JDBC_URL, "trades_staging", mode="append", properties=JDBC_PROPS)
    with psycopg2.connect(host=PG_HOST, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT merge_trades_staging()")


def write_candles(batch_df, batch_id):
    rows = [
        (
            r["symbol"], r["window_start"], r["open"], r["high"], r["low"], r["close"],
            r["volume"], r["vwap"], r["trade_count"],
        )
        for r in batch_df.collect()  # aggregated rows only: tiny compared to raw trades
    ]
    if not rows:
        return
    with psycopg2.connect(host=PG_HOST, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD) as conn:
        with conn.cursor() as cur:
            execute_values(cur, UPSERT_CANDLES, rows)


def main():
    spark = (
        SparkSession.builder.appName("trade-pipeline")
        .config("spark.sql.shuffle.partitions", os.getenv("SHUFFLE_PARTITIONS", "8"))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", MAX_OFFSETS)
        .option("failOnDataLoss", "false")
        .load()
    )

    trades = valid(
        raw.select(F.from_json(F.col("value").cast("string"), SCHEMA).alias("t")).select("t.*")
    ).withColumn("event_time", (F.col("ts_ms") / 1000).cast("timestamp"))

    # Raw sink (before aggregation).
    raw_q = (
        trades.writeStream.foreachBatch(write_raw)
        .option("checkpointLocation", f"{CHECKPOINT}/raw")
        .trigger(processingTime="2 seconds")
        .start()
    )

    candles = (
        trades.withWatermark("event_time", "30 seconds")  # tolerate 30 s of out-of-order data
        .dropDuplicates(["trade_id", "event_time"])  # at-least-once -> effectively-once
        .groupBy(F.window("event_time", "1 minute").alias("w"), "symbol")
        .agg(
            F.min_by("price", "ts_ms").alias("open"),
            F.max("price").alias("high"),
            F.min("price").alias("low"),
            F.max_by("price", "ts_ms").alias("close"),
            F.sum("size").alias("volume"),
            (F.sum(F.col("price") * F.col("size")) / F.sum("size")).alias("vwap"),
            F.count("*").alias("trade_count"),
        )
        .select("symbol", F.col("w.start").alias("window_start"), "open", "high", "low", "close", "volume", "vwap", "trade_count")
    )

    candle_q = (
        candles.writeStream.outputMode("update")
        .foreachBatch(write_candles)
        .option("checkpointLocation", f"{CHECKPOINT}/candles")
        .trigger(processingTime="2 seconds")
        .start()
    )

    spark.streams.awaitAnyTermination()
    raw_q.stop()
    candle_q.stop()


if __name__ == "__main__":
    main()
