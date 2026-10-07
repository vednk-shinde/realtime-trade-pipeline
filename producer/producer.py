"""Publishes trade events to Kafka.

SOURCE=simulated (default): synthetic random-walk ticks at RATE events/second.
SOURCE=binance:             live public trade stream (no API key needed).
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import sys
import time

from confluent_kafka import Producer

from pipeline.generator import trade_stream
from pipeline.schema import Trade

log = logging.getLogger("producer")
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("TOPIC", "trades")
RATE = int(os.getenv("RATE", "5000"))  # events/second (simulated)
SOURCE = os.getenv("SOURCE", "simulated")
STREAMS = os.getenv("BINANCE_STREAMS", "btcusdt@trade/ethusdt@trade/solusdt@trade")

stopping = False


def make_producer() -> Producer:
    return Producer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "enable.idempotence": True,  # no duplicates from producer retries
            "acks": "all",
            "linger.ms": 20,  # micro-batching for throughput
            "compression.type": "lz4",
            "batch.num.messages": 10000,
        }
    )


def send(p: Producer, t: Trade) -> None:
    # Keyed by symbol => per-symbol ordering within a partition.
    while True:
        try:
            p.produce(TOPIC, key=t.symbol, value=t.to_json())
            return
        except BufferError:
            p.poll(0.1)


def run_simulated(p: Producer) -> None:
    sent, t0 = 0, time.perf_counter()
    for trade in trade_stream():
        if stopping:
            break
        send(p, trade)
        sent += 1
        if sent % 1000 == 0:
            p.poll(0)
            # pace to RATE events/s
            ahead = sent / RATE - (time.perf_counter() - t0)
            if ahead > 0:
                time.sleep(ahead)
        if sent % (RATE * 10) == 0:
            log.info("sent %d events (%.0f/s)", sent, sent / (time.perf_counter() - t0))


def run_binance(p: Producer) -> None:
    import websockets  # imported lazily: only needed for live mode

    async def main() -> None:
        url = f"wss://stream.binance.com:9443/stream?streams={STREAMS}"
        async for ws in websockets.connect(url, ping_interval=20):  # auto-reconnects
            try:
                async for msg in ws:
                    d = json.loads(msg)["data"]
                    send(p, Trade(str(d["t"]) + d["s"], d["s"], float(d["p"]), float(d["q"]), int(d["T"])))
                    p.poll(0)
                    if stopping:
                        return
            except websockets.ConnectionClosed:
                log.warning("websocket closed, reconnecting")

    asyncio.run(main())


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    global stopping

    def stop(*_):
        global stopping
        stopping = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    p = make_producer()
    log.info("source=%s topic=%s bootstrap=%s", SOURCE, TOPIC, BOOTSTRAP)
    try:
        (run_binance if SOURCE == "binance" else run_simulated)(p)
    finally:
        p.flush(10)


if __name__ == "__main__":
    sys.exit(main())
