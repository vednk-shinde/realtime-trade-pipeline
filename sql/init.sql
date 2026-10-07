-- Analytics schema. Loaded automatically by the postgres container on first start.

CREATE TABLE IF NOT EXISTS trades (
    trade_id   text        PRIMARY KEY,           -- natural key => dedupe
    symbol     text        NOT NULL,
    price      double precision NOT NULL,
    size       double precision NOT NULL,
    event_time timestamptz NOT NULL
);
-- Dashboard / ad-hoc queries are always "one symbol, recent time range".
CREATE INDEX IF NOT EXISTS trades_symbol_time_idx ON trades (symbol, event_time DESC);
-- Cheap range index for append-mostly time data (retention jobs, full scans by time).
CREATE INDEX IF NOT EXISTS trades_time_brin_idx ON trades USING brin (event_time);

-- Spark appends here; merge_trades_staging() moves rows into `trades` ignoring duplicates.
CREATE UNLOGGED TABLE IF NOT EXISTS trades_staging (
    trade_id text, symbol text, price double precision, size double precision, event_time timestamptz
);

CREATE OR REPLACE FUNCTION merge_trades_staging() RETURNS bigint LANGUAGE plpgsql AS $$
DECLARE n bigint;
BEGIN
    WITH moved AS (
        DELETE FROM trades_staging RETURNING *
    )
    INSERT INTO trades SELECT * FROM moved ON CONFLICT (trade_id) DO NOTHING;
    GET DIAGNOSTICS n = ROW_COUNT;
    RETURN n;
END $$;

CREATE TABLE IF NOT EXISTS candles_1m (
    symbol       text        NOT NULL,
    window_start timestamptz NOT NULL,
    open         double precision NOT NULL,
    high         double precision NOT NULL,
    low          double precision NOT NULL,
    close        double precision NOT NULL,
    volume       double precision NOT NULL,
    vwap         double precision NOT NULL,
    trade_count  integer     NOT NULL,
    updated_at   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (symbol, window_start)
);
CREATE INDEX IF NOT EXISTS candles_time_idx ON candles_1m (window_start DESC);

-- Pipeline health: end-to-end freshness per symbol.
CREATE OR REPLACE VIEW pipeline_lag AS
SELECT symbol,
       max(event_time)                      AS last_event_time,
       extract(epoch FROM now() - max(event_time)) AS lag_seconds,
       count(*)                             AS trades_total
FROM trades GROUP BY symbol;
