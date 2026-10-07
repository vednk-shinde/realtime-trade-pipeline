"""Live Streamlit dashboard. Only the chart fragment reruns (every 2 s), so the page never
flickers or re-renders the sidebar."""
import os

import pandas as pd
import plotly.graph_objects as go
import psycopg2
import streamlit as st

st.set_page_config(page_title="Real-time trade pipeline", layout="wide")

DSN = dict(
    host=os.getenv("PG_HOST", "localhost"),
    dbname=os.getenv("PG_DB", "trades"),
    user=os.getenv("PG_USER", "pipeline"),
    password=os.getenv("PG_PASSWORD", "pipeline"),
)


@st.cache_resource
def conn():
    c = psycopg2.connect(**DSN)
    c.autocommit = True
    return c


def query(sql: str, params=()) -> pd.DataFrame:
    try:
        return pd.read_sql_query(sql, conn(), params=params)
    except Exception:  # connection dropped -> reconnect once
        st.cache_resource.clear()
        return pd.read_sql_query(sql, conn(), params=params)


st.title("Real-time trade pipeline")
st.caption("Kafka → Spark Structured Streaming → PostgreSQL → Streamlit")

symbols = query("SELECT DISTINCT symbol FROM candles_1m ORDER BY 1")["symbol"].tolist()
symbol = st.sidebar.selectbox("Symbol", symbols or ["(waiting for data)"])
minutes = st.sidebar.slider("History (minutes)", 5, 240, 60)


@st.fragment(run_every=2)
def live():
    candles = query(
        "SELECT * FROM candles_1m WHERE symbol = %s AND window_start > now() - %s * interval '1 minute' ORDER BY window_start",
        (symbol, minutes),
    )
    lag = query("SELECT * FROM pipeline_lag ORDER BY symbol")
    total = int(lag["trades_total"].sum()) if not lag.empty else 0
    worst_lag = float(lag["lag_seconds"].max()) if not lag.empty else float("nan")

    a, b, c = st.columns(3)
    a.metric("Trades stored", f"{total:,}")
    b.metric("Worst end-to-end lag (s)", f"{worst_lag:.1f}")
    if not candles.empty:
        last = candles.iloc[-1]
        c.metric(f"{symbol} last close", f"{last['close']:,.2f}", f"{last['close'] - last['open']:+.2f} in window")

        fig = go.Figure(
            go.Candlestick(
                x=candles["window_start"], open=candles["open"], high=candles["high"],
                low=candles["low"], close=candles["close"], name=symbol,
            )
        )
        fig.add_trace(go.Scatter(x=candles["window_start"], y=candles["vwap"], name="VWAP", line=dict(width=1.5)))
        fig.update_layout(height=460, xaxis_rangeslider_visible=False, margin=dict(l=0, r=0, t=10, b=0))
        st.plotly_chart(fig, use_container_width=True)
        st.bar_chart(candles.set_index("window_start")["volume"], height=160)
    else:
        st.info("Waiting for the first candles… is the producer running?")
    st.dataframe(lag, use_container_width=True, hide_index=True)


live()
