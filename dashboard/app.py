"""
Streamlit dashboard for the log analytics warehouse.

Reads directly from the DuckDB file dbt writes to (`/opt/warehouse/log_analytics.duckdb`).
No API layer in between — DuckDB is fast enough to query straight from the
app process, which is the whole point of putting DuckDB in this stack.
"""
from __future__ import annotations

import os
from pathlib import Path

import duckdb
import pandas as pd
import streamlit as st

WAREHOUSE_PATH = os.environ.get("WAREHOUSE_PATH", "/opt/warehouse/log_analytics.duckdb")
MARKER_PATH = "/opt/warehouse/_last_successful_run.txt"

st.set_page_config(page_title="Log Analytics", layout="wide", page_icon="\U0001F4CA")


@st.cache_resource
def get_connection():
    return duckdb.connect(WAREHOUSE_PATH, read_only=True)


def last_pipeline_run() -> str:
    p = Path(MARKER_PATH)
    if p.exists():
        return p.read_text().strip()
    return "no successful run recorded yet"


def load_df(query: str) -> pd.DataFrame:
    con = get_connection()
    return con.execute(query).fetch_df()


st.title("Log Analytics — Cloud Data Lake Portfolio Project")
st.caption(f"Last successful pipeline run: {last_pipeline_run()}")

if not Path(WAREHOUSE_PATH).exists():
    st.warning(
        "No warehouse file found yet. Trigger the `log_pipeline_dag` DAG in "
        "Airflow at least once, then reload this page."
    )
    st.stop()

daily = load_df("select * from marts.fct_requests_daily order by event_date")
endpoints = load_df(
    "select * from marts.dim_endpoints order by request_count desc limit 15"
)

col1, col2, col3, col4 = st.columns(4)
if not daily.empty:
    latest = daily.iloc[-1]
    col1.metric("Requests (latest day)", f"{int(latest['request_count']):,}")
    col2.metric("Server error rate", f"{latest['server_error_rate'] * 100:.2f}%")
    col3.metric("p95 latency", f"{latest['p95_latency_ms']:.0f} ms")
    col4.metric("Unique users", f"{int(latest['unique_users']):,}")
else:
    st.info("No data yet — run the pipeline to populate the warehouse.")

st.subheader("Traffic & error rate over time")
if not daily.empty:
    left, right = st.columns(2)
    left.line_chart(daily.set_index("event_date")[["request_count"]])
    right.line_chart(daily.set_index("event_date")[["server_error_rate"]])

st.subheader("Latency percentiles over time")
if not daily.empty:
    st.line_chart(
        daily.set_index("event_date")[["p50_latency_ms", "p95_latency_ms", "p99_latency_ms"]]
    )

st.subheader("Top endpoints by traffic")
if not endpoints.empty:
    st.dataframe(
        endpoints[
            [
                "endpoint", "method", "request_count", "server_error_rate",
                "avg_latency_ms", "p95_latency_ms", "traffic_rank",
            ]
        ],
        use_container_width=True,
        hide_index=True,
    )
    st.bar_chart(endpoints.set_index("endpoint")[["request_count"]])

st.divider()
st.caption(
    "Pipeline: MinIO (raw) -> Spark/Delta Lake (bronze, silver) -> dbt -> "
    "DuckDB (marts) -> this dashboard. Orchestrated by Airflow every 15 minutes."
)
