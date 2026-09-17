"""
Interface MarketPulse AI : dashboard (métriques Gold) + chat vers les agents.
Lancer avec : streamlit run dashboard/app.py
"""

from datetime import datetime, timezone

import pandas as pd
import plotly.express as px
import streamlit as st

from agents.graph import ask_agents
from agents.tools import _connect
from ingestion.config import S3_BUCKET_GOLD

st.set_page_config(page_title="MarketPulse AI", layout="wide")
st.title("📈 MarketPulse AI")

tab_dashboard, tab_chat = st.tabs(["Dashboard", "Agents"])

today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

with tab_dashboard:
    date = st.text_input("Date (YYYY-MM-DD)", value=today)

    con = _connect()
    try:
        df = con.execute(f"""
            SELECT * FROM read_parquet('s3://{S3_BUCKET_GOLD}/agg_stock_metrics/date={date}/agg_stock_metrics.parquet')
        """).fetchdf()
    except Exception:
        df = pd.DataFrame()

    if df.empty:
        st.warning(f"Aucune donnée Gold trouvée pour {date}.")
    else:
        col1, col2, col3 = st.columns(3)
        col1.metric("Symboles suivis", len(df))
        col2.metric("Volume total", f"{df['total_volume'].sum():,.0f}")
        col3.metric("Volatilité moyenne", f"{df['volatility'].mean():.3f}")

        st.subheader("VWAP par symbole")
        st.plotly_chart(px.bar(df, x="symbol", y="vwap"), use_container_width=True)

        st.subheader("Volatilité par symbole")
        st.plotly_chart(px.bar(df, x="symbol", y="volatility", color="symbol"), use_container_width=True)

        symbol = st.selectbox("Voir l'évolution 5min pour :", df["symbol"].unique())
        trend = con.execute(f"""
            SELECT * FROM read_parquet('s3://{S3_BUCKET_GOLD}/agg_5min/date={date}/agg_5min.parquet')
            WHERE symbol = '{symbol}' ORDER BY event_time
        """).fetchdf()
        if not trend.empty:
            st.plotly_chart(px.line(trend, x="event_time", y="avg_price_5min", title=f"Prix moyen 5min — {symbol}"), use_container_width=True)

with tab_chat:
    st.caption("Pose une question aux agents (insight, anomalies, tendance) sur les données du jour.")

    if "history" not in st.session_state:
        st.session_state.history = []

    for role, content in st.session_state.history:
        st.chat_message(role).write(content)

    question = st.chat_input("Ex : Y a-t-il une anomalie de volatilité aujourd'hui ?")
    if question:
        st.session_state.history.append(("user", question))
        st.chat_message("user").write(question)
        with st.spinner("Les agents analysent..."):
            answer = ask_agents(question)
        st.session_state.history.append(("assistant", answer))
        st.chat_message("assistant").write(answer)