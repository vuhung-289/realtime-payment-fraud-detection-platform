import os
from datetime import datetime, timedelta

import pandas as pd
import plotly.express as px
import streamlit as st
from google.cloud import bigquery


from dotenv import load_dotenv
load_dotenv()

GCP_PROJECT = os.getenv("GCP_PROJECT_ID", "")
DATASET = os.getenv("BIGQUERY_DATASET", "fraud_analytics")

st.set_page_config(page_title="Fraud Monitoring Dashboard", layout="wide")
st.title("Real-time Payment Fraud Monitoring")


@st.cache_data(ttl=5)
def load_scored_data(hours: int = 6) -> pd.DataFrame:
    client = bigquery.Client(project=GCP_PROJECT)
    start_time = (datetime.utcnow() - timedelta(hours=hours)).strftime("%Y-%m-%d %H:%M:%S")
    query = f"""
        SELECT
          event_time,
          user_id,
          country,
          amount,
          payment_status,
          risk_score,
          risk_band
        FROM `{GCP_PROJECT}.{DATASET}.fraud_scored_transactions`
        WHERE CAST(event_time AS TIMESTAMP) >= TIMESTAMP('{start_time}')
        ORDER BY event_time DESC
        LIMIT 50000
    """
    return client.query(query).to_dataframe()


df = load_scored_data()
if df.empty:
    st.warning("No data found yet. Start producer + streaming job first.")
    st.stop()

total_tx = len(df)
high_risk = len(df[df["risk_score"] >= 75])
fraud_rate = (high_risk / total_tx) * 100 if total_tx else 0
avg_score = df["risk_score"].mean()

c1, c2, c3, c4 = st.columns(4)
c1.metric("Transactions", f"{total_tx:,}")
c2.metric("High Risk (>=75)", f"{high_risk:,}")
c3.metric("High Risk Rate", f"{fraud_rate:.2f}%")
c4.metric("Avg Risk Score", f"{avg_score:.1f}")

left, right = st.columns(2)
with left:
    fig_band = px.histogram(df, x="risk_band", color="risk_band", title="Risk Band Distribution")
    st.plotly_chart(fig_band, use_container_width=True)

with right:
    top_countries = df.groupby("country", as_index=False)["risk_score"].mean().sort_values("risk_score", ascending=False)
    fig_country = px.bar(top_countries.head(10), x="country", y="risk_score", title="Top Countries by Avg Risk Score")
    st.plotly_chart(fig_country, use_container_width=True)

st.subheader("Recent High-risk Transactions")
st.dataframe(
    df[df["risk_score"] >= 75][["event_time", "user_id", "country", "amount", "payment_status", "risk_score", "risk_band"]]
    .sort_values("event_time", ascending=False)
    .head(100),
    use_container_width=True,
)

st.sidebar.markdown("---")
auto_refresh = st.sidebar.checkbox("Auto-refresh (5s)", value=True)
if auto_refresh:
    import time
    time.sleep(5)
    st.rerun()

