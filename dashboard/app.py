from __future__ import annotations

import requests
import streamlit as st
import plotly.express as px


API_BASE_URL = "http://127.0.0.1:8000"

st.set_page_config(
    page_title="NYC Taxi Streaming Analytics",
    page_icon="🚕",
    layout="wide",
)

st.title("🚕 NYC Taxi Streaming Analytics")
st.caption("Kafka → PostgreSQL → FastAPI → Streamlit")


@st.cache_data(ttl=5)
def get_json(path: str) -> dict | list:
    response = requests.get(f"{API_BASE_URL}{path}", timeout=5)
    response.raise_for_status()
    return response.json()


if st.sidebar.button("Refresh data"):
    st.cache_data.clear()
    st.rerun()

try:
    streaming_status = get_json("/streaming/status")
    borough_metrics = get_json("/analytics/boroughs")
    hourly_metrics = get_json("/analytics/hourly")

    first, second, third = st.columns(3)
    first.metric("Streamed Trips", streaming_status["streamed_trip_count"])
    second.metric("Latest Kafka Offset", streaming_status["latest_kafka_offset"])
    third.metric(
        "Latest Event Received",
        streaming_status["latest_received_at"].replace("T", " ")[:19],
    )

    st.subheader("Total Revenue by Pickup Borough")

    chart = px.bar(
        borough_metrics,
        x="borough",
        y="total_revenue",
        color="borough",
        text_auto=".2s",
        labels={
            "borough": "Pickup Borough",
            "total_revenue": "Total Revenue ($)",
        },
    )
    chart.update_layout(showlegend=False)
    st.plotly_chart(chart, use_container_width=True)

    st.subheader("Borough Metrics")
    st.dataframe(borough_metrics, use_container_width=True)

    st.subheader("NYC Taxi Trips by Hour and Weekday")

    weekday_order = [
        "Monday",
        "Tuesday",
        "Wednesday",
        "Thursday",
        "Friday",
        "Saturday",
        "Sunday",
    ]

    hourly_chart = px.line(
        hourly_metrics,
        x="pickup_hour",
        y="trip_count",
        color="weekday",
        category_orders={"weekday": weekday_order},
        markers=True,
        labels={
            "pickup_hour": "Pickup Hour",
            "trip_count": "Number of Trips",
            "weekday": "Weekday",
        },
    )

    hourly_chart.update_layout(
        hovermode="x unified",
        xaxis={"dtick": 1, "range": [0, 23]},
    )

    st.plotly_chart(hourly_chart, use_container_width=True)

except requests.RequestException as error:
    st.error("Cannot reach the FastAPI service.")
    st.code(str(error))
    st.info("Start the API first: python -m uvicorn api.main:app --reload")