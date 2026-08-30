from __future__ import annotations

import requests
import streamlit as st
import plotly.express as px
import json
from pathlib import Path

import pydeck as pdk


API_BASE_URL = "http://127.0.0.1:8000"
PROJECT_ROOT = Path(__file__).resolve().parents[1]
GEOJSON_FILE = PROJECT_ROOT / "data" / "reference" / "taxi_zones.geojson"

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

@st.cache_data
def load_zone_geojson(zone_metrics: list[dict]) -> dict:
    with GEOJSON_FILE.open(encoding="utf-8") as file:
        geojson = json.load(file)

    trip_counts = {
        str(row["location_id"]): row["trip_count"]
        for row in zone_metrics
    }

    maximum_trip_count = max(trip_counts.values())

    for feature in geojson["features"]:
        properties = feature["properties"]
        location_id = (
            properties.get("location_id")
            or properties.get("locationid")
            or properties.get("LocationID")
            or properties.get("OBJECTID")
            or properties.get("objectid")
        )

        location_id = str(location_id)
        properties["zone_name"] = properties.get("zone", properties.get("Zone", "Unknown"))

        trip_count = trip_counts.get(location_id, 0)
        intensity = int(255 * (trip_count / maximum_trip_count) ** 0.5)

        properties["trip_count"] = trip_count
        properties["fill_color"] = [255, 255 - intensity, 0, 180]

    return geojson


if st.sidebar.button("Refresh data"):
    st.cache_data.clear()
    st.rerun()

try:
    streaming_status = get_json("/streaming/status")
    borough_metrics = get_json("/analytics/boroughs")
    hourly_metrics = get_json("/analytics/hourly")
    zone_metrics = get_json("/analytics/pickup-zones")

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

    st.subheader("Pickup Density by NYC Taxi Zone")

    zone_geojson = load_zone_geojson(zone_metrics)

    density_layer = pdk.Layer(
        "GeoJsonLayer",
        data=zone_geojson,
        opacity=0.8,
        stroked=True,
        filled=True,
        get_fill_color="properties.fill_color",
        get_line_color=[255, 255, 255],
        line_width_min_pixels=1,
        pickable=True,
    )

    density_map = pdk.Deck(
        layers=[density_layer],
        initial_view_state=pdk.ViewState(
            latitude=40.7128,
            longitude=-74.0060,
            zoom=9.8,
            pitch=0,
        ),
        map_style="https://basemaps.cartocdn.com/gl/positron-gl-style/style.json",
        tooltip={
            "html": "<b>{zone_name}</b><br/>Trips: {trip_count}",
        },
    )

    st.pydeck_chart(density_map, use_container_width=True)

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