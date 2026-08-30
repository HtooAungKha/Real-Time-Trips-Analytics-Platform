from __future__ import annotations

import json
import time
from pathlib import Path

import polars as pl
from kafka import KafkaProducer


PROJECT_ROOT = Path(__file__).resolve().parents[1]
CURATED_FILE = (
    PROJECT_ROOT / "data" / "curated" / "yellow_tripdata_2024-01_curated.parquet"
)

KAFKA_SERVER = "localhost:9092"
TOPIC_NAME = "taxi-trips"
EVENT_START = 10
EVENT_LIMIT = 10
SECONDS_BETWEEN_EVENTS = 1


def load_trip_events() -> list[dict]:
    trips = (
        pl.scan_parquet(CURATED_FILE)
        .select(
            [
                pl.col("VendorID").alias("vendor_id"),
                pl.col("tpep_pickup_datetime")
                .dt.strftime("%Y-%m-%dT%H:%M:%S")
                .alias("pickup_datetime"),
                pl.col("tpep_dropoff_datetime")
                .dt.strftime("%Y-%m-%dT%H:%M:%S")
                .alias("dropoff_datetime"),
                pl.col("PULocationID").alias("pickup_location_id"),
                pl.col("DOLocationID").alias("dropoff_location_id"),
                pl.col("passenger_count"),
                pl.col("trip_distance"),
                pl.col("trip_duration_minutes"),
                pl.col("payment_type"),
                pl.col("fare_amount"),
                pl.col("tip_amount"),
                pl.col("total_amount"),
            ]
        )
        .slice(EVENT_START, EVENT_LIMIT)
        .collect()
    )

    return trips.to_dicts()


def main() -> None:
    if not CURATED_FILE.exists():
        raise FileNotFoundError(f"Curated file not found: {CURATED_FILE}")

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        acks="all",
        key_serializer=lambda value: value.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )

    events = load_trip_events()
    print(f"Publishing {len(events)} real NYC taxi trip events to '{TOPIC_NAME}'...")

    for index, event in enumerate(events, start=EVENT_START + 1):
        event_id = f"yellow-2024-01-{index:08d}"
        event["event_id"] = event_id
        event["source"] = "nyc_tlc_yellow_taxi"

        metadata = producer.send(
            TOPIC_NAME,
            key=event_id,
            value=event,
        ).get(timeout=10)

        print(
            f"Sent {event_id} "
            f"(partition={metadata.partition}, offset={metadata.offset})"
        )
        time.sleep(SECONDS_BETWEEN_EVENTS)

    producer.flush()
    producer.close()
    print("Finished publishing trip events.")


if __name__ == "__main__":
    main()