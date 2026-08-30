from __future__ import annotations

import json

import psycopg
from kafka import KafkaConsumer


KAFKA_SERVER = "localhost:9092"
TOPIC_NAME = "taxi-trips"
CONSUMER_GROUP = "trip-loader-v1"
DATABASE_URL = "postgresql://postgres:postgres@localhost:5432/taxi_analytics"

INSERT_SQL = """
    INSERT INTO analytics.streamed_trips (
        event_id,
        vendor_id,
        pickup_datetime,
        dropoff_datetime,
        pickup_location_id,
        dropoff_location_id,
        passenger_count,
        trip_distance,
        trip_duration_minutes,
        payment_type,
        fare_amount,
        tip_amount,
        total_amount,
        source,
        kafka_topic,
        kafka_partition,
        kafka_offset
    )
    VALUES (
        %(event_id)s,
        %(vendor_id)s,
        %(pickup_datetime)s,
        %(dropoff_datetime)s,
        %(pickup_location_id)s,
        %(dropoff_location_id)s,
        %(passenger_count)s,
        %(trip_distance)s,
        %(trip_duration_minutes)s,
        %(payment_type)s,
        %(fare_amount)s,
        %(tip_amount)s,
        %(total_amount)s,
        %(source)s,
        %(kafka_topic)s,
        %(kafka_partition)s,
        %(kafka_offset)s
    )
    ON CONFLICT (event_id) DO NOTHING;
"""


def main() -> None:
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=KAFKA_SERVER,
        group_id=CONSUMER_GROUP,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )

    print(f"Listening for events on '{TOPIC_NAME}'... Press Ctrl+C to stop.")

    try:
        with psycopg.connect(DATABASE_URL) as connection:
            for message in consumer:
                event = message.value
                event["kafka_topic"] = message.topic
                event["kafka_partition"] = message.partition
                event["kafka_offset"] = message.offset

                with connection.cursor() as cursor:
                    cursor.execute(INSERT_SQL, event)

                connection.commit()
                consumer.commit()

                status = "inserted" if cursor.rowcount == 1 else "already exists"
                print(f"{status}: {event['event_id']}")

    except KeyboardInterrupt:
        print("\nConsumer stopped.")

    finally:
        consumer.close()


if __name__ == "__main__":
    main()