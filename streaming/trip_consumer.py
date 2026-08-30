from __future__ import annotations

import json
from datetime import datetime

import psycopg
from kafka import KafkaConsumer, KafkaProducer


KAFKA_SERVER = "localhost:9092"
TOPIC_NAME = "taxi-trips"
DLQ_TOPIC_NAME = "taxi-trips-dlq"
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


def validate_event(event: dict) -> str | None:
    required_fields = [
        "event_id",
        "pickup_datetime",
        "dropoff_datetime",
        "pickup_location_id",
        "dropoff_location_id",
        "trip_distance",
        "trip_duration_minutes",
        "total_amount",
        "source",
    ]

    missing_fields = [
        field for field in required_fields if event.get(field) is None
    ]
    if missing_fields:
        return f"missing required fields: {', '.join(missing_fields)}"

    try:
        pickup_time = datetime.fromisoformat(event["pickup_datetime"])
        dropoff_time = datetime.fromisoformat(event["dropoff_datetime"])
    except ValueError:
        return "invalid timestamp format"

    if dropoff_time <= pickup_time:
        return "dropoff must be after pickup"

    if event["pickup_location_id"] <= 0 or event["dropoff_location_id"] <= 0:
        return "location IDs must be positive"

    if event["trip_distance"] < 0:
        return "trip distance cannot be negative"

    if event["trip_duration_minutes"] <= 0:
        return "trip duration must be positive"

    if event["total_amount"] < 0:
        return "total amount cannot be negative"

    return None


def main() -> None:
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=KAFKA_SERVER,
        group_id=CONSUMER_GROUP,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )

    dlq_producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )

    print(f"Listening for events on '{TOPIC_NAME}'... Press Ctrl+C to stop.")

    try:
        with psycopg.connect(DATABASE_URL) as connection:
            for message in consumer:
                event = message.value
                rejection_reason = validate_event(event)

                if rejection_reason:
                    dead_letter_event = {
                        "event": event,
                        "rejection_reason": rejection_reason,
                        "original_topic": message.topic,
                        "original_partition": message.partition,
                        "original_offset": message.offset,
                    }

                    dlq_producer.send(
                        DLQ_TOPIC_NAME,
                        value=dead_letter_event,
                    ).get(timeout=10)

                    consumer.commit()
                    print(
                        f"sent to DLQ: {event.get('event_id')} "
                        f"({rejection_reason})"
                    )
                    continue

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
        dlq_producer.close()


if __name__ == "__main__":
    main()