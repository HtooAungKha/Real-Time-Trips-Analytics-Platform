import json

from kafka import KafkaProducer


producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
)

invalid_event = {
    "event_id": "invalid-trip-0001",
    "vendor_id": 1,
    "pickup_datetime": "2024-01-01T10:00:00",
    "dropoff_datetime": "2024-01-01T10:00:00",
    "pickup_location_id": 100,
    "dropoff_location_id": 200,
    "passenger_count": 1,
    "trip_distance": 2.5,
    "trip_duration_minutes": 0,
    "payment_type": 1,
    "fare_amount": 10.0,
    "tip_amount": 2.0,
    "total_amount": -5.0,
    "source": "validation_test",
}

producer.send("taxi-trips", value=invalid_event).get(timeout=10)
producer.close()

print("Invalid test event sent.")