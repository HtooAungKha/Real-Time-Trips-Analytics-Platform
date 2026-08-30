from streaming.trip_consumer import validate_event


def valid_event() -> dict:
    return {
        "event_id": "test-event-1",
        "pickup_datetime": "2024-01-01T10:00:00",
        "dropoff_datetime": "2024-01-01T10:10:00",
        "pickup_location_id": 100,
        "dropoff_location_id": 200,
        "trip_distance": 2.5,
        "trip_duration_minutes": 10,
        "total_amount": 15.0,
        "source": "test",
    }


def test_valid_event_passes_validation() -> None:
    assert validate_event(valid_event()) is None


def test_invalid_duration_is_rejected() -> None:
    event = valid_event()
    event["trip_duration_minutes"] = 0

    assert validate_event(event) == "trip duration must be positive"


def test_invalid_timestamps_are_rejected() -> None:
    event = valid_event()
    event["dropoff_datetime"] = "2024-01-01T10:00:00"

    assert validate_event(event) == "dropoff must be after pickup"