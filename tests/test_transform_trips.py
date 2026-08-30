import polars as pl

from src.transform_trips import classify_trips


def test_classify_trips_assigns_expected_rejection_reasons() -> None:
    trips = pl.DataFrame(
        {
            "trip_duration_minutes": [10, 0, 5, 5, 5, 5],
            "trip_distance": [1.5, 1.5, -1.0, 1.5, 1.5, 1.5],
            "total_amount": [12.0, 12.0, 12.0, -5.0, 12.0, 12.0],
            "pickup_zone": [
                "SoHo",
                "SoHo",
                "SoHo",
                "SoHo",
                None,
                "SoHo",
            ],
            "dropoff_zone": [
                "East Village",
                "East Village",
                "East Village",
                "East Village",
                "East Village",
                None,
            ],
        }
    )

    result = classify_trips(trips)

    assert result["rejection_reason"].to_list() == [
        None,
        "non_positive_duration",
        "negative_distance",
        "negative_total_amount",
        "missing_pickup_zone",
        "missing_dropoff_zone",
    ]

    assert result["record_status"].to_list() == [
        "valid",
        "quarantine",
        "quarantine",
        "quarantine",
        "quarantine",
        "quarantine",
    ]