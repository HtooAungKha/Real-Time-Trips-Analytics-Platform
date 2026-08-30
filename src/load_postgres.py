from pathlib import Path

import polars as pl
import psycopg


PROJECT_ROOT = Path(__file__).resolve().parents[1]
ZONE_FILE = PROJECT_ROOT / "data" / "reference" / "taxi_zone_lookup.csv"

CURATED_FILE = (
    PROJECT_ROOT
    / "data"
    / "curated"
    / "yellow_tripdata_2024-01_curated.parquet"
)

DATABASE_URL = (
    "postgresql://postgres:postgres@localhost:5432/taxi_analytics"
)


def load_zones() -> None:
    """Load the taxi-zone lookup table into PostgreSQL."""
    zones = (
        pl.read_csv(ZONE_FILE)
        .select(
            [
                pl.col("LocationID").cast(pl.Int32).alias("location_id"),
                pl.col("Borough").alias("borough"),
                pl.col("Zone").alias("zone"),
                pl.col("service_zone").alias("service_zone"),
            ]
        )
    )

    rows = list(zones.iter_rows())

    query = """
        INSERT INTO analytics.dim_zone (
            location_id,
            borough,
            zone,
            service_zone
        )
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (location_id)
        DO UPDATE SET
            borough = EXCLUDED.borough,
            zone = EXCLUDED.zone,
            service_zone = EXCLUDED.service_zone;
    """

    with psycopg.connect(DATABASE_URL) as connection:
        with connection.cursor() as cursor:
            cursor.executemany(query, rows)

        connection.commit()

    print(f"Loaded {len(rows)} taxi zones into analytics.dim_zone.")

def load_trips() -> None:

    """Load curated trip records into PostgreSQL."""
    trips = pl.read_parquet(CURATED_FILE).select(
        [
            pl.col("VendorID").alias("vendor_id"),
            pl.col("tpep_pickup_datetime").alias("pickup_datetime"),
            pl.col("tpep_dropoff_datetime").alias("dropoff_datetime"),
            pl.col("PULocationID").alias("pickup_location_id"),
            pl.col("DOLocationID").alias("dropoff_location_id"),
            pl.col("passenger_count"),
            pl.col("trip_distance"),
            pl.col("trip_duration_minutes"),
            pl.col("RatecodeID").alias("rate_code_id"),
            pl.col("store_and_fwd_flag"),
            pl.col("payment_type"),
            pl.col("fare_amount"),
            pl.col("extra"),
            pl.col("mta_tax"),
            pl.col("tip_amount"),
            pl.col("tolls_amount"),
            pl.col("total_amount"),
            pl.col("congestion_surcharge"),
            pl.col("Airport_fee").alias("airport_fee"),
        ]
    )

    copy_query = """
        COPY analytics.fact_trips (
            vendor_id,
            pickup_datetime,
            dropoff_datetime,
            pickup_location_id,
            dropoff_location_id,
            passenger_count,
            trip_distance,
            trip_duration_minutes,
            rate_code_id,
            store_and_fwd_flag,
            payment_type,
            fare_amount,
            extra,
            mta_tax,
            tip_amount,
            tolls_amount,
            total_amount,
            congestion_surcharge,
            airport_fee
        )
        FROM STDIN
    """

    with psycopg.connect(DATABASE_URL) as connection:
        with connection.cursor() as cursor:
            # Full-refresh behavior for the local January 2024 dataset.
            cursor.execute("TRUNCATE TABLE analytics.fact_trips RESTART IDENTITY;")

            with cursor.copy(copy_query) as copy:
                for row in trips.iter_rows():
                    copy.write_row(row)

        connection.commit()

    print(f"Loaded {trips.height:,} curated trips into analytics.fact_trips.")


if __name__ == "__main__":
    load_zones()
    load_trips()