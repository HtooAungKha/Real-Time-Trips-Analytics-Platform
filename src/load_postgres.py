from pathlib import Path

import polars as pl
import psycopg


PROJECT_ROOT = Path(__file__).resolve().parents[1]
ZONE_FILE = PROJECT_ROOT / "data" / "reference" / "taxi_zone_lookup.csv"

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


if __name__ == "__main__":
    load_zones()