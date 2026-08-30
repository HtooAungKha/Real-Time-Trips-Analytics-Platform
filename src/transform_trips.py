from pathlib import Path

import polars as pl


PROJECT_ROOT = Path(__file__).resolve().parents[1]

RAW_FILE = PROJECT_ROOT / "data" / "raw" / "yellow_tripdata_2024-01.parquet"
ZONE_FILE = PROJECT_ROOT / "data" / "reference" / "taxi_zone_lookup.csv"

CURATED_FILE = (
    PROJECT_ROOT / "data" / "curated" / "yellow_tripdata_2024-01_curated.parquet"
)
QUARANTINE_FILE = (
    PROJECT_ROOT
    / "data"
    / "quarantine"
    / "yellow_tripdata_2024-01_quarantine.parquet"
)


def load_trips() -> pl.DataFrame:
    """Load raw NYC Yellow Taxi trip data."""
    return pl.read_parquet(RAW_FILE)


def load_zones() -> pl.DataFrame:
    """Load TLC location IDs and zone names."""
    return pl.read_csv(ZONE_FILE).with_columns(
        pl.col("LocationID").cast(pl.Int32)
    )


def enrich_trips(trips: pl.DataFrame, zones: pl.DataFrame) -> pl.DataFrame:
    """Calculate duration and add readable pickup/drop-off zone names."""
    pickup_zones = zones.select(
        [
            pl.col("LocationID").alias("PULocationID"),
            pl.col("Borough").alias("pickup_borough"),
            pl.col("Zone").alias("pickup_zone"),
        ]
    )

    dropoff_zones = zones.select(
        [
            pl.col("LocationID").alias("DOLocationID"),
            pl.col("Borough").alias("dropoff_borough"),
            pl.col("Zone").alias("dropoff_zone"),
        ]
    )

    return (
        trips.with_columns(
            (
                pl.col("tpep_dropoff_datetime") - pl.col("tpep_pickup_datetime")
            )
            .dt.total_minutes()
            .alias("trip_duration_minutes")
        )
        .join(pickup_zones, on="PULocationID", how="left")
        .join(dropoff_zones, on="DOLocationID", how="left")
    )


def classify_trips(trips: pl.DataFrame) -> pl.DataFrame:
    """Assign a rejection reason to records that fail validation rules."""
    return (
        trips.with_columns(
            pl.when(pl.col("trip_duration_minutes") <= 0)
            .then(pl.lit("non_positive_duration"))
            .when(pl.col("trip_distance") < 0)
            .then(pl.lit("negative_distance"))
            .when(pl.col("total_amount") < 0)
            .then(pl.lit("negative_total_amount"))
            .when(pl.col("pickup_zone").is_null())
            .then(pl.lit("missing_pickup_zone"))
            .when(pl.col("dropoff_zone").is_null())
            .then(pl.lit("missing_dropoff_zone"))
            .otherwise(pl.lit(None))
            .alias("rejection_reason")
        )
        .with_columns(
            pl.when(pl.col("rejection_reason").is_null())
            .then(pl.lit("valid"))
            .otherwise(pl.lit("quarantine"))
            .alias("record_status")
        )
    )


def write_outputs(trips: pl.DataFrame) -> None:
    """Write valid and quarantined trips into separate Parquet layers."""
    CURATED_FILE.parent.mkdir(parents=True, exist_ok=True)
    QUARANTINE_FILE.parent.mkdir(parents=True, exist_ok=True)

    valid_trips = trips.filter(pl.col("record_status") == "valid")
    quarantine_trips = trips.filter(pl.col("record_status") == "quarantine")

    valid_trips.write_parquet(CURATED_FILE, compression="zstd")
    quarantine_trips.write_parquet(QUARANTINE_FILE, compression="zstd")

    print(f"Valid trips written: {valid_trips.height:,}")
    print(f"Quarantined trips written: {quarantine_trips.height:,}")

    print("\nQuarantine summary:")
    print(quarantine_trips.group_by("rejection_reason").len())


def main() -> None:
    if not RAW_FILE.exists():
        raise FileNotFoundError(f"Raw trip file not found: {RAW_FILE}")

    if not ZONE_FILE.exists():
        raise FileNotFoundError(f"Zone lookup file not found: {ZONE_FILE}")

    print("Loading raw trip data...")
    trips = load_trips()

    print("Loading taxi-zone lookup data...")
    zones = load_zones()

    print("Transforming and enriching trips...")
    enriched_trips = enrich_trips(trips, zones)

    print("Running data-quality checks...")
    classified_trips = classify_trips(enriched_trips)

    print("Writing output files...")
    write_outputs(classified_trips)


if __name__ == "__main__":
    main()