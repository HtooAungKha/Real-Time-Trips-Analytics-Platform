from pathlib import Path

import polars as pl

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_FILE = PROJECT_ROOT / "data" / "raw" / "yellow_tripdata_2024-01.parquet"


def main() -> None:
    if not DATA_FILE.exists():
        raise FileNotFoundError(f"Data file not found: {DATA_FILE}")

    trips = pl.read_parquet(DATA_FILE, n_rows=5)

    print("\nSchema:")
    print(trips.schema)

    print("\nFirst 5 rows:")
    print(trips)


if __name__ == "__main__":
    main()