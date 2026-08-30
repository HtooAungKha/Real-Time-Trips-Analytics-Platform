from fastapi import FastAPI, HTTPException
import psycopg
from psycopg.rows import dict_row


DATABASE_URL = "postgresql://postgres:postgres@localhost:5432/taxi_analytics"

app = FastAPI(
    title="Real-Time Trips Analytics API",
    version="0.1.0",
)


@app.get("/health")
def health() -> dict:
    try:
        with psycopg.connect(DATABASE_URL) as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT 1;")

        return {"status": "ok", "database": "connected"}

    except psycopg.Error as error:
        raise HTTPException(
            status_code=503,
            detail=f"Database connection failed: {error}",
        ) from error


@app.get("/analytics/boroughs")
def borough_metrics() -> list[dict]:
    query = """
        SELECT
            borough,
            trip_count,
            total_revenue::DOUBLE PRECISION AS total_revenue,
            average_distance::DOUBLE PRECISION AS average_distance,
            average_duration_minutes::DOUBLE PRECISION
                AS average_duration_minutes
        FROM analytics.v_borough_trip_metrics
        ORDER BY total_revenue DESC;
    """

    with psycopg.connect(DATABASE_URL) as connection:
        with connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(query)
            return cursor.fetchall()

@app.get("/streaming/status")
def streaming_status() -> dict:
    query = """
        SELECT
            COUNT(*) AS streamed_trip_count,
            MAX(received_at) AS latest_received_at,
            MAX(kafka_offset) AS latest_kafka_offset
        FROM analytics.streamed_trips;
    """

    try:
        with psycopg.connect(DATABASE_URL) as connection:
            with connection.cursor(row_factory=dict_row) as cursor:
                cursor.execute(query)
                return cursor.fetchone()

    except psycopg.Error as error:
        raise HTTPException(
            status_code=503,
            detail=f"Database query failed: {error}",
        ) from error

@app.get("/analytics/hourly")
def hourly_trip_metrics() -> list[dict]:
    query = """
        SELECT
            pickup_hour,
            weekday,
            weekday_number,
            COUNT(*) AS trip_count
        FROM analytics.v_trip_dashboard
        WHERE pickup_borough NOT IN ('Unknown', 'N/A', 'EWR')
        GROUP BY pickup_hour, weekday, weekday_number
        ORDER BY weekday_number, pickup_hour;
    """

    try:
        with psycopg.connect(DATABASE_URL) as connection:
            with connection.cursor(row_factory=dict_row) as cursor:
                cursor.execute(query)
                return cursor.fetchall()

    except psycopg.Error as error:
        raise HTTPException(
            status_code=503,
            detail=f"Database query failed: {error}",
        ) from error

@app.get("/analytics/pickup-zones")
def pickup_zone_metrics() -> list[dict]:
    query = """
        SELECT
            zone.location_id,
            zone.borough,
            zone.zone,
            COUNT(*) AS trip_count
        FROM analytics.fact_trips AS trip
        JOIN analytics.dim_zone AS zone
            ON trip.pickup_location_id = zone.location_id
        WHERE zone.borough NOT IN ('Unknown', 'N/A', 'EWR')
        GROUP BY zone.location_id, zone.borough, zone.zone
        ORDER BY trip_count DESC;
    """

    try:
        with psycopg.connect(DATABASE_URL) as connection:
            with connection.cursor(row_factory=dict_row) as cursor:
                cursor.execute(query)
                return cursor.fetchall()

    except psycopg.Error as error:
        raise HTTPException(
            status_code=503,
            detail=f"Database query failed: {error}",
        ) from error