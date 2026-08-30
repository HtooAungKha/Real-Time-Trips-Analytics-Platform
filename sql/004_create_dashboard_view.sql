CREATE OR REPLACE VIEW analytics.v_trip_dashboard AS
SELECT
    trip.pickup_datetime::DATE AS pickup_date,
    TO_CHAR(trip.pickup_datetime, 'Month') AS pickup_month,
    EXTRACT(MONTH FROM trip.pickup_datetime)::INTEGER AS pickup_month_number,
    TO_CHAR(trip.pickup_datetime, 'Day') AS weekday,
    EXTRACT(ISODOW FROM trip.pickup_datetime)::INTEGER AS weekday_number,
    EXTRACT(HOUR FROM trip.pickup_datetime)::INTEGER AS pickup_hour,
    pickup_zone.borough AS pickup_borough,
    pickup_zone.zone AS pickup_zone,
    trip.payment_type,
    trip.trip_distance,
    trip.trip_duration_minutes,
    trip.total_amount
FROM analytics.fact_trips AS trip
JOIN analytics.dim_zone AS pickup_zone
    ON trip.pickup_location_id = pickup_zone.location_id;