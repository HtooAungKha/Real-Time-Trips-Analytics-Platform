CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.dim_zone (
    location_id INTEGER PRIMARY KEY,
    borough TEXT NOT NULL,
    zone TEXT NOT NULL,
    service_zone TEXT
);

CREATE TABLE IF NOT EXISTS analytics.fact_trips (
    trip_key BIGSERIAL PRIMARY KEY,
    vendor_id INTEGER,
    pickup_datetime TIMESTAMP NOT NULL,
    dropoff_datetime TIMESTAMP NOT NULL,
    pickup_location_id INTEGER REFERENCES analytics.dim_zone (location_id),
    dropoff_location_id INTEGER REFERENCES analytics.dim_zone (location_id),
    passenger_count INTEGER,
    trip_distance DOUBLE PRECISION,
    trip_duration_minutes INTEGER,
    rate_code_id INTEGER,
    store_and_fwd_flag TEXT,
    payment_type INTEGER,
    fare_amount DOUBLE PRECISION,
    extra DOUBLE PRECISION,
    mta_tax DOUBLE PRECISION,
    tip_amount DOUBLE PRECISION,
    tolls_amount DOUBLE PRECISION,
    total_amount DOUBLE PRECISION,
    congestion_surcharge DOUBLE PRECISION,
    airport_fee DOUBLE PRECISION,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);