-- =============================================================================
-- sql/01_schema.sql
-- PostgreSQL Schema for Travel Bookings Data Warehouse
-- =============================================================================

-- Create dedicated database (run this as superuser before the rest)
-- CREATE DATABASE travel_db;
-- \c travel_db

-- Enable useful extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS pg_trgm;        -- Trigram index for LIKE queries

-- =============================================================================
-- ENUM types  (data integrity via DB constraint)
-- =============================================================================

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'booking_status') THEN
        CREATE TYPE booking_status AS ENUM (
            'Confirmed', 'Pending', 'Cancelled', 'Refunded'
        );
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'booking_category') THEN
        CREATE TYPE booking_category AS ENUM (
            'Hotel', 'Resort', 'Hostel', 'Villa', 'Apartment',
            'Boutique Hotel', 'Budget Hotel', 'Luxury Hotel'
        );
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'payment_method_type') THEN
        CREATE TYPE payment_method_type AS ENUM (
            'Credit Card', 'Debit Card', 'PayPal',
            'Bank Transfer', 'Crypto'
        );
    END IF;
END
$$;


-- =============================================================================
-- MAIN TABLE: travel_bookings
-- =============================================================================

CREATE TABLE IF NOT EXISTS travel_bookings (
    -- Surrogate key (auto-increment for internal joins)
    id               BIGSERIAL       PRIMARY KEY,

    -- Natural / business key
    booking_id       VARCHAR(20)     NOT NULL UNIQUE,

    -- Customer info
    customer_name    VARCHAR(255)    NOT NULL,
    email            VARCHAR(320),                   -- RFC 5321 max length
    phone            VARCHAR(50),

    -- Property info
    hotel_name       VARCHAR(255)    NOT NULL,
    category         booking_category NOT NULL,
    country          VARCHAR(100)    NOT NULL,

    -- Booking period
    check_in_date    DATE            NOT NULL,
    check_out_date   DATE            NOT NULL,
    nights           SMALLINT        NOT NULL CHECK (nights > 0),

    -- Financial
    price_per_night  NUMERIC(10, 2)  NOT NULL CHECK (price_per_night > 0),
    total_price      NUMERIC(12, 2)  NOT NULL CHECK (total_price > 0),

    -- Metadata
    rating           NUMERIC(3, 1)   CHECK (rating BETWEEN 1.0 AND 5.0),
    status           booking_status  NOT NULL,
    payment_method   payment_method_type,
    created_date     DATE            NOT NULL,

    -- Audit columns (set automatically)
    inserted_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    -- Cross-column constraint: check_out must be after check_in
    CONSTRAINT chk_checkout_after_checkin
        CHECK (check_out_date > check_in_date),

    -- Cross-column constraint: total_price ≈ nights × price_per_night (±5%)
    CONSTRAINT chk_total_price_consistency
        CHECK (
            ABS(total_price - (nights * price_per_night)) / (nights * price_per_night) <= 0.05
        )
);

COMMENT ON TABLE  travel_bookings              IS 'Cleaned travel / hotel booking transactions';
COMMENT ON COLUMN travel_bookings.booking_id   IS 'Business-level booking identifier (BKxxxxxx)';
COMMENT ON COLUMN travel_bookings.nights       IS 'Number of nights stayed';
COMMENT ON COLUMN travel_bookings.rating       IS 'Guest rating 1.0–5.0; NULL if not provided';


-- =============================================================================
-- REJECTION LOG TABLE
-- =============================================================================

CREATE TABLE IF NOT EXISTS etl_rejected_records (
    id               BIGSERIAL   PRIMARY KEY,
    run_timestamp    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    raw_booking_id   TEXT,
    raw_data         JSONB,
    rejection_reason TEXT        NOT NULL
);

COMMENT ON TABLE etl_rejected_records IS 'Records that failed ETL validation with reasons';


-- =============================================================================
-- ETL RUN LOG TABLE
-- =============================================================================

CREATE TABLE IF NOT EXISTS etl_run_log (
    id               BIGSERIAL   PRIMARY KEY,
    run_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source           VARCHAR(20),
    records_extracted INTEGER,
    records_loaded    INTEGER,
    records_rejected  INTEGER,
    duration_seconds  NUMERIC(10, 3),
    status           VARCHAR(20) CHECK (status IN ('SUCCESS', 'FAILURE', 'PARTIAL')),
    error_message    TEXT
);

COMMENT ON TABLE etl_run_log IS 'Audit log of every ETL pipeline run';
