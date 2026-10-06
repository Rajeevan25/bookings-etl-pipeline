-- =============================================================================
-- sql/02_indexes.sql
-- Performance Indexes for travel_bookings
--
-- Design principle:
--   • B-Tree indexes on high-cardinality columns used in WHERE / ORDER BY
--   • Partial indexes to reduce index size for common filtered queries
--   • Composite indexes aligned to the analytical queries in 03_queries.sql
--   • GIN index for full-text search on hotel_name
-- =============================================================================

-- ---------------------------------------------------------------------------
-- 1. Index on booking_id  (already covered by UNIQUE constraint / primary key)
--    Listed here for documentation only.
-- ---------------------------------------------------------------------------
-- CREATE UNIQUE INDEX IF NOT EXISTS uq_booking_id ON travel_bookings(booking_id);


-- ---------------------------------------------------------------------------
-- 2. Covering index for "revenue by category" queries
--    Query pattern: GROUP BY category, SUM(total_price), ORDER BY revenue DESC
-- ---------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_bookings_category_revenue
    ON travel_bookings (category, total_price);

COMMENT ON INDEX idx_bookings_category_revenue
    IS 'Supports GROUP BY category with SUM(total_price) — avoids full-table scan';


-- ---------------------------------------------------------------------------
-- 3. Index on country for per-country aggregations (avg rating, revenue)
-- ---------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_bookings_country
    ON travel_bookings (country);

COMMENT ON INDEX idx_bookings_country
    IS 'Accelerates WHERE country = … and GROUP BY country';


-- ---------------------------------------------------------------------------
-- 4. Index on check_in_date for date-range and monthly grouping queries
-- ---------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_bookings_checkin_date
    ON travel_bookings (check_in_date);

COMMENT ON INDEX idx_bookings_checkin_date
    IS 'Accelerates date-range filters and DATE_TRUNC(month) GROUP BY';


-- ---------------------------------------------------------------------------
-- 5. Composite index for monthly growth analysis
--    Query: GROUP BY DATE_TRUNC('month', check_in_date), category
-- ---------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_bookings_month_category
    ON travel_bookings (check_in_date, category);

COMMENT ON INDEX idx_bookings_month_category
    IS 'Composite index accelerating monthly date grouping and category aggregations';


-- ---------------------------------------------------------------------------
-- 6. Index on status — frequent filter in operational queries
-- ---------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_bookings_status
    ON travel_bookings (status);

COMMENT ON INDEX idx_bookings_status
    IS 'Accelerates WHERE status = ''Confirmed'' etc.';


-- ---------------------------------------------------------------------------
-- 7. Partial index: only Confirmed + Refunded bookings (most analytical queries
--    exclude Cancelled / Pending)
-- ---------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_bookings_confirmed
    ON travel_bookings (check_in_date, total_price)
    WHERE status IN ('Confirmed', 'Refunded');

COMMENT ON INDEX idx_bookings_confirmed
    IS 'Partial index: reduces scan to completed (non-pending/cancelled) bookings';


-- ---------------------------------------------------------------------------
-- 8. Index on rating for country-level average calculations
-- ---------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_bookings_rating_country
    ON travel_bookings (country, rating)
    WHERE rating IS NOT NULL;

COMMENT ON INDEX idx_bookings_rating_country
    IS 'Partial covering index for AVG(rating) GROUP BY country';


-- ---------------------------------------------------------------------------
-- 9. GIN trigram index for fuzzy hotel_name search
--    Requires: CREATE EXTENSION pg_trgm  (done in 01_schema.sql)
-- ---------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_bookings_hotel_trgm
    ON travel_bookings USING GIN (hotel_name gin_trgm_ops);

COMMENT ON INDEX idx_bookings_hotel_trgm
    IS 'Enables fast LIKE/ILIKE and similarity searches on hotel_name';


-- ---------------------------------------------------------------------------
-- 10. Index on created_date for pipeline deduplication
-- ---------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_bookings_created_date
    ON travel_bookings (created_date DESC);

COMMENT ON INDEX idx_bookings_created_date
    IS 'Supports recent-records queries and dedup checks';


-- =============================================================================
-- ANALYZE — update planner statistics after index creation
-- =============================================================================
ANALYZE travel_bookings;
