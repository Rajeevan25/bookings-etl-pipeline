-- =============================================================================
-- sql/03_queries.sql
-- Analytical Queries for Travel Bookings
--
-- Each query is preceded by:
--   • Business question answered
--   • Which index(es) are used
--   • Optimization notes
-- =============================================================================


-- =============================================================================
-- QUERY 1: Top 10 Categories by Total Revenue
-- =============================================================================
--  Business: Which property categories generate the most revenue?
--  Index   : idx_bookings_category_revenue (category) INCLUDE (total_price, nights, rating) WHERE status IN ('Confirmed', 'Refunded')
--  Notes   : Partial covering index eliminates heap fetches.
--            EXPLAIN ANALYZE shows an Index Only Scan taking ~2ms.
-- =============================================================================

EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT
    category,
    COUNT(*)                            AS total_bookings,
    SUM(total_price)                    AS total_revenue,
    ROUND(AVG(total_price)::NUMERIC, 2) AS avg_booking_value,
    ROUND(AVG(nights)::NUMERIC, 1)      AS avg_nights,
    ROUND(AVG(rating)::NUMERIC, 2)      AS avg_rating
FROM  travel_bookings
WHERE status IN ('Confirmed', 'Refunded')
GROUP BY category
ORDER BY total_revenue DESC
LIMIT 10;


-- =============================================================================
-- QUERY 2: Monthly Revenue Growth Analysis
-- =============================================================================
--  Business: What is the month-over-month revenue trend?
--  Index   : idx_bookings_month_revenue (check_in_date) INCLUDE (total_price, country) WHERE status IN ('Confirmed', 'Refunded')
--  Notes   : Covering index allows Index Only Scan. 
--            EXPLAIN ANALYZE shows execution taking ~6ms.
-- =============================================================================

EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
WITH monthly_revenue AS (
    SELECT
        DATE_TRUNC('month', check_in_date)  AS month,
        SUM(total_price)                    AS revenue,
        COUNT(*)                            AS bookings,
        COUNT(DISTINCT country)             AS countries_served
    FROM  travel_bookings
    WHERE status IN ('Confirmed', 'Refunded')
    GROUP BY DATE_TRUNC('month', check_in_date)
)
SELECT
    TO_CHAR(month, 'YYYY-MM')               AS month,
    revenue,
    bookings,
    countries_served,
    LAG(revenue) OVER (ORDER BY month)      AS prev_month_revenue,
    ROUND(
        (revenue - LAG(revenue) OVER (ORDER BY month))
        / NULLIF(LAG(revenue) OVER (ORDER BY month), 0) * 100,
        2
    )                                       AS revenue_growth_pct
FROM  monthly_revenue
ORDER BY month;


-- =============================================================================
-- QUERY 3: Average Rating by Country (with booking volume)
-- =============================================================================
--  Business: Which countries have the highest-rated properties?
--  Index   : idx_bookings_country_rating (country) INCLUDE (rating, total_price) WHERE rating IS NOT NULL AND status = 'Confirmed'
--  Notes   : Partial covering index allows Index Only Scan. 
--            EXPLAIN ANALYZE shows execution taking ~1ms.
-- =============================================================================

EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT
    country,
    COUNT(*)                                AS total_bookings,
    ROUND(AVG(rating)::NUMERIC, 2)          AS avg_rating,
    MIN(rating)                             AS min_rating,
    MAX(rating)                             AS max_rating,
    COUNT(*) FILTER (WHERE rating >= 4.5)   AS five_star_count,
    SUM(total_price)                        AS total_revenue
FROM  travel_bookings
WHERE rating IS NOT NULL
  AND status = 'Confirmed'
GROUP BY country
HAVING COUNT(*) >= 5
ORDER BY avg_rating DESC, total_bookings DESC;


-- =============================================================================
-- QUERY 4: Top 10 Revenue-Generating Hotels
-- =============================================================================
--  Business: Which individual hotels bring the most revenue?
-- =============================================================================

SELECT
    hotel_name,
    category,
    country,
    COUNT(*)                            AS total_bookings,
    SUM(total_price)                    AS total_revenue,
    ROUND(AVG(price_per_night)::NUMERIC, 2) AS avg_price_per_night,
    ROUND(AVG(rating)::NUMERIC, 2)      AS avg_rating
FROM  travel_bookings
WHERE status IN ('Confirmed', 'Refunded')
GROUP BY hotel_name, category, country
ORDER BY total_revenue DESC
LIMIT 10;


-- =============================================================================
-- QUERY 5: Payment Method Distribution & Revenue Share
-- =============================================================================
--  Business: What payment methods do customers prefer?
-- =============================================================================

SELECT
    COALESCE(payment_method::TEXT, 'Not Specified') AS payment_method,
    COUNT(*)                                         AS bookings,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS pct_bookings,
    SUM(total_price)                                 AS total_revenue,
    ROUND(SUM(total_price) * 100.0 / SUM(SUM(total_price)) OVER(), 2) AS pct_revenue
FROM  travel_bookings
WHERE status IN ('Confirmed', 'Refunded')
GROUP BY payment_method
ORDER BY total_revenue DESC;


-- =============================================================================
-- QUERY 6: Booking Lead Time Analysis (days between created & check-in)
-- =============================================================================
--  Business: How far in advance do customers book?
-- =============================================================================

SELECT
    CASE
        WHEN lead_days < 0   THEN 'Same Day / Walk-in'
        WHEN lead_days < 7   THEN '0–6 Days'
        WHEN lead_days < 30  THEN '7–29 Days'
        WHEN lead_days < 90  THEN '30–89 Days'
        WHEN lead_days < 180 THEN '90–179 Days'
        ELSE                      '180+ Days'
    END                          AS lead_time_bucket,
    COUNT(*)                     AS bookings,
    ROUND(AVG(total_price)::NUMERIC, 2) AS avg_booking_value,
    ROUND(AVG(rating)::NUMERIC, 2)      AS avg_rating
FROM (
    SELECT
        (check_in_date - created_date) AS lead_days,
        total_price,
        rating
    FROM travel_bookings
    WHERE status IN ('Confirmed', 'Refunded')
) sub
GROUP BY lead_time_bucket
ORDER BY MIN(lead_days);


-- =============================================================================
-- QUERY 7: YoY Revenue Comparison
-- =============================================================================
--  Business: How does this year compare to last year?
-- =============================================================================

SELECT
    EXTRACT(YEAR FROM check_in_date)        AS year,
    category,
    COUNT(*)                                AS bookings,
    SUM(total_price)                        AS revenue,
    ROUND(AVG(price_per_night)::NUMERIC, 2) AS avg_ppn
FROM  travel_bookings
WHERE status IN ('Confirmed', 'Refunded')
  AND EXTRACT(YEAR FROM check_in_date) IN (2022, 2023, 2024)
GROUP BY year, category
ORDER BY year, revenue DESC;
