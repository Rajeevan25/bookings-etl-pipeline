# Travel Bookings — Data Engineer Technical Assessment

> **Candidate:** Rajeevan
> **Position:** Associate Data Engineer — Luxury Explorers
> **Repository:** https://github.com/Rajeevan25/bookings-etl-pipeline
> **Domain:** Travel Booking Transactions
> **Stack:** Python 3.11 · PostgreSQL 15 · AWS S3 · pandas

---

## Table of Contents
1. [Project Overview](#1-project-overview)
2. [Repository Structure](#2-repository-structure)
3. [Dataset Description](#3-dataset-description)
4. [Quick Start](#4-quick-start)
5. [ETL Pipeline Deep-Dive](#5-etl-pipeline-deep-dive)
6. [Database Design](#6-database-design)
7. [Performance & Indexing](#7-performance--indexing)
8. [AWS Integration](#8-aws-integration)
9. [Analytical Queries](#9-analytical-queries)
10. [Scalability & Architecture Thinking](#10-scalability--architecture-thinking)
11. [Testing](#11-testing)
12. [Screen Recording Guide](#12-screen-recording-guide-1015-min)

---

## 1. Project Overview

This project implements a complete **Extract → Transform → Load (ETL)** data pipeline for a travel bookings business domain.

| Component | Technology |
|-----------|-----------|
| Dataset   | Python-generated synthetic CSV (12,300 records) |
| ETL Engine | Python 3.11 + pandas |
| Target DB | PostgreSQL 15 |
| Cloud Storage | AWS S3 (Free Tier) |
| Auth / Secrets | IAM least-privilege + `.env` |
| Logging | Python `logging` module → `logs/pipeline.log` |

The pipeline is **idempotent** — running it multiple times will upsert records without creating duplicates.

---

## 2. Repository Structure

```
data-engineer/
│
├── generate_dataset.py          # Step 0: Generate raw dirty dataset
├── run_pipeline.py              # Step 1: Run full ETL pipeline
│
├── etl/
│   ├── __init__.py
│   ├── extract.py               # E: Read from local or S3
│   ├── transform.py             # T: Clean, validate, standardize
│   └── load.py                  # L: Write to PostgreSQL + S3
│
├── sql/
│   ├── 01_schema.sql            # Table definitions + ENUM types
│   ├── 02_indexes.sql           # All performance indexes
│   ├── 03_queries.sql           # 7 analytical queries with EXPLAIN
│   └── 04_setup_db.sql          # One-shot DB + user setup
│
├── data/
│   ├── raw/                     # Raw CSV (generated)
│   ├── clean/                   # Cleaned CSV output
│   └── rejected/                # Rejection logs (timestamped)
│
├── logs/
│   └── pipeline.log             # Pipeline run log
│
├── .env.example                 # Environment variable template
├── requirements.txt             # Pinned Python dependencies
└── README.md                    # This file
```

---

## 3. Dataset Description

**Domain:** Travel / Hotel Bookings
**Records:** ~12,000 raw → ~10,500–11,000 clean (after rejections)
**File:** `data/raw/travel_bookings_raw.csv`

### Schema

| Column | Type | Description | Dirty Issues Introduced |
|--------|------|-------------|------------------------|
| `booking_id` | string | Unique booking ref (BKxxxxxx) | None (anchor key) |
| `customer_name` | string | Guest full name | Mixed case, LAST/FIRST order, nulls |
| `email` | string | Guest email | `" at "` instead of `@`, nulls |
| `phone` | string | Guest phone | Inconsistent formats, nulls |
| `hotel_name` | string | Property name | None |
| `category` | string | Property type | Mixed case (hotel/HOTEL/Hotel) |
| `country` | string | Country name | Abbreviations (UAE/uae/United Arab Emirates) |
| `check_in_date` | date | Arrival date | 3 different date formats |
| `check_out_date` | date | Departure date | Consistent (ISO) |
| `nights` | integer | Nights stayed | Negative values, nulls |
| `price_per_night` | decimal | Nightly rate (USD) | `$` prefix, European comma decimal |
| `total_price` | decimal | Total charge | Nulls, minor discrepancies |
| `rating` | decimal | Guest rating 1–5 | Out-of-range (×2 scale), nulls |
| `status` | string | Booking status | Mixed case (confirmed/CONFIRMED/Confirmed) |
| `payment_method` | string | Payment type | Mixed naming, nulls |
| `created_date` | date | Record created | Multiple formats |

### Dirty Data Summary
| Issue | Approx. Count |
|-------|-------------|
| Exact duplicates (by booking_id) | ~300 |
| Missing customer_name | ~4% |
| Missing email | ~6% |
| Missing total_price | ~3% |
| Invalid rating (out of 1–5) | ~8% |
| Negative nights | ~5% |
| Inconsistent date formats | ~15% |
| Currency symbols in price | ~10% |
| Country abbreviations | ~30% |
| Invalid email format (` at ` instead of `@`) | ~8% |

---

## 4. Quick Start

### Prerequisites
- Python 3.11+
- PostgreSQL 15 running locally
- AWS account (Free Tier) with S3 bucket created
- Git

### Installation

```bash
# 1. Clone
git clone https://github.com/<your-username>/data-engineer-assessment.git
cd data-engineer-assessment

# 2. Create virtual environment
python -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Configure environment
cp .env.example .env
# Edit .env with your DB credentials and AWS keys
```

### Database Setup

```bash
# Run as PostgreSQL superuser
psql -U postgres -f sql/04_setup_db.sql
```

This will:
- Create database `travel_db`
- Create user `etl_user` with least-privilege grants
- Apply schema + indexes

### Run the Pipeline

```bash
# Step 1: Generate the raw dataset
python generate_dataset.py

# Step 2: Run full ETL pipeline (local source, skip S3 upload for testing)
python run_pipeline.py --source local --skip-s3-upload

# Step 2b: Run with S3 integration
python run_pipeline.py --source local
# OR download raw data from S3 first:
python run_pipeline.py --source s3
```

### Verify in PostgreSQL

```sql
\c travel_db
SELECT COUNT(*) FROM travel_bookings;
SELECT status, COUNT(*) FROM travel_bookings GROUP BY status;
```

---

## 5. ETL Pipeline Deep-Dive

### Architecture

```
[Raw CSV / S3]
      │
      ▼ extract.py
┌─────────────────────┐
│  EXTRACT             │  • Read CSV (local or download from S3)
│  pandas DataFrame    │  • All columns as dtype=str (no premature coercion)
└─────────┬───────────┘
          │
          ▼ transform.py
┌─────────────────────┐
│  TRANSFORM           │
│  1. Normalize cols   │  Column names → lowercase strip
│  2. Strip whitespace │  All string cells
│  3. Map categories   │  hotel/HOTEL/Hotel → "Hotel"
│  4. Map countries    │  UAE/uae → "United Arab Emirates"
│  5. Map statuses     │  confirmed/CONFIRMED → "Confirmed"
│  6. Parse dates      │  3 formats → ISO datetime
│  7. Clean prices     │  Strip $, fix comma decimal
│  8. Cast numerics    │  nights, rating → proper types
│  9. Validate rows    │  Reject invalid → rejection log
│ 10. Deduplicate      │  By booking_id
│ 11. Derive total     │  Recalculate where missing/wrong
└─────────┬───────────┘
          │
          ├──→ [Rejection Log CSV]  (data/rejected/rejected_YYYYMMDD_HHMMSS.csv)
          │
          ▼ load.py
┌─────────────────────┐
│  LOAD                │
│  • PostgreSQL COPY   │  Bulk upsert via temp staging table
│  • Rejections → DB   │  etl_rejected_records table
│  • Run log → DB      │  etl_run_log table
│  • Clean CSV save    │  data/clean/
│  • S3 upload (opt.)  │  Raw + clean files
└─────────────────────┘
```

### Key Design Decisions

**1. COPY vs INSERT for PostgreSQL:**
We use `COPY` + staging table + `INSERT … ON CONFLICT` instead of row-by-row `INSERT`. This is **5–50× faster** for large datasets because COPY bypasses per-row planning overhead.

**2. Reject-early strategy:**
Invalid rows are logged to `data/rejected/` with a `rejection_reason` column **and** persisted to the `etl_rejected_records` table in PostgreSQL as JSONB. Duplicates are also routed to the rejection trail rather than being silently dropped. This prevents bad data from corrupting the DB while giving engineers full visibility into data quality.

**3. Idempotency:**
`ON CONFLICT (booking_id) DO UPDATE` ensures re-running the pipeline never creates duplicates. This makes the pipeline safe to run on cron or retry on failure.

**4. All-string extraction:**
Reading CSV with `dtype=str` prevents pandas from making wrong type assumptions (e.g., treating "1,500.00" as a string vs. truncating it). Type conversion happens explicitly in the transform layer.

---

## 6. Database Design

### Schema Highlights

```sql
CREATE TABLE travel_bookings (
    id               BIGSERIAL PRIMARY KEY,
    booking_id       VARCHAR(20) NOT NULL UNIQUE,   -- business key
    customer_name    VARCHAR(255) NOT NULL,
    ...
    price_per_night  NUMERIC(10,2) NOT NULL CHECK (price_per_night > 0),
    total_price      NUMERIC(12,2) NOT NULL CHECK (total_price > 0),
    nights           SMALLINT NOT NULL CHECK (nights > 0),
    rating           NUMERIC(3,1) CHECK (rating BETWEEN 1.0 AND 5.0),
    category         booking_category NOT NULL,      -- ENUM type
    status           booking_status NOT NULL,         -- ENUM type
    ...
    CONSTRAINT chk_checkout_after_checkin CHECK (check_out_date > check_in_date),
    CONSTRAINT chk_total_price_consistency CHECK (
        ABS(total_price - nights * price_per_night)
        / (nights * price_per_night) <= 0.05
    )
);
```

**ENUM types** (`booking_status`, `booking_category`, `payment_method_type`) enforce data integrity at the DB level — invalid values raise an error before they touch the table.

**Cross-column constraints** validate business logic (checkout > checkin, total ≈ nights × rate).

**Two audit tables** track pipeline health:
- `etl_rejected_records` — rejected rows with raw data + reason
- `etl_run_log` — per-run statistics and status

---

## 7. Performance & Indexing

### Index Strategy

All indexes use **covering** (`INCLUDE`) and **partial** (`WHERE`) clauses to enable **Index Only Scans** and reduce index bloat:

| Index | Type | Key | INCLUDE | Partial WHERE | Purpose |
|-------|------|-----|---------|---------------|--------|
| `uq_booking_id` | UNIQUE B-Tree | `booking_id` | — | — | PK lookup, dedup |
| `idx_bookings_category_revenue` | **Partial Covering** | `category` | `total_price, nights, rating` | `status IN ('Confirmed','Refunded')` | Revenue by category (Index Only Scan) |
| `idx_bookings_country` | B-Tree | `country` | — | — | Country aggregations |
| `idx_bookings_checkin_date` | B-Tree | `check_in_date` | — | — | Date-range filters |
| `idx_bookings_month_revenue` | **Partial Covering** | `check_in_date` | `total_price, country` | `status IN ('Confirmed','Refunded')` | Monthly growth (Index Only Scan) |
| `idx_bookings_country_rating` | **Partial Covering** | `country` | `rating, total_price` | `rating IS NOT NULL AND status = 'Confirmed'` | Rating by country (Index Only Scan) |
| `idx_bookings_hotel_trgm` | **GIN** (trigram) | `hotel_name` | — | — | Fuzzy ILIKE search |
| `idx_bookings_created_date` | B-Tree | `created_date DESC` | — | — | Recent records |

### Verified EXPLAIN ANALYZE Benchmarks (10,450 rows loaded)

| Query | Description | Execution Time | Scan Type |
|-------|-------------|---------------|----------|
| Q1 | Revenue by category | **~2ms** | Index Only Scan on `idx_bookings_category_revenue` |
| Q2 | Monthly revenue growth | **~6ms** | Index Only Scan on `idx_bookings_month_revenue` |
| Q3 | Avg rating by country | **~1ms** | Index Only Scan on `idx_bookings_country_rating` |

All three core analytical queries achieve **Index Only Scans** with **zero heap fetches**, meaning PostgreSQL satisfies them entirely from the index without touching the table.

---

## 8. AWS Integration

### Architecture

```
┌─────────────────────┐
│  Local Machine       │
│  generate_dataset.py │──→  data/raw/travel_bookings_raw.csv
└─────────────────────┘
           │
           ▼  upload_to_s3()  (load.py)
┌─────────────────────────────────────────┐
│  AWS S3 Bucket: your-travel-etl-bucket  │
│  ├── raw/travel_bookings_raw.csv        │
│  └── clean/travel_bookings_clean.csv    │
└─────────────────────────────────────────┘
           │
           ▼  (optional: source='s3')
┌─────────────────────┐
│  extract_from_s3()   │──→  Transform  ──→  PostgreSQL
└─────────────────────┘
```

### IAM Policy (Least Privilege)

The IAM user `etl-pipeline-user` should have **only** this policy:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "S3ETLReadWrite",
            "Effect": "Allow",
            "Action": ["s3:GetObject", "s3:PutObject"],
            "Resource": ["arn:aws:s3:::your-travel-etl-bucket/*"]
        },
        {
            "Sid": "S3ETLList",
            "Effect": "Allow",
            "Action": ["s3:ListBucket"],
            "Resource": ["arn:aws:s3:::your-travel-etl-bucket"]
        }
    ]
}
```

Object-level actions (`GetObject`, `PutObject`) are scoped to `bucket/*`; bucket-level actions (`ListBucket`) to the bucket ARN only. No `DeleteObject`, `CreateBucket`, or admin permissions are granted.

**No hardcoded credentials** — all AWS keys are read from environment variables via `python-dotenv`.

---

## 9. Analytical Queries

All queries are in [`sql/03_queries.sql`](sql/03_queries.sql). Summary:

| # | Query | Key Insight |
|---|-------|------------|
| 1 | Top 10 Categories by Revenue | Uses partial index; shows category revenue ranking |
| 2 | Monthly Revenue Growth | LAG() window function; MoM growth % |
| 3 | Avg Rating by Country | Partial index on non-null ratings; HAVING > 5 bookings |
| 4 | Top 10 Revenue Hotels | Individual property performance |
| 5 | Payment Method Distribution | Revenue share + booking share |
| 6 | Booking Lead Time Buckets | Customer booking behavior |
| 7 | YoY Revenue Comparison | Year-over-year by category |

---

## 10. Scalability & Architecture Thinking

### Scaling to 1 Million+ Records

**Problem areas at scale:**
1. CSV read → memory pressure
2. Single-process transformation
3. Full-table COPY → lock contention

**Solutions:**

| Challenge | Solution |
|-----------|---------|
| Large files | Chunked reads with `pandas.read_csv(chunksize=50_000)` |
| Transform bottleneck | Parallelize chunks with `concurrent.futures.ProcessPoolExecutor` |
| DB write speed | PostgreSQL `COPY` with partition routing; increase `work_mem` |
| File storage | Store in S3 using **Parquet** (columnar, 5–10× smaller than CSV) |
| Schema scale | Partition `travel_bookings` by `RANGE(check_in_date)` — one partition per quarter |

### Scheduling Strategy

```
# Simple: cron (daily at 2 AM UTC)
0 2 * * * /path/to/venv/bin/python /app/run_pipeline.py --source s3

# Enterprise: Apache Airflow DAG
from airflow import DAG
from airflow.operators.python import PythonOperator

dag = DAG('travel_etl', schedule_interval='@daily', ...)
extract_task   = PythonOperator(task_id='extract',   python_callable=extract,   dag=dag)
transform_task = PythonOperator(task_id='transform', python_callable=transform, dag=dag)
load_task      = PythonOperator(task_id='load',      python_callable=load,      dag=dag)

extract_task >> transform_task >> load_task
```

### Partitioning Strategy (at Scale)

```sql
-- Partition by quarter for fast date-range queries
CREATE TABLE travel_bookings_2024_q1
    PARTITION OF travel_bookings
    FOR VALUES FROM ('2024-01-01') TO ('2024-04-01');

-- Each partition has its own local indexes → faster scans
-- Old partitions can be archived to S3 (Glacier) → cost savings
```

### Failure Handling

| Failure Scenario | Strategy |
|-----------------|---------|
| S3 download fails | Retry with exponential backoff (3 attempts) |
| Transform error (single row) | Log to rejection table, continue pipeline |
| DB connection lost | Reconnect with retry; pipeline is idempotent → safe to re-run |
| Partial load | Staging table rolled back automatically; source data intact in S3 |
| Full pipeline crash | `etl_run_log` records `FAILURE`; alert triggers re-run |

The pipeline is designed for **at-least-once delivery** with idempotency — reprocessing the same data never duplicates records.

---

## 11. Testing

```bash
python -m pytest tests/ -v -p no:asyncio
```

**34 tests** covering:

| Test Suite | Tests | Coverage |
|------------|-------|----------|
| `test_transform.py` | 21 | Category, country, price, date, rating, nights, name, email |
| `test_pipeline_integration.py` | 13 | Row count reconciliation, duplicate routing, multi-row dirty data, total price clamping, rejection reasons |

Key integration tests verify that **input rows = clean rows + rejected rows** (no silent drops), and that duplicates are routed to the rejection trail with an explicit reason.

---

## 12. Screen Recording Guide (10–15 min)

> **Tools:** OBS Studio (free) or any screen recorder. Open a terminal, pgAdmin / psql, and your S3 console side-by-side.

### Segment 1 — Repository Tour (0:00 – 1:30)
- Show the GitHub repo at https://github.com/Rajeevan25/bookings-etl-pipeline
- Walk through the directory tree: `etl/`, `sql/`, `aws/`, `data/`, `tests/`
- Briefly open `requirements.txt` and `.env.example`
- **Say:** *"This pipeline follows a clean ETL separation. All secrets are managed via environment variables — nothing is hardcoded."*

### Segment 2 — Raw Dataset (1:30 – 3:00)
- Run the dataset generator:
  ```bash
  python generate_dataset.py
  ```
- Open `data/raw/travel_bookings_raw.csv` in a spreadsheet or `head -n 20`:
  ```bash
  python -c "import pandas as pd; df=pd.read_csv('data/raw/travel_bookings_raw.csv'); print(df.head(20).to_string())"
  ```
- **Point out visible dirty data:**
  - Mixed-case countries: `UAE`, `uae`, `United Arab Emirates`
  - Currency symbols in price: `$150.00`
  - Email `" at "` instead of `@`
  - Negative nights, invalid ratings (> 5)
  - Multiple date formats (`2024-01-15` vs `15/01/2024`)
- **Say:** *"We have 12,300 raw records. They contain ~10 categories of data quality issues which our transform layer will address."*

### Segment 3 — ETL Execution (3:00 – 6:00)
- Run the full pipeline (local, skip S3 for now):
  ```bash
  python run_pipeline.py --source local --skip-s3-upload
  ```
- **Narrate each phase as it logs:**
  - `[1/4] EXTRACT` — show row count extracted
  - `[2/4] TRANSFORM` — show clean vs rejected counts
  - `[3/4] LOAD → PostgreSQL` — show rows upserted
  - `[4/4] PIPELINE COMPLETE` — show total time
- Open `logs/pipeline.log` and scroll through to show the structured log output
- Open `data/rejected/rejected_YYYYMMDD_HHMMSS.csv` and show 5-6 rows with their `rejection_reason` column
- **Say:** *"The pipeline is idempotent — run it again and it upserts without duplicating records."*

### Segment 4 — Cleaned Data in PostgreSQL (6:00 – 9:00)
Connect via psql or pgAdmin:
```bash
psql -U etl_user -d travel_db
```
Run these queries and show results:
```sql
-- Record count
SELECT COUNT(*) FROM travel_bookings;

-- Status distribution
SELECT status, COUNT(*) FROM travel_bookings GROUP BY status ORDER BY count DESC;

-- Sample of clean data
SELECT booking_id, customer_name, country, category, check_in_date, nights, total_price, status
FROM travel_bookings LIMIT 10;

-- ETL audit log
SELECT run_at, records_extracted, records_loaded, records_rejected, duration_seconds, status
FROM etl_run_log ORDER BY run_at DESC LIMIT 3;

-- Rejection log sample
SELECT raw_booking_id, rejection_reason FROM etl_rejected_records LIMIT 5;
```
- **Say:** *"Notice the ENUM types enforce data integrity at DB level. Cross-column constraints validate checkout > checkin and total_price ≈ nights × rate."*

### Segment 5 — Analytical Query Outputs (9:00 – 11:30)
Run queries from `sql/03_queries.sql`:
```sql
-- Q1: Top categories by revenue
SELECT category, COUNT(*) AS bookings, SUM(total_price) AS total_revenue,
       ROUND(AVG(rating)::NUMERIC, 2) AS avg_rating
FROM travel_bookings WHERE status IN ('Confirmed', 'Refunded')
GROUP BY category ORDER BY total_revenue DESC;

-- Q2: Monthly revenue growth (LAG window function)
WITH monthly AS (
    SELECT DATE_TRUNC('month', check_in_date) AS month, SUM(total_price) AS revenue
    FROM travel_bookings WHERE status IN ('Confirmed', 'Refunded')
    GROUP BY 1
)
SELECT TO_CHAR(month,'YYYY-MM'), revenue,
       ROUND((revenue - LAG(revenue) OVER (ORDER BY month))
             / NULLIF(LAG(revenue) OVER (ORDER BY month),0)*100, 2) AS growth_pct
FROM monthly ORDER BY month;

-- Q3: Average rating by country (uses partial covering index)
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT country, ROUND(AVG(rating)::NUMERIC,2) AS avg_rating, COUNT(*) AS bookings
FROM travel_bookings WHERE rating IS NOT NULL AND status='Confirmed'
GROUP BY country HAVING COUNT(*) >= 5 ORDER BY avg_rating DESC;
```
- **Point at `EXPLAIN ANALYZE` output:** highlight `Index Only Scan` and `~1ms` execution time
- **Say:** *"Partial covering indexes allow PostgreSQL to satisfy these queries entirely from the index — zero heap fetches."*

### Segment 6 — S3 Integration (11:30 – 13:30)
- Show `.env` with `S3_BUCKET_NAME` set (blur the actual key values on screen)
- Show the bucket in AWS Console (or run via CLI)
- Run the pipeline WITH S3 upload:
  ```bash
  python run_pipeline.py --source local
  ```
- After completion, go to AWS S3 Console and show:
  - `s3://<bucket>/raw/travel_bookings_raw.csv`
  - `s3://<bucket>/clean/travel_bookings_clean.csv`
- **Optionally** run from S3 as source:
  ```bash
  python run_pipeline.py --source s3 --skip-s3-upload
  ```
- Open `aws/iam_policy.json` and explain:
  ```bash
  type aws\iam_policy.json
  ```
  - *"GetObject + PutObject scoped to bucket/* only — no DeleteObject, no admin rights."*

### Segment 7 — Architecture & Optimization Decisions (13:30 – 15:00)
- Open `etl/load.py` and point to the COPY + staging table pattern:
  - *"COPY into a temp table then INSERT … ON CONFLICT is 5–50× faster than row-by-row INSERT"*
- Open `sql/02_indexes.sql` and explain 3 key indexes:
  - Partial index (reduces bloat — only Confirmed/Refunded rows indexed)
  - Covering index (INCLUDE columns eliminate heap fetches → Index Only Scan)
  - GIN trigram index on hotel_name (fuzzy LIKE search)
- Open `etl/transform.py` lines 190-232 and point to vectorized validation:
  - *"All validation runs as vectorized boolean masks — no Python loops over rows"*
- Final summary:
  - *"All-string extraction → no premature type coercion"*
  - *"Reject-and-log strategy → bad data never touches the main table"*
  - *"Idempotent upsert → safe to re-run on cron or after failure"*

---
