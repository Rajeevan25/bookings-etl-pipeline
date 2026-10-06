"""
etl/load.py
-----------
Load layer of the ETL pipeline.

Responsibilities:
  - Load clean DataFrame into PostgreSQL (using psycopg2 + COPY for performance)
  - Save cleaned CSV to local filesystem
  - Optionally upload raw + cleaned files to AWS S3
  - Save rejection log to CSV
"""

import os
import io
import csv
import logging
from datetime import datetime

import pandas as pd
import psycopg2
from psycopg2 import sql, extras
import boto3
from botocore.exceptions import BotoCoreError, ClientError

logger = logging.getLogger(__name__)


# ── PostgreSQL helpers ────────────────────────────────────────────────────────

def _get_pg_connection(config: dict):
    """Create and return a PostgreSQL connection."""
    return psycopg2.connect(
        host     = config["pg_host"],
        port     = config.get("pg_port", 5432),
        dbname   = config["pg_dbname"],
        user     = config["pg_user"],
        password = config["pg_password"],
    )


def load_to_postgres(df: pd.DataFrame, config: dict) -> int:
    """
    Upsert clean records into PostgreSQL using COPY + ON CONFLICT DO UPDATE.
    Returns the number of rows inserted/updated.
    """
    logger.info("Connecting to PostgreSQL …")
    conn = _get_pg_connection(config)

    try:
        with conn:
            with conn.cursor() as cur:
                # Use a staging temp table for efficient bulk load
                cur.execute("""
                    CREATE TEMP TABLE IF NOT EXISTS _staging_bookings
                    (LIKE travel_bookings INCLUDING ALL)
                    ON COMMIT PRESERVE ROWS;
                """)
                cur.execute("TRUNCATE _staging_bookings;")

                # COPY from in-memory CSV buffer → fastest bulk insert method
                buffer = io.StringIO()
                df.to_csv(
                    buffer, index=False, header=False,
                    date_format="%Y-%m-%d",
                    quoting=csv.QUOTE_MINIMAL,
                )
                buffer.seek(0)

                cur.copy_expert(
                    """
                    COPY _staging_bookings (
                        booking_id, customer_name, email, phone,
                        hotel_name, category, country,
                        check_in_date, check_out_date, nights,
                        price_per_night, total_price,
                        rating, status, payment_method, created_date
                    )
                    FROM STDIN
                    WITH (FORMAT CSV, NULL '')
                    """,
                    buffer,
                )

                cur.execute("""
                    INSERT INTO travel_bookings (
                        booking_id, customer_name, email, phone,
                        hotel_name, category, country,
                        check_in_date, check_out_date, nights,
                        price_per_night, total_price,
                        rating, status, payment_method, created_date
                    )
                    SELECT
                        booking_id, customer_name, email, phone,
                        hotel_name, category, country,
                        check_in_date, check_out_date, nights,
                        price_per_night, total_price,
                        rating, status, payment_method, created_date
                    FROM _staging_bookings
                    ON CONFLICT (booking_id) DO UPDATE SET
                        customer_name  = EXCLUDED.customer_name,
                        email          = EXCLUDED.email,
                        phone          = EXCLUDED.phone,
                        hotel_name     = EXCLUDED.hotel_name,
                        category       = EXCLUDED.category,
                        country        = EXCLUDED.country,
                        check_in_date  = EXCLUDED.check_in_date,
                        check_out_date = EXCLUDED.check_out_date,
                        nights         = EXCLUDED.nights,
                        price_per_night = EXCLUDED.price_per_night,
                        total_price    = EXCLUDED.total_price,
                        rating         = EXCLUDED.rating,
                        status         = EXCLUDED.status,
                        payment_method = EXCLUDED.payment_method,
                        created_date   = EXCLUDED.created_date,
                        updated_at     = NOW();
                """)

                rows_affected = cur.rowcount
                logger.info("Upserted %d rows into travel_bookings", rows_affected)

    finally:
        conn.close()

    return rows_affected


def save_rejection_log(rejected_df: pd.DataFrame, output_dir: str) -> str:
    """Save rejected records with reasons to a timestamped CSV file."""
    os.makedirs(output_dir, exist_ok=True)
    ts   = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(output_dir, f"rejected_{ts}.csv")
    rejected_df.to_csv(path, index=False)
    logger.info("Rejection log saved: %s (%d records)", path, len(rejected_df))
    return path


def save_clean_csv(df: pd.DataFrame, output_dir: str) -> str:
    """Save cleaned DataFrame as CSV."""
    os.makedirs(output_dir, exist_ok=True)
    ts   = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(output_dir, f"travel_bookings_clean_{ts}.csv")
    df.to_csv(path, index=False, date_format="%Y-%m-%d")
    logger.info("Cleaned CSV saved: %s", path)
    return path


# ── S3 helpers ────────────────────────────────────────────────────────────────

def _get_s3_client():
    """Create S3 client using environment credentials (no hardcoded secrets)."""
    return boto3.client(
        "s3",
        aws_access_key_id     = os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key = os.environ["AWS_SECRET_ACCESS_KEY"],
        region_name           = os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
    )


def upload_to_s3(local_path: str, bucket: str, s3_key: str) -> bool:
    """Upload a local file to S3. Returns True on success."""
    try:
        s3 = _get_s3_client()
        logger.info("Uploading %s → s3://%s/%s", local_path, bucket, s3_key)
        s3.upload_file(local_path, bucket, s3_key)
        logger.info("Upload successful.")
        return True
    except (BotoCoreError, ClientError) as e:
        logger.error("S3 upload failed: %s", e)
        return False


def upload_dataframe_to_s3(df: pd.DataFrame, bucket: str, s3_key: str) -> bool:
    """Serialize DataFrame to CSV in-memory and upload directly to S3."""
    try:
        s3      = _get_s3_client()
        buffer  = io.StringIO()
        df.to_csv(buffer, index=False, date_format="%Y-%m-%d")
        body    = buffer.getvalue().encode("utf-8")

        logger.info("Uploading DataFrame → s3://%s/%s", bucket, s3_key)
        s3.put_object(
            Bucket      = bucket,
            Key         = s3_key,
            Body        = body,
            ContentType = "text/csv",
        )
        logger.info("Upload successful.")
        return True
    except (BotoCoreError, ClientError) as e:
        logger.error("S3 upload failed: %s", e)
        return False
