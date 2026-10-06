"""
run_pipeline.py
---------------
Entry point for the ETL pipeline.

Usage:
    python run_pipeline.py [--source local|s3] [--skip-s3-upload]

Environment variables (set in .env or export):
    DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
    AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION
    S3_BUCKET_NAME, S3_RAW_KEY, S3_CLEAN_KEY
"""

import argparse
import logging
import os
import sys
import time
from dotenv import load_dotenv

# Load .env before anything else
load_dotenv()

from etl.extract   import extract
from etl.transform import transform
from etl.load      import (
    load_to_postgres,
    save_clean_csv,
    save_rejection_log,
    upload_to_s3,
    upload_dataframe_to_s3,
)

# ── Logging setup ─────────────────────────────────────────────────────────────
os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    level   = logging.INFO,
    format  = "%(asctime)s  [%(levelname)-8s]  %(name)s — %(message)s",
    handlers = [
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(
            os.path.join("logs", "pipeline.log"),
            encoding = "utf-8",
        ),
    ],
)
logger = logging.getLogger("run_pipeline")


# ── Config builder ────────────────────────────────────────────────────────────

def build_config(args) -> dict:
    return {
        # Extract
        "source":        args.source,
        "local_path":    os.path.join("data", "raw", "travel_bookings_raw.csv"),
        "s3_bucket":     os.environ.get("S3_BUCKET_NAME", ""),
        "s3_key":        os.environ.get("S3_RAW_KEY",     "raw/travel_bookings_raw.csv"),
        "s3_cache_path": os.path.join("data", "raw", "s3_cache.csv"),

        # PostgreSQL
        "pg_host":     os.environ.get("DB_HOST",     "localhost"),
        "pg_port":     int(os.environ.get("DB_PORT", "5432")),
        "pg_dbname":   os.environ.get("DB_NAME",     "travel_db"),
        "pg_user":     os.environ.get("DB_USER",     "postgres"),
        "pg_password": os.environ.get("DB_PASSWORD", ""),

        # S3 output
        "s3_clean_key":    os.environ.get("S3_CLEAN_KEY", "clean/travel_bookings_clean.csv"),
        "skip_s3_upload":  args.skip_s3_upload,
    }


# ── Main pipeline ─────────────────────────────────────────────────────────────

def run(config: dict) -> None:
    start = time.perf_counter()
    logger.info("=" * 70)
    logger.info("  TRAVEL BOOKINGS ETL PIPELINE  —  starting")
    logger.info("=" * 70)

    # ── EXTRACT ───────────────────────────────────────────────────────────────
    logger.info("[1/4] EXTRACT")
    t0       = time.perf_counter()
    raw_df   = extract(config)
    logger.info(
        "      Extracted %d rows in %.2fs",
        len(raw_df), time.perf_counter() - t0
    )

    # Upload raw file to S3 (if enabled)
    if not config["skip_s3_upload"] and config["s3_bucket"]:
        upload_to_s3(
            config["local_path"],
            config["s3_bucket"],
            config["s3_key"],
        )

    # ── TRANSFORM ─────────────────────────────────────────────────────────────
    logger.info("[2/4] TRANSFORM")
    t0 = time.perf_counter()
    clean_df, rejected_df = transform(raw_df)
    elapsed = time.perf_counter() - t0
    logger.info(
        "      Clean: %d  |  Rejected: %d  |  Elapsed: %.2fs",
        len(clean_df), len(rejected_df), elapsed
    )

    # Save rejection log
    if len(rejected_df) > 0:
        rej_path = save_rejection_log(rejected_df, os.path.join("data", "rejected"))
        logger.info("      Rejection log → %s", rej_path)

    # Save cleaned CSV locally
    clean_path = save_clean_csv(clean_df, os.path.join("data", "clean"))

    # Upload cleaned file to S3 (if enabled)
    if not config["skip_s3_upload"] and config["s3_bucket"]:
        upload_dataframe_to_s3(
            clean_df,
            config["s3_bucket"],
            config["s3_clean_key"],
        )

    # ── LOAD ──────────────────────────────────────────────────────────────────
    logger.info("[3/4] LOAD → PostgreSQL")
    t0 = time.perf_counter()
    try:
        rows_loaded = load_to_postgres(clean_df, config)
        logger.info(
            "      Loaded %d rows in %.2fs",
            rows_loaded, time.perf_counter() - t0
        )
    except Exception as exc:
        logger.error("PostgreSQL load failed: %s", exc, exc_info=True)
        logger.warning("Data was cleaned and saved locally. Fix DB connection and retry.")
        sys.exit(1)

    # ── SUMMARY ───────────────────────────────────────────────────────────────
    total_elapsed = time.perf_counter() - start
    logger.info("[4/4] PIPELINE COMPLETE")
    logger.info("=" * 70)
    logger.info("  Total records processed : %d", len(raw_df))
    logger.info("  Records loaded to DB    : %d", rows_loaded)
    logger.info("  Records rejected        : %d", len(rejected_df))
    logger.info("  Total time              : %.2f seconds", total_elapsed)
    logger.info("=" * 70)


# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(
        description="Travel Bookings ETL Pipeline",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--source",
        choices=["local", "s3"],
        default="local",
        help="Data source: 'local' reads from data/raw/, 's3' downloads from S3",
    )
    parser.add_argument(
        "--skip-s3-upload",
        action="store_true",
        default=False,
        help="Skip uploading files to S3 (useful for local-only testing)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args   = parse_args()
    config = build_config(args)
    run(config)
