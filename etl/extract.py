"""
etl/extract.py
--------------
Extraction layer of the ETL pipeline.

Responsibilities:
  - Read raw CSV from local filesystem  OR  download from AWS S3
  - Return a raw pandas DataFrame for the Transform stage
  - Log basic extraction stats
"""

import os
import logging
import pandas as pd
import boto3
from botocore.exceptions import BotoCoreError, ClientError

logger = logging.getLogger(__name__)


def extract_from_local(file_path: str) -> pd.DataFrame:
    """Read raw CSV from local filesystem."""
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Raw data file not found: {file_path}")

    logger.info("Extracting from local file: %s", file_path)
    df = pd.read_csv(file_path, dtype=str, keep_default_na=False)
    logger.info("Extracted %d records, %d columns", len(df), len(df.columns))
    return df


def extract_from_s3(bucket: str, key: str, local_cache_path: str) -> pd.DataFrame:
    """
    Download raw CSV from S3 to a local cache path, then read it.
    Uses IAM credentials from environment variables (no hardcoded secrets).
    """
    aws_access_key = os.environ.get("AWS_ACCESS_KEY_ID")
    aws_secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
    aws_region     = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")

    if not aws_access_key or not aws_secret_key:
        raise EnvironmentError(
            "AWS credentials not found. "
            "Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables."
        )

    logger.info("Connecting to S3 bucket='%s', key='%s'", bucket, key)

    try:
        s3_client = boto3.client(
            "s3",
            aws_access_key_id     = aws_access_key,
            aws_secret_access_key = aws_secret_key,
            region_name           = aws_region,
        )

        os.makedirs(os.path.dirname(local_cache_path), exist_ok=True)
        s3_client.download_file(bucket, key, local_cache_path)
        logger.info("Downloaded S3 object to: %s", local_cache_path)

    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        logger.error("S3 ClientError [%s]: %s", error_code, e)
        raise
    except BotoCoreError as e:
        logger.error("BotoCoreError: %s", e)
        raise

    return extract_from_local(local_cache_path)


def extract(config: dict) -> pd.DataFrame:
    """
    Main extract entry point.

    Config keys:
        source        : 'local' | 's3'
        local_path    : path to local CSV file
        s3_bucket     : S3 bucket name
        s3_key        : S3 object key
        s3_cache_path : local path to cache downloaded file
    """
    source = config.get("source", "local")

    if source == "s3":
        return extract_from_s3(
            bucket           = config["s3_bucket"],
            key              = config["s3_key"],
            local_cache_path = config.get("s3_cache_path", "data/raw/s3_cache.csv"),
        )
    else:
        return extract_from_local(config["local_path"])
