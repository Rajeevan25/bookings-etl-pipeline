"""
aws/create_s3_bucket.py
-----------------------
Utility script to:
  1. Create the S3 bucket (if it doesn't exist)
  2. Enable versioning (for audit trail)
  3. Block all public access (security baseline)
  4. Create the raw/ and clean/ "folders" (prefixes)

Usage:
    python aws/create_s3_bucket.py

Requires:
    AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION, S3_BUCKET_NAME
    set in your .env file or environment.
"""

import os
import json
import logging
from dotenv import load_dotenv
import boto3
from botocore.exceptions import ClientError

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s")
logger = logging.getLogger(__name__)


def create_bucket(s3, bucket_name: str, region: str) -> bool:
    try:
        if region == "us-east-1":
            s3.create_bucket(Bucket=bucket_name)
        else:
            s3.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={"LocationConstraint": region},
            )
        logger.info("✅ Bucket created: s3://%s", bucket_name)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "BucketAlreadyOwnedByYou":
            logger.info("Bucket already exists (owned by you): %s", bucket_name)
            return True
        logger.error("Failed to create bucket: %s", e)
        return False


def enable_versioning(s3, bucket_name: str):
    s3.put_bucket_versioning(
        Bucket=bucket_name,
        VersioningConfiguration={"Status": "Enabled"},
    )
    logger.info("✅ Versioning enabled on %s", bucket_name)


def block_public_access(s3, bucket_name: str):
    s3.put_public_access_block(
        Bucket=bucket_name,
        PublicAccessBlockConfiguration={
            "BlockPublicAcls":       True,
            "IgnorePublicAcls":      True,
            "BlockPublicPolicy":     True,
            "RestrictPublicBuckets": True,
        },
    )
    logger.info("✅ Public access blocked on %s", bucket_name)


def create_prefix(s3, bucket_name: str, prefix: str):
    """Create an empty key to simulate a 'folder'."""
    s3.put_object(Bucket=bucket_name, Key=f"{prefix}/")
    logger.info("   Created prefix: s3://%s/%s/", bucket_name, prefix)


def main():
    bucket_name = os.environ.get("S3_BUCKET_NAME")
    region      = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")

    if not bucket_name:
        raise ValueError("S3_BUCKET_NAME not set in environment / .env")

    s3 = boto3.client(
        "s3",
        aws_access_key_id     = os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key = os.environ["AWS_SECRET_ACCESS_KEY"],
        region_name           = region,
    )

    logger.info("Setting up S3 bucket: %s (region: %s)", bucket_name, region)

    if not create_bucket(s3, bucket_name, region):
        return

    block_public_access(s3, bucket_name)
    enable_versioning(s3, bucket_name)

    for prefix in ["raw", "clean", "rejected"]:
        create_prefix(s3, bucket_name, prefix)

    logger.info("=" * 50)
    logger.info("S3 bucket ready: s3://%s", bucket_name)
    logger.info("  raw/     → upload raw CSV here")
    logger.info("  clean/   → pipeline writes cleaned CSV here")
    logger.info("  rejected/→ pipeline writes rejection logs here")


if __name__ == "__main__":
    main()
