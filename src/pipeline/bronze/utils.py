# src/consumers/utils.py

import boto3
from botocore.client import Config


def get_s3_client():
    from ingestion.config import S3_ACCESS_KEY, S3_ENDPOINT_URL, S3_SECRET_KEY
    """Client boto3 partagé, configuré pour MinIO (S3-compatible)."""
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT_URL,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )