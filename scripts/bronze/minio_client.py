import boto3
from botocore.client import Config

from config import (
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
    MINIO_BUCKET,
    validate_config,
)


def get_minio_client():
    """Create and return a MinIO S3 client."""

    validate_config()

    client = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
        ),
    )

    return client


def bucket_exists(client, bucket_name=MINIO_BUCKET):
    """Check whether the bucket exists."""

    buckets = client.list_buckets()

    return any(
        bucket["Name"] == bucket_name
        for bucket in buckets["Buckets"]
    )


if __name__ == "__main__":
    client = get_minio_client()

    print("MinIO client created successfully!")
    print(f"Endpoint: {MINIO_ENDPOINT}")
    print(f"Target bucket: {MINIO_BUCKET}")