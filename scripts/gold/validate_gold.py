
import io
import logging

import pandas as pd

from scripts.common.minio_client import get_minio_client
from scripts.common.config import MINIO_BUCKET

# ============================================================
# CONFIGURATION
# ============================================================

GOLD_PREFIX = "gold/predictions_by_route/"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)


# ============================================================
# FIND LATEST GOLD FILE
# ============================================================

def get_latest_gold_file(client):
    """Find the latest Gold Parquet file in MinIO."""

    response = client.list_objects_v2(
        Bucket=MINIO_BUCKET,
        Prefix=GOLD_PREFIX
    )

    files = []

    while True:
        for obj in response.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                files.append(obj)

        if not response.get("IsTruncated"):
            break

        response = client.list_objects_v2(
            Bucket=MINIO_BUCKET,
            Prefix=GOLD_PREFIX,
            ContinuationToken=response["NextContinuationToken"]
        )

    if not files:
        logger.warning("No Gold Parquet files found.")
        return None

    latest_file = max(files, key=lambda obj: obj["LastModified"])

    return latest_file["Key"]


# ============================================================
# READ GOLD PARQUET
# ============================================================

def read_gold_file(client, file_key):
    """Read a Gold Parquet file from MinIO."""

    response = client.get_object(
        Bucket=MINIO_BUCKET,
        Key=file_key
    )

    try:
        content = response["Body"].read()

        df = pd.read_parquet(
            io.BytesIO(content)
        )

        return df

    finally:
        response["Body"].close()


# ============================================================
# VALIDATION
# ============================================================

def validate_gold():

    logger.info("=" * 60)
    logger.info("Starting Gold Validation")
    logger.info("=" * 60)

    client = get_minio_client()

    # Find latest file
    file_key = get_latest_gold_file(client)

    if file_key is None:
        logger.error("Validation stopped: no Gold file found.")
        return

    logger.info("Gold file selected: %s", file_key)

    # Read file
    df = read_gold_file(client, file_key)

    # Display data
    print("\n" + "=" * 60)
    print("GOLD DATA")
    print("=" * 60)

    print(df.to_string(index=False))

    # Display schema
    print("\n" + "=" * 60)
    print("GOLD SCHEMA")
    print("=" * 60)

    print(df.dtypes)

    # Display statistics
    print("\n" + "=" * 60)
    print("GOLD STATISTICS")
    print("=" * 60)

    print(f"Number of rows: {len(df)}")
    print(f"Number of columns: {len(df.columns)}")

    print("\nNull values:")
    print(df.isnull().sum())

    logger.info("Gold validation completed successfully.")
    logger.info("Validated file: %s", file_key)


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    validate_gold()
