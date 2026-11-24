
import json
import sys
import io
import logging
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# ============================================================
# CONFIGURATION
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]
COMMON_DIR = PROJECT_ROOT / "scripts" / "common"

# Reuse shared MinIO configuration and client
sys.path.insert(0, str(COMMON_DIR))

from minio_client import get_minio_client
from config import MINIO_BUCKET

BRONZE_PREFIX = "bronze/predictions_arrets/"
SILVER_PREFIX = "silver/predictions_arrets/"

LOG_LEVEL = logging.INFO

# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)

# ============================================================
# SILVER SCHEMA
# ============================================================

SILVER_COLUMNS = [
    "source",
    "prediction_id",
    "schedule_id",
    "service_date",
    "route_id",
    "trip_id",
    "stop_id",
    "stop_sequence",
    "vehicle_id",
    "predicted_arrival",
    "predicted_departure",
    "event_id",
    "ingested_at",
    "kafka_topic",
    "kafka_partition",
    "kafka_offset",
    "kafka_timestamp",
]

STRING_COLUMNS = [
    "source",
    "prediction_id",
    "schedule_id",
    "route_id",
    "trip_id",
    "stop_id",
    "vehicle_id",
    "event_id",
    "kafka_topic",
]

INTEGER_COLUMNS = [
    "stop_sequence",
    "kafka_partition",
    "kafka_offset",
]

TIMESTAMP_COLUMNS = [
    "predicted_arrival",
    "predicted_departure",
    "ingested_at",
    "kafka_timestamp",
]

# ============================================================
# MINIO
# ============================================================

def get_minio():
    """Return the existing MinIO client."""
    return get_minio_client()


def list_bronze_files(client):
    """List all JSONL files from the Bronze predictions prefix."""

    response = client.list_objects_v2(
        Bucket=MINIO_BUCKET,
        Prefix=BRONZE_PREFIX
    )

    files = []

    while True:
        for obj in response.get("Contents", []):
            if obj["Key"].endswith(".jsonl"):
                files.append(obj["Key"])

        if not response.get("IsTruncated"):
            break

        response = client.list_objects_v2(
            Bucket=MINIO_BUCKET,
            Prefix=BRONZE_PREFIX,
            ContinuationToken=response["NextContinuationToken"]
        )

    return files


# ============================================================
# BRONZE READING
# ============================================================

def read_jsonl_from_minio(client, object_key):
    """Read a JSONL file from MinIO."""

    response = client.get_object(
        Bucket=MINIO_BUCKET,
        Key=object_key
    )

    try:
        content = response["Body"].read().decode("utf-8")
    finally:
        response["Body"].close()

    records = []

    for line_number, line in enumerate(content.splitlines(), start=1):

        if not line.strip():
            continue

        try:
            record = json.loads(line)
            records.append(record)

        except json.JSONDecodeError as error:
            logger.warning(
                "Invalid JSON in %s at line %s: %s",
                object_key,
                line_number,
                error
            )

    return records


def read_all_bronze_data(client):
    """Read all Bronze JSONL files."""

    files = list_bronze_files(client)

    logger.info("Bronze files found: %s", len(files))

    all_records = []

    for file_key in files:

        logger.info("Reading Bronze file: %s", file_key)

        records = read_jsonl_from_minio(client, file_key)

        all_records.extend(records)

        logger.info("Records loaded: %s", len(records))

    logger.info("Total Bronze records loaded: %s", len(all_records))

    return all_records


# ============================================================
# FLATTENING
# ============================================================

def flatten_record(record):
    """Flatten data and Kafka metadata into a single row."""

    data = record.get("data") or {}
    metadata = record.get("kafka_metadata") or {}

    row = {}

    # Business data
    for column in SILVER_COLUMNS:
        if column in data:
            row[column] = data[column]

    # Kafka metadata
    row["kafka_topic"] = metadata.get("topic")
    row["kafka_partition"] = metadata.get("partition")
    row["kafka_offset"] = metadata.get("offset")

    kafka_timestamp = metadata.get("timestamp")

    if kafka_timestamp is not None:
        try:
            row["kafka_timestamp"] = datetime.fromtimestamp(
                int(kafka_timestamp) / 1000,
                tz=timezone.utc
            )
        except (ValueError, TypeError, OverflowError):
            row["kafka_timestamp"] = None

    return row


def flatten_records(records):
    """Convert nested Bronze records into flat rows."""

    rows = [flatten_record(record) for record in records]

    return pd.DataFrame(rows)


# ============================================================
# DATA CLEANING
# ============================================================

def normalize_data(df):
    """Normalize Silver columns and data types."""

    if df.empty:
        return df

    # Ensure all expected columns exist
    for column in SILVER_COLUMNS:
        if column not in df.columns:
            df[column] = None

    df = df[SILVER_COLUMNS].copy()

    # String normalization
    for column in STRING_COLUMNS:

        df[column] = df[column].astype("string")

        df[column] = df[column].str.strip()

        df[column] = df[column].replace("", pd.NA)

    # Integer normalization
    for column in INTEGER_COLUMNS:

        df[column] = pd.to_numeric(
            df[column],
            errors="coerce"
        ).astype("Int64")

    # Date normalization
    df["service_date"] = pd.to_datetime(
        df["service_date"],
        errors="coerce"
    ).dt.date

    # Timestamp normalization
    for column in TIMESTAMP_COLUMNS:

        df[column] = pd.to_datetime(
            df[column],
            errors="coerce",
            utc=True
        )

    return df


# ============================================================
# DEDUPLICATION
# ============================================================
def deduplicate_data(df):
    """Remove duplicate Kafka records with detailed diagnostics."""

    if df.empty:
        return df

    initial_count = len(df)

    print("\n" + "=" * 50)
    print("DEDUPLICATION DIAGNOSTICS")
    print("=" * 50)

    print(f"Records before deduplication: {initial_count}")

    kafka_key_columns = [
        "kafka_topic",
        "kafka_partition",
        "kafka_offset"
    ]

    if all(column in df.columns for column in kafka_key_columns):

        kafka_mask = df[kafka_key_columns].notna().all(axis=1)

        kafka_records = df[kafka_mask].copy()
        non_kafka_records = df[~kafka_mask].copy()

        print(f"Records with Kafka metadata: {len(kafka_records)}")
        print(f"Records without Kafka metadata: {len(non_kafka_records)}")

        duplicate_mask = kafka_records.duplicated(
            subset=kafka_key_columns,
            keep=False
        )

        duplicates = kafka_records[duplicate_mask]

        print(f"Duplicate Kafka rows detected: {len(duplicates)}")

        if not duplicates.empty:
            print("\nDuplicate Kafka keys:")
            print(
                duplicates[kafka_key_columns]
                .sort_values(kafka_key_columns)
                .to_string(index=False)
            )

        kafka_records = kafka_records.drop_duplicates(
            subset=kafka_key_columns,
            keep="last"
        )

        df = pd.concat(
            [kafka_records, non_kafka_records],
            ignore_index=True
        )

    final_count = len(df)
    removed_count = initial_count - final_count

    print(f"\nRecords after deduplication: {final_count}")
    print(f"Records removed: {removed_count}")
    print("=" * 50)

    return df
# ============================================================
# PARQUET
# ============================================================

def create_parquet_buffer(df):
    """Convert the DataFrame into Parquet bytes."""

    buffer = io.BytesIO()

    table = pa.Table.from_pandas(
        df,
        preserve_index=False
    )

    pq.write_table(
        table,
        buffer,
        compression="snappy"
    )

    buffer.seek(0)

    return buffer


def write_silver_to_minio(client, df):
    """Write Silver Parquet data into MinIO."""

    if df.empty:
        logger.warning("No data to write to Silver.")
        return None

    timestamp = datetime.now(timezone.utc).strftime(
        "%Y%m%d_%H%M%S"
    )

    object_key = (
        f"{SILVER_PREFIX}"
        f"silver_predictions_arrets_{timestamp}.parquet"
    )

    parquet_buffer = create_parquet_buffer(df)

    client.put_object(
        Bucket=MINIO_BUCKET,
        Key=object_key,
        Body=parquet_buffer.getvalue(),
        ContentLength=parquet_buffer.getbuffer().nbytes,
        ContentType="application/octet-stream"
    )

    logger.info("Silver file uploaded: %s", object_key)

    return object_key


# ============================================================
# SILVER PIPELINE
# ============================================================

def run_silver_pipeline():

    logger.info("=" * 60)
    logger.info("SILVER PIPELINE - PREDICTIONS ARRETS")
    logger.info("=" * 60)

    client = get_minio()

    # Step 1: Read Bronze
    records = read_all_bronze_data(client)

    if not records:
        logger.warning("No Bronze records found.")
        return

    # Step 2: Flatten
    df = flatten_records(records)

    logger.info("Records before cleaning: %s", len(df))

    # Step 3: Normalize
    df = normalize_data(df)

    # Step 4: Deduplicate
    df = deduplicate_data(df)

    logger.info("Records after cleaning: %s", len(df))

    # Step 5: Write Silver
    object_key = write_silver_to_minio(client, df)

    logger.info("=" * 60)
    logger.info("SILVER PIPELINE COMPLETED")
    logger.info("Records processed: %s", len(df))
    logger.info("Output: %s", object_key)
    logger.info("=" * 60)


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":
    run_silver_pipeline()
