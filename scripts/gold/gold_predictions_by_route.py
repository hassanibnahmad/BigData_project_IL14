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

sys.path.insert(0, str(COMMON_DIR))

from minio_client import get_minio_client
from config import MINIO_BUCKET

SILVER_PREFIX = "silver/predictions_arrets/"
GOLD_PREFIX = "gold/predictions_by_route/"

# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)

# ============================================================
# SILVER DATA READING
# ============================================================

def list_silver_files(client):
    """List all Parquet files from the Silver predictions prefix."""

    response = client.list_objects_v2(
        Bucket=MINIO_BUCKET,
        Prefix=SILVER_PREFIX
    )

    files = []

    while True:
        for obj in response.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                files.append(obj["Key"])

        if not response.get("IsTruncated"):
            break

        response = client.list_objects_v2(
            Bucket=MINIO_BUCKET,
            Prefix=SILVER_PREFIX,
            ContinuationToken=response["NextContinuationToken"]
        )

    return files

# ============================================================
# READ SILVER PARQUET DATA
# ============================================================

def read_silver_data(client):
    """Read all Silver Parquet files from MinIO into one DataFrame."""

    files = list_silver_files(client)

    logger.info("Silver Parquet files found: %s", len(files))

    if not files:
        logger.warning("No Silver Parquet files found.")
        return pd.DataFrame()

    dataframes = []

    for file_key in files:
        logger.info("Reading Silver file: %s", file_key)

        response = client.get_object(
            Bucket=MINIO_BUCKET,
            Key=file_key
        )

        try:
            content = response["Body"].read()
            df = pd.read_parquet(io.BytesIO(content))
            dataframes.append(df)

            logger.info("Records loaded: %s", len(df))

        finally:
            response["Body"].close()

    if not dataframes:
        return pd.DataFrame()

    combined_df = pd.concat(
        dataframes,
        ignore_index=True
    )

    logger.info("Total Silver records loaded: %s", len(combined_df))

    return combined_df

# ============================================================
# GOLD AGGREGATION - BY ROUTE
# ============================================================

def aggregate_predictions_by_route(df):
    """Aggregate transport predictions by route."""

    if df.empty:
        logger.warning("No Silver data available for aggregation.")
        return pd.DataFrame()

    # Ensure route_id is valid
    df = df.dropna(subset=["route_id"]).copy()

    if df.empty:
        logger.warning("No valid route_id found.")
        return pd.DataFrame()

    # Convert timestamps
    df["predicted_arrival"] = pd.to_datetime(
        df["predicted_arrival"],
        errors="coerce",
        utc=True
    )

    # Aggregate by route
    gold_df = df.groupby("route_id").agg(
        total_predictions=("prediction_id", "count"),
        total_trips=("trip_id", "nunique"),
        total_stops=("stop_id", "nunique"),
        total_vehicles=("vehicle_id", "nunique"),
        first_prediction=("predicted_arrival", "min"),
        last_prediction=("predicted_arrival", "max"),
    ).reset_index()



    # Average prediction interval
    interval_df = df.dropna(
        subset=["route_id", "predicted_arrival"]
    ).copy()

    interval_df["predicted_arrival"] = pd.to_datetime(
        interval_df["predicted_arrival"],
        errors="coerce",
        utc=True
    )

    interval_df = interval_df.dropna(
        subset=["predicted_arrival"]
    )

    interval_df = interval_df.sort_values(
        ["route_id", "predicted_arrival"]
    )

    prediction_intervals = (
        interval_df
        .groupby("route_id")["predicted_arrival"]
        .diff()
    )

    avg_intervals = (
        prediction_intervals
        .groupby(interval_df["route_id"])
        .mean()
        .dt.total_seconds()
    )

    gold_df["avg_prediction_interval"] = (
        gold_df["route_id"]
        .map(avg_intervals)
    )

    logger.info("Gold aggregation completed.")
    logger.info("Routes aggregated: %s", len(gold_df))

    return gold_df

# ============================================================
# WRITE GOLD PARQUET
# ============================================================

def write_gold_to_minio(client, df):
    """Write Gold route aggregations to MinIO as Parquet."""

    if df.empty:
        logger.warning("No Gold data to write.")
        return None

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    object_key = (
        f"{GOLD_PREFIX}"
        f"gold_predictions_by_route_{timestamp}.parquet"
    )

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

    client.put_object(
        Bucket=MINIO_BUCKET,
        Key=object_key,
        Body=buffer.getvalue(),
        ContentLength=buffer.getbuffer().nbytes,
        ContentType="application/octet-stream"
    )

    logger.info("Gold file uploaded: %s", object_key)

    return object_key