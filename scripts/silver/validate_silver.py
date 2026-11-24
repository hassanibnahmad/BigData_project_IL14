import io
import sys
from pathlib import Path

import pandas as pd

# Reuse shared MinIO configuration and client
PROJECT_ROOT = Path(__file__).resolve().parents[2]
COMMON_DIR = PROJECT_ROOT / "scripts" / "common"

sys.path.insert(0, str(COMMON_DIR))

from minio_client import get_minio_client
from config import MINIO_BUCKET

SILVER_PREFIX = "silver/predictions_arrets/"


def main():
    client = get_minio_client()

    # List Silver Parquet files
    response = client.list_objects_v2(
        Bucket=MINIO_BUCKET,
        Prefix=SILVER_PREFIX
    )

    files = [
        obj["Key"]
        for obj in response.get("Contents", [])
        if obj["Key"].endswith(".parquet")
    ]

    if not files:
        print("No Silver Parquet files found.")
        return

    # Select the most recent file
    latest_file = sorted(files)[-1]

    print("=" * 60)
    print("SILVER DATA VALIDATION")
    print("=" * 60)
    print(f"File: {latest_file}")

    # Download Parquet from MinIO
    response = client.get_object(
        Bucket=MINIO_BUCKET,
        Key=latest_file
    )

    try:
        parquet_bytes = response["Body"].read()
        parquet_buffer = io.BytesIO(parquet_bytes)

        df = pd.read_parquet(parquet_buffer, engine="pyarrow")

    finally:
        response["Body"].close()

    # Basic information
    print("\n--- Dataset Information ---")
    print(f"Rows: {len(df)}")
    print(f"Columns: {len(df.columns)}")
    print(f"Memory usage: {df.memory_usage(deep=True).sum() / 1024:.2f} KB")

    # Schema
    print("\n--- Schema ---")
    print(df.dtypes)

    # Missing values
    print("\n--- Missing Values ---")
    missing = df.isnull().sum()
    print(missing[missing > 0] if missing.any() else "No missing values")

    # Duplicate records
    print("\n--- Duplicate Records ---")
    print(f"Exact duplicates: {df.duplicated().sum()}")

    if "prediction_id" in df.columns:
        print(
            "Duplicate prediction IDs:",
            df["prediction_id"].duplicated().sum()
        )

    # Preview
    print("\n--- Preview ---")
    print(df.head(5).to_string(index=False))

    # Statistics
    print("\n--- Data Quality Summary ---")
    print(f"Unique routes: {df['route_id'].nunique()}")
    print(f"Unique trips: {df['trip_id'].nunique()}")
    print(f"Unique stops: {df['stop_id'].nunique()}")
    print(f"Unique vehicles: {df['vehicle_id'].nunique()}")

    print("\nValidation completed.")


if __name__ == "__main__":
    main()
