"""Recompute Gold route-hour metrics from the Silver Hudi snapshot."""

import argparse
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F

from spark.mbta_live import read_arrivals
from spark.processing import route_hours
from spark.storage import (
    GOLD_HOURLY, SILVER_DELAYS, exists, read_hudi_snapshot, session, storage_root, upsert_hudi, write_hudi,
)


def utc_hour(text):
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:00:00Z", text):
        raise argparse.ArgumentTypeError("Use an aligned UTC hour, e.g. 2026-11-11T06:00:00Z")
    try:
        return datetime.fromisoformat(text[:-1] + "+00:00")
    except ValueError as error:
        raise argparse.ArgumentTypeError(str(error)) from error


def refresh_hours(spark, root, *, from_hour, to_hour, computed_at, stop_input="gtfs"):
    path = f"{root}/{SILVER_DELAYS}"
    delays = read_hudi_snapshot(spark, path) if exists(spark, path) else None
    live = None
    if stop_input == "mbta":
        if delays is not None:
            delays = delays.where("kind = 'simulated'")
        live = read_arrivals(spark, root, computed_at)
        if delays is None:
            delays = live
        elif live is not None:
            delays = delays.unionByName(live, allowMissingColumns=True)
    if delays is None:
        return False
    hours = route_hours(delays, from_hour=from_hour, to_hour=to_hour, computed_at=computed_at)
    updated = upsert_hudi(hours, root, GOLD_HOURLY, key="metric_id", ordering="computed_at")
    gold_path = f"{root}/{GOLD_HOURLY}"
    if live is None or not exists(spark, gold_path):
        return updated
    # A schedule correction can move the last matched stop outside its former hour.
    old_live = read_hudi_snapshot(spark, gold_path).where(
        (F.col("dataset_id") == "live")
        & (F.col("hour_start") >= F.lit(from_hour).cast("timestamp"))
        & (F.col("hour_start") < F.lit(to_hour).cast("timestamp"))
    )
    obsolete = (
        old_live.join(hours.select("metric_id"), "metric_id", "left_anti")
        .select(*hours.columns).withColumn("computed_at", F.lit(computed_at).cast("timestamp"))
    )
    removed = write_hudi(obsolete, root, GOLD_HOURLY, key="metric_id", ordering="computed_at",
                         operation="delete")
    return updated or removed


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--from-hour", type=utc_hour, required=True)
    parser.add_argument("--to-hour", type=utc_hour, required=True)
    parser.add_argument("--local-root", type=Path, help="Use an isolated local Hudi directory")
    parser.add_argument("--stop-input", choices=("gtfs", "mbta"), default="gtfs",
                        help="Choose the live prediction input. Simulated arrivals are included in either mode.")
    args = parser.parse_args()
    computed_at = datetime.now(timezone.utc)
    if not args.from_hour < args.to_hour <= computed_at:
        parser.error("Require --from-hour < --to-hour <= current time")
    root = storage_root(args.local_root)
    spark = session("IL14 Gold route hours", root)
    spark.sparkContext.setLogLevel("WARN")
    try:
        changed = refresh_hours(
            spark, root, from_hour=args.from_hour, to_hour=args.to_hour,
            computed_at=computed_at, stop_input=args.stop_input,
        )
        print("Updated Gold route hours" if changed else "No Silver stop delays in this hour range")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
