"""Recompute Gold route-hour metrics from the Silver Hudi snapshot."""

import argparse
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from spark.processing import route_hours
from spark.storage import GOLD_HOURLY, SILVER_DELAYS, exists, session, storage_root, upsert_hudi


def utc_hour(text):
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:00:00Z", text):
        raise argparse.ArgumentTypeError("Use an aligned UTC hour, e.g. 2026-11-11T06:00:00Z")
    try:
        return datetime.fromisoformat(text[:-1] + "+00:00")
    except ValueError as error:
        raise argparse.ArgumentTypeError(str(error)) from error


def refresh_hours(spark, root, *, from_hour, to_hour, computed_at):
    path = f"{root}/{SILVER_DELAYS}"
    if not exists(spark, path):
        return False
    delays = spark.read.format("hudi").load(path)
    hours = route_hours(delays, from_hour=from_hour, to_hour=to_hour, computed_at=computed_at)
    return upsert_hudi(hours, root, GOLD_HOURLY, key="metric_id", ordering="computed_at")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--from-hour", type=utc_hour, required=True)
    parser.add_argument("--to-hour", type=utc_hour, required=True)
    parser.add_argument("--local-root", type=Path, help="Use an isolated local Hudi directory")
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
            computed_at=computed_at,
        )
        print("Updated Gold route hours" if changed else "No Silver stop delays in this hour range")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
