"""Run Student C's offline Spark job against the shared contract fixture."""

import argparse
import base64
import json
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession, functions as F, types as T

from spark.processing import METADATA, process_positions, process_stops, read_gtfs, route_hours

FIXTURE = Path(__file__).resolve().parents[1] / "contracts" / "team" / "fixtures"
INPUT_SCHEMA = T.StructType([
    T.StructField("kafka_topic", T.StringType()),
    T.StructField("kafka_partition", T.IntegerType()),
    T.StructField("kafka_offset", T.LongType()),
    T.StructField("kafka_timestamp", T.TimestampType()),
    T.StructField("key", T.StringType()),
    T.StructField("value", T.StringType()),
])


def collect_rows(frame, *order):
    """Serialize small fixture results with UTC timestamps and explicit nulls."""
    timestamp_columns = [
        field.name for field in frame.schema if isinstance(field.dataType, T.TimestampType)
    ]
    encoded = F.to_json(
        F.struct(*frame.columns),
        {"ignoreNullFields": "false", "timestampFormat": "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX"},
    )
    data = [json.loads(row.value) for row in frame.orderBy(*order).select(encoded.alias("value")).collect()]
    for row in data:
        for column in timestamp_columns:
            if row[column] is not None:
                instant = datetime.fromisoformat(row[column].replace("Z", "+00:00"))
                row[column] = instant.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    return data


def check_results(results, expected):
    keys = {
        "silver_vehicle_positions_v1": ("event_id",),
        "silver_stop_delays_v1": ("event_id",),
        "gold_route_hourly_v1": ("metric_id",),
        "quarantine": METADATA[:3],
    }

    def project(rows, fields):
        return sorted(tuple(row[field] for field in fields) for row in rows)

    if results["input_count"] != expected["bronze_count"]:
        raise ValueError("Fixture input count differs from expected")
    for table, columns in keys.items():
        actual, reference = results[table], expected[table]
        if table == "quarantine":
            same = project(actual, (*columns, "error_code")) == project(reference, (*columns, "error_code"))
        else:
            same = sorted(actual, key=lambda row: tuple(row[c] for c in columns)) == sorted(
                reference, key=lambda row: tuple(row[c] for c in columns)
            )
        if not same:
            raise ValueError(f"{table} differs from expected fixture rows")

    fixture_records = [
        json.loads(line)
        for line in (FIXTURE / "kafka_records.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    original = {tuple(row[field] for field in METADATA[:3]): row for row in fixture_records}
    for rejected in results["quarantine"]:
        source = original[tuple(rejected[field] for field in METADATA[:3])]
        for name in ("key", "value"):
            raw = source[name]
            encoded = base64.b64encode(raw.encode("utf-8")).decode("ascii") if raw is not None else None
            if rejected[f"{name}_b64"] != encoded:
                raise ValueError(f"Quarantine did not preserve the original {name} bytes")


def run(spark):
    records = (
        spark.read.schema(INPUT_SCHEMA).option("mode", "FAILFAST").json((FIXTURE / "kafka_records.jsonl").as_uri())
        .select(*METADATA, F.encode("key", "UTF-8").alias("key"), F.encode("value", "UTF-8").alias("value"))
        .cache()
    )
    input_count = records.count()
    unexpected = records.where(~F.col("kafka_topic").isin("positions_vehicules", "stop_events_v1")
                               | F.col("kafka_topic").isNull()).take(1)
    if unexpected:
        raise ValueError("Unexpected topic in the offline fixture")
    positions, rejected_positions = process_positions(records.where("kafka_topic = 'positions_vehicules'"))
    delays, rejected_stops = process_stops(
        records.where("kafka_topic = 'stop_events_v1'"), read_gtfs(spark, FIXTURE / "gtfs")
    )
    delays = delays.cache()
    try:
        hours = route_hours(
            delays, from_hour="2026-11-11T06:00:00Z", to_hour="2026-11-11T07:00:00Z",
            computed_at="2026-11-11T07:10:00Z",
        )
        return {
            "input_count": input_count,
            "silver_vehicle_positions_v1": collect_rows(positions, "event_id"),
            "silver_stop_delays_v1": collect_rows(delays, "event_id"),
            "quarantine": collect_rows(rejected_positions.unionByName(rejected_stops), *METADATA[:3]),
            "gold_route_hourly_v1": collect_rows(hours, "dataset_id", "route_id", "hour_start"),
        }
    finally:
        delays.unpersist()
        records.unpersist()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, help="Write full results to a new JSON file")
    args = parser.parse_args()
    spark = (
        SparkSession.builder.master("local[2]").appName("IL14 offline Spark fixture")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    try:
        spark.sparkContext.setLogLevel("WARN")
        results = run(spark)
        expected = json.loads((FIXTURE / "expected.json").read_text(encoding="utf-8"))
        check_results(results, expected)
        if args.output:
            with args.output.open("x", encoding="utf-8") as output:
                json.dump(results, output, indent=2)
                output.write("\n")
        print(
            f"Spark fixture passed: {results['input_count']} input records, "
            f"{len(results['silver_vehicle_positions_v1'])} position, "
            f"{len(results['silver_stop_delays_v1'])} stop delays, "
            f"{len(results['quarantine'])} rejected records."
        )
        for hour in results["gold_route_hourly_v1"]:
            for kind in ("predicted", "simulated"):
                value = hour[f"{kind}_mean_delay_seconds"]
                if value is not None:
                    print(f"Route {hour['route_id']} | {hour['dataset_id']} | {kind}: {value:+g} seconds")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
