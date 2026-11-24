"""Validate the offline contract fixture without Kafka, Spark, or MinIO."""

import base64
import csv
import hashlib
import json
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from statistics import mean
from zoneinfo import ZoneInfo

import jsonschema

FIXTURE = Path(__file__).resolve().parent
TEAM = FIXTURE.parent
METADATA = ("kafka_topic", "kafka_partition", "kafka_offset", "kafka_timestamp")


def load(path):
    return json.loads(path.read_text(encoding="utf-8"))


def validator(path):
    schema = load(path)
    # All v1 validation keywords also exist in Draft 7. This allows the
    # fixture check to run with the older system package on some machines.
    validator_class = getattr(jsonschema, "Draft202012Validator", jsonschema.Draft7Validator)
    validator_class.check_schema(schema)
    return validator_class(schema, format_checker=jsonschema.FormatChecker())


def digest(parts):
    encoded = json.dumps(parts, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def epoch(timestamp):
    return int(datetime.fromisoformat(timestamp.replace("Z", "+00:00")).timestamp())


def read_gtfs(name):
    with (FIXTURE / "gtfs" / f"{name}.txt").open(encoding="utf-8", newline="") as file:
        return list(csv.DictReader(file))


def scheduled_time(service_date, gtfs_time, agency_timezone):
    noon = datetime.combine(date.fromisoformat(service_date), time(12), agency_timezone)
    origin = noon.astimezone(timezone.utc) - timedelta(hours=12)
    hours, minutes, seconds = map(int, gtfs_time.split(":"))
    scheduled = origin + timedelta(hours=hours, minutes=minutes, seconds=seconds)
    return scheduled.strftime("%Y-%m-%dT%H:%M:%SZ")


def main():
    schemas = {
        path.name.removesuffix(".schema.json"): validator(path)
        for path in (TEAM / "schemas").glob("*.schema.json")
    }
    position_schema = validator(TEAM.parent / "vehicle_position.schema.json")
    expected = load(FIXTURE / "expected.json")
    snapshot_id = load(FIXTURE / "gtfs" / "manifest.json")["sha256"]
    agency_timezone = ZoneInfo(read_gtfs("agency")[0]["agency_timezone"])
    trips = {row["trip_id"]: row for row in read_gtfs("trips")}
    stops = {(row["trip_id"], int(row["stop_sequence"])): row for row in read_gtfs("stop_times")}
    active = {
        (row["service_id"], row["date"])
        for row in read_gtfs("calendar_dates")
        if row["exception_type"] == "1"
    }
    positions, delays, quarantine = {}, {}, []
    raw_ids = set()
    lines = (FIXTURE / "kafka_records.jsonl").read_text(encoding="utf-8").splitlines()
    for line in lines:
        record = json.loads(line)
        metadata = {field: record[field] for field in METADATA}
        record_id = tuple(record[field] for field in METADATA[:3])
        assert record_id not in raw_ids
        raw_ids.add(record_id)
        raw = {
            **metadata,
            "key_b64": base64.b64encode(record["key"].encode()).decode(),
            "value_b64": base64.b64encode(record["value"].encode()).decode(),
        }
        schemas["bronze_record"].validate(raw)
        try:
            event = json.loads(record["value"])
        except json.JSONDecodeError:
            reason = "invalid_json"
        else:
            if record["kafka_topic"] == "positions_vehicules":
                position_schema.validate(event)
                assert record["key"] == event["vehicle_id"]
                row = {**event, **metadata}
                schemas["silver_vehicle_position"].validate(row)
                positions[event["event_id"]] = row
                continue
            assert record["kafka_topic"] == "stop_events_v1"
            schemas["stop_event"].validate(event)
            stop_key = digest([
                event["dataset_id"], event["service_date"], event["trip_id"], event["stop_sequence"]
            ])
            assert record["key"] == event["stop_key"] == stop_key
            assert event["event_id"] == digest([
                stop_key, event["kind"], epoch(event["event_time"]), epoch(event["arrival_at"])
            ])
            assert (
                event["event_time"] < event["arrival_at"]
                if event["kind"] == "predicted"
                else event["event_time"] == event["arrival_at"]
            )
            trip = trips[event["trip_id"]]
            assert trip["route_id"] == event["route_id"]
            assert (trip["service_id"], event["service_date"].replace("-", "")) in active
            stop = stops.get((event["trip_id"], event["stop_sequence"]))
            if stop is None:
                reason = "unmatched_schedule"
            else:
                assert stop["stop_id"] == event["stop_id"]
                scheduled = scheduled_time(event["service_date"], stop["arrival_time"], agency_timezone)
                row = {
                    **event, **metadata, "gtfs_snapshot_id": snapshot_id,
                    "scheduled_arrival_at": scheduled,
                    "delay_seconds": epoch(event["arrival_at"]) - epoch(scheduled),
                }
                schemas["silver_stop_delay"].validate(row)
                previous = delays.get(row["event_id"])
                if previous is None or row["kafka_offset"] > previous["kafka_offset"]:
                    delays[row["event_id"]] = row
                continue
        schemas["quarantine_record"].validate({**raw, "error_code": reason})
        quarantine.append({**{field: record[field] for field in METADATA[:3]}, "error_code": reason})

    computed_at = "2026-11-11T07:10:00Z"
    latest = {}
    for row in delays.values():
        if row["event_time"] > computed_at:
            continue
        if row["kind"] == "predicted" and row["event_time"] > row["scheduled_arrival_at"]:
            continue
        key = (row["stop_key"], row["kind"])
        if key not in latest or (row["event_time"], row["event_id"]) > (
            latest[key]["event_time"], latest[key]["event_id"]
        ):
            latest[key] = row
    groups = {}
    for row in latest.values():
        hour = row["scheduled_arrival_at"][:13] + ":00:00Z"
        group = groups.setdefault((row["dataset_id"], row["route_id"], hour), {})
        group.setdefault(row["kind"], []).append(row["delay_seconds"])
    metrics = []
    for (dataset, route, hour), group in groups.items():
        metric = {
            "metric_id": digest([dataset, route, hour]), "dataset_id": dataset,
            "route_id": route, "hour_start": hour, "computed_at": computed_at,
        }
        for kind in ("predicted", "simulated"):
            values = group.get(kind, [])
            metric[f"{kind}_stop_count"] = len(values)
            metric[f"{kind}_mean_delay_seconds"] = mean(values) if values else None
        schemas["gold_route_hour"].validate(metric)
        metrics.append(metric)

    assert len(raw_ids) == expected["bronze_count"]
    assert quarantine == expected["quarantine"]
    comparisons = [
        ("silver_vehicle_positions_v1", positions.values(), "event_id"),
        ("silver_stop_delays_v1", delays.values(), "event_id"),
        ("gold_route_hourly_v1", metrics, "metric_id"),
    ]
    for table, rows, key in comparisons:
        assert {row[key]: row for row in rows} == {row[key]: row for row in expected[table]}, table
    assert expected["gold_delay_predictions_v1"] == []
    print("Fixture verified: 8 Bronze records, 2 rejected, 1 position, 4 stop delays, 2 route-hour rows.")


if __name__ == "__main__":
    main()
