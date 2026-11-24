"""Spark transformations shared by the offline prototype and future microbatches."""

import base64
import hashlib
import json
import math
import re
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from urllib.parse import urlsplit

from jsonschema import Draft202012Validator, FormatChecker
from pyspark.sql import functions as F, types as T
from pyspark.sql.window import Window

from spark.storage import exists

CONTRACTS = Path(__file__).resolve().parents[1] / "contracts"
TEAM_SCHEMAS = CONTRACTS / "team" / "schemas"
METADATA = ("kafka_topic", "kafka_partition", "kafka_offset", "kafka_timestamp")
STOP_JOIN = ["trip_id", "route_id", "stop_sequence", "stop_id"]
STOP_SCHEDULE_COLUMNS = ["service_id", "_arrival_seconds", *STOP_JOIN]


def read_schema(path):
    return json.loads(path.read_text(encoding="utf-8"))


def digest(parts):
    raw = json.dumps(parts, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON field")
        result[key] = value
    return result


def finite_float(text):
    number = float(text)
    if not math.isfinite(number):
        raise ValueError("non-finite JSON number")
    return number


def schema_fields(schema):
    types = {"string": T.StringType, "number": T.DoubleType, "integer": T.LongType}
    fields = []
    for name, rule in schema["properties"].items():
        kind = rule.get("type", "string")
        if isinstance(kind, list):
            kind = next(item for item in kind if item != "null")
        field_type = T.IntegerType() if name == "stop_sequence" else types[kind]()
        fields.append(T.StructField(name, field_type))
    return T.StructType(fields)


@lru_cache(maxsize=2)
def payload_validator(schema_json):
    return Draft202012Validator(json.loads(schema_json), format_checker=FormatChecker())


def decode_records(records, schema, *, key_fields, verify_stop_identity=False):
    """Validate original bytes, preserving their metadata for either output."""
    schema_json = json.dumps(schema)
    output_fields = schema_fields(schema)
    result_type = T.StructType([
        T.StructField("payload", output_fields),
        T.StructField("error_code", T.StringType()),
    ])

    @F.udf(result_type)
    def decode(raw_key, raw_value):
        try:
            key = bytes(raw_key).decode("utf-8") if raw_key is not None else None
            value = bytes(raw_value).decode("utf-8") if raw_value is not None else None
        except UnicodeDecodeError:
            return None, "invalid_encoding"
        if value is None:
            return None, "invalid_json"
        try:
            event = json.loads(value, object_pairs_hook=unique_object,
                               parse_float=finite_float, parse_constant=finite_float)
            # Reject unpaired surrogate escapes before Spark serializes the payload.
            json.dumps(event, ensure_ascii=False).encode("utf-8")
        except (ValueError, RecursionError):
            return None, "invalid_json"
        if not payload_validator(schema_json).is_valid(event):
            return None, "invalid_schema"
        if event.get("stop_sequence", 0) > 2147483647:
            return None, "invalid_schema"
        expected_key = next((event[field] for field in key_fields if event[field] is not None), None)
        if key is None or key != expected_key:
            return None, "key_mismatch"
        if verify_stop_identity:
            stop_key = digest([
                event["dataset_id"], event["service_date"], event["trip_id"],
                int(event["stop_sequence"]),
            ])
            arrival = int(datetime.fromisoformat(event["arrival_at"][:-1] + "+00:00").timestamp())
            issued = int(datetime.fromisoformat(event["event_time"][:-1] + "+00:00").timestamp())
            if stop_key != key or event["event_id"] != digest([stop_key, event["kind"], issued, arrival]):
                return None, "bad_event_id"

        # Spark's Python boundary requires actual floats and ints for these types.
        payload = {
            field.name: float(event[field.name]) if isinstance(field.dataType, T.DoubleType)
            else int(event[field.name]) if isinstance(field.dataType, T.IntegerType)
            else event[field.name]
            for field in output_fields
        }
        return payload, None

    decoded = records.withColumn("_decoded", decode("key", "value"))
    return decoded.select(*METADATA, "key", "value", "_decoded.error_code", "_decoded.payload.*")


def cast_timestamps(rows, schema):
    for name, rule in schema["properties"].items():
        if rule.get("format") == "date-time":
            rows = rows.withColumn(name, F.to_timestamp(name))
    return rows


def quarantine_rows(rejected):
    # Kafka envelopes use unwrapped Base64, while Spark can wrap long values.
    def encode_bytes(column):
        return F.regexp_replace(F.base64(column), r"[\r\n]", "")

    return rejected.select(
        *METADATA, encode_bytes("key").alias("key_b64"), encode_bytes("value").alias("value_b64"),
        "error_code",
    )


def latest_offsets(rows):
    order = Window.partitionBy("event_id").orderBy(F.desc("kafka_offset"))
    return rows.withColumn("_rank", F.row_number().over(order)).where("_rank = 1").drop("_rank")


def process_positions(records):
    schema = read_schema(CONTRACTS / "vehicle_position.schema.json")
    decoded = decode_records(records, schema, key_fields=("vehicle_id",))
    valid = cast_timestamps(decoded.where("error_code IS NULL"), schema)
    silver = latest_offsets(valid.select(*schema["properties"], *METADATA))
    return silver, quarantine_rows(decoded.where("error_code IS NOT NULL"))


def read_gtfs(spark, directory):
    """Load the pinned extracted GTFS snapshot through Spark's filesystem."""
    directory = str(directory)
    scheme = urlsplit(directory).scheme
    if not scheme:
        directory = Path(directory).resolve().as_uri()
    elif scheme not in ("file", "s3a"):
        raise ValueError("GTFS snapshot must use a local path, file:/ URI or s3a:// URI")
    directory = directory.rstrip("/")

    def uri(name):
        return f"{directory}/{name}"

    def read(name):
        return spark.read.option("header", True).csv(uri(name + ".txt"))

    zone = [row.agency_timezone for row in read("agency").select("agency_timezone").distinct().collect()]
    if zone != ["America/New_York"]:
        raise ValueError("Expected one GTFS agency timezone: America/New_York")
    routes = read("routes").where(F.col("route_type") == "3").select("route_id")
    trips = read("trips").select("trip_id", "route_id", "service_id").join(routes, "route_id")
    if exists(spark, uri("frequencies.txt")):
        trips = trips.join(read("frequencies").select("trip_id"), "trip_id", "left_anti")
    stops = read("stops").select("stop_id")
    stop_times = read("stop_times").select("trip_id", "stop_id", "stop_sequence", "arrival_time")
    clock = F.split("arrival_time", ":")
    schedule = (
        stop_times.where(F.col("arrival_time").rlike(r"^[0-9]{1,5}:[0-5][0-9]:[0-5][0-9]$"))
        .withColumn("stop_sequence", F.expr("try_cast(stop_sequence AS INT)"))
        .withColumn("_arrival_seconds", clock[0].cast("long") * 3600
                    + clock[1].cast("long") * 60 + clock[2].cast("long"))
        .join(stops, "stop_id").join(trips, "trip_id").select(*STOP_SCHEDULE_COLUMNS)
    )
    calendar = read("calendar") if exists(spark, uri("calendar.txt")) else None
    exceptions = read("calendar_dates") if exists(spark, uri("calendar_dates.txt")) else None
    if calendar is None and exceptions is None:
        raise ValueError("GTFS needs calendar.txt, calendar_dates.txt, or both")
    manifest = spark.read.text(uri("manifest.json"), wholetext=True).first()
    snapshot_id = json.loads(manifest.value)["sha256"]
    if not isinstance(snapshot_id, str) or not re.fullmatch(r"[0-9a-f]{64}", snapshot_id):
        raise ValueError("GTFS manifest sha256 must be a lowercase SHA-256 string")
    return schedule, calendar, exceptions, snapshot_id


def join_schedule(events, schedule, calendar, exceptions):
    matched = events.join(schedule, STOP_JOIN, "left")
    service_day = F.to_date("service_date")
    if calendar is not None:
        weekdays = ("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday")
        weekly = calendar.select(
            "service_id", F.to_date("start_date", "yyyyMMdd").alias("_start"),
            F.to_date("end_date", "yyyyMMdd").alias("_end"), F.array(*weekdays).alias("_weekdays"),
        )
        matched = matched.join(weekly, "service_id", "left")
        active = service_day.between(F.col("_start"), F.col("_end")) & (
            F.element_at("_weekdays", F.weekday(service_day) + 1) == "1"
        )
    else:
        active = F.lit(False)
    if exceptions is not None:
        overrides = exceptions.select(
            "service_id", F.date_format(F.to_date("date", "yyyyMMdd"), "yyyy-MM-dd").alias("service_date"),
            "exception_type",
        )
        matched = matched.join(overrides, ["service_id", "service_date"], "left")
        active = F.coalesce(F.col("exception_type") == "1", active)

    local_noon = F.to_timestamp(F.concat(F.col("service_date"), F.lit(" 12:00:00")))
    noon_utc = F.to_utc_timestamp(local_noon, "America/New_York")
    scheduled = F.timestamp_seconds(F.unix_seconds(noon_utc) - 43200 + F.col("_arrival_seconds"))
    return matched.withColumn("scheduled_arrival_at", F.when(active, scheduled))


def process_stops(records, gtfs):
    schema = read_schema(TEAM_SCHEMAS / "stop_event.schema.json")
    decoded = decode_records(records, schema, key_fields=("stop_key",), verify_stop_identity=True)
    early_rejected = quarantine_rows(decoded.where("error_code IS NOT NULL"))
    events = cast_timestamps(decoded.where("error_code IS NULL"), schema)
    schedule, calendar, exceptions, snapshot_id = gtfs
    joined = join_schedule(events, schedule, calendar, exceptions).withColumn(
        "error_code",
        F.when(F.col("scheduled_arrival_at").isNull(), F.lit("unmatched_schedule"))
        .when((F.col("kind") == "predicted") & (F.col("event_time") >= F.col("arrival_at")),
              F.lit("invalid_prediction_time"))
        .when((F.col("kind") == "simulated") & (F.col("event_time") != F.col("arrival_at")),
              F.lit("invalid_simulated_time")),
    )
    silver = joined.where("error_code IS NULL").select(
        *schema["properties"], *METADATA, F.lit(snapshot_id).alias("gtfs_snapshot_id"),
        "scheduled_arrival_at",
        (F.unix_seconds("arrival_at") - F.unix_seconds("scheduled_arrival_at")).alias("delay_seconds"),
    )
    rejected = early_rejected.unionByName(quarantine_rows(joined.where("error_code IS NOT NULL")))
    return latest_offsets(silver), rejected


def route_hours(delays, *, from_hour, to_hour, computed_at):
    eligible = delays.where(
        (F.col("scheduled_arrival_at") >= F.lit(from_hour).cast("timestamp"))
        & (F.col("scheduled_arrival_at") < F.lit(to_hour).cast("timestamp"))
        & (F.col("event_time") <= F.lit(computed_at).cast("timestamp"))
        & ((F.col("kind") != "predicted") | (F.col("event_time") <= F.col("scheduled_arrival_at")))
    )
    revision_order = [F.desc("event_time")]
    if "revision_offset" in eligible.columns:
        revision_order.append(F.desc_nulls_last("revision_offset"))
    order = Window.partitionBy("stop_key", "kind").orderBy(*revision_order, F.desc("event_id"))
    latest = (
        eligible.withColumn("_rank", F.row_number().over(order)).where("_rank = 1")
        .withColumn("hour_start", F.date_trunc("hour", "scheduled_arrival_at"))
    )
    predicted = F.when(F.col("kind") == "predicted", F.col("delay_seconds"))
    simulated = F.when(F.col("kind") == "simulated", F.col("delay_seconds"))
    hours = latest.groupBy("dataset_id", "route_id", "hour_start").agg(
        F.count(predicted).alias("predicted_stop_count"),
        F.avg(predicted).alias("predicted_mean_delay_seconds"),
        F.count(simulated).alias("simulated_stop_count"),
        F.avg(simulated).alias("simulated_mean_delay_seconds"),
    )
    metric_key = F.udf(lambda dataset, route, hour: digest([dataset, route, hour]), T.StringType())
    return (
        hours.withColumn("metric_id", metric_key(
            "dataset_id", "route_id", F.date_format("hour_start", "yyyy-MM-dd'T'HH:mm:ss'Z'")
        ))
        .withColumn("computed_at", F.lit(computed_at).cast("timestamp"))
    )
