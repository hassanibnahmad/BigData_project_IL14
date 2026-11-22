"""Stage Student A's Kafka events and match timed predictions to schedules."""

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from spark.processing import CONTRACTS, METADATA, cast_timestamps, decode_records, quarantine_rows, read_schema
from spark.storage import exists, read_hudi_snapshot, upsert_hudi, write_hudi, write_quarantine

SILVER_MBTA_SCHEDULES = "silver/mbta_schedules_v1"
SILVER_MBTA_PREDICTIONS = "silver/mbta_predictions_v1"
SILVER_MBTA_DELAYS = "silver/mbta_stop_delays_v1"
JOIN_KEYS = ("schedule_id", "service_date", "route_id", "trip_id", "stop_id", "stop_sequence")


def stage_records(records, kind):
    """Validate against A's schema and preserve every Kafka observation."""
    schema = read_schema(CONTRACTS / f"{kind}.schema.json")
    keys = ("schedule_id", "prediction_id") if kind == "prediction" else ("schedule_id",)
    decoded = decode_records(records, schema, key_fields=keys)
    accepted = cast_timestamps(decoded.where("error_code IS NULL"), schema)
    kafka_identity = F.struct("kafka_topic", "kafka_partition", "kafka_offset")
    accepted = accepted.select(
        *schema["properties"], *METADATA, F.sha2(F.to_json(kafka_identity), 256).alias("record_id")
    )
    return accepted, quarantine_rows(decoded.where("error_code IS NOT NULL"))


def stage_batch(spark, batch, root, *, predictions_topic, schedules_topic):
    schedules, bad_schedules = stage_records(batch.where(F.col("kafka_topic") == schedules_topic), "schedule")
    predictions, bad_predictions = stage_records(
        batch.where(F.col("kafka_topic") == predictions_topic), "prediction"
    )
    upsert_hudi(schedules, root, SILVER_MBTA_SCHEDULES, key="record_id", ordering="kafka_offset")
    upsert_hudi(predictions, root, SILVER_MBTA_PREDICTIONS, key="record_id", ordering="kafka_offset")
    affected = schedules.select(*JOIN_KEYS).unionByName(predictions.select(*JOIN_KEYS)).dropna().distinct()
    refresh_matched_stops(spark, root, affected)
    write_quarantine(spark, bad_schedules.unionByName(bad_predictions), root)


def match_delays(predictions, schedules, computed_at=None):
    """Return one row per comparable arrival/departure for each prediction revision."""
    if computed_at is not None:
        cutoff = F.lit(computed_at).cast("timestamp")
        schedules = schedules.where(F.col("ingested_at") <= cutoff)
        predictions = predictions.where(F.col("ingested_at") <= cutoff)
    latest_order = Window.partitionBy(*JOIN_KEYS).orderBy(F.desc("kafka_offset"))
    latest_schedules = (
        schedules.withColumn("_rank", F.row_number().over(latest_order)).where("_rank = 1")
        .select(*JOIN_KEYS, "scheduled_arrival", "scheduled_departure",
                F.col("kafka_offset").alias("schedule_kafka_offset"),
                F.col("record_id").alias("schedule_record_id"))
    )
    joined = predictions.join(latest_schedules, list(JOIN_KEYS))
    pairs = F.array(*[
        F.struct(
            F.lit(side).alias("comparison"),
            F.col(f"predicted_{side}").alias("predicted_at"),
            F.col(f"scheduled_{side}").alias("scheduled_at"),
        )
        for side in ("arrival", "departure")
    ])
    fields = (*JOIN_KEYS, "record_id", "schedule_record_id", "schedule_kafka_offset",
              "prediction_id", "vehicle_id", "ingested_at", *METADATA)
    times = joined.select(*fields, F.explode(pairs).alias("times")).select(*fields, "times.*")
    return (
        times.where(F.col("scheduled_at").isNotNull() & (F.col("ingested_at") < F.col("predicted_at")))
        .withColumn(
            "delay_seconds",
            ((F.unix_micros("predicted_at") - F.unix_micros("scheduled_at")) / 1000000).cast("long"),
        )
    )


def read_inputs(spark, root):
    paths = [f"{root}/{table}" for table in (SILVER_MBTA_PREDICTIONS, SILVER_MBTA_SCHEDULES)]
    if not all(exists(spark, path) for path in paths):
        return None
    return [read_hudi_snapshot(spark, path) for path in paths]


def refresh_matched_stops(spark, root, affected):
    inputs = read_inputs(spark, root)
    if inputs is None or affected.isEmpty():
        return
    predictions, schedules = (rows.join(affected, list(JOIN_KEYS), "left_semi") for rows in inputs)
    matched = match_delays(predictions, schedules).withColumnRenamed("record_id", "prediction_record_id")
    identity = F.struct("prediction_record_id", "comparison")
    matched = matched.withColumn("record_id", F.sha2(F.to_json(identity), 256))
    upsert_hudi(matched, root, SILVER_MBTA_DELAYS, key="record_id", ordering="schedule_kafka_offset")

    path = f"{root}/{SILVER_MBTA_DELAYS}"
    if exists(spark, path):
        old = read_hudi_snapshot(spark, path).join(affected, list(JOIN_KEYS), "left_semi")
        obsolete = old.join(matched.select("record_id"), "record_id", "left_anti")
        write_hudi(obsolete.select(*matched.columns), root, SILVER_MBTA_DELAYS, key="record_id",
                   ordering="schedule_kafka_offset", operation="delete")


def arrivals_for_gold(predictions, schedules, computed_at):
    matched = match_delays(predictions, schedules, computed_at).where("comparison = 'arrival'")
    # The key is internal to aggregation. Keep service date and sequence so a trip
    # returning to the same stop or running again tomorrow remains distinct.
    stop_identity = F.struct("service_date", "trip_id", "stop_sequence", "stop_id")
    return matched.select(
        "route_id", "delay_seconds",
        F.lit("live").alias("dataset_id"), F.lit("predicted").alias("kind"),
        F.sha2(F.to_json(stop_identity), 256).alias("stop_key"),
        F.col("record_id").alias("event_id"),
        F.col("ingested_at").alias("event_time"),
        F.col("scheduled_at").alias("scheduled_arrival_at"),
        F.col("kafka_offset").alias("revision_offset"),
    )


def read_arrivals(spark, root, computed_at):
    inputs = read_inputs(spark, root)
    return arrivals_for_gold(*inputs, computed_at=computed_at) if inputs is not None else None
