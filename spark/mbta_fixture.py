"""Verify A's two event schemas and live Spark join without a Kafka broker."""

import hashlib
import json
from pathlib import Path

from pyspark.sql import SparkSession, functions as F, types as T

from spark.mbta_live import arrivals_for_gold, match_delays, stage_records
from spark.processing import route_hours

FIXTURE = Path(__file__).resolve().parent / "fixtures" / "mbta-matched.json"
CUTOFF = "2026-11-22T01:00:00Z"
RAW = T.StructType([
    T.StructField("kafka_topic", T.StringType()),
    T.StructField("kafka_partition", T.IntegerType()),
    T.StructField("kafka_offset", T.LongType()),
    T.StructField("key", T.BinaryType()),
    T.StructField("value", T.BinaryType()),
])


def revision(event, **changes):
    updated = {**event, **changes}
    content = {key: value for key, value in updated.items() if key not in ("event_id", "ingested_at")}
    updated["event_id"] = hashlib.sha256(
        json.dumps(content, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return updated


def records(spark, topic, envelopes):
    rows = [(topic, 0, offset, key.encode(), json.dumps(payload).encode())
            for offset, key, payload in envelopes]
    return spark.createDataFrame(rows, RAW).withColumn(
        "kafka_timestamp", F.lit(CUTOFF).cast("timestamp")
    )


def verify(spark):
    pair = json.loads(FIXTURE.read_text(encoding="utf-8"))
    prediction, schedule = pair["prediction"], pair["schedule"]
    schedule_id = schedule["schedule_id"]

    departure_prediction = revision(
        prediction, schedule_id="departure-only", prediction_id="departure-only-prediction",
        predicted_arrival=None, predicted_departure="2026-11-21T15:00:00-05:00",
    )
    departure_schedule = revision(
        schedule, schedule_id="departure-only", scheduled_arrival=None,
        scheduled_departure="2026-11-21T14:55:00-05:00",
    )
    no_date = revision(prediction, service_date=None)
    wrong_stop = revision(prediction, stop_id="wrong-stop")
    wrong_day = revision(prediction, service_date="2026-11-20")
    newer = revision(
        prediction, predicted_arrival="2026-11-21T14:47:00-05:00",
        ingested_at="2026-11-21T17:59:00Z",
    )
    reverted = revision(prediction, ingested_at=newer["ingested_at"])
    assert reverted["event_id"] == prediction["event_id"]
    assert newer["event_id"] != reverted["event_id"]

    raw_predictions = records(spark, "predictions_arrets", [
        (0, schedule_id, prediction), (1, schedule_id, no_date),
        (2, schedule_id, wrong_stop), (3, "departure-only", departure_prediction),
        (4, schedule_id, newer), (5, schedule_id, reverted), (6, "bad-key", prediction),
        (7, schedule_id, wrong_day),
    ])
    predictions, rejected = stage_records(raw_predictions, "prediction")
    schedules, bad_schedules = stage_records(records(spark, "horaires_theoriques", [
        (0, schedule_id, schedule), (1, "departure-only", departure_schedule),
    ]), "schedule")

    assert {row.kafka_offset for row in predictions.collect()} == {0, 1, 2, 3, 4, 5, 7}
    assert [(row.kafka_offset, row.error_code) for row in rejected.collect()] == [(6, "key_mismatch")]
    assert bad_schedules.isEmpty()
    assert match_delays(predictions, schedules.limit(0), CUTOFF).isEmpty()
    matched = match_delays(predictions, schedules, CUTOFF)
    assert {row.kafka_offset: (row.comparison, row.delay_seconds) for row in matched.collect()} == {
        0: ("arrival", -639), 3: ("departure", 300), 4: ("arrival", -480), 5: ("arrival", -639),
    }
    assert matched.select("record_id").distinct().count() == 4

    no_comparable_time = schedules.withColumn("scheduled_departure", F.lit(None).cast("timestamp"))
    assert match_delays(predictions.where("kafka_offset = 3"), no_comparable_time, CUTOFF).isEmpty()
    gold = route_hours(
        arrivals_for_gold(predictions, schedules, CUTOFF),
        from_hour="2026-11-21T19:00:00Z", to_hour="2026-11-21T20:00:00Z", computed_at=CUTOFF,
    ).collect()
    assert len(gold) == 1
    assert gold[0].predicted_stop_count == 1 and gold[0].predicted_mean_delay_seconds == -639.0
    print("MBTA Spark fixture passed: -639s arrival, +300s departure, pending records, and revision replay.")


def main():
    spark = (
        SparkSession.builder.master("local[2]").appName("IL14 MBTA fixture")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        verify(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
