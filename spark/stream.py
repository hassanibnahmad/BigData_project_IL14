"""Run one IL-14 Kafka query and write its checkpointed Silver Hudi output."""

import argparse
import os
import signal
import sys
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F

from spark.mbta_live import stage_batch
from spark.processing import process_positions, process_stops, read_gtfs
from spark.storage import (
    SILVER_DELAYS, SILVER_POSITIONS, required_env, session, storage_root, upsert_hudi, write_quarantine,
)

SOURCES = {
    "positions": ("KAFKA_TOPIC", "positions_vehicules", SILVER_POSITIONS),
    "stops": ("KAFKA_STOP_TOPIC", "stop_events_v1", SILVER_DELAYS),
}
MBTA_CHECKPOINT = "silver/mbta_live_v1"


def kafka_records(spark, bootstrap, topic):
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "true")
        .load()
        .select(
            "key", "value",
            F.col("topic").alias("kafka_topic"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_timestamp"),
        )
    )


def start_stream(spark, *, source, trigger, root):
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    if source == "mbta":
        predictions_topic = os.environ.get("KAFKA_PREDICTIONS_TOPIC", "predictions_arrets")
        schedules_topic = os.environ.get("KAFKA_SCHEDULES_TOPIC", "horaires_theoriques")
        if not predictions_topic or not schedules_topic or predictions_topic == schedules_topic:
            raise ValueError("Configure distinct Kafka topics for MBTA predictions and schedules")
        topic = f"{predictions_topic},{schedules_topic}"
        table = MBTA_CHECKPOINT
        gtfs = None
    else:
        variable, default, table = SOURCES[source]
        topic = os.environ.get(variable, default)
        gtfs = read_gtfs(spark, required_env("IL14_GTFS_URI")) if source == "stops" else None

    def process_batch(batch, epoch):
        batch = batch.persist()
        try:
            if source == "mbta":
                stage_batch(spark, batch, root, predictions_topic=predictions_topic,
                            schedules_topic=schedules_topic)
            else:
                silver, rejected = process_stops(batch, gtfs) if gtfs else process_positions(batch)
                upsert_hudi(silver, root, table, key="event_id", ordering="kafka_offset")
                write_quarantine(spark, rejected, root)
            print(f"Completed {source} batch {epoch}", flush=True)
        finally:
            batch.unpersist()

    query = (
        kafka_records(spark, bootstrap, topic).writeStream.queryName(f"il14_{source}")
        .option("checkpointLocation", f"{root}/checkpoints/{table}")
        .foreachBatch(process_batch)
    )
    if trigger == "available-now":
        return query.trigger(availableNow=True).start()
    return query.trigger(processingTime="10 seconds").start()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", choices=(*SOURCES, "mbta"), required=True)
    parser.add_argument("--trigger", choices=("available-now", "continuous"), default="available-now")
    parser.add_argument("--local-root", type=Path, help="Use an isolated local Hudi and checkpoint directory")
    args = parser.parse_args()
    root = storage_root(args.local_root)
    spark = session(f"IL14 Silver {args.source}", root)
    spark.sparkContext.setLogLevel("WARN")
    query = None

    def stop_requested(_signal, _frame):
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, stop_requested)
    try:
        query = start_stream(spark, source=args.source, trigger=args.trigger, root=root)
        query.awaitTermination()
    except KeyboardInterrupt:
        print(f"Stopping {args.source} stream", flush=True)
        if args.trigger == "available-now":
            raise SystemExit(130)
    finally:
        if query is not None and query.isActive:
            query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
