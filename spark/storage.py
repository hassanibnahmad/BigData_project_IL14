"""Section 4 storage paths and Hudi writes."""

import os
from pathlib import Path
from urllib.parse import urlsplit

from pyspark.sql import SparkSession, functions as F

SILVER_POSITIONS = "silver/vehicle_positions_v1"
SILVER_DELAYS = "silver/stop_delays_v1"
GOLD_HOURLY = "gold/route_hourly_v1"


def required_env(name):
    value = os.environ.get(name, "").strip()
    if not value:
        raise ValueError(f"Set {name} before running this job")
    return value


def storage_root(local_root):
    if local_root is not None:
        return Path(local_root).resolve().as_uri()
    return f"s3a://{os.environ.get('IL14_BUCKET', 'il14')}"


def session(name, root):
    builder = (
        SparkSession.builder.appName(name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
    )
    if root.startswith("s3a://"):
        endpoint = urlsplit(required_env("IL14_S3_ENDPOINT"))
        if endpoint.scheme not in ("http", "https") or not endpoint.netloc:
            raise ValueError("IL14_S3_ENDPOINT must be an HTTP(S) URL with a host")
        builder = (
            builder.config("spark.hadoop.fs.s3a.endpoint", endpoint.geturl())
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(endpoint.scheme == "https").lower())
            .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                    "com.amazonaws.auth.EnvironmentVariableCredentialsProvider")
        )
    return builder.getOrCreate()


def hadoop_path(spark, uri):
    context = spark.sparkContext
    path = context._jvm.org.apache.hadoop.fs.Path(uri)
    return path.getFileSystem(context._jsc.hadoopConfiguration()), path


def exists(spark, uri):
    fs, path = hadoop_path(spark, uri)
    return fs.exists(path)


def read_hudi_snapshot(spark, uri):
    # Hudi 1.1.1's column-statistics lookup can wait on Spark's cache lock
    # while a broadcast join is materializing a cached snapshot.
    return spark.read.format("hudi").option("hoodie.enable.data.skipping", "false").load(uri)


def write_hudi(rows, root, table, *, key, ordering, operation):
    rows = rows.persist()
    try:
        if rows.isEmpty():
            return False
        options = {
            "hoodie.table.name": table.rsplit("/", 1)[-1],
            "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
            "hoodie.datasource.write.operation": operation,
            "hoodie.datasource.write.keygenerator.class": "org.apache.hudi.keygen.NonpartitionedKeyGenerator",
            "hoodie.datasource.write.recordkey.field": key,
            "hoodie.table.ordering.fields": ordering,
            "hoodie.write.record.merge.mode": "EVENT_TIME_ORDERING",
        }
        (rows.write.format("hudi").options(**options).mode("append").save(f"{root}/{table}"))
        return True
    finally:
        rows.unpersist()


def upsert_hudi(rows, root, table, *, key, ordering):
    """Merge by logical key and keep the winner by offset or computation time."""
    return write_hudi(rows, root, table, key=key, ordering=ordering, operation="upsert")


def write_quarantine(spark, rows, root):
    # This small side output uses a deterministic object name per Kafka offset.
    payload = F.to_json(
        F.struct(*rows.columns),
        {"ignoreNullFields": "false", "timestampFormat": "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX"},
    ).alias("payload")
    records = rows.select("kafka_topic", "kafka_partition", "kafka_offset", payload)
    for row in records.toLocalIterator():
        relative = f"quarantine/spark/{row.kafka_topic}/{row.kafka_partition}/{row.kafka_offset}.json"
        fs, path = hadoop_path(spark, f"{root}/{relative}")
        fs.mkdirs(path.getParent())
        output = fs.create(path, True)
        try:
            output.write(bytearray((row.payload + "\n").encode("utf-8")))
        finally:
            output.close()
