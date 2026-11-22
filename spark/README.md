# Student C — Spark processing

## Offline fixture

This local PySpark job reads the shared Kafka fixture as a batch, validates the JSON payloads, joins stop events to the extracted GTFS files, and computes Silver rows, quarantine records and Gold route-hour metrics. The job checks these results against the contract fixture and exits with an error if they differ.

From the repository root, use an environment with Java 17 and run:

```bash
python3 -m pip install -r spark/requirements-offline.txt
SPARK_LOCAL_IP=127.0.0.1 python3 -B -m spark.offline
```

Add `--output /tmp/il14-offline-results.json` to save the full logical rows in a new file. The output includes the original key and value in Base64 for quarantined records. Use a different path for a repeat export.

The expected hourly results for route 1 are **+300 seconds predicted** and **−120 seconds simulated**. The same run must return one Silver position, four distinct Silver stop delays, and two rejected records. The fixture includes a trip scheduled after midnight, a republished estimate, and an estimate issued after scheduled arrival. See the [fixture details](../contracts/team/fixtures/README.md).

## Kafka and Hudi

Use the same Python dependencies as the offline job and a Spark 3.5.9 / Scala 2.12 runtime. The examples use the matching Spark Kafka connector and Hudi 1.1.1. For MinIO, include `org.apache.hadoop:hadoop-aws` matching the Hadoop version bundled with your runtime (3.3.4 for the pinned PySpark distribution) and its matching AWS SDK dependencies. B supplies the shared runtime. [Deployment contract](../contracts/team/integration.md)

```bash
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export IL14_GTFS_URI="$PWD/contracts/team/fixtures/gtfs"
export PYTHONPATH="$PWD${PYTHONPATH:+:$PYTHONPATH}"
export SPARK_LOCAL_IP=127.0.0.1
PACKAGES=org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9,org.apache.hudi:hudi-spark3.5-bundle_2.12:1.1.1
STATE=/tmp/il14-spark-demo

spark-submit --master 'local[2]' --packages "$PACKAGES" spark/stream.py \
  --source positions --trigger available-now --local-root "$STATE"
spark-submit --master 'local[2]' --packages "$PACKAGES" spark/stream.py \
  --source stops --trigger available-now --local-root "$STATE"
spark-submit --master 'local[2]' --packages "$PACKAGES" spark/route_hours.py \
  --from-hour 2026-11-11T06:00:00Z --to-hour 2026-11-11T07:00:00Z --local-root "$STATE"
```

Use fresh topics and a new state directory for a new fixture run. Publish the fixture records to the broker before running the jobs. Use the same state directory when checking restart behavior. The default topic names are `positions_vehicules` and `stop_events_v1`. Set `KAFKA_TOPIC` and `KAFKA_STOP_TOPIC` if the broker provisions different names. See the [fixture publication instructions](../contracts/team/fixtures/README.md).

- `available-now` processes the current backlog and exits. `continuous` keeps consuming through 10-second microbatches. Run at most one writer for each source and its checkpoint.
- The two streams maintain separate checkpoints under `checkpoints/silver/`. A batch succeeds after the Hudi upsert and quarantine writes finish. A retry uses the same logical record keys and Kafka offset paths.
- Shared deployment omits `--local-root` and sets `IL14_BUCKET`, `IL14_S3_ENDPOINT`, `AWS_ACCESS_KEY_ID`, and `AWS_SECRET_ACCESS_KEY`. Provide the exact GTFS snapshot URI to the stop stream. Make the Python modules and dependencies available to all Spark workers and the contract files available to the driver.
- For the hourly job, use UTC hour boundaries. The start is inclusive and the end is exclusive. In Airflow, use B's agreed hourly window. [Student C contract](../contracts/team/student-c/README.md)
