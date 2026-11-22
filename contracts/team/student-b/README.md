# B — MinIO, Hudi and Airflow

**Owns Sections 3 and 7.** Work in `lakehouse/`, `airflow/` and storage/orchestration portions of Compose.

## Your handoff

You provide the storage and job runtime that C and D use. Your Bronze consumer archives raw Kafka messages independently of C's Spark stream. C owns the Silver and hourly Gold writes, and D owns the ML prediction write. You provide the Hudi configuration, paths, and checkpoint storage for those jobs.

Start with MinIO, the Bronze record format, and the Spark/Hudi runtime using the offline fixture. You can prepare Airflow using the documented job commands while C and D implement them. For the handoff, give both teammates the tested runtime versions, bucket and endpoint settings, how to obtain credentials, and the checkpoint paths. Use the shared [data contract](../data.md), [deployment settings](../integration.md), and [offline fixture](../fixtures/README.md).

## Storage

1. Provide MinIO and the bucket `il14` by default. Stage A's immutable reference revisions under `reference/gtfs/<sha256>/`. Publish the access variables and Hudi options listed in [integration.md](../integration.md). Test path-style S3A from the Spark runtime.
2. Run an independent Bronze consumer with group ID `il14-bronze-v1` on the dedicated broker generation, subscribed to both application topics. Start at earliest only on first deployment. Resume from committed offsets and fail if a previously committed offset has expired. Persist each Kafka record to its deterministic offset path using [the raw schema](../schemas/bronze_record.schema.json). Preserve bytes even if the payload is malformed. Confirm the object write before advancing offsets and never skip an unwritten offset in the same partition. Replaying a record reuses the same object path. On a topic reset, use a new bucket for the entire generation. For a Bronze replay, B republishes the original key and value bytes in partition offset order to an isolated broker with fresh checkpoints and outputs. Keep source event fields unchanged. Kafka assigns new offsets, so compare logical event IDs and values separately from Kafka metadata.
3. Provide the compatible Spark/Hudi/S3A runtime and table configuration. Use the paths, keys and ordering in [data.md](../data.md). C initializes and writes its three Hudi tables. D initializes and writes the ML prediction table. B verifies the resulting table configuration and snapshot reads; table initialization and all other writes remain with each table's writer.
4. Provide durable paths `checkpoints/silver/vehicle_positions_v1` and `checkpoints/silver/stop_delays_v1` in the same bucket, with independent offset progress. Define separate buckets for local, shared and replay generations.

## Airflow

Use three independently scheduled workflows. Maintain one active writer per table, stop superseded runs before starting replacements, propagate process exit codes, and alert on failed jobs.

| Workflow | Default schedule | Job order |
|---|---|---|
| `il14_silver` | Every minute, no catchup | Launch C's two bounded `available-now` streams against their own checkpoints. Mark success when both finish. |
| `il14_hourly` | At minute 10 each hour, no catchup | Call C's route-hours job with UTC hours covering the last 24 closed hours. After Hudi commit, call D's Grafana refresh. |
| `il14_ml` | Daily at 02:00 UTC, no catchup | Train a versioned D model, score its saved holdout, then refresh Grafana. Pass one `--dataset-id` from DAG configuration and derive `--model-version` as `<run-id>-<Airflow logical date in UTC YYYYMMDDTHHMMSSZ>`, stripping the `sim:` prefix from the run ID. Retries reuse that version. Enable after simulator history passes D's data gate. |

Set max active runs to 1 per workflow and put Grafana refresh tasks in a shared Airflow pool of one slot. Both refresh callers use the same entry point. Bronze is a continuous service with its own restart policy. For a live demo, B may run C's continuous mode instead of scheduling the same Silver jobs. Pause `il14_silver` before switching modes and retain checkpoints.

When using A's live MBTA V3 topics, include `predictions_arrets` and `horaires_theoriques` in Bronze. Run C's `--source mbta` stream alongside positions and any simulator stream. It uses `checkpoints/silver/mbta_live_v1` and C's two MBTA Silver staging tables. Pass `--stop-input mbta` to the hourly job. [Live Spark handoff](../../../spark/README.md#live-mbta-input-from-student-a)

**Acceptance:** A malformed record lands in Bronze unchanged. Simulate a retry and confirm the raw object count is stable. C and D can write and read their respective Hudi tables over S3A. Restart the Silver workflow and confirm it resumes progress. Kill a bounded job and confirm Airflow reports failure and a later retry uses the prior checkpoint. When services run in separate containers, validate advertised Kafka listener and MinIO reachability from those containers.
