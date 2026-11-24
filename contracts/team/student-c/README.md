# C — Spark real-time processing and Spark SQL

**Owns Section 4.** Work in `spark/`. Use B's shared runtime and the [data contract](../data.md). Begin with the fixtures.

## Your handoff

You take A's position and stop events from Kafka and use A's GTFS schedule snapshot to calculate stop delays. Your Silver tables give D clean positions and stop-level history. Your hourly Gold table gives Grafana route metrics. B provides the storage and checkpoints, then Airflow calls your job entry points.

Start with the fixture and reproduce the scheduled time, delay in seconds, and expected hourly rows. When A's broker and B's storage are ready, run the same checks through Kafka and Hudi. Give B the launch commands and restart behavior. Give D the Silver and Gold paths and a sample of the published rows. Use the shared [data contract](../data.md), [deployment settings](../integration.md), and [offline fixture](../fixtures/README.md).

## Streaming

- Provide one Kafka Structured Streaming query for positions and one for stop events, with B's independent checkpoints. Honor the deployment settings and use `earliest` only on the initial run. Fail if needed Kafka offsets disappear. Keep Kafka metadata and decode/validate in Spark. Persist rejected records as [quarantine envelopes](../schemas/quarantine_record.schema.json) and valid records to Hudi before acknowledging the batch. In a retry, write both destinations idempotently.
- Validate the existing position payload, convert its timestamps to UTC, then write [Silver positions](../schemas/silver_vehicle_position.schema.json). Keep GPS events even when their trip cannot be matched to static GTFS. The position contract carries no service date.
- Validate stop events and recompute the stop key and event ID. Join active service by `service_date`, `trip_id` and `stop_sequence`. Verify route and stop IDs against static GTFS. Reject unresolved or excluded matches. For simulated arrivals require `event_time == arrival_at`. Compute `delay_seconds = epoch(arrival_at) - epoch(scheduled_arrival_at)`. Positive means late, negative means early. Write [Silver stop delays](../schemas/silver_stop_delay.schema.json). Keep distinct prediction revisions to support historical analysis.
- Upsert by `event_id`, keeping a deterministic Kafka-offset winner. Use no lateness cutoff for Silver in v1. A replay preserves source time and offsets. For Hudi, preserve original schema case and Spark types.

## Hourly Spark SQL

Use [Gold route-hour rows](../schemas/gold_route_hour.schema.json). For each requested UTC hour, group Silver within that hour by `dataset_id` and `route_id`. Use the scheduled arrival's UTC hour and a half-open interval. Filter both kinds by `event_time <= computed_at`. For predictions also require `event_time <= scheduled_arrival_at`. A simulated arrival may occur after its scheduled hour. After these filters, keep the latest eligible event per stop key and kind by `event_time`, then the lexicographically greatest `event_id` on ties. Produce one row per populated group. Compute distinct stop counts and arithmetic mean delay independently for the two kinds. A mean is null if its count is zero, and empty groups produce no row. `metric_id = hash([dataset_id, route_id, UTC_hour_start_string])`. Recompute requested hours from Silver snapshots and upsert. A regular run revisits the previous 24 hours. B may request an older backfill. Capture one UTC computation time per run after previous runs have ended. Use UTC second precision for hour keys.

## Entry points for B

- `spark-submit spark/stream.py --source positions|stops --trigger available-now|continuous`. Both modes use the same source-specific checkpoint in a given environment. The continuous setting uses a 10-second processing-time trigger with the same microbatch sink as available-now, with graceful stop and nonzero exit on failure.
- `spark-submit spark/route_hours.py --from-hour <UTC> --to-hour <UTC>`. Both arguments align to UTC hours, from inclusive and to exclusive. `computed_at` must be at or after the exclusive end hour. Store `computed_at` as a UTC timestamp. Return nonzero on a failed write.
- For A's MBTA V3 inputs, also run `spark-submit spark/stream.py --source mbta --trigger available-now` and pass `--stop-input mbta` to the hourly job. This uses A's published schedule topic for live predictions. Keep the `stops` job for simulated arrivals. The live stream owns its independent checkpoint and two Silver staging tables. [Spark live input guide](../../../spark/README.md#live-mbta-input-from-student-a)

**Acceptance:** Reproduce the fixture's Silver and Gold results, including the midnight join, negative simulated delay, replays, stale estimates and GPS events with no arrival. Demonstrate one restart using an unchanged checkpoint. Prove that a malformed Kafka payload is quarantined without preventing a valid subsequent record from reaching Silver.
