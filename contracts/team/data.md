# Data contract v1

## Common rules

- Treat GTFS identifiers as case-sensitive strings. Preserve leading zeros. Use the MBTA static snapshot valid on the event's service day.
- Wire timestamps use UTC RFC 3339 with a trailing `Z`. Persist timestamps as Spark `TIMESTAMP` and run Spark with `spark.sql.session.timeZone=UTC`. Service dates use `YYYY-MM-DD` in MBTA's agency timezone (`America/New_York`).
- `hash([parts…])` means lowercase SHA-256 hex of the UTF-8 JSON array with no whitespace or ASCII escaping. Use the types specified for the fields. For example, stop sequence is a number in the array. The existing v1 position event ID algorithm is an exception defined below.
- `dataset_id` is `live` for production MBTA inputs or `sim:<run-id>` for a simulator run. Run IDs contain lowercase alphanumeric segments separated by single hyphens. Choose a new simulator run ID for a new scenario. Keep a run ID when replaying identical input.

## GTFS Static

Download [MBTA GTFS Static](https://cdn.mbta.com/MBTA_GTFS.zip). A records the original ZIP SHA-256, source URL, and retrieval time in a manifest. B stages the extracted snapshot at `s3a://<bucket>/reference/gtfs/<sha256>/`. Use extracted UTF-8 GTFS CSVs as supplied, preserving headers and field values. C selects one immutable snapshot per deployment generation via `IL14_GTFS_URI`, and copies its manifest `sha256` to `gtfs_snapshot_id`. A uses the same snapshot. Replay uses the original snapshot. For a refresh, stop both ingestion and processing, start a clean topic and bucket generation with the new snapshot, then switch consumers together. Keep the old generation readable. Reprocess only records compatible with the target snapshot.

| File | Fields consumed in v1 |
|---|---|
| `agency.txt` | `agency_timezone` |
| `routes.txt` | `route_id`, `route_type` (3 for bus) |
| `trips.txt` | `route_id`, `service_id`, `trip_id` |
| `stops.txt` | `stop_id`, `stop_lat`, `stop_lon` |
| `stop_times.txt` | `trip_id`, `stop_sequence`, `stop_id`, `arrival_time` |
| `calendar.txt` | `service_id`, day-of-week flags, `start_date`, `end_date` |
| `calendar_dates.txt` | `service_id`, `date`, `exception_type` |
| `frequencies.txt` (if present) | `trip_id`, used to exclude frequency trips in v1 |

At least one of the two calendar files is required. Apply exceptions as overrides (1 adds service, 2 removes it). A service date must have active service for the matched trip. Parse GTFS times as seconds since local noon minus 12 elapsed hours on the service date. Add seconds on the UTC timeline, including hours >=24. This preserves the GTFS convention across DST. [GTFS Schedule reference](https://gtfs.org/documentation/schedule/reference/#field-types)

## Kafka

Kafka keys and values are UTF-8. Values are single JSON objects with no wrapping array and no null tombstones. A provisions topics explicitly for the local and shared profiles. Use three partitions, replication factor one for the single-broker setup, delete cleanup and seven days of retention (`604800000` ms). B and C read independently. Preserve Kafka topic, partition, offset and timestamp. Kafka offsets order records within a partition and do not define event time.

| Topic | Key | Value | Status |
|---|---|---|---|
| `positions_vehicules` | `vehicle_id` | [Existing schema](../vehicle_position.schema.json) | Implemented producer |
| `stop_events_v1` | `stop_key` | [Stop event schema](schemas/stop_event.schema.json) | Proposed v1 |

### Positions

Keep the existing nine-field schema and producer behavior. The producer generates lowercase SHA-256 of the UTF-8 text `vehicle_id|route_id|trip_id|UTC_timestamp`. The example permits a different ID format. Consumers treat IDs as opaque. `vehicle_timestamp` is the position event time and `ingested_at` is when A normalized it. Coordinates are decimal degrees. Position data supports the map and movement analysis. It does not establish a stop arrival.

### Stop events

A publishes one event per accepted stop update. Live input is the [MBTA Trip Updates JSON feed](https://cdn.mbta.com/realtime/TripUpdates.json). Stop prediction fields and their interpretation are defined in [MBTA GTFS-Realtime documentation](https://github.com/mbta/gtfs-documentation/blob/master/reference/gtfs-realtime.md) and the [GTFS-Realtime reference](https://gtfs.org/documentation/realtime/reference/).

- Forward scheduled trips and scheduled stop updates with an absolute `arrival.time` and source `trip.start_date`. Use the trip-update timestamp if present, otherwise the feed-header timestamp. Skip updates with no source timestamp, no service date, or no absolute arrival. Record per-reason counters. Trip cancellations, skipped stops, added/replacement trips and frequency trips are outside v1. Gold aggregates describe the accepted estimates, without corrections for later cancellations.
- Normalize the service date from `YYYYMMDD`. Resolve a missing `stop_sequence` only if a supplied `stop_id` occurs once in the matching trip. Resolve a missing `stop_id` by sequence. If both are supplied, require agreement. Derive `route_id` from static GTFS and require any upstream route ID to agree. The normalized event always carries all join fields.
- `kind=predicted` means `arrival_at` is an estimate from the live feed for the given stop. `kind=simulated` means `arrival_at` is the arrival generated by A's simulator, emitted when the simulated clock reaches that arrival. V1 has no real-world observed arrival source.
- `event_time` is the timestamp of the update or simulated occurrence. Use `ingested_at` for pipeline latency, never for schedule matching. Ignore source estimates with `event_time >= arrival_at` so past arrival estimates are excluded.
- `stop_key = hash([dataset_id, service_date, trip_id, stop_sequence])`. `event_id = hash([stop_key, kind, event_time_epoch_seconds, arrival_at_epoch_seconds])`. Stop event times have whole-second precision and use the exact `YYYY-MM-DDTHH:MM:SSZ` form. Generate the ID before publishing. Different estimates are distinct events. Republishing the same estimate may change `ingested_at` and retains its ID.
- Simulator input reuses the static GTFS identifiers and publishes `kind=simulated` on the stop topic. Use a separate demo broker or topic namespace if simulating GPS, since the existing position schema identifies the source as MBTA. Mixing synthetic positions into the live position topic would mislabel them.

## Storage interfaces

Base URI is `s3a://<IL14_BUCKET>/`. Table paths below are relative to it. Detailed write and read responsibilities are in the role pages.

| Path | Grain | Record key | Writer |
|---|---|---|---|
| `bronze/kafka/<topic>/<partition>/<offset>.json` | One raw Kafka record per object | topic / partition / offset | B |
| `quarantine/spark/<topic>/<partition>/<offset>.json` | One rejected Kafka record per object | topic / partition / offset | C |
| `silver/vehicle_positions_v1` | One valid GPS observation | `event_id` | C |
| `silver/stop_delays_v1` | One prediction revision or simulated arrival | `event_id` | C |
| `gold/route_hourly_v1` | One dataset, route and UTC scheduled hour | `metric_id` | C |
| `gold/delay_predictions_v1` | One prediction per model and forecasted stop occurrence | `prediction_id` | D |

Bronze and quarantine use their JSON schemas. For Hudi logical rows, use the corresponding [row schemas](schemas/). JSON wire representations of timestamps become Spark `TIMESTAMP` columns. Other strings remain STRING, `number` is DOUBLE, `stop_sequence` and `kafka_partition` are INT, and remaining integers are BIGINT. All fields are non-nullable unless stated. Hudi metadata columns are outside the logical schemas.

Common Silver columns are `kafka_topic`, `kafka_partition`, `kafka_offset`, `kafka_timestamp`. Bronze and quarantine store the original key and value as standard padded Base64 so invalid UTF-8 survives. A Kafka null becomes JSON null in those envelopes. C accepts only strict UTF-8 and JSON objects with unique property names and finite numbers. A null value is invalid JSON, a valid JSON scalar is invalid schema, and a null key is a key mismatch. C uses the first applicable rejection reason in this order: `invalid_encoding`, `invalid_json`, `invalid_schema`, `key_mismatch`, `bad_event_id` (stop events only), `unmatched_schedule`, `invalid_prediction_time`, `invalid_simulated_time`. Require `dataset_id=live` for predicted events and a `sim:` dataset for simulated events as part of schema validation. A Kafka key differing from the payload `stop_key` is `key_mismatch`. A payload with an incorrectly calculated `stop_key` or stop event ID is `bad_event_id`. No valid Silver row can point to a rejected offset.

All Hudi tables use Copy-on-Write and are non-partitioned in v1. Use upserts. Each table has one active writer. For Silver, use Kafka offset as the event ordering field, with stable key partitioning and no topic recreation within a checkpoint lineage. The same event ID must remain in one partition. If a topic needs recreation or partition-count changes, use a new isolated output/checkpoint generation and plan replay. Gold uses `computed_at` ordering. Using full payloads and deterministic keys means replays do not add duplicate logical rows.
