# Shared offline fixture

These are synthetic examples of the MBTA interfaces. The `live` dataset label represents the live contract shape. The records and GTFS identifiers here are synthetic and must stay in local or isolated demo environments. The all-zero snapshot ID is reserved for this fixture and is not the hash of an MBTA ZIP.

From the repository root, run `python3 -B contracts/team/fixtures/verify.py` with the project's Python dependencies available. This checks schema conformance and recomputes the offline joins and expected aggregates.

1. Point `IL14_GTFS_URI` to the absolute file URI of [gtfs/](gtfs/). The fixture has a one-day service exception and a trip crossing midnight.
2. Use [kafka_records.jsonl](kafka_records.jsonl) as stand-in Kafka rows. Each line contains metadata, a UTF-8 key in `key`, and raw Kafka JSON text in `value` (one malformed value is intentionally included). For a real Kafka smoke test, publish only the key and value to the line's topic in order. Kafka assigns timestamps and offsets. Expect different Kafka metadata on that run. When testing the exact expected rows offline, use the fixture metadata.
3. Compare results to [expected.json](expected.json) by record key. Use `computed_at=2026-11-11T07:10:00Z` for the offline Gold evaluation. For a normal run, use its computation time and derive the fixture hour range explicitly. Compare logical values, ignoring current Kafka metadata and computed time when testing through a broker. Hudi internal metadata is excluded from comparison.

There are eight raw records and two rejections. The four Silver stop delay rows include three distinct live predictions (one was republished and must upsert) and one simulated arrival. The scheduled time `25:15:00` on service date November 10 becomes November 11 at 06:15 UTC. Gold picks the pre-schedule estimate of 06:20 (+300 seconds), excludes the later-issued 06:23 estimate, and shows the simulated 06:13 arrival (-120 seconds) separately. The GPS event creates a position row without adding a stop delay.

The ML table should be empty for this fixture because it covers only one service date. A's multi-day simulator output is a separate ML acceptance dependency.
