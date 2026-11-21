# BigData IL-14 | Student A ingestion

MBTA V3 routes 1, 66 and 77 flow through Python normalization, Draft 2020-12 validation and deduplication into Apache Kafka 3.9.1. Run the vehicle and stop-time producers in separate terminals. Spark can consume the resulting JSON values and perform the delay analysis.

| Kafka topic | Partitions / replicas | Kafka key | Contract and example |
| --- | --- | --- | --- |
| `positions_vehicules` | 3 / 1 | `vehicle_id` | [Schema](contracts/vehicle_position.schema.json) / [example](contracts/vehicle_position.example.json) |
| `predictions_arrets` | 3 / 1 | `schedule_id`, falling back to `prediction_id` | [Schema](contracts/prediction.schema.json) / [example](contracts/prediction.example.json) |
| `horaires_theoriques` | 3 / 1 | `schedule_id` | [Schema](contracts/schedule.schema.json) / [example](contracts/schedule.example.json) |

## Start on Windows PowerShell

Run from the repository root. Copy the example only when preparing a new environment. Add an optional MBTA API key to the local `.env`.

```powershell
python -m venv .venv
$py = '.\.venv\Scripts\python.exe'
& $py -m pip install -r requirements.txt
if (-not (Test-Path .env)) { Copy-Item .env.example .env }
docker compose up -d kafka
$env:KAFKA_BOOTSTRAP_SERVERS = '127.0.0.1:9092'
& $py -m kafka_ingestion.stop_times_producer --create-topics
& $py -m kafka_ingestion.stop_times_producer
```

Use another terminal for the existing vehicle producer and the vehicle validation consumer.

```powershell
$env:KAFKA_BOOTSTRAP_SERVERS = '127.0.0.1:9092'
.\.venv\Scripts\python.exe -m kafka_ingestion.producer
# Run the optional vehicle consumer in its own terminal.
.\.venv\Scripts\python.exe -m kafka_ingestion.consumer
```

The stop-time producer creates its missing topics on startup. On a fresh broker, create the existing vehicle topic before starting its producer.

```powershell
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --create --if-not-exists --topic positions_vehicules --partitions 3 --replication-factor 1
```

| Client location | Bootstrap server |
| --- | --- |
| Windows broker host | `127.0.0.1:9092` |
| Container on the Compose network | `kafka:29092` |
| Tailscale client | `100.105.157.123:39092` |

For an existing Windows `.env`, set `KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092` or override it in the terminal as above. The Docker loopback listener and its advertised address both use IPv4. The Tailscale listener has been tested from the broker host. Student B must run the Python check from their PC to verify that network path.

## Stop-time ingestion and joining

- Configuration uses `MBTA_API_URL`, `MBTA_API_KEY`, `MBTA_ROUTES`, `MBTA_POLL_INTERVAL` (20 seconds between polls by default) and `MBTA_SCHEDULE_REFRESH_INTERVAL` (21,600 seconds by default). API reads follow all next links and retry timeouts, connection errors and transient HTTP responses. HTTP 429 retries honor `Retry-After`.
- The background schedule refresh fetches the previous and current Boston dates at startup, when the Boston date changes, and after the configured interval. Date-filtered MBTA schedules carry the requested **service date**, even for stops after midnight. Each successful refresh replaces the in-memory lookup. For a finite run that fetches schedules before predictions, start the stop-time producer with `--once`. [MBTA V3 API reference](https://api-v3.mbta.com/docs/swagger/index.html)
- Both schemas retain MBTA IDs and source timezone offsets. Arrival and departure are nullable individually. Untimed schedules can appear, and predictions require at least one time. Predictions may have null `vehicle_id`, `schedule_id` or `service_date`. A prediction gets its service date from a unique identity-consistent match in the date-filtered schedule lookup, by matching the timestamps and identifiers of the included schedule. Startup, missing links, unavailable schedules or ambiguous dates can leave it null.
- If MBTA supplies one malformed prediction timestamp and one usable timestamp, the producer retains the usable timestamp and publishes null for the malformed side. Untimed cancellations are counted separately in the logs. Consumers should expire stale predictions because these contracts carry timed estimates.
- Retain one schedule version per `schedule_id`, `service_date`, route, trip, stop and sequence. Join predictions on these same fields. Compare arrival with arrival or departure with departure when both are present. Defer predictions with unresolved keys. Treat these events as estimates and retain ingestion timestamps and Kafka partition/offset to help trace updates. Ordering is per partition.
- The stop-time producer tracks up to 100,000 acknowledged entity versions per topic in memory. Changes to prediction timestamps or other normalized fields are published. Restart, cache eviction and ambiguous acknowledgments can produce repeats. The stable content hash in `event_id` excludes `ingested_at`. A previous prediction state can reappear as a legitimate update, so a global distinct on `event_id` can hide a reversion.

## Verify and test

```powershell
$py = '.\.venv\Scripts\python.exe'
$env:KAFKA_BOOTSTRAP_SERVERS = '127.0.0.1:9092' # Student B uses 100.105.157.123:39092
& $py -m kafka_ingestion.verify_stop_times --vehicles
$env:KAFKA_TEST_BOOTSTRAP_SERVERS = $env:KAFKA_BOOTSTRAP_SERVERS
& $py -m pytest -q
```

To run the stop-time producer for 30 minutes, use `.\.venv\Scripts\python.exe -m kafka_ingestion.stop_times_producer --duration-seconds 1800`. Poll and refresh summaries show published records, duplicates, rejected records by validation category and Kafka delivery errors. MBTA HTTP 429 responses are logged even when a later retry succeeds.

The snapshot verifier uses a Python consumer with no group commits, checks the JSON contracts and prints a matched pair if present. It reads retained messages and holds a schedule lookup in memory. Use `--timeout` for large snapshots. The test suite uses mocked MBTA responses and real Kafka integration tests. Test topics with unique names are retained.

See the [implementation report](docs/STUDENT_A_IMPLEMENTATION_REPORT.md) for test results and Kafka samples, and the [technical handoff](docs/BigData_IL14_Student_A_Hassan_Handoff_UPDATED.md) for the vehicle pipeline and team context.
