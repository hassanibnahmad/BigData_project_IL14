# BigData IL-14 — Student A Technical & Functional Handoff

**Project:** Real-Time Public Transport Monitoring  
**Contributor:** Student A — Hassan Ibnahmad  
**Scope:** MBTA vehicle positions, stop predictions and scheduled stop times, Kafka publication and reference validation.

**Audience:** Spark, Lakehouse, ML, Dashboard, Airflow and integration team members.

## Stop-time handoff for Student B

Use the [current setup and contract guide](../README.md) and [implementation report](STUDENT_A_IMPLEMENTATION_REPORT.md) for run commands, current evidence and the three topic schemas. The vehicle-focused sections below preserve historical project context.

The two additional topics are `predictions_arrets` and `horaires_theoriques`, with three partitions and one replica each. Join on MBTA `schedule_id` and confirmed `service_date`. Check `route_id`, `trip_id`, `stop_id` and `stop_sequence` before comparing timestamps. A null service date indicates that the producer could not establish a unique match to the schedules it loaded.

From Student B's PowerShell terminal after installing the project requirements, run:

```powershell
$env:KAFKA_BOOTSTRAP_SERVERS = '100.105.157.123:39092'
.\.venv\Scripts\python.exe -m kafka_ingestion.verify_stop_times --vehicles
```

This verifies Kafka access from the PC running the command and validates the retained events. Use `127.0.0.1:9092` on the Windows broker host. The Tailscale path from Student B's PC still needs verification.

## 1. Executive summary

Student A implemented the upstream ingestion layer that converts MBTA vehicle-position data into standardized JSON events and publishes them to Apache Kafka. The working pipeline uses a Python producer to fetch, normalize, validate, deduplicate and publish data; a separate Python consumer validates events against a JSON Schema, logs valid vehicle positions, and commits offsets manually. Kafka runs in Docker with internal, Windows-local, and Tailscale listeners. The Tailscale endpoint `100.105.157.123:39092` was verified from Student A's Windows PC. The Windows-local listener now advertises `127.0.0.1:9092` to match the IPv4 Docker binding. The primary Kafka topic is `positions_vehicules`, configured with three partitions. Kafka integration tests create isolated topics and retain them.

**Historical result (15 November 2026):** The previously supplied pytest execution reported **12 passed in 11.65 seconds**. This verifies tested Kafka publication/consumption, schema rules, actual consumer rejection/acceptance, key-based partition consistency, in-partition ordering, explicit commits and restart from committed offsets. It does not by itself verify end-to-end Spark, Hudi, MinIO, Airflow, Grafana, or a complete GTFS-based simulation.

## 2. Student A's functional responsibilities

The upstream system must supply a continuous stream of vehicle positions to downstream processing. Each event identifies a vehicle, route and trip, reports coordinates, and carries source and ingestion timestamps. Downstream students should not need to call the MBTA API themselves; they should read the Kafka topic and apply the agreed schema.

**Business / functional flow:**

1. Obtain current bus locations from the MBTA real-time feed (the working prototype targets routes 1, 66 and 77).
2. Convert the external response into the shared internal vehicle-event format.
3. Reject malformed records and remove duplicate observations before publication.
4. Send accepted events to Kafka, using the vehicle identifier as the message key.
5. Allow downstream consumers to process vehicle movements independently.
6. Validate the stream with a reference consumer and automated tests.

**Important boundary:** Position events alone do not calculate arrival times, punctuality or delay predictions. Those require GTFS schedule matching and downstream Spark/ML work. The original project specification additionally requested a GTFS-driven bus simulator, realistic simulated delays, and multiple concurrent simulated buses. This handoff does not claim those simulator capabilities were verified.

## 3. System architecture

```text
                 MBTA real-time vehicle API
                            |
                            v
                   Python producer
       Fetch -> Normalize -> Validate -> Deduplicate
                            |
                 JSON serialization
                            |
                  Kafka broker (Docker)
              Kafka topic: positions_vehicules
                            |
           positions_vehicules (3 partitions)
                  /                    \
                 v                      v
       Python validation          Spark Structured
           consumer                Streaming (team)
     schema -> log -> commit             |
                                       v
                          Bronze / Silver / Gold
                            Hudi / MinIO / ML
                                       |
                                  Grafana
```

Kafka decouples data acquisition from analytics. A producer can publish independently of the consumers, while consumer groups maintain their own reading progress. Partitioning by `vehicle_id` keeps messages with the same key in the same partition under a stable partition configuration. Kafka guarantees order **within a partition**, not a total order across all three partitions.

## 4. Implemented components and repository map

```text
BigData_IL14/
├── kafka_ingestion/
│   ├── producer.py                  # MBTA -> normalized Kafka events
│   └── consumer.py                  # Kafka -> schema validation / logs
├── contracts/
│   ├── vehicle_position.schema.json # Draft 2020-12 contract
│   └── vehicle_position.example.json
├── tests/
│   └── integration/
│       ├── test_kafka_integration.py
│       ├── test_consumer_validation.py
│       ├── test_kafka_partitioning.py
│       ├── test_consumer_offsets.py
│       └── test_schema_validation.py
├── requirements.txt
├── .env                              # local configuration, do not commit
└── run_integration_tests.ps1         # optional PowerShell runner
```

This tree reflects the working filenames used in the test sessions. The actual source tree should be checked for any additional deployment files. The `contracts/` directory is the agreed integration boundary; coordinate schema changes with the team contract owner before altering fields or types.

## 5. Infrastructure and Kafka configuration

| Setting | Value / meaning |
|---|---|
| Local Windows broker | `127.0.0.1:9092` (Windows host) |
| Docker-network broker | `kafka:29092` (Kafka CLI verified from container) |
| Tailscale broker | `100.105.157.123:39092` (**Python Kafka consumer verified on Student A's PC**) |
| Local host binding | `127.0.0.1:9092` |
| Tailscale host binding | `100.105.157.123:39092` |
| Kafka listeners | `INTERNAL`, `LOCAL`, `REMOTE`, `CONTROLLER` |
| Kafka data directory | `/tmp/kafka-logs`, with a persistent Docker named volume (verify current Compose file) |
| Auto-create topics | Disabled (`KAFKA_AUTO_CREATE_TOPICS_ENABLE=false`) |
| Docker container | `il14-kafka` (observed working setup) |
| Kafka version | Apache Kafka 3.9.1 reported for the local setup |
| Main topic | `positions_vehicules` |
| Main topic partitions | 3 |
| Integration topic | `positions_vehicules_test` (3 partitions) |
| Test-topic replication | 1, appropriate for a single-broker local lab |
| Event encoding | JSON, UTF-8 |
| Producer key | `vehicle_id` |
| Consumer group default | `il14-python-consumer` |
| Consumer offset reset default | `earliest` |
| Consumer auto-commit | Disabled; manual commits |

**Operational commands (PowerShell):**

```powershell
docker ps
docker exec il14-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --list
docker exec il14-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --describe --topic positions_vehicules
.\.venv\Scripts\python.exe -m pytest tests/integration/ -v
```

The Kafka CLI path above is typical of the Apache Kafka container image used in this lab; if the image differs, locate `kafka-topics.sh` inside the container. Do not recreate the main topic without checking the team's existing deployment configuration.

## 6. Producer pipeline — technical behavior

**Fetch.** Request live vehicle-position records from the MBTA API. The upstream feed is external and may be delayed, incomplete or unavailable. The precise endpoint, query parameters, polling interval and API-key handling should be confirmed in `kafka_ingestion/producer.py`.

**Normalize.** Map MBTA-specific response attributes into the common flat event contract. Normalize identifiers to strings, coordinates to numeric values and timestamps to ISO 8601 strings. `vehicle_timestamp` represents when the source reported the position; `ingested_at` represents when ingestion recorded the event.

**Validate.** Enforce the shared JSON Schema before publishing. A missing required field, impossible coordinate, wrong source, invalid timestamp format or unexpected property makes an event invalid.

**Deduplicate.** Avoid publishing repeated observations for the same vehicle and source timestamp. The event ID in the supplied example combines source, vehicle and observation time (`mbta-y1898-20261110T221532Z`). Verify the exact producer deduplication key, cache lifetime and restart behavior in source before relying on persistent exactly-once semantics.

**Publish.** Serialize valid events as UTF-8 JSON and send to `positions_vehicules` with `vehicle_id` as key. This key is important for per-vehicle partition consistency. The project requirements call for retries on Kafka connection failures; the precise retry policy, backoff and failure behavior were not independently inspected here.

**Delivery semantics.** Kafka may redeliver events after retries or consumer restarts. Consumers should be idempotent using `event_id` or another stable observation key. Producer-side deduplication is not a substitute for downstream upserts.

## 7. Event contract — JSON Schema

**Contract file:** `contracts/vehicle_position.schema.json`  
**Schema dialect:** JSON Schema Draft 2020-12  
**Root type:** object  
**Additional properties:** forbidden (`additionalProperties: false`)

| Field | Type | Required | Rule / meaning |
|---|---|---|---|
| `event_id` | string | Yes | Nonempty unique event identifier |
| `source` | string | Yes | Must equal `MBTA` |
| `vehicle_id` | string | Yes | Nonempty bus/vehicle identifier; Kafka key |
| `route_id` | string | Yes | Nonempty route identifier |
| `trip_id` | string | Yes | Nonempty trip identifier |
| `latitude` | number | Yes | -90 to 90 inclusive |
| `longitude` | number | Yes | -180 to 180 inclusive |
| `vehicle_timestamp` | string | Yes | RFC 3339 / JSON Schema `date-time` format |
| `ingested_at` | string | Yes | RFC 3339 / JSON Schema `date-time` format |

**Actual example event supplied with the project:**

```json
{
  "event_id": "mbta-y1898-20261110T221532Z",
  "source": "MBTA",
  "vehicle_id": "y1898",
  "route_id": "1",
  "trip_id": "78182009",
  "latitude": 42.343594,
  "longitude": -71.086029,
  "vehicle_timestamp": "2026-11-10T22:15:32Z",
  "ingested_at": "2026-11-10T22:15:37Z"
}
```

**Schema implications:** Every listed field is required. The `source` field is not generic: this contract currently accepts only `MBTA`. Any future GTFS simulator or other provider must either emit the agreed `MBTA` value only when truthful, or obtain a versioned contract change. `route_id` and `trip_id` are string identifiers, not guaranteed to match a separate GTFS dataset unless an explicit mapping is created. Coordinates are WGS84-style latitude/longitude values, but the schema does not specify a CRS property. JSON Schema `format` validation is enforced by the Python validator's `FormatChecker`.

## 8. Reference consumer — verified source behavior

**Source inspected:** `consumer.py`, corresponding to the working `kafka_ingestion/consumer.py` layout.

The consumer loads `.env` with `python-dotenv`, reads the JSON Schema from `contracts/`, builds a `Draft202012Validator` with `FormatChecker`, and subscribes to the configured topic using `kafka-python`. It deserializes each record from UTF-8 JSON. On a valid event it logs vehicle ID, route ID, coordinates, partition and offset, then calls `consumer.commit()`. On an invalid event it logs a warning and increments an invalid counter. It emits a summary (`Processed=N | Invalid=M`) after each polling cycle with activity and handles keyboard interruption by closing the consumer.

**Environment variables:**

```dotenv
KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092
KAFKA_TOPIC=positions_vehicules
KAFKA_CONSUMER_GROUP=il14-python-consumer
KAFKA_AUTO_OFFSET_RESET=earliest
```

**Run locally:**

```powershell
.\.venv\Scripts\Activate.ps1
python kafka_ingestion/consumer.py
```

**Important consumer limitation:** `consumer.commit()` without explicit per-partition offsets can commit the current positions of assigned partitions. Consequently, a failed validation followed by a successfully committed later record may advance the committed position beyond the invalid record. The code logs bad records but has **no dead-letter queue (DLQ)** and no durable retry/recovery mechanism for them. The test confirms rejection and continuation, not guaranteed replay of invalid records. This should be addressed before production use.

## 9. Automated tests and evidence

The user executed `python -m pytest tests/integration/ -v` on Windows, Python 3.13.7 and pytest 9.2.0. Final observed result: **12 passed in 11.65s** (15 November 2026).

| File | Test | Verified purpose |
|---|---|---|
| `test_kafka_integration.py` | `test_kafka_publish_consume` | Publish and receive a matching JSON event through Kafka |
| `test_consumer_validation.py` | `test_consumer_rejects_invalid_event` | Actual consumer rejects latitude 120, accepts a valid record, logs 1/1 summary |
| `test_kafka_partitioning.py` | `test_same_vehicle_key_uses_same_partition` | Same vehicle key maps to the same partition |
| `test_kafka_partitioning.py` | `test_messages_keep_order_within_partition` | Sequence 0..4 is preserved in one partition |
| `test_consumer_offsets.py` | `test_explicit_offset_commit` | Explicit commit records the next offset (observed 3) |
| `test_consumer_offsets.py` | `test_restart_resumes_from_committed_offset` | Same group resumes from committed progress |
| `test_schema_validation.py` | `test_valid_event` | Accepts the sample event |
| `test_schema_validation.py` | `test_missing_vehicle_id` | Rejects missing required identifier |
| `test_schema_validation.py` | `test_invalid_latitude` | Rejects latitude 120 |
| `test_schema_validation.py` | `test_invalid_longitude` | Rejects longitude -200 |
| `test_schema_validation.py` | `test_invalid_source` | Rejects source other than MBTA |
| `test_schema_validation.py` | `test_additional_property` | Rejects unknown fields |

Tests use a separate persistent integration topic or temporary test topics to avoid polluting the main stream. The consumer validation test launches the actual consumer subprocess and inspects logs. Offset tests were updated for `kafka-python 2.3.3`, whose `OffsetAndMetadata` constructor requires `leader_epoch` (the tested correction used `-1` for unknown epoch).

**Not yet demonstrated by this 12-test suite:** uncommitted-message replay, producer API outage handling, persistent deduplication, load/throughput benchmarks, multi-broker resilience, complete Spark-to-Grafana integration, and GTFS simulator accuracy. A third offset replay test was proposed but is not in the final 12 collected tests.

## 10. Reproduction guide for teammates

**Prerequisites:** Docker with the Kafka broker running; Python 3.13-compatible virtual environment; project dependencies installed; access to the MBTA feed if running the producer.

```powershell
# From BigData_IL14 project root
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt

docker ps

# Run reference consumer (separate terminal)
python kafka_ingestion/consumer.py

# Run producer (another terminal)
python kafka_ingestion/producer.py

# Validate the Kafka integration tests
python -m pytest tests/integration/ -v -s
```

The observed dependencies include `requests`, `pandas`, `python-dotenv`, `kafka-python`, `pytest` and `jsonschema`. Check the committed `requirements.txt` for the authoritative version ranges. Do not commit `.env` files, API keys, `.venv/`, logs or temporary test data.

**Typical healthy log lines:**

```text
INFO | Kafka consumer started
INFO | Topic: positions_vehicules
INFO | Vehicle=y1898 | Route=1 | Lat=42.343594 | Lon=-71.086029 | Partition=... | Offset=...
INFO | Summary | Processed=... | Invalid=...
```

## 11. Integration contract for other students

**Student C — Spark streaming:** Read `positions_vehicules` from Kafka. Kafka values are JSON bytes and keys identify vehicles. Parse the nine schema fields, retain Kafka `partition`, `offset` and timestamp for traceability, and validate event-time behavior. Do not assume total cross-partition order. Use `vehicle_timestamp` for event-time analysis and `ingested_at` for ingestion latency. Coordinate route/trip ID mapping with the GTFS reference dataset before computing arrivals or delays.

**Student B — Bronze/Silver/Gold and Hudi:** Persist raw Kafka payloads plus ingestion metadata in Bronze; use stable `event_id` for deduplication/upsert logic in Silver. Agree on a separate Silver/Gold schema: this upstream event does not include stop ID, scheduled arrival, actual arrival, delay minutes, or prediction values.

**Student D — ML and Grafana:** Expect position events only at this interface. ML features and dashboard measures such as delays and punctuality must be derived downstream and should not be inferred directly from latitude/longitude.

**Shared integration / Airflow:** Agree on Docker network hostnames. `127.0.0.1:9092` is the verified Windows host address; a containerized Spark job needs a broker address reachable from its own container network and matching Kafka advertised listeners. Coordinate service versions, retention, consumer group names, topic ownership and startup order.

**Contract ownership:** The team distribution document states that a nominated contract owner maintains shared contracts, with other section owners validating changes. Treat any field rename, schema extension, source expansion or timestamp semantic change as a coordinated, versioned change.

## 12. Functional and technical limitations

1. **Source mismatch with original simulation requirement:** The assignment specifies GTFS data parsing and a bus simulator; the demonstrated stream uses live MBTA positions. The team should explicitly accept this substitution or complete the simulator separately.
2. **Incomplete GTFS linkage:** Route and trip identifiers may not match the chosen GTFS dataset without a mapping and consistent service dates.
3. **No DLQ:** Invalid messages are logged but not durably quarantined.
4. **Commit granularity:** Manual `commit()` is not a per-message retry guarantee; later commits may skip earlier invalid messages.
5. **Delivery and duplicates:** At-least-once behavior and retry duplicates should be assumed; downstream writes should be idempotent.
6. **Local single-broker configuration:** Replication factor 1 is suitable for lab testing, not high availability.
7. **Observability:** Logs and counters exist; alerting, persistent metrics and operational dashboards are not shown.
8. **Security:** Credentials, TLS/SASL and production secrets management are not covered by the demonstrated setup.
9. **Integration:** No evidence yet of a full Kafka → Spark → Hudi → ML → Grafana run.

## 13. Handoff checklist

- [x] Kafka broker reachable and topic created in the tested environment.
- [x] Producer/consumer workflow established for MBTA position events.
- [x] Shared JSON Schema and example event supplied.
- [x] Kafka key partitioning and ordering tested.
- [x] Consumer schema validation and manual offset behavior tested.
- [x] Original local test suite: 12/12 passed (before networking changes).
- [x] Re-run the full suite with the IPv4 listener. See the implementation report for results.
- [ ] Verify code and documentation are pushed to the shared Git repository.
- [ ] Agree whether MBTA live data replaces or complements the requested GTFS simulator.
- [ ] Confirm route/trip ID compatibility with the GTFS dataset.
- [ ] Validate connectivity from the actual Spark container and process real events.
- [ ] Assign contract ownership and document schema version/change policy.
- [ ] Consider DLQ, explicit offset handling, producer failure tests and replay tests.

## 14. Source basis and verification notes

This handoff was prepared from: (a) the supplied `BigData.pdf` IL-14 work breakdown; (b) `Projet14_IL14_Repartition_et_Contrats.pdf`; (c) the supplied `vehicle_position.schema.json` and `vehicle_position.example.json`; (d) the supplied `consumer.py`; (e) available generated partition/offset test sources; and (f) the recorded local pytest outputs in the project conversation. Some producer details are described at the functional level because its final source was not included in the materials inspected for this document.

**Bottom line:** Student A's Kafka ingestion and validation/testing handoff is ready for the team. Completion of the original Section 1 GTFS simulator specification and full downstream end-to-end integration must be assessed separately.


## 15. November 2026 networking, persistence and remote handoff update

> **This section supersedes older local-only connectivity instructions in Sections 3–11.**
> It records observed results, not assumptions about what will work on another machine.

### 15.1 Broker endpoints and tested status

| Client location | Bootstrap server | Verified status |
|---|---|---|
| Inside `il14-kafka` / same Docker network | `kafka:29092` | Kafka broker API versions command succeeded |
| Student A Windows Python, local | `127.0.0.1:9092` | Topic metadata and Kafka integration tests passed |
| Student A Windows Python, via Tailscale | `100.105.157.123:39092` | Python Kafka metadata returned all three ingestion topics |
| Student B/C machine on same Tailscale network | `100.105.157.123:39092` | **Not tested yet**; requires Tailscale membership, access policy, firewall and actual Kafka consumer test |

**Observed host port mappings:**
```text
9092/tcp  -> 127.0.0.1:9092
39092/tcp -> 100.105.157.123:39092
```

**Observed broker environment:**
```text
KAFKA_LISTENERS=INTERNAL://0.0.0.0:29092, LOCAL://0.0.0.0:9092, REMOTE://0.0.0.0:39092, CONTROLLER://0.0.0.0:9093
KAFKA_ADVERTISED_LISTENERS=INTERNAL://kafka:29092, LOCAL://127.0.0.1:9092, REMOTE://100.105.157.123:39092
KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=INTERNAL:PLAINTEXT, LOCAL:PLAINTEXT, REMOTE:PLAINTEXT, CONTROLLER:PLAINTEXT
KAFKA_INTER_BROKER_LISTENER_NAME=INTERNAL
KAFKA_LOG_DIRS=/tmp/kafka-logs
KAFKA_AUTO_CREATE_TOPICS_ENABLE=false
```

These listener settings are taken from the current Docker Compose file. The Kafka image is `apache/kafka:3.9.1`. The volume should be mounted to `/tmp/kafka-logs`, which is the active log directory. **Do not run `docker compose down -v`**, as it deletes named volumes and stored Kafka data.

### 15.2 Student A — quick checks on Windows PowerShell

From the project root with `.venv` installed:

```powershell
docker ps
docker port il14-kafka
docker exec il14-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --list
docker exec il14-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --describe --topic positions_vehicules
.\.venv\Scripts\python.exe -c "from kafka import KafkaConsumer; c=KafkaConsumer(bootstrap_servers='100.105.157.123:39092', request_timeout_ms=15000); print('Connected to Kafka:', c.topics()); c.close()"
```

**Historical successful Python output from the vehicle-only setup:**
```text
Connected to Kafka: {'positions_vehicules', 'positions_vehicules_test'}
```

The command checks broker metadata. The current end-to-end evidence is in the [implementation report](STUDENT_A_IMPLEMENTATION_REPORT.md).

### 15.3 Student B/C — remote access prerequisites and commands

1. Install Tailscale and join the **same authorized tailnet** as Student A. Student A must approve/invite the teammate as appropriate.
2. Ensure Student A's computer, Docker Desktop, Kafka container and Tailscale are running. The IP is specific to Student A's Tailscale device and may change if device/network configuration changes.
3. On the teammate's Windows PC, check connectivity:

```powershell
tailscale ping 100.105.157.123
Test-NetConnection 100.105.157.123 -Port 39092
```

4. Install `kafka-python` in the teammate's own virtual environment:

```powershell
python -m pip install kafka-python
```

5. Save the following as `remote_consumer_test.py` and run `python remote_consumer_test.py`. It reads existing records from the earliest available offset and continues waiting for new ones; stop with Ctrl+C.

```python
import json
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "positions_vehicules",
    bootstrap_servers="100.105.157.123:39092",
    group_id="student-b-handoff-test",
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    value_deserializer=lambda raw: json.loads(raw.decode("utf-8")),
    consumer_timeout_ms=30000,
)

print("Connected; checking for events (up to 30 seconds of inactivity)...")
try:
    for record in consumer:
        event = record.value
        print(
            f"vehicle={event.get('vehicle_id')} "
            f"route={event.get('route_id')} "
            f"lat={event.get('latitude')} lon={event.get('longitude')} "
            f"partition={record.partition} offset={record.offset}"
        )
finally:
    consumer.close()
```

**Interpretation:** Receiving and printing a real JSON event verifies end-to-end teammate access. An empty result does not prove failure; the topic may contain no retained records, or the producer may be stopped. Student A should start the producer and confirm records are published. Use a **separate consumer group** for each independent application. This example intentionally disables commits because it is a read-only diagnostic.

### 15.4 Student C — Spark Structured Streaming connection example

The following is an **integration starting point**, not an executed or verified Spark job. Ensure the appropriate Spark/Kafka connector JAR matches the team's Spark/Scala versions.

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("IL14-Positions-Streaming").getOrCreate()
raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "100.105.157.123:39092")
    .option("subscribe", "positions_vehicules")
    .option("startingOffsets", "earliest")
    .load()
)

positions_json = raw.selectExpr(
    "CAST(key AS STRING) AS vehicle_key",
    "CAST(value AS STRING) AS value_json",
    "topic", "partition", "offset", "timestamp"
)
```

Use the shared `contracts/vehicle_position.schema.json` to define the structured fields and validation behavior. Keep `topic`, `partition`, `offset` and Kafka timestamp for traceability. Spark must connect from the **actual runtime network** (teammate PC, VM or container); testing Windows TCP alone does not establish that a Spark container can connect.

### 15.5 Known issues and remaining acceptance tests

- **Windows loopback:** The old `localhost:9092` advertisement led the Python client to IPv6 while Docker bound IPv4. Use `127.0.0.1:9092`. Integration tests allow explicit broker selection through `KAFKA_TEST_BOOTSTRAP_SERVERS`.
- **Tailscale access:** Student A verified the Tailscale address from the broker host. Run the verifier from Student B's PC to confirm remote connectivity.
- **Persistence:** Existing topics and their data persisted across the Compose recreation on 21 November 2026.
- **Security:** Kafka listeners currently use PLAINTEXT and have no demonstrated SASL/TLS authentication. Restrict access to trusted tailnet members with Tailscale policy and Windows firewall. Do **not** expose port 39092 on the public internet.
- **Source fidelity:** this handoff documents the observed MBTA live-data pipeline, not a fully demonstrated GTFS simulator. Consult the original assignment before treating the source substitution as approved.

### 15.6 Minimal information to provide an LLM

Share this Markdown **and the current Git repository**. The document explains design and operational context; the repository is authoritative for exact code, dependencies and configuration. A downstream LLM should inspect at least:

```text
kafka_ingestion/producer.py
kafka_ingestion/consumer.py
contracts/vehicle_position.schema.json
contracts/vehicle_position.example.json
requirements.txt
docker-compose.yml (or the actual Compose file)
tests/integration/
```

Prompt for teammate:

> I am implementing a downstream component of BigData_IL14. Read Student A's handoff and repository. Treat the shared JSON Schema and real code as authoritative. The remote Kafka bootstrap server is `100.105.157.123:39092` over an authorized Tailscale network; remote access from my own PC is not yet verified. First explain the input contract and dependencies, then help me implement and test my assigned component. Do not invent Kafka fields, topics, working tests or deployment results.
