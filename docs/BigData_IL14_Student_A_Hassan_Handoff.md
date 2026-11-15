# BigData IL-14 — Student A Technical & Functional Handoff

**Project:** Real-Time Public Transport Monitoring  
**Contributor:** Student A — Hassan Ibnahmad  
**Scope:** Data acquisition / Kafka ingestion (Sections 1 and 2), event contract, validation consumer, and integration testing  
**Status:** Kafka ingestion implementation demonstrated; 12 integration/contract tests passed on 15 November 2026  
**Audience:** Spark, Lakehouse, ML, Dashboard, Airflow and integration team members

> **Scope note.** This handoff documents the implementation discussed and tested in our work, the supplied event schema and consumer source, and the project's task distribution. The complete current producer source and Docker Compose file were not available for direct inspection when this handoff was prepared. Producer internals and exact commands should be cross-checked with the repository before deployment. The broader GTFS-driven simulator required by the initial task specification is not demonstrated by the Kafka test results; the working ingestion source is MBTA live vehicle positions.

## 1. Executive summary

Student A implemented the upstream ingestion layer that converts MBTA vehicle-position data into standardized JSON events and publishes them to Apache Kafka. The working pipeline uses a Python producer to fetch, normalize, validate, deduplicate and publish data; a separate Python consumer validates events against a JSON Schema, logs valid vehicle positions, and commits offsets manually. Kafka runs locally in Docker and exposes `localhost:9092`. The primary Kafka topic is `positions_vehicules`, configured with three partitions. A separate topic `positions_vehicules_test` is used for integration tests; other tests create temporary isolated topics.

**Verified result:** The final supplied pytest execution reported **12 passed in 11.65 seconds**. This verifies tested Kafka publication/consumption, schema rules, actual consumer rejection/acceptance, key-based partition consistency, in-partition ordering, explicit commits and restart from committed offsets. It does not by itself verify end-to-end Spark, Hudi, MinIO, Airflow, Grafana, or a complete GTFS-based simulation.

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
                    localhost:9092
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
| Broker endpoint | `localhost:9092` for local host-based Python clients |
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
docker exec il14-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
docker exec il14-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic positions_vehicules
python -m pytest tests/integration/ -v
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
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
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

**Shared integration / Airflow:** Agree on Docker network hostnames. `localhost:9092` works for Python on the host, but a containerized Spark job typically needs a broker address reachable from its own container network and matching Kafka advertised listeners. Coordinate service versions, retention, consumer group names, topic ownership and startup order.

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
- [x] Final local test suite: 12/12 passed.
- [ ] Verify code and documentation are pushed to the shared Git repository.
- [ ] Agree whether MBTA live data replaces or complements the requested GTFS simulator.
- [ ] Confirm route/trip ID compatibility with the GTFS dataset.
- [ ] Validate connectivity from the actual Spark container and process real events.
- [ ] Assign contract ownership and document schema version/change policy.
- [ ] Consider DLQ, explicit offset handling, producer failure tests and replay tests.

## 14. Source basis and verification notes

This handoff was prepared from: (a) the supplied `BigData.pdf` IL-14 work breakdown; (b) `Projet14_IL14_Repartition_et_Contrats.pdf`; (c) the supplied `vehicle_position.schema.json` and `vehicle_position.example.json`; (d) the supplied `consumer.py`; (e) available generated partition/offset test sources; and (f) the recorded local pytest outputs in the project conversation. Some producer details are described at the functional level because its final source was not included in the materials inspected for this document.

**Bottom line:** Student A's Kafka ingestion and validation/testing handoff is ready for the team. Completion of the original Section 1 GTFS simulator specification and full downstream end-to-end integration must be assessed separately.
