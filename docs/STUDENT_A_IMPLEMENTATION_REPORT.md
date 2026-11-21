# Student A implementation report

**BigData IL-14 · 21 November 2026**

## 1. Summary

Added MBTA stop predictions and scheduled stop times for routes 1, 66 and 77. The pipeline follows API pagination, normalizes IDs and timestamps, validates events against Draft 2020-12 contracts, and deduplicates acknowledged versions before publishing subsequent observations. The existing vehicle producer and vehicle contract remain in service.

## 2. Files Created or Modified

| Paths | Purpose |
| --- | --- |
| `simulator/mbta_client.py`, `simulator/stop_times.py` | Paginated API reads, retries, normalization and service-date matching |
| `kafka_ingestion/stop_times_producer.py`, `kafka_ingestion/verify_stop_times.py` | Topic provisioning, publication and read-only Kafka verification |
| `contracts/{prediction,schedule}.{schema,example}.json` | Two contracts and two example events |
| `docker-compose.yml`, `.env.example`, `requirements.txt` | IPv4 loopback advertisement, configuration defaults and validation/timezone dependencies |
| `tests/unit/test_stop_times_contracts.py`, `tests/integration/test_stop_times_kafka.py` | Stop-time unit and real-broker integration coverage |
| `tests/integration/{test_consumer_offsets,test_consumer_validation,test_kafka_integration,test_kafka_partitioning}.py` | Configurable test broker, isolated topics and restart-offset assertion |
| `README.md`, `docs/BigData_IL14_Student_A_Hassan_Handoff_UPDATED.md`, this report | Setup, contracts, verification and handoff |

## 3. Kafka Topics

| Topic | Partitions / replicas | Purpose |
| --- | --- | --- |
| `positions_vehicules` | 3 / 1 | Existing live vehicle positions |
| `predictions_arrets` | 3 / 1 | Versioned stop arrival/departure predictions |
| `horaires_theoriques` | 3 / 1 | Scheduled stop times for each loaded service date |

The Kafka CLI verified all three topics via `kafka:29092`. Python metadata worked via `127.0.0.1:9092` and from the same PC via `100.105.157.123:39092`. The container recreation retained the existing Kafka volume and data.

## 4. JSON Contracts

Events retain MBTA route, trip, stop and schedule identifiers, stop sequence, source offsets and a UTC ingestion timestamp. Each stop-time field is nullable. Predictions require at least one arrival or departure time and allow null vehicle ID, schedule ID and service date. Schedules carry the service date from MBTA's date filter. Match on `schedule_id` plus `service_date` and verify route, trip, stop and sequence. [MBTA schedule API](https://api-v3.mbta.com/docs/swagger/index.html)

## 5. Tests Executed

`.\.venv\Scripts\python.exe -m pytest -q` on Windows against Kafka at `127.0.0.1:9092` returned **30 passed, 0 failed, 0 skipped** in **12.27 seconds** during the initial implementation. This covered schema formats and nullable fields, missing relationships, overnight and ambiguous dates, pagination, retries and rate limiting, deduplication and acknowledgment failure, publish/consume on the broker, and vehicle regressions including offsets and ordering.

The same command after the handoff fixes returned **34 passed, 0 failed, 0 skipped** in **12.73 seconds**. The additional checks cover either malformed prediction timestamp with a valid counterpart, mutually exclusive rejection counts, publishing a prediction when its date resolves, and keeping revisions of recurring schedule IDs separate by date. Coverage also checks ambiguous dates, equivalent timestamp offsets and nullable schedule times. Both JSON Schemas retain their original rules.

## 6. End-to-End Verification

- The existing vehicle producer fetched and acknowledged **25** live position events.
- `python -m kafka_ingestion.stop_times_producer --once` acknowledged **38,342** schedule events for the current and previous Boston dates and **1,271** predictions. It skipped **354** prediction records that failed contract validation.
- `python -m kafka_ingestion.verify_stop_times --vehicles --timeout 120` validated **25** vehicle events, **93,557** retained schedule messages, and **1,271** predictions. The schedule count includes earlier partial runs. All **1,271** published predictions matched a schedule with a comparable arrival or departure.

One matched pair read from Kafka is shown below. The first message came from `predictions_arrets` and the second from `horaires_theoriques`.

```json
{
  "event_id": "961d546a4dc16b98b976df8b953f42ce59cefc3b93671654ec9dc24da3b7d7cf",
  "source": "MBTA", "prediction_id": "prediction-78982154-64000-36-66",
  "schedule_id": "schedule-78982154-64000-36", "service_date": "2026-11-21",
  "route_id": "66", "trip_id": "78982154", "stop_id": "64000", "stop_sequence": 36,
  "vehicle_id": "y1851", "predicted_arrival": "2026-11-21T14:44:21-05:00",
  "predicted_departure": null, "ingested_at": "2026-11-21T17:58:07.387409+00:00"
}
```

```json
{
  "event_id": "5e28f39ae29735c2c949aaef94aecf33b7c057aadd279abd8c70aa0ac7f6c9f6",
  "source": "MBTA", "schedule_id": "schedule-78982154-64000-36",
  "service_date": "2026-11-21", "route_id": "66", "trip_id": "78982154",
  "stop_id": "64000", "stop_sequence": 36, "scheduled_arrival": "2026-11-21T14:55:00-05:00",
  "scheduled_departure": null, "ingested_at": "2026-11-21T17:57:57.479364+00:00"
}
```

### 30-minute sustained test

Ran both live producers for 1,800.00 seconds each with routes 1, 66 and 77 and a 20-second wait between polls. The stop-time run began at 18:30:57 UTC on 21 November 2026 with `--duration-seconds 1800`. The vehicle run began at 18:31:40 UTC and used the existing producer with a test wrapper to stop after 30 minutes. Both processes exited with code 0.

| Stream | Completed cycles | Failed cycles | Published | Duplicates | Skipped |
| --- | ---: | ---: | ---: | ---: | ---: |
| Predictions | 42 | 4 | 31,718 | 26,796 | 7,092 |
| Schedules | 1 refresh | 0 | 38,342 | 0 | 0 |
| Vehicle positions | 50 | 15 | 1,543 | 33 | 0 |

**HTTP 429 responses and Kafka delivery failures were both 0.** Prediction polls exhausted retries twice on read timeouts and twice on connection errors. Vehicle polls failed nine times on read timeouts, five times on DNS resolution, and once on a connection reset. Both producers subsequently completed successful polls. The schedule worker performed one refresh at startup under the normal six-hour interval.

Prediction validation counted **7,092** observations rejected for both timestamps being null, all marked `CANCELLED` by MBTA. No other validation category or malformed-side timestamp recovery occurred during the run. Counts are per observation and can include the same cancellation on multiple polls.

| Windows Python process | Memory samples | Peak working set | Median working set, early settled to final 5 minutes | Peak sampled private memory |
| --- | ---: | ---: | ---: | ---: |
| Stop times | 338 | 139.50 MiB | 133.00 to 135.46 MiB | 122.87 MiB |
| Vehicles | 346 | 54.38 MiB | 47.13 to 54.36 MiB | 39.37 MiB |

Working sets were sampled about every five seconds from the Python runtimes. Stop-time memory sampling began 42 seconds after polling started. The Windows peak working set covers the process lifetime up to each reading. The legacy vehicle producer keeps an in-memory set of seen events, so the 30-minute measurement does not establish its memory bound for longer runs.

The post-run verifier validated **131,899** retained schedules, **32,989** predictions and **1,568** vehicle positions. The increase from the earlier snapshot matched the three producer publication totals. **30,336** predictions joined to a schedule with comparable timestamps. The other **2,653** had a schedule ID and a null service date. Of those, **2,012** were ingested before the first schedule refresh completed.

## 7. Issues and Limitations

The original run logged one aggregate count of **354** invalid predictions and retained neither the rejected payloads nor error details. The exact breakdown of that historical batch is unavailable. A fresh audit before the soak examined **1,592** observations and found **228 rejects**. Every reject was an MBTA `CANCELLED` prediction with null arrival and departure and failed only the schema's at-least-one-time rule.

The producer now counts each rejected record under its exact combination of schema errors, with separate source counts for untimed cancellations. It preserves a valid prediction timestamp if the other timestamp is malformed, using null on the malformed side. Equivalent offset representations of the same scheduled time can now resolve to the same service date. Untimed cancellations remain excluded by the timed-prediction contract. Student B should expire stale estimates, including cases where a later update is a cancellation.

Initial live schedule attempts encountered HTTP 429. The completed run used larger schedule pages, with pagination and backoff retained. Predictions can have unresolved dates during startup or when MBTA lacks an identifiable schedule occurrence. Retries and restarts can leave repeat messages. The broker uses single-node replication. Student B's PC, live date rollover and a second scheduled refresh remain unverified.

## 8. Student B Handoff

With the broker and producers running on Student A's PC, run from the repository root on Student B's Windows PC.

```powershell
if (-not (Test-Path .venv)) { python -m venv .venv }
$py = '.\.venv\Scripts\python.exe'
& $py -m pip install -r requirements.txt
Test-NetConnection 100.105.157.123 -Port 39092
$env:KAFKA_BOOTSTRAP_SERVERS = '100.105.157.123:39092'
& $py -m kafka_ingestion.verify_stop_times --vehicles --timeout 120
```

Subscribe to `predictions_arrets,horaires_theoriques` from the Spark runtime. Parse the UTF-8 JSON values using their contracts. Keep the latest schedule version per join key and retain prediction updates. Join on the identifiers listed in Section 4 and compare like timestamps. Keep Kafka topic, partition and offset with each event for traceability. [Setup and contract guide](../README.md)

## 9. Final Status

| Component | Status |
| --- | --- |
| Vehicle ingestion and contract | COMPLETED |
| Prediction and schedule ingestion and contracts | COMPLETED |
| Topic provisioning and Windows IPv4 loopback | COMPLETED |
| Local live MBTA-to-Kafka round trip and joins | COMPLETED |
| Tailscale connection from Student B's PC | NOT TESTED |
| 30-minute sustained ingestion | COMPLETED |
| Live Boston date rollover and second scheduled refresh | NOT TESTED |
