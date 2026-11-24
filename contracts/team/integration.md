# Deployment and team acceptance

## Configuration

B records exact dependency and image versions before shared deployment. Initial compatibility target is Spark 3.5.x with Scala 2.12, the Kafka Spark connector for that exact Spark build, a Hudi Spark 3.5 Scala 2.12 bundle, and `hadoop-aws` matching Spark's bundled Hadoop version with its matching AWS dependencies. Pin and smoke test the specific combination before sharing it. Kafka is currently pinned to 3.9.1 in Compose. Use the same tested Spark runtime for C, D and Airflow submissions.

| Variable | Required value / default | Owner |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:29092` on Kafka's Compose network, `localhost:9092` on broker host, or configured shared hostname and port | A |
| `KAFKA_TOPIC` | `positions_vehicules` | A |
| `KAFKA_STOP_TOPIC` | `stop_events_v1` | A |
| `MBTA_ROUTES` | `1,66,77` | A |
| `MBTA_POLL_INTERVAL` | `20` seconds | A |
| `MBTA_API_KEY` | Optional, passed to A's MBTA API client | A |
| `IL14_GTFS_URI` | Exact extracted snapshot URI for C. A may use a local copy with the same manifest hash. Use a file URI for fixture mode | A, B |
| `IL14_BUCKET` | `il14` for shared integration. Distinct buckets for replay or local generations | B |
| `IL14_S3_ENDPOINT` | MinIO URL, including scheme and port | B |
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` | MinIO credentials via environment or deployment secrets | B |
| `IL14_GRAFANA_DB_URL` | PostgreSQL JDBC URL, e.g. `jdbc:postgresql://postgres:5432/il14` | D |
| `IL14_GRAFANA_DB_USER`, `IL14_GRAFANA_DB_PASSWORD` | Serving writer credentials; provide separate read credentials to Grafana | D |

Configure Spark S3A with the supplied endpoint, path-style access enabled and TLS matching the endpoint scheme. Keep credentials out of Git and logs. Connect to MinIO using the S3 API port. Use UTC for Spark and Airflow. B shares one runtime setup with A, C and D.

## Shared broker

A owns Kafka provisioning. Choose a shared university server, VPS or existing teammate host reachable on the team's private network. Make the advertised address reachable from each Kafka client's runtime. Use restricted private access for the current PLAINTEXT setup. Keep the Docker-internal hostname for services on Kafka's Compose network. The current Compose file binds to one teammate's Tailscale address. A must parameterize or replace that binding for a different deployment. For local development A should document a profile that works without the teammate's IP, and everyone can use the committed fixtures while networking is prepared.

## Acceptance sequence

1. **Offline** — Each owner uses the [fixture](fixtures/README.md) and role-page acceptance checks. No live feed or shared IP is required.
2. **Runtime** — A shares topic metadata and a newly published record. B and C consume it from inside their runtime containers. B verifies MinIO read, write and Spark/Hudi compatibility. D verifies its PostgreSQL datasource.
3. **Vertical slice** — Use empty, isolated Kafka topics, a fresh bucket and new checkpoints. Publish the same fixture values with the documented keys. Compare rejected records by decoded key/value and reason, or use producer metadata to map fixture records to offsets. Verify Bronze, quarantine, Silver, Gold and Grafana against the expected results. Have B restart a stream and verify logical rows are unchanged.
4. **Live route pilot** — Start with MBTA route 1. Compare observed join counts for the position trip IDs and normalized stop updates to the selected reference snapshot. Report counts for unmatched service dates, trips and stops separately. Verify MBTA stop estimates reach Grafana.
5. **Simulator and ML** — Replay multiple service days, verify repeatability, train on earlier dates and check holdout forecasts. Document where live data and simulation appear in the demo.
6. **Delivery** — All members record exact versions, shared access instructions, commands, known limitations, and actual acceptance evidence in the project report. Confirm live data and simulator requirements against the original brief when it is available.
