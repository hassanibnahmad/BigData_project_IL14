# IL-14 team integration contract v1.0.0

This is the proposed implementation baseline for Sections 1–8. The repository currently implements the MBTA position producer, the position schema, a diagnostic consumer, and a Kafka Compose service. The other interfaces below are agreed design defaults for starting parallel work and still need team adoption and implementation. Live feed coverage, a shared broker, and deployed service compatibility require integration checks. The professor's original brief was unavailable during the repository review.

## Start here

| Who | Build | Start with |
|---|---|---|
| A | GTFS data, simulator, producer and Kafka provisioning | [A — ingestion](student-a/README.md) |
| B | MinIO Bronze, Hudi configuration and Airflow | [B — storage and orchestration](student-b/README.md) |
| C | Spark Structured Streaming, schedule joins and Spark SQL metrics | [C — processing](student-c/README.md) |
| D | Spark MLlib prediction and Grafana | [D — ML and dashboard](student-d/README.md) |
| All | Compose integration, report and demo | [Integration and acceptance](integration.md) |

Start with your role folder and follow its implementation order. The shared [data contract](data.md) and [fixture instructions](fixtures/README.md) apply to all four roles. The JSON schemas define field types and required keys. The data contract adds rules that JSON Schema cannot express, including joins, time handling and replay. [Deployment settings](integration.md) identify environment-specific values.

## Team process

1. Each member reads their role README and checks the inputs, outputs, and acceptance criteria with the teammates who depend on them.
2. Build in parallel using the shared fixture. Use the proposed topic, column, path, and command names consistently.
3. Exchange a small real handoff first. A supplies events and a matching schedule snapshot, B supplies the runtime and storage, C supplies example Hudi rows, and D connects the serving layer and validates the dashboard.
4. Run the full acceptance sequence together. Record the versions, deployment settings, and evidence used in the demo. [Integration and acceptance](integration.md)

## Flow and ownership

- **A** publishes live or simulated events to Kafka and prepares versioned GTFS reference files.
- **B** mirrors both Kafka topics to raw Bronze on MinIO. **C** reads Kafka directly with independent Spark checkpoints. Bronze supports inspection and replay.
- **C** owns writes to Silver positions, Silver stop delays and Gold hourly metrics. **B** owns the storage configuration that C uses.
- **D** owns writes to Gold ML predictions and serves Gold data to Grafana. **B** schedules and monitors C and D jobs through Airflow.

## Give this to your coding agent

> Implement only my role (A, B, C or D). Start with contracts/team/student-<letter>/README.md (use my lowercase role letter). Read contracts/team/data.md, contracts/team/integration.md, and contracts/team/fixtures/README.md first. Check the current repository before assuming a proposed component exists. Preserve compatibility with the existing vehicle position schema. Use the fixture and expected output to develop locally. Implement the acceptance checks assigned to my role. Report what passed, any open dependency, and any contract change needed by another owner. Ask before changing another owner's code or a shared interface.

Changes to schemas, keys, routes between systems, or data meaning require review by the producer and every affected consumer. For an incompatible change, create a new schema and a versioned topic or table. Keep deployment values out of schemas. Update examples and acceptance checks with any contract change.
