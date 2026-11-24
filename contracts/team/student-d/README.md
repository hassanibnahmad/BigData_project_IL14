# D — MLlib and Grafana

**Owns Sections 5 and 6.** Work in `ml/`, `grafana/` and Grafana/serving portions of Compose. C produces Silver and operational Gold. B provides the runtime and orchestrates your jobs.

## Your handoff

You use C's Silver simulated arrivals to train and evaluate the model. For Grafana, you serve C's vehicle positions and hourly metrics alongside your own model results. B schedules the jobs and supplies the Spark/Hudi runtime. A supplies the simulator history needed for the chronological split.

Start with the expected Silver and Gold fixture rows to build the dashboard and serving queries. Prepare the ML pipeline in parallel. The fixture has one service date, so the training acceptance run needs at least five active dates from A's simulator. Give B your launch commands and version parameters. Give the team the dashboard access details and model evaluation results. Use the shared [data contract](../data.md), [deployment settings](../integration.md), and [offline fixture](../fixtures/README.md).

## MLlib

V1 trains and evaluates on simulated arrivals. The target is `delay_seconds` from `silver/stop_delays_v1` rows whose kind is `simulated`. Live MBTA estimates remain operational predictions from A's feed. A real observed arrival source and a new validation plan are required for observed-delay model evaluation.

- Use one label per `stop_key` (latest by event time then event ID). Features for v1 are route ID, stop sequence, scheduled local hour and scheduled local day of week. All are known from the static schedule. Fit preprocessing on training data only and handle unseen routes. Use Spark MLlib regression and report MAE in seconds beside the zero-delay baseline.
- Select one simulator dataset with `--dataset-id`. Sort its distinct service dates. Require at least five dates, with at least one training row and one holdout row. Train on the first floor(0.8 × date count) dates and hold out the rest. Keep evaluation dates outside training. The tiny integration fixture verifies wiring, while A's multi-day simulator history drives training.
- Forecasts in v1 are offline simulated predictions. Their `forecast_at` is the scheduled arrival minus five minutes. The forecast is computed retrospectively using schedule features available at that time. Score only the model's holdout service dates. Save the exact holdout input alongside the model so a retry uses the same rows. Document model metrics and save the model with its dataset ID, split and feature list at `s3a://<bucket>/models/delay_v1/<model_version>/`. Assign a unique version per training artifact.
- Write [Gold ML predictions](../schemas/gold_delay_prediction.schema.json). Use `prediction_id = hash([model_version, stop_key])`. Store prediction seconds as a finite DOUBLE. Write `computed_at` at real execution time. Gold recomputes a given model's forecast idempotently.

Provide `spark-submit ml/train.py --dataset-id <sim:run-id> --model-version <version>` and `spark-submit ml/predict.py --model-version <version>`. B supplies the version and passes the same one to both jobs. Reuse an existing complete training artifact for the same version. Stage artifacts until complete and return nonzero on a failed run.

## Grafana

Provide a PostgreSQL serving database (`il14`), a Grafana PostgreSQL datasource, and `spark-submit grafana/refresh_serving.py`. Refresh the relations below from Hudi snapshots, through staging and one transaction so Grafana sees complete reads. Each relation uses the logical columns of its source schema. Map UTC timestamps to PostgreSQL `timestamptz`.

| PostgreSQL relation | Hudi source / rule |
|---|---|
| `il14_latest_vehicle_positions` | Silver positions, latest per vehicle by `vehicle_timestamp`, then `event_id`. |
| `il14_route_hourly` | Gold route hours, all rows. |
| `il14_delay_predictions` | Gold ML predictions, all rows. Empty until an ML run succeeds. |

Initialize the prediction relation with the contracted columns even if its Hudi table has never been created. Treat an absent prediction path as an empty input on the initial refresh. An unreadable or invalid existing table fails the refresh.

Expose a vehicle map and hourly estimates for live or simulated datasets, with clear labels for **MBTA predicted delay**, **simulated delay**, and **model predictions on simulation**. Show stop counts alongside averages. Let users filter by route, date range and dataset where applicable. Query only the serving database from Grafana. Give Grafana a read-only login and keep write credentials for the refresh job.

**Acceptance:** A refresh exposes the fixture's live estimate and simulated delay separately. Grafana shows a position and both route-hour series. A training run on at least five simulator service dates saves a model and publishes holdout predictions, with date split and metrics recorded.
