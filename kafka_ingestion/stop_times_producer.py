"""Publish validated MBTA schedules and stop predictions to Kafka."""

import argparse
from collections import Counter, OrderedDict, defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
import json
import os
from threading import Event
import time
from zoneinfo import ZoneInfo

import requests
from jsonschema import Draft202012Validator, FormatChecker
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import KafkaError, TopicAlreadyExistsError

from kafka_ingestion.producer import BROKER, POLL_INTERVAL, ROOT, create_producer, logger
from simulator.mbta_client import MBTAClient
from simulator.stop_times import normalize_prediction, normalize_schedule, relationship_id


TOPICS = {"schedule": "horaires_theoriques", "prediction": "predictions_arrets"}
BOSTON = ZoneInfo("America/New_York")


def validator_for(kind):
    schema = ROOT / "contracts" / f"{kind}.schema.json"
    return Draft202012Validator(
        json.loads(schema.read_text(encoding="utf-8")), format_checker=FormatChecker()
    )


def failure_reason(error):
    if isinstance(error, requests.HTTPError) and error.response is not None:
        return f"HTTP {error.response.status_code}"
    return type(error).__name__


def ensure_topics():
    admin = KafkaAdminClient(bootstrap_servers=BROKER.split(","))
    try:
        existing = set(admin.list_topics())
        for topic in TOPICS.values():
            if topic not in existing:
                try:
                    result = admin.create_topics([NewTopic(topic, 3, 1)])
                    for _, code, *_ in result.topic_errors:
                        if code not in (0, TopicAlreadyExistsError.errno):
                            raise KafkaError(f"Could not create {topic}: Kafka error {code}")
                except TopicAlreadyExistsError:
                    pass
        # Topic creation can return before the broker serves the new metadata.
        deadline = time.monotonic() + 15
        while True:
            descriptions = admin.describe_topics(list(TOPICS.values()))
            if all(
                description["error_code"] == 0
                and description["partitions"]
                and all(p["leader"] >= 0 for p in description["partitions"])
                for description in descriptions
            ):
                break
            if time.monotonic() >= deadline:
                raise KafkaError("Stop-time topic metadata did not become ready")
            time.sleep(0.25)
        for description in descriptions:
            partitions = description["partitions"]
            if len(partitions) != 3 or any(len(p["replicas"]) != 1 for p in partitions):
                raise KafkaError(f"Unexpected topic configuration for {description['topic']}")
    finally:
        admin.close()


class StopTimePublisher:
    def __init__(self, producer, cache_size=100_000):
        self.producer = producer
        self.cache_size = cache_size
        self.latest = {kind: OrderedDict() for kind in TOPICS}
        self.validators = {kind: validator_for(kind) for kind in TOPICS}

    def publish(self, kind, events, counts=None):
        if counts is None:
            counts = Counter()
        latest = self.latest[kind]
        pending = {}

        def acknowledge():
            failure = None
            for identity, (event_id, future) in pending.items():
                try:
                    future.get(timeout=30)
                except KafkaError as error:
                    counts["kafka_failures"] += 1
                    failure = failure or error
                    continue
                latest[identity] = event_id
                latest.move_to_end(identity)
                if len(latest) > self.cache_size:
                    latest.popitem(last=False)
                counts["published"] += 1
            pending.clear()
            if failure is not None:
                raise failure

        for event in events:
            errors = list(self.validators[kind].iter_errors(event))
            if errors:
                counts["invalid"] += 1
                reasons = {
                    "both_timestamps_null" if error.validator == "anyOf" else
                    f"{'.'.join(map(str, error.path)) or 'record'}.{error.validator}"
                    for error in errors
                }
                counts[f"rejected:{'+'.join(sorted(reasons))}"] += 1
                continue
            event_id = event["event_id"]
            if kind == "schedule":
                identity = (event["schedule_id"], event["service_date"])
            else:
                identity = (event["prediction_id"],)
            if identity in pending:
                if pending[identity][0] == event_id:
                    counts["duplicate"] += 1
                    continue
                acknowledge()
            if latest.get(identity) == event_id:
                latest.move_to_end(identity)
                counts["duplicate"] += 1
                continue
            key = event["schedule_id"] or event["prediction_id"]
            try:
                future = self.producer.send(TOPICS[kind], key=key, value=event)
            except KafkaError:
                counts["kafka_failures"] += 1
                acknowledge()
                raise
            pending[identity] = (event_id, future)
            if len(pending) >= 500:
                acknowledge()
        acknowledge()
        return counts


class StopTimeIngestion:
    def __init__(self, client, publisher, refresh_interval):
        self.client = client
        self.publisher = publisher
        self.refresh_interval = refresh_interval
        self.stop = Event()
        self.schedules = {}
        self.snapshot_day = None
        self.next_refresh = 0

    def refresh_schedules(self, day, client):
        counts = Counter()
        outcome = "incomplete"
        try:
            if self._refresh_schedules(day, client, counts):
                outcome = "complete"
        finally:
            logger.info("Schedules | %s | %s", outcome, json.dumps(counts, sort_keys=True))

    def _refresh_schedules(self, day, client, counts):
        schedules = defaultdict(list)
        validator = self.publisher.validators["schedule"]
        for offset in (-1, 0):
            date = (day + timedelta(days=offset)).isoformat()
            for page in client.iter_stop_time_pages("schedules", **{"filter[date]": date}):
                if self.stop.is_set():
                    return
                events = [normalize_schedule(record, date) for record in page["data"]]
                self.publisher.publish("schedule", events, counts)
                for event in events:
                    if validator.is_valid(event):
                        schedules[event["schedule_id"]].append(event)
        self.schedules = schedules
        self.snapshot_day = day
        self.next_refresh = time.monotonic() + self.refresh_interval
        return True

    def publish_predictions(self):
        counts = Counter()
        outcome = "incomplete"
        try:
            self._publish_predictions(counts)
            outcome = "complete"
        finally:
            logger.info("Predictions | %s | %s", outcome, json.dumps(counts, sort_keys=True))

    def _publish_predictions(self, counts):
        schedules = self.schedules
        for page in self.client.iter_stop_time_pages("predictions", include="schedule"):
            included = {
                record["id"]: record
                for record in page.get("included") or []
                if record.get("type") == "schedule"
            }

            def events():
                for record in page["data"]:
                    schedule_id = relationship_id(record, "schedule")
                    event = normalize_prediction(
                        record, schedules.get(schedule_id, ()), included.get(schedule_id)
                    )
                    attrs = record.get("attributes") or {}
                    if event["predicted_arrival"] is None and event["predicted_departure"] is None:
                        status = "cancelled" if attrs.get("schedule_relationship") == "CANCELLED" else "other"
                        counts[f"untimed:{status}"] += 1
                    if any(
                        event[f"predicted_{side}"] is None and attrs.get(f"{side}_time") is not None
                        for side in ("arrival", "departure")
                    ):
                        counts["recovered_timestamp"] += 1
                    yield event

            self.publisher.publish("prediction", events(), counts)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--once", action="store_true", help="Refresh schedules and poll predictions once")
    mode.add_argument("--create-topics", action="store_true", help="Create missing topics and exit")
    mode.add_argument("--duration-seconds", type=int, help="Stop starting new polls after this interval")
    args = parser.parse_args()
    if args.create_topics:
        ensure_topics()
        return

    refresh_interval = int(os.getenv("MBTA_SCHEDULE_REFRESH_INTERVAL", "21600"))
    if refresh_interval <= 0 or POLL_INTERVAL <= 0:
        parser.error("Polling and refresh intervals must be positive")
    if args.duration_seconds is not None and args.duration_seconds <= 0:
        parser.error("Duration must be positive")

    ensure_topics()
    producer = create_producer()
    worker = ThreadPoolExecutor(max_workers=1)
    ingestion = StopTimeIngestion(MBTAClient(), StopTimePublisher(producer), refresh_interval)
    try:
        schedule_client = MBTAClient()
        if args.once:
            try:
                ingestion.refresh_schedules(datetime.now(BOSTON).date(), schedule_client)
                ingestion.publish_predictions()
            except (requests.RequestException, KafkaError, ValueError) as error:
                logger.error("Stop-time run failed (%s)", failure_reason(error))
                raise SystemExit(1) from None
            return

        retry_refresh = 0
        refresh = None
        started = time.monotonic()
        deadline = started + args.duration_seconds if args.duration_seconds else float("inf")
        logger.info("Continuous polling started | interval=%ds", POLL_INTERVAL)
        while time.monotonic() < deadline:
            now = time.monotonic()
            day = datetime.now(BOSTON).date()
            if refresh is not None and refresh.done():
                try:
                    refresh.result()
                except (requests.RequestException, KafkaError, ValueError) as error:
                    logger.error("Schedule refresh failed (%s)", failure_reason(error))
                    retry_refresh = now + 60
                refresh = None
            if refresh is None and now >= retry_refresh and (
                day != ingestion.snapshot_day or now >= ingestion.next_refresh
            ):
                refresh = worker.submit(ingestion.refresh_schedules, day, schedule_client)
            try:
                ingestion.publish_predictions()
            except (requests.RequestException, KafkaError, ValueError) as error:
                logger.error("Prediction poll failed (%s)", failure_reason(error))
            time.sleep(max(0, min(POLL_INTERVAL, deadline - time.monotonic())))
        logger.info("Continuous polling finished | elapsed=%.2fs", time.monotonic() - started)
    except KeyboardInterrupt:
        logger.info("Stopping stop-time producer")
    finally:
        ingestion.stop.set()
        worker.shutdown()
        producer.close(timeout=30)


if __name__ == "__main__":
    main()
