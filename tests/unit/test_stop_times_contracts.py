import json
from datetime import date
from unittest.mock import Mock

import pytest
import requests
from jsonschema import Draft202012Validator
from kafka.errors import KafkaTimeoutError

from kafka_ingestion.stop_times_producer import (
    ROOT, StopTimeIngestion, StopTimePublisher, validator_for,
)
from simulator.mbta_client import MBTAClient
from simulator.stop_times import normalize_prediction, normalize_schedule


def record():
    return {
        "id": "prediction-id",
        "relationships": {
            key: {"data": {"id": value}}
            for key, value in {"route": "66", "trip": "t", "stop": "s", "schedule": "id"}.items()
        },
        "attributes": {"departure_time": "2026-11-22T00:10:00-05:00", "stop_sequence": 4},
    }


@pytest.mark.parametrize("kind", ["prediction", "schedule"])
def test_examples(kind):
    validator = validator_for(kind)
    Draft202012Validator.check_schema(validator.schema)
    validator.validate(json.loads((ROOT / "contracts" / f"{kind}.example.json").read_text()))


def test_prediction_nullables_and_timestamp_formats():
    event = normalize_prediction(record(), [], None)
    validator = validator_for("prediction")
    assert event["vehicle_id"] is None
    assert event["predicted_arrival"] is None
    assert event["service_date"] is None
    validator.validate(event)
    assert not validator.is_valid(dict(event, predicted_departure=None))
    assert not validator.is_valid(dict(event, predicted_departure="2026-11-22T00:10:00"))
    validator.validate(dict(event, predicted_arrival=event["predicted_departure"], predicted_departure=None))
    assert not validator.is_valid(dict(event, service_date="2026-02-30"))


@pytest.mark.parametrize("side", ["arrival", "departure"])
def test_invalid_prediction_timestamp_does_not_discard_valid_counterpart(side):
    raw = record()
    valid = "2026-11-22T00:10:00-05:00"
    other = "departure" if side == "arrival" else "arrival"
    raw["attributes"].update({f"{side}_time": "2026-11-22T00:10:00", f"{other}_time": valid})
    event = normalize_prediction(raw, [], None)
    assert event[f"predicted_{side}"] is None
    assert event[f"predicted_{other}"] == valid
    validator_for("prediction").validate(event)


def test_rejection_reports_exact_combination_once():
    raw = record()
    raw["attributes"]["departure_time"] = None
    raw["relationships"].pop("trip")
    event = normalize_prediction(raw, [], None)
    publisher = StopTimePublisher(Mock())
    assert publisher.publish("prediction", [event]) == {
        "invalid": 1, "rejected:both_timestamps_null+trip_id.type": 1,
    }


def test_missing_required_relationship_and_nullable_schedule_link():
    raw = record()
    raw["relationships"]["schedule"] = {"data": None}
    event = normalize_prediction(raw, [], None)
    assert event["schedule_id"] is None
    validator_for("prediction").validate(event)
    raw["relationships"].pop("trip")
    assert not validator_for("prediction").is_valid(normalize_prediction(raw, [], None))


def test_service_date_from_schedule_with_overnight_and_recurring_id():
    raw = record()
    included = {**raw, "id": "id"}
    overnight = normalize_schedule(included, "2026-11-21")
    other = {**overnight, "service_date": "2026-11-22",
             "scheduled_departure": "2026-11-23T00:10:00-05:00"}
    validator = validator_for("schedule")
    validator.validate(overnight)
    assert overnight["scheduled_arrival"] is None
    validator.validate(dict(overnight, scheduled_departure=None))
    validator.validate(dict(overnight, scheduled_arrival=overnight["scheduled_departure"],
                            scheduled_departure=None))
    assert not validator.is_valid(dict(overnight, service_date=None))
    assert not validator.is_valid(dict(overnight, scheduled_departure="2026-11-22"))
    assert normalize_prediction(raw, [overnight, other], included)["service_date"] == "2026-11-21"
    included_utc = {**included, "attributes": {**included["attributes"],
                    "departure_time": "2026-11-22T05:10:00Z"}}
    normalized = normalize_prediction(raw, [overnight, other], included_utc)
    assert normalized["service_date"] == "2026-11-21"
    assert normalized["predicted_departure"] == raw["attributes"]["departure_time"]
    ambiguous = dict(overnight, service_date="2026-11-22")
    assert normalize_prediction(raw, [overnight, ambiguous], included)["service_date"] is None
    assert normalize_prediction(raw, [overnight], None)["service_date"] is None
    assert normalize_prediction(raw, [overnight, other], None)["service_date"] is None
    assert normalize_prediction(raw, [dict(overnight, stop_id="other")], None)["service_date"] is None


def test_prediction_updates_and_bounded_dedup():
    kafka = Mock()
    publisher = StopTimePublisher(kafka, cache_size=1)
    raw = record()
    first = normalize_prediction(raw, [], None)
    raw["attributes"]["departure_time"] = "2026-11-22T00:12:00-05:00"
    second = normalize_prediction(raw, [], None)
    assert first["event_id"] != second["event_id"]
    counts = publisher.publish("prediction", [first, first, second, first])
    assert counts == {"published": 3, "duplicate": 1}
    kafka.send.assert_called_with("predictions_arrets", key="id", value=first)
    publisher.publish("prediction", [dict(second, prediction_id="other")])
    assert publisher.publish("prediction", [first]) == {"published": 1}


def test_resolved_date_is_a_prediction_update_and_schedule_dates_are_distinct():
    raw = record()
    linked = dict(raw, id="id")
    yesterday = normalize_schedule(linked, "2026-11-21")
    today = normalize_schedule(linked, "2026-11-22")
    revised = normalize_schedule(
        dict(linked, attributes=dict(linked["attributes"], departure_time="2026-11-23T00:10:00-05:00")),
        "2026-11-22",
    )
    unresolved = normalize_prediction(raw, [], linked)
    resolved = normalize_prediction(raw, [yesterday], linked)
    repeat = dict(resolved, ingested_at="2026-11-22T05:11:00Z")
    publisher = StopTimePublisher(Mock())
    assert publisher.publish("schedule", [yesterday, today, yesterday, revised, revised]) == {
        "published": 3, "duplicate": 2,
    }
    assert publisher.publish("prediction", [unresolved, resolved, repeat]) == {
        "published": 2, "duplicate": 1,
    }


def test_failed_ack_is_retried():
    kafka = Mock()
    publisher = StopTimePublisher(kafka)
    event = normalize_prediction(record(), [], None)
    kafka.send.return_value.get.side_effect = [KafkaTimeoutError(), None]
    with pytest.raises(KafkaTimeoutError):
        publisher.publish("prediction", [event])
    assert publisher.publish("prediction", [event]) == {"published": 1}


def response(status, payload=None, headers=None):
    result = requests.Response()
    result.status_code = status
    result._content = json.dumps(payload).encode()
    result.headers.update(headers or {})
    return result


def test_schedule_pagination_preserves_query_and_follows_next_link():
    client = MBTAClient()
    client.base_url = "https://api.example"
    client.session = Mock()
    client.session.get.side_effect = [
        response(200, {"data": [1], "links": {"next": {"href": "?offset=1"}}}),
        response(200, {"data": [2]}),
    ]
    pages = list(client.iter_stop_time_pages("schedules", **{"filter[date]": "2026-11-21"}))
    assert [item for page in pages for item in page["data"]] == [1, 2]
    first, second = client.session.get.call_args_list
    assert first.kwargs["params"]["filter[date]"] == "2026-11-21"
    assert second.args[0] == "https://api.example/schedules?offset=1"
    assert second.kwargs["params"] is None


def test_pagination_rejects_cross_origin_links():
    client = MBTAClient()
    client.session = Mock()
    client.session.get.return_value = response(
        200, {"data": [], "links": {"next": "https://different.example/schedules"}}
    )
    with pytest.raises(ValueError):
        list(client.iter_stop_time_pages("schedules"))
    assert client.session.get.call_count == 1


def test_api_retries_timeout_429_and_503(monkeypatch):
    sleep = Mock()
    monkeypatch.setattr("simulator.mbta_client.time.sleep", sleep)
    client = MBTAClient()
    client.session = Mock()
    client.session.get.side_effect = [
        requests.Timeout(),
        response(429, headers={"Retry-After": "7"}),
        response(503),
        response(200, {"data": []}),
    ]
    assert list(client.iter_stop_time_pages("schedules")) == [{"data": []}]
    assert [call.args[0] for call in sleep.call_args_list] == [1, 7, 4]


@pytest.mark.parametrize("status, attempts", [(400, 1), (503, 4)])
def test_api_surfaces_failures(monkeypatch, status, attempts):
    monkeypatch.setattr("simulator.mbta_client.time.sleep", Mock())
    client = MBTAClient()
    client.session = Mock()
    client.session.get.return_value = response(status)
    with pytest.raises(requests.HTTPError):
        list(client.iter_stop_time_pages("predictions"))
    assert client.session.get.call_count == attempts


def test_rate_limit_without_retry_after_uses_longer_backoff(monkeypatch):
    sleep = Mock()
    monkeypatch.setattr("simulator.mbta_client.time.sleep", sleep)
    client = MBTAClient()
    client.session = Mock()
    client.session.get.side_effect = [response(429), response(429), response(200, {"data": []})]
    assert list(client.iter_stop_time_pages("predictions")) == [{"data": []}]
    assert [call.args[0] for call in sleep.call_args_list] == [15, 30]


def test_schedule_snapshot_survives_incomplete_refresh():
    kafka = Mock()
    client = Mock()
    ingestion = StopTimeIngestion(client, StopTimePublisher(kafka), refresh_interval=21600)
    raw = record()
    raw["id"] = "id"
    page = {"data": [raw]}
    client.iter_stop_time_pages.side_effect = [[page], [page]]
    ingestion.refresh_schedules(date(2026, 11, 22), client)
    assert {event["service_date"] for event in ingestion.schedules["id"]} == {
        "2026-11-21", "2026-11-22"
    }
    assert kafka.send.call_count == 2
    previous = ingestion.schedules
    client.iter_stop_time_pages.side_effect = [[page], requests.Timeout()]
    with pytest.raises(requests.Timeout):
        ingestion.refresh_schedules(date(2026, 11, 23), client)
    assert ingestion.schedules is previous
    assert ingestion.snapshot_day == date(2026, 11, 22)
