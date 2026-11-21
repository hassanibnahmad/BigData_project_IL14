"""Normalize MBTA stop times while retaining original IDs and offsets."""

from datetime import datetime, timezone
import hashlib
import json
from rfc3339_validator import validate_rfc3339


STOP_FIELDS = ("route_id", "trip_id", "stop_id", "stop_sequence")


def valid_time(value):
    return isinstance(value, str) and validate_rfc3339(value.upper())


def same_time(left, right):
    if left == right:
        return True
    return bool(
        valid_time(left) and valid_time(right)
        and datetime.fromisoformat(left.upper()) == datetime.fromisoformat(right.upper())
    )


def relationship_id(record, name):
    relationship = (record.get("relationships") or {}).get(name) or {}
    data = relationship.get("data")
    return data.get("id") if isinstance(data, dict) else None


def stop_fields(record):
    return {
        "route_id": relationship_id(record, "route"),
        "trip_id": relationship_id(record, "trip"),
        "stop_id": relationship_id(record, "stop"),
        "stop_sequence": (record.get("attributes") or {}).get("stop_sequence"),
    }


def finish(event):
    payload = json.dumps(event, sort_keys=True, separators=(",", ":"))
    event["event_id"] = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    event["ingested_at"] = datetime.now(timezone.utc).isoformat()
    return event


def normalize_schedule(record, service_date):
    attrs = record.get("attributes") or {}
    return finish({
        "source": "MBTA",
        "schedule_id": record.get("id"),
        "service_date": service_date,
        **stop_fields(record),
        "scheduled_arrival": attrs.get("arrival_time"),
        "scheduled_departure": attrs.get("departure_time"),
    })


def normalize_prediction(record, candidates, included_schedule):
    attrs = record.get("attributes") or {}
    fields = stop_fields(record)
    schedule_id = relationship_id(record, "schedule")
    dates = set()
    # Schedule IDs can recur on different service dates. Match the linked
    # schedule's absolute times to a date-filtered schedule occurrence.
    if included_schedule and included_schedule.get("id") == schedule_id:
        scheduled = included_schedule.get("attributes") or {}
        arrival, departure = scheduled.get("arrival_time"), scheduled.get("departure_time")
        if (arrival or departure) and stop_fields(included_schedule) == fields:
            dates = {
                schedule["service_date"]
                for schedule in candidates
                if schedule["schedule_id"] == schedule_id
                and all(schedule[field] == fields[field] for field in STOP_FIELDS)
                and same_time(schedule["scheduled_arrival"], arrival)
                and same_time(schedule["scheduled_departure"], departure)
            }
    predicted_arrival = attrs.get("arrival_time")
    predicted_departure = attrs.get("departure_time")
    if valid_time(predicted_arrival) and not valid_time(predicted_departure):
        predicted_departure = None
    if valid_time(predicted_departure) and not valid_time(predicted_arrival):
        predicted_arrival = None
    return finish({
        "source": "MBTA",
        "prediction_id": record.get("id"),
        "schedule_id": schedule_id,
        "service_date": next(iter(dates)) if len(dates) == 1 else None,
        **fields,
        "vehicle_id": relationship_id(record, "vehicle"),
        "predicted_arrival": predicted_arrival,
        "predicted_departure": predicted_departure,
    })
