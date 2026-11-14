
from datetime import datetime, timezone
import hashlib


def to_utc(value):
    if not value:
        return None

    dt = datetime.fromisoformat(
        value.replace("Z", "+00:00")
    )

    if dt.tzinfo is None:
        raise ValueError("Timestamp must include timezone")

    return dt.astimezone(
        timezone.utc
    ).isoformat().replace("+00:00", "Z")


def normalize_vehicle(vehicle):
    attributes = vehicle.get("attributes") or {}
    relationships = vehicle.get("relationships") or {}

    vehicle_id = vehicle.get("id")

    route_id = (
        (relationships.get("route") or {})
        .get("data") or {}
    ).get("id")

    trip_id = (
        (relationships.get("trip") or {})
        .get("data") or {}
    ).get("id")

    latitude = attributes.get("latitude")
    longitude = attributes.get("longitude")
    updated_at = attributes.get("updated_at")

    if (
        not vehicle_id
        or not route_id
        or not trip_id
        or latitude is None
        or longitude is None
        or not updated_at
    ):
        return None

    timestamp = to_utc(updated_at)

    unique_key = (
        f"{vehicle_id}|{route_id}|"
        f"{trip_id}|{timestamp}"
    )

    event_id = hashlib.sha256(
        unique_key.encode("utf-8")
    ).hexdigest()

    return {
        "event_id": event_id,
        "source": "MBTA",
        "vehicle_id": str(vehicle_id),
        "route_id": str(route_id),
        "trip_id": str(trip_id),
        "latitude": float(latitude),
        "longitude": float(longitude),
        "vehicle_timestamp": timestamp,
        "ingested_at": datetime.now(
            timezone.utc
        ).isoformat().replace("+00:00", "Z")
    }
