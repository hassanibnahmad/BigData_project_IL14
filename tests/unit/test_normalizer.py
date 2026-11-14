
from simulator.normalizer import normalize_vehicle


def test_normalize_vehicle():
    raw = {
        "id": "y1898",
        "attributes": {
            "latitude": 42.343594,
            "longitude": -71.086029,
            "updated_at": "2026-11-10T17:15:32-05:00"
        },
        "relationships": {
            "route": {
                "data": {"id": "1"}
            },
            "trip": {
                "data": {"id": "78182009"}
            }
        }
    }

    event = normalize_vehicle(raw)

    assert event["source"] == "MBTA"
    assert event["vehicle_id"] == "y1898"
    assert event["route_id"] == "1"
    assert event["trip_id"] == "78182009"
    assert event["latitude"] == 42.343594
    assert event["vehicle_timestamp"] == (
        "2026-11-10T22:15:32Z"
    )


def test_missing_trip_is_skipped():
    raw = {
        "id": "y1898",
        "attributes": {
            "latitude": 42.34,
            "longitude": -71.08,
            "updated_at": "2026-11-10T17:15:32-05:00"
        },
        "relationships": {
            "route": {
                "data": {"id": "1"}
            }
        }
    }

    assert normalize_vehicle(raw) is None
