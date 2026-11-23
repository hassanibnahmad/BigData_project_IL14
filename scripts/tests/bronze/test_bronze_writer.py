from datetime import datetime, timezone

from bronze_writer import BronzeWriter


# Simulated Kafka messages for testing
messages = [
    {
        "topic": "predictions_arrets",
        "partition": 2,
        "offset": 100,
        "timestamp": int(datetime.now(timezone.utc).timestamp() * 1000),
        "value": {
            "source": "MBTA",
            "prediction_id": "test-prediction-001",
            "route_id": "1",
            "trip_id": "test-trip-001",
            "stop_id": "110",
            "vehicle_id": "test-vehicle-001",
            "predicted_arrival": "2026-11-23T19:00:00Z",
            "predicted_departure": "2026-11-23T19:01:00Z",
        },
    },
    {
        "topic": "predictions_arrets",
        "partition": 2,
        "offset": 101,
        "timestamp": int(datetime.now(timezone.utc).timestamp() * 1000),
        "value": {
            "source": "MBTA",
            "prediction_id": "test-prediction-002",
            "route_id": "1",
            "trip_id": "test-trip-002",
            "stop_id": "64",
            "vehicle_id": "test-vehicle-002",
            "predicted_arrival": "2026-11-23T19:10:00Z",
            "predicted_departure": "2026-11-23T19:11:00Z",
        },
    },
]


def main():
    print(f"Test messages created: {len(messages)}")

    writer = BronzeWriter()

    file_path = writer.write_batch(
        topic="predictions_arrets",
        messages=messages,
    )

    print("\nBronze Writer test completed.")
    print(f"File created in MinIO: {file_path}")


if __name__ == "__main__":
    main()