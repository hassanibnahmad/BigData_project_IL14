
import json
from pathlib import Path

from jsonschema import (
    Draft202012Validator,
    FormatChecker
)

from simulator.mbta_client import MBTAClient
from simulator.normalizer import normalize_vehicle


ROOT = Path(__file__).resolve().parents[1]

SCHEMA_PATH = (
    ROOT / "contracts" /
    "vehicle_position.schema.json"
)

OUTPUT_PATH = (
    ROOT / "data" /
    "samples" /
    "vehicle_positions.jsonl"
)


def main():
    schema = json.loads(
        SCHEMA_PATH.read_text(encoding="utf-8")
    )

    validator = Draft202012Validator(
        schema,
        format_checker=FormatChecker()
    )

    client = MBTAClient()

    print("Connecting to MBTA API...")

    vehicles = client.get_vehicles()

    print(f"Received {len(vehicles)} vehicles")

    valid_events = []
    skipped = 0

    for vehicle in vehicles:
        try:
            event = normalize_vehicle(vehicle)

            if event is None:
                skipped += 1
                continue

            validator.validate(event)
            valid_events.append(event)

        except (ValueError, TypeError) as error:
            skipped += 1
            print(f"Skipped invalid record: {error}")

    OUTPUT_PATH.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    with OUTPUT_PATH.open(
        "w", encoding="utf-8"
    ) as file:
        for event in valid_events:
            file.write(
                json.dumps(event) + "\n"
            )

    print(f"Valid events: {len(valid_events)}")
    print(f"Skipped events: {skipped}")
    print(f"Saved to: {OUTPUT_PATH}")

    if valid_events:
        print("\nSample event:")
        print(json.dumps(valid_events[0], indent=2))


if __name__ == "__main__":
    main()
