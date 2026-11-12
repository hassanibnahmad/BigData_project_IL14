
import json
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker

ROOT = Path(__file__).resolve().parents[2]
CONTRACTS = ROOT / "contracts"

def test_vehicle_position_contract():
    schema = json.loads(
        (CONTRACTS / "vehicle_position.schema.json").read_text()
    )
    event = json.loads(
        (CONTRACTS / "vehicle_position.example.json").read_text()
    )

    Draft202012Validator.check_schema(schema)

    validator = Draft202012Validator(
        schema,
        format_checker=FormatChecker()
    )
    validator.validate(event)
