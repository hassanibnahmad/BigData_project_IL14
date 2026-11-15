
import copy
import json
from pathlib import Path

import pytest
from jsonschema import (
    Draft202012Validator,
    FormatChecker,
    ValidationError,
)

ROOT = Path(__file__).resolve().parents[2]

SCHEMA_PATH = (
    ROOT / "contracts" / "vehicle_position.schema.json"
)

EXAMPLE_PATH = (
    ROOT / "contracts" / "vehicle_position.example.json"
)


@pytest.fixture
def validator():
    schema = json.loads(
        SCHEMA_PATH.read_text(encoding="utf-8")
    )

    return Draft202012Validator(
        schema,
        format_checker=FormatChecker()
    )


@pytest.fixture
def valid_event():
    return json.loads(
        EXAMPLE_PATH.read_text(encoding="utf-8")
    )


def test_valid_event(validator, valid_event):
    validator.validate(valid_event)
    print("\nValid vehicle event ACCEPTED")


def test_missing_vehicle_id(validator, valid_event):
    event = copy.deepcopy(valid_event)
    del event["vehicle_id"]

    with pytest.raises(ValidationError):
        validator.validate(event)

    print("\nMissing vehicle_id REJECTED")


def test_invalid_latitude(validator, valid_event):
    event = copy.deepcopy(valid_event)
    event["latitude"] = 120

    with pytest.raises(ValidationError):
        validator.validate(event)

    print("\nInvalid latitude REJECTED")


def test_invalid_longitude(validator, valid_event):
    event = copy.deepcopy(valid_event)
    event["longitude"] = -200

    with pytest.raises(ValidationError):
        validator.validate(event)

    print("\nInvalid longitude REJECTED")


def test_invalid_source(validator, valid_event):
    event = copy.deepcopy(valid_event)
    event["source"] = "GPS"

    with pytest.raises(ValidationError):
        validator.validate(event)

    print("\nInvalid source REJECTED")


def test_additional_property(validator, valid_event):
    event = copy.deepcopy(valid_event)
    event["unexpected_field"] = "test"

    with pytest.raises(ValidationError):
        validator.validate(event)

    print("\nAdditional property REJECTED")
