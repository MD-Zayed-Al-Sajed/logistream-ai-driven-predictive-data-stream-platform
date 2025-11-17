import json
import io
from pathlib import Path
from typing import Any, Dict

from fastavro import schemaless_reader


# Confluent wire format:
# [0]  = magic byte (0)
# [1-4]= schema id (big-endian int)
# [5:] = Avro binary payload
HEADER_SIZE = 5


class AvroDecodeError(Exception):
    """Custom error for Avro decoding issues."""


def _load_schema(schema_name: str) -> Dict[str, Any]:
    """
    Load an Avro schema JSON from the repo's schemas/avro directory.

    schema_name is something like "shipment_event.avsc".
    """
    # .../Logistream/services/stream_processor/logistream_stream/avro_utils.py
    # parents[3] -> .../Logistream/
    root_dir = Path(__file__).resolve().parents[3]
    schema_path = root_dir / "schemas" / "avro" / schema_name

    if not schema_path.exists():
        raise FileNotFoundError(f"Schema file not found: {schema_path}")

    with schema_path.open("r", encoding="utf-8") as f:
        return json.load(f)


# Cache loaded schemas so we don't re-read them for every message
_SCHEMA_CACHE: Dict[str, Dict[str, Any]] = {}


def get_schema(schema_name: str) -> Dict[str, Any]:
    if schema_name not in _SCHEMA_CACHE:
        _SCHEMA_CACHE[schema_name] = _load_schema(schema_name)
    return _SCHEMA_CACHE[schema_name]


def decode_confluent_avro(payload: bytes, schema_name: str) -> Dict[str, Any]:
    """
    Decode a Confluent-encoded Avro record (magic byte + schema id + payload)
    into a Python dict using a local .avsc schema.

    NOTE: We currently ignore the schema id and assume a single schema per topic.
    This keeps complexity low while still giving us structured data.
    """
    if len(payload) <= HEADER_SIZE:
        raise AvroDecodeError("Payload too short to contain Confluent Avro header")

    if payload[0] != 0:
        raise AvroDecodeError(f"Unexpected magic byte: {payload[0]}")

    # We ignore bytes[1:5] (schema id) for now.
    avro_bytes = payload[HEADER_SIZE:]

    schema = get_schema(schema_name)
    bio = io.BytesIO(avro_bytes)

    try:
        record = schemaless_reader(bio, schema)
    except Exception as exc:
        raise AvroDecodeError(f"Failed to decode Avro payload: {exc}") from exc

    if not isinstance(record, dict):
        raise AvroDecodeError(f"Decoded Avro record is not a dict: {type(record)}")

    return record
