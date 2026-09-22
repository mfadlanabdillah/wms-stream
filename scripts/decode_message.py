#!/usr/bin/env python3
"""Decode a Confluent-wire-format Avro message copied out of the Cloud Console.

The Cloud Console message browser sometimes shows a value as

    { "__raw__": "AAABhqgMClNQTVRXMlNlcnZpY2Ug..." }

That is not corruption: it is the base64 of the raw Kafka record bytes,
shown because the UI could not resolve the schema for that record. The
data underneath is intact. This script proves it by parsing the wire
format and decoding the Avro payload.

Wire format (Confluent):
    byte 0      magic byte, always 0
    bytes 1-4   schema ID, big-endian int32
    bytes 5..   Avro binary payload

Usage:
    .venv/bin/python scripts/decode_message.py '<base64 from __raw__>'
    .venv/bin/python scripts/decode_message.py '<base64>' --table warehouses

Without --table it reports the schema ID and payload only (no schema
needed). With --table it decodes using the expected CDC row shape.
"""
import argparse
import base64
import io
import json
import sys

from fastavro import parse_schema, schemaless_reader

TS = {"type": "long", "logicalType": "timestamp-micros"}

# Row shapes the connector emits after the ExtractNewRecordState SMT:
# the flat table row, plus the metadata fields from add.fields.
META = [
    {"name": "__deleted", "type": ["null", "string"], "default": None},
    {"name": "__op", "type": ["null", "string"], "default": None},
    {"name": "__source_ts_ms", "type": ["null", "long"], "default": None},
]

TABLES = {
    "warehouses": [
        {"name": "id", "type": "long"},
        {"name": "code", "type": "string"},
        {"name": "name", "type": "string"},
        {"name": "created_at", "type": ["null", TS], "default": None},
        {"name": "updated_at", "type": ["null", TS], "default": None},
    ],
    "products": [
        {"name": "id", "type": "long"},
        {"name": "sku", "type": "string"},
        {"name": "name", "type": "string"},
        {"name": "unit", "type": "string"},
        {"name": "barcode", "type": ["null", "string"], "default": None},
        {"name": "min_stock", "type": "int"},
        {"name": "created_at", "type": ["null", TS], "default": None},
        {"name": "updated_at", "type": ["null", TS], "default": None},
    ],
    "locations": [
        {"name": "id", "type": "long"},
        {"name": "warehouse_id", "type": "long"},
        {"name": "code", "type": "string"},
        {"name": "name", "type": "string"},
        {"name": "created_at", "type": ["null", TS], "default": None},
        {"name": "updated_at", "type": ["null", TS], "default": None},
    ],
    "stock_movements": [
        {"name": "id", "type": "long"},
        {"name": "product_id", "type": "long"},
        {"name": "location_id", "type": "long"},
        {"name": "type", "type": "string"},
        {"name": "qty", "type": "int"},
        {"name": "reference_no", "type": ["null", "string"], "default": None},
        {"name": "note", "type": ["null", "string"], "default": None},
        {"name": "user_id", "type": "long"},
        {"name": "created_at", "type": ["null", TS], "default": None},
        {"name": "updated_at", "type": ["null", TS], "default": None},
    ],
}

OP = {
    "r": "read (initial snapshot)",
    "c": "create (INSERT)",
    "u": "update (UPDATE)",
    "d": "delete (DELETE)",
}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("b64", help="base64 string from the __raw__ field")
    ap.add_argument("--table", choices=sorted(TABLES), help="decode as this table's row")
    args = ap.parse_args()

    try:
        raw = base64.b64decode(args.b64, validate=True)
    except Exception as exc:  # noqa: BLE001
        print(f"error: not valid base64 -> {exc}", file=sys.stderr)
        return 1

    if len(raw) < 5:
        print("error: too short to be a Confluent wire-format record", file=sys.stderr)
        return 1

    magic = raw[0]
    schema_id = int.from_bytes(raw[1:5], "big")
    payload = raw[5:]

    print(f"total bytes : {len(raw)}")
    print(f"magic byte  : {magic}" + ("  (0 = Confluent wire format, OK)" if magic == 0 else "  (UNEXPECTED: should be 0)"))
    print(f"schema id   : {schema_id}")
    print(f"payload     : {len(payload)} bytes")

    if magic != 0:
        print("\nThis is not a schema-registry-encoded record.", file=sys.stderr)
        return 1

    if not args.table:
        print("\nPass --table <name> to decode the payload.")
        print(f"available: {', '.join(sorted(TABLES))}")
        return 0

    schema = parse_schema({
        "type": "record",
        "name": "Value",
        "namespace": f"wms.public.{args.table}",
        "fields": TABLES[args.table] + META,
    })

    try:
        record = schemaless_reader(io.BytesIO(payload), schema)
    except Exception as exc:  # noqa: BLE001
        print(f"\ncould not decode as '{args.table}': {exc}", file=sys.stderr)
        print("The bytes are still fine — the field order or table guess is wrong.", file=sys.stderr)
        return 1

    print("\ndecoded value:")
    print(json.dumps(record, default=str, indent=2))

    if isinstance(record, dict):
        op = record.get("__op")
        if op:
            print(f"\n__op = {op!r}  ->  {OP.get(op, 'unknown')}")
        if record.get("__source_ts_ms") is not None:
            print("__source_ts_ms present -> the ExtractNewRecordState SMT is applied correctly")

    return 0


if __name__ == "__main__":
    sys.exit(main())
