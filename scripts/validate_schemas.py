#!/usr/bin/env python3
"""Validate every .avsc in schemas/ by parsing it with a real Avro parser and
round-tripping a sample record through serialise -> deserialise.

Run: .venv/bin/python scripts/validate_schemas.py
"""
import glob
import io
import json
import os
import sys
from datetime import datetime

from fastavro import parse_schema, schemaless_reader, schemaless_writer

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

SAMPLES = {
    "stock_movements-value": {
        "id": 90231,
        "product_id": 412,
        "location_id": 77,
        "type": "out",
        "qty": -48,
        "reference_no": "SO-2026-00912",
        "note": None,
        "user_id": 5,
        "created_at": 1790000000000000,
        "updated_at": 1790000000000000,
    },
    "products-value": {
        "id": 412,
        "sku": "FLT-OIL-1045",
        "name": "Oil Filter Komatsu PC200",
        "unit": "PCS",
        "barcode": "8991234567890",
        "min_stock": 60,
        "created_at": 1780000000000,
        "updated_at": 1790000000000,
    },
    "locations-value": {
        "id": 77,
        "warehouse_id": 3,
        "code": "BPN-A-01-02",
        "name": "Rack A Level 1 Bin 2",
        "created_at": 1780000000000,
        "updated_at": None,
    },
    "low_stock_alerts-value": {
        "warehouse_code": "BPN",
        "warehouse_name": "Balikpapan",
        "sku": "FLT-OIL-1045",
        "product_name": "Oil Filter Komatsu PC200",
        "unit": "PCS",
        "on_hand": 12,
        "min_stock": 60,
        "shortfall": 48,
        "severity": "CRITICAL",
        "detected_at": 1790000000000,
    },
}


def main() -> int:
    paths = sorted(glob.glob(os.path.join(HERE, "schemas", "*.avsc")))
    if not paths:
        print("no schemas found", file=sys.stderr)
        return 1

    failures = 0
    for path in paths:
        name = os.path.basename(path)[: -len(".avsc")]
        with open(path) as fh:
            raw = json.load(fh)

        try:
            schema = parse_schema(raw)
        except Exception as exc:  # noqa: BLE001
            print(f"FAIL  {name}: schema does not parse -> {exc}")
            failures += 1
            continue

        sample = SAMPLES.get(name)
        if sample is None:
            print(f"PARSE {name}: valid Avro (no sample record registered)")
            continue

        try:
            buf = io.BytesIO()
            schemaless_writer(buf, schema, sample)
            buf.seek(0)
            decoded = schemaless_reader(buf, schema)
        except Exception as exc:  # noqa: BLE001
            print(f"FAIL  {name}: round-trip error -> {exc}")
            failures += 1
            continue

        mismatch = {}
        for key, expected in sample.items():
            actual = decoded.get(key) if isinstance(decoded, dict) else None
            # fastavro decodes logicalType timestamp-millis into an aware
            # datetime, so normalise back to epoch millis before comparing.
            if isinstance(actual, datetime) and isinstance(expected, int):
                # timestamp-millis decodes to ms-precision, timestamp-micros
                # to us-precision; normalise using the field's own scale.
                epoch = actual.timestamp()
                actual = int(round(epoch * (1_000_000 if expected > 10**14 else 1_000)))
            if actual != expected:
                mismatch[key] = (expected, actual)
        if mismatch:
            print(f"FAIL  {name}: round-trip mismatch {mismatch}")
            failures += 1
        else:
            print(f"OK    {name}: parses + round-trips {len(sample)} fields")

    print()
    if failures:
        print(f"{failures} schema(s) FAILED")
        return 1
    print(f"all {len(paths)} schema(s) valid")
    return 0


if __name__ == "__main__":
    sys.exit(main())
