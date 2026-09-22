#!/usr/bin/env python3
"""Ask Confluent Cloud Schema Registry directly what it holds.

The Cloud Console's Schemas page is filtered by environment and can look
empty when you are in the wrong one, or when a schema is registered under
a subject name you did not expect. This talks to the Schema Registry REST
API instead, so the answer is authoritative.

Setup — create a Schema Registry API key (distinct from the Kafka one):
    Console -> Environments -> <your env> -> API Keys -> Add key
               -> choose "Schema Registry"

Find the SR endpoint on the same environment page, e.g.
    https://psrc-xxxxx.us-east-2.aws.confluent.cloud

Then:
    export SR_URL='https://psrc-xxxxx.us-east-2.aws.confluent.cloud'
    export SR_KEY='<schema-registry-api-key>'
    export SR_SECRET='<schema-registry-api-secret>'

    .venv/bin/python scripts/check_schemas.py
    .venv/bin/python scripts/check_schemas.py --id 100008

Credentials are read from the environment only; nothing is written to disk.
"""
import argparse
import base64
import json
import os
import sys
import urllib.error
import urllib.request


def call(base: str, path: str, key: str, secret: str):
    url = base.rstrip("/") + path
    token = base64.b64encode(f"{key}:{secret}".encode()).decode()
    req = urllib.request.Request(url, headers={
        "Authorization": f"Basic {token}",
        "Accept": "application/vnd.schemaregistry.v1+json",
    })
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode()), None
    except urllib.error.HTTPError as exc:
        body = exc.read().decode(errors="replace")[:400]
        return None, f"HTTP {exc.code}: {body}"
    except Exception as exc:  # noqa: BLE001
        return None, str(exc)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--id", type=int, help="look up one schema by its ID (e.g. 100008)")
    args = ap.parse_args()

    base = os.environ.get("SR_URL", "").strip()
    key = os.environ.get("SR_KEY", "").strip()
    secret = os.environ.get("SR_SECRET", "").strip()

    missing = [n for n, v in (("SR_URL", base), ("SR_KEY", key), ("SR_SECRET", secret)) if not v]
    if missing:
        print(f"error: missing environment variable(s): {', '.join(missing)}", file=sys.stderr)
        print(__doc__, file=sys.stderr)
        return 1

    if args.id is not None:
        data, err = call(base, f"/schemas/ids/{args.id}", key, secret)
        if err:
            print(f"schema id {args.id}: NOT FOUND -> {err}")
            return 1
        print(f"schema id {args.id}: FOUND")
        raw = data.get("schema") if isinstance(data, dict) else None
        if raw:
            try:
                print(json.dumps(json.loads(raw), indent=2)[:3000])
            except Exception:  # noqa: BLE001
                print(raw[:3000])
        return 0

    subjects, err = call(base, "/subjects", key, secret)
    if err:
        print(f"error: could not list subjects -> {err}", file=sys.stderr)
        print("\nIf this is HTTP 401, the API key is not a Schema Registry key "
              "(a Kafka cluster key will not work here).", file=sys.stderr)
        return 1

    if not subjects:
        print("Schema Registry is reachable but holds NO subjects.")
        print("\nThat means the connector never registered a schema. Check that")
        print("output.data.format is AVRO (not JSON) on the connector.")
        return 1

    print(f"Schema Registry holds {len(subjects)} subject(s):\n")
    wms = [s for s in subjects if "wms" in s.lower()]
    for s in sorted(subjects):
        mark = "  <-- this pipeline" if s in wms else ""
        print(f"  {s}{mark}")

    expected = [
        "wms.public.stock_movements-value",
        "wms.public.products-value",
        "wms.public.locations-value",
        "wms.public.warehouses-value",
    ]
    print("\nexpected from the CDC connector:")
    for e in expected:
        print(f"  {'OK   ' if e in subjects else 'MISS '} {e}")

    if wms:
        print(f"\n{len(wms)} subject(s) for this pipeline are registered — "
              "Stream Governance is working.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
