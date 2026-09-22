#!/usr/bin/env bash
# Open a public TCP tunnel to the CDC demo Postgres so that Confluent
# Cloud's fully-managed connector can reach it.
#
# A fully-managed connector runs in Confluent's VPC, not on your machine,
# so it cannot dial a laptop behind NAT. This publishes port 55433 at a
# public host:port that you paste into the connector config.
#
# Usage:
#   ./scripts/start_tunnel.sh
#
# Uses bore (https://github.com/ekzhang/bore) — open source, no signup,
# no credit card. Install:
#   curl -sL https://github.com/ekzhang/bore/releases/download/v0.6.0/bore-v0.6.0-x86_64-unknown-linux-musl.tar.gz \
#     | tar xz -C ~/.local/bin
#
# Why not ngrok: TCP endpoints on a free ngrok account are refused until a
# credit card is added (ERR_NGROK_8013), and this pipeline needs raw TCP,
# not HTTP. bore has no such gate.
#
# SECURITY: this exposes the database to the whole internet, protected only
# by its password. Fine for a demo holding synthetic data. Never point it
# at a database with real customer data, and stop it when finished.
#
# Leave this running for as long as the connector needs the database.

set -euo pipefail

PORT="${DEMO_DB_PORT:-55433}"
BORE="${BORE_BIN:-$HOME/.local/bin/bore}"
BORE_SERVER="${BORE_SERVER:-bore.pub}"
CONTAINER="wms-cdc-demo"
LOG=/tmp/bore-wms.log

if [ ! -x "$BORE" ]; then
    echo "error: bore not found at $BORE" >&2
    echo "install: curl -sL https://github.com/ekzhang/bore/releases/download/v0.6.0/bore-v0.6.0-x86_64-unknown-linux-musl.tar.gz | tar xz -C ~/.local/bin" >&2
    exit 1
fi

if ! docker ps --format '{{.Names}}' | grep -qx "$CONTAINER"; then
    echo "error: $CONTAINER is not running" >&2
    echo "start it with: docker compose -f docker-compose.demo.yml up -d" >&2
    exit 1
fi

# Confirm the database actually answers before exposing it.
if ! docker exec "$CONTAINER" psql -U postgres -d wms -c 'SELECT 1' >/dev/null 2>&1; then
    echo "error: Postgres in $CONTAINER is not accepting queries yet" >&2
    echo "wait for initdb to finish, then retry" >&2
    exit 1
fi

echo "starting TCP tunnel to localhost:$PORT via $BORE_SERVER ..."
: > "$LOG"
"$BORE" local "$PORT" --to "$BORE_SERVER" >>"$LOG" 2>&1 &
BORE_PID=$!
trap 'kill $BORE_PID 2>/dev/null || true' EXIT

# bore logs "listening at <host>:<port>" once the tunnel is established.
ENDPOINT=""
for _ in $(seq 1 30); do
    if ! kill -0 "$BORE_PID" 2>/dev/null; then
        echo "error: bore exited; log follows" >&2
        tail -20 "$LOG" >&2
        exit 1
    fi
    ENDPOINT=$(grep -oE "listening at [^ ]+" "$LOG" 2>/dev/null | head -1 | awk '{print $3}') || true
    [ -n "$ENDPOINT" ] && break
    sleep 1
done

if [ -z "$ENDPOINT" ]; then
    echo "error: tunnel did not come up; see $LOG" >&2
    exit 1
fi

HOST="${ENDPOINT%%:*}"
TCP_PORT="${ENDPOINT##*:}"

cat <<EOF

  Tunnel is up.

  Paste into connectors/postgres-cdc-source.json:

    "database.hostname" : "$HOST"
    "database.port"     : "$TCP_PORT"
    "database.dbname"   : "wms"
    "database.user"     : "confluent_cdc"
    "database.password" : see .env.demo.local
    "database.sslmode"  : "prefer"

  sslmode must be 'prefer' or 'disable' — this demo Postgres has no TLS
  certificate, so 'require' will fail.

  Verify from outside before launching the connector:

    PW=\$(grep '^CDC_PASSWORD=' .env.demo.local | cut -d= -f2-)
    docker run --rm -e PGPASSWORD="\$PW" postgres:16 \\
        psql -h $HOST -p $TCP_PORT -U confluent_cdc -d wms -c 'SELECT 1'

  Keep this terminal open. Ctrl-C closes the tunnel and the connector
  will start failing to reach the database. The public port changes on
  every restart, so update the connector config if you restart this.

EOF

wait $BORE_PID
