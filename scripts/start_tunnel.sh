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
# Requires an ngrok authtoken (free account is enough):
#   https://dashboard.ngrok.com/get-started/your-authtoken
#   ngrok config add-authtoken <token>
#
# Leave this running for as long as the connector needs the database.

set -euo pipefail

PORT="${DEMO_DB_PORT:-55433}"
NGROK="${NGROK_BIN:-$HOME/.local/bin/ngrok}"
CONTAINER="wms-cdc-demo"

if [ ! -x "$NGROK" ]; then
    echo "error: ngrok not found at $NGROK" >&2
    echo "install: curl -sL https://bin.equinox.io/c/bNyj1mQVY4c/ngrok-v3-stable-linux-amd64.tgz | tar xz -C ~/.local/bin" >&2
    exit 1
fi

if ! "$NGROK" config check >/dev/null 2>&1; then
    echo "error: no ngrok authtoken configured" >&2
    echo "get one at https://dashboard.ngrok.com/get-started/your-authtoken then run:" >&2
    echo "  $NGROK config add-authtoken <token>" >&2
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
    exit 1
fi

echo "starting TCP tunnel to localhost:$PORT ..."
"$NGROK" tcp "$PORT" --log stdout --log-format logfmt >/tmp/ngrok-wms.log 2>&1 &
NGROK_PID=$!
trap 'kill $NGROK_PID 2>/dev/null || true' EXIT

# Poll the local ngrok API for the assigned public address.
PUBLIC_URL=""
for _ in $(seq 1 30); do
    PUBLIC_URL=$(curl -s --max-time 3 http://127.0.0.1:4040/api/tunnels 2>/dev/null \
        | grep -o '"public_url":"tcp://[^"]*"' \
        | head -1 | sed 's/.*tcp:\/\///; s/"$//') || true
    [ -n "$PUBLIC_URL" ] && break
    sleep 1
done

if [ -z "$PUBLIC_URL" ]; then
    echo "error: tunnel did not come up; see /tmp/ngrok-wms.log" >&2
    exit 1
fi

HOST="${PUBLIC_URL%%:*}"
TCP_PORT="${PUBLIC_URL##*:}"

cat <<EOF

  Tunnel is up.

    database.hostname : $HOST
    database.port     : $TCP_PORT
    database.dbname   : wms
    database.user     : confluent_cdc
    database.password : see .env.demo.local
    database.sslmode  : prefer

  Paste the hostname and port into connectors/postgres-cdc-source.json,
  then launch the connector. Note sslmode: this demo Postgres has no TLS
  certificate, so 'require' will fail — use 'prefer' or 'disable'.

  Keep this terminal open. Ctrl-C closes the tunnel and the connector
  will start failing to reach the database.

EOF

wait $NGROK_PID
