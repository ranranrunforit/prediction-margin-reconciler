#!/usr/bin/env bash
# Kill the real process, not a simulated one.
#
# The chaos harness restarts the app in-process, which is fast and reproducible
# but still leaves the Go runtime in charge of the teardown. This script sends an
# actual SIGKILL to an actual OS process, several times, in the middle of live
# traffic. The simulated chain persists to disk, so it keeps its state while the
# service is dead -- which is the point: recovery has to work against an
# authority that moved on without us.
set -euo pipefail

cd "$(dirname "$0")/.."

: "${DATABASE_URL:=postgres://postgres:postgres@127.0.0.1:5432/pmr?sslmode=disable}"
export DATABASE_URL
ADDR="${ADDR:-:8099}"
BASE="http://127.0.0.1${ADDR}"
CHAIN_STATE="${CHAIN_STATE:-/tmp/pmr-crashtest-chain.json}"
KILLS="${KILLS:-6}"

BIN=$(mktemp -u /tmp/pmr-crashtest-XXXX)
go build -o "$BIN" ./cmd/pmr
rm -f "$CHAIN_STATE"

PID=""
cleanup() { [[ -n "$PID" ]] && kill -9 "$PID" 2>/dev/null || true; rm -f "$BIN"; }
trap cleanup EXIT

start() {
  "$BIN" serve -addr "$ADDR" -chain-state "$CHAIN_STATE" ${1:-} >/tmp/pmr-crashtest.log 2>&1 &
  PID=$!
  for _ in $(seq 1 50); do
    curl -fsS "$BASE/api/state" >/dev/null 2>&1 && return 0
    sleep 0.2
  done
  echo "service never came up; log:"; tail -20 /tmp/pmr-crashtest.log; exit 1
}

traffic() {
  for u in alice bob carol dave; do
    curl -fsS -XPOST "$BASE/api/deposit"  -d "{\"user\":\"$u\",\"amount\":250}" >/dev/null
    curl -fsS -XPOST "$BASE/api/withdraw" -d "{\"user\":\"$u\",\"amount\":11}"  >/dev/null 2>&1 || true
    curl -fsS -XPOST "$BASE/api/position" -d "{\"user\":\"$u\",\"market\":\"m00\",\"side\":\"long\",\"size\":40}" >/dev/null 2>&1 || true
  done
}

echo "== starting service (faults on, chain persists to $CHAIN_STATE)"
start "-fresh"
traffic
sleep 1

for i in $(seq 1 "$KILLS"); do
  traffic
  sleep 0.6
  echo "== kill -9 #$i (pid $PID)"
  kill -9 "$PID"; wait "$PID" 2>/dev/null || true
  start
done

echo "== draining, then verifying strictly"
traffic
sleep 2
kill -9 "$PID"; wait "$PID" 2>/dev/null || true
PID=""

"$BIN" verify -chain-state "$CHAIN_STATE" -settle
echo "== survived $KILLS hard kills with every invariant intact"
