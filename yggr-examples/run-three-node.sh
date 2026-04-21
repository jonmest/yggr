#!/usr/bin/env bash
# Demo: bring up a 3-node yggr-kv cluster locally, run a few
# SET/GET/DEL commands against it, shut it down.
#
# Run from the workspace root:
#   ./yggr-examples/run-three-node.sh

set -euo pipefail

BIN="cargo run --quiet --release --bin kv --"
BASE="$(mktemp -d -t yggr-kv-demo.XXXXXX)"
PIDS=()

cleanup() {
    echo
    echo "cleaning up..."
    for pid in "${PIDS[@]+"${PIDS[@]}"}"; do
        kill "$pid" 2>/dev/null || true
    done
    wait 2>/dev/null || true
    rm -rf "$BASE"
}
trap cleanup EXIT

echo "data dirs under: $BASE"

# Build once up-front so the three nodes start together.
cargo build --quiet --release --bin kv

for i in 1 2 3; do
    mkdir -p "$BASE/n$i"
done

start_node() {
    local id=$1
    local peer_port=$2
    local client_port=$3
    local peers=()
    for j in 1 2 3; do
        if [ "$j" != "$id" ]; then
            peers+=("--peer" "$j=127.0.0.1:700$j")
        fi
    done
    $BIN \
        --node-id "$id" \
        --peer-addr "127.0.0.1:$peer_port" \
        --client-addr "127.0.0.1:$client_port" \
        --data-dir "$BASE/n$id" \
        "${peers[@]}" \
        > "$BASE/n$id.log" 2>&1 &
    PIDS+=($!)
    echo "node $id: pid ${PIDS[-1]}, peer :$peer_port, client :$client_port"
}

start_node 1 7001 8001
start_node 2 7002 8002
start_node 3 7003 8003

echo "waiting for election..."
sleep 2

send() {
    local port=$1
    shift
    printf "%s\n" "$*" | nc -q1 127.0.0.1 "$port"
}

try_write() {
    local cmd=$*
    for port in 8001 8002 8003; do
        out=$(send "$port" "$cmd")
        if [ "$out" = "OK" ]; then
            echo "wrote via :$port: $cmd -> $out"
            return 0
        fi
        if [[ "$out" == REDIRECT* ]]; then
            continue
        fi
        echo "port $port said: $out"
    done
    echo "every port refused the write: $cmd" >&2
    return 1
}

try_write "SET hello world"
try_write "SET count 42"

for port in 8001 8002 8003; do
    echo ":$port GET hello -> $(send $port 'GET hello')"
    echo ":$port GET count -> $(send $port 'GET count')"
done

try_write "DEL hello"

for port in 8001 8002 8003; do
    echo ":$port GET hello -> $(send $port 'GET hello')"
done

echo
echo "press enter to stop the cluster"
read -r
