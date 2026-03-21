#!/bin/bash
# Test eviction: starts master + basic, kills basic, verifies DOWN marking.
cd "$(dirname "$0")/.."

echo "=== Building ==="
mvn -q compile

echo ""
echo "=== Starting master node (port 7001) ==="
# Master: ls at 4s (both alive), then ls at 20s (after basic killed + eviction), quit
(sleep 4; echo "ls"; sleep 16; echo "ls"; sleep 1; echo "quit") | \
    mvn -q exec:java -Dexec.args="--port 7001 --type master" 2>&1 | sed 's/^/[MASTER] /' &
MASTER_PID=$!

sleep 2

echo ""
echo "=== Starting basic node (port 7002) — will be killed after 5s ==="
mvn -q exec:java -Dexec.args="--port 7002 --type basic --seed 127.0.0.1:7001" 2>&1 | sed 's/^/[BASIC]  /' &
BASIC_PID=$!

# Let gossip propagate, then kill the basic node abruptly
sleep 5
echo ""
echo "=== Killing basic node (simulating crash) ==="
kill $BASIC_PID 2>/dev/null
wait $BASIC_PID 2>/dev/null

# Wait for master to finish its eviction check + ls + quit
wait $MASTER_PID

echo ""
echo "=== Test complete ==="
