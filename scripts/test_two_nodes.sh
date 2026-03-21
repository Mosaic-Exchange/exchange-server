#!/bin/bash
# Test script: starts seed + basic node, verifies gossip and exchange.
cd "$(dirname "$0")/.."

echo "=== Building ==="
mvn -q compile

echo ""
echo "=== Starting seed node (port 7001) ==="
# Seed node: waits 4s, then prints ls, waits for exchange, then quits
(sleep 4; echo "ls"; sleep 5; echo "quit") | mvn -q exec:java -Dexec.args="--port 7001 --type seed" 2>&1 | sed 's/^/[SEED] /' &
SEED_PID=$!

sleep 2

echo ""
echo "=== Starting basic node (port 7002) ==="
# Basic node: waits 3s for gossip, prints ls, sends hello, then quits
(sleep 3; echo "ls"; sleep 1; echo "hello"; sleep 3; echo "quit") | mvn -q exec:java -Dexec.args="--port 7002 --type basic --seed 127.0.0.1:7001" 2>&1 | sed 's/^/[BASIC] /' &
BASIC_PID=$!

wait $SEED_PID
wait $BASIC_PID

echo ""
echo "=== Test complete ==="
