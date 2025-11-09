#!/bin/bash

# Script to start a multi-node distributed queue cluster
# Usage: ./run-cluster.sh [num_nodes]

set -e

NUM_NODES=${1:-3}
BASE_PORT=9001

echo "=========================================="
echo "Starting $NUM_NODES-node Queue Cluster"
echo "=========================================="
echo ""

# Function to cleanup on exit
cleanup() {
    echo ""
    echo "Shutting down cluster..."
    jobs -p | xargs -r kill
    exit 0
}

trap cleanup SIGINT SIGTERM

# Build the project
echo "Building project..."
mvn -q clean compile

# Start nodes
for i in $(seq 1 $NUM_NODES); do
    NODE_ID=$i
    PORT=$((BASE_PORT + i - 1))

    if [ $i -eq 1 ]; then
        # First node is seed
        echo "Starting Node $NODE_ID (Seed) on port $PORT..."
        mvn -q exec:java -Dexec.mainClass="com.dqueue.examples.MultiNodeClusterExample" \
            -Dexec.args="$NODE_ID" > "node-$NODE_ID.log" 2>&1 &
    else
        # Other nodes join via seed
        echo "Starting Node $NODE_ID on port $PORT (joining seed at localhost:$BASE_PORT)..."
        sleep 2  # Wait for previous node to start
        mvn -q exec:java -Dexec.mainClass="com.dqueue.examples.MultiNodeClusterExample" \
            -Dexec.args="$NODE_ID localhost:$BASE_PORT" > "node-$NODE_ID.log" 2>&1 &
    fi
done

echo ""
echo "=========================================="
echo "Cluster started successfully!"
echo "=========================================="
echo ""
echo "Node ports:"
for i in $(seq 1 $NUM_NODES); do
    PORT=$((BASE_PORT + i - 1))
    echo "  Node $i: localhost:$PORT"
done
echo ""
echo "Logs:"
for i in $(seq 1 $NUM_NODES); do
    echo "  Node $i: node-$i.log"
done
echo ""
echo "Press Ctrl+C to shutdown cluster"
echo ""

# Wait for all background jobs
wait
