#!/bin/bash

# Script to run stress test against a running cluster
# Usage: ./run-stress-test.sh [num_producers] [messages_per_producer] [port1] [port2] [port3]...

set -e

NUM_PRODUCERS=${1:-10}
MESSAGES_PER_PRODUCER=${2:-10000}
shift 2 || true

# Default ports if not specified
if [ $# -eq 0 ]; then
    PORTS="9001 9002 9003"
else
    PORTS="$@"
fi

echo "=========================================="
echo "Distributed Queue Stress Test"
echo "=========================================="
echo "Producers: $NUM_PRODUCERS"
echo "Messages per producer: $MESSAGES_PER_PRODUCER"
echo "Total messages: $((NUM_PRODUCERS * MESSAGES_PER_PRODUCER))"
echo "Target nodes: $PORTS"
echo "=========================================="
echo ""

# Build if needed
echo "Building project..."
mvn -q clean compile

# Run stress test
echo "Starting stress test..."
mvn -q exec:java -Dexec.mainClass="com.dqueue.examples.DistributedStressTest" \
    -Dexec.args="$NUM_PRODUCERS $MESSAGES_PER_PRODUCER $PORTS"

echo ""
echo "Stress test completed!"
