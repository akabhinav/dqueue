# Distributed Testing Guide

This guide explains how to test all distributed features of the queue system, including node coordination, cluster formation, partition distribution, and failover scenarios.

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Node Coordination Implementation](#node-coordination-implementation)
3. [Running Integration Tests](#running-integration-tests)
4. [Manual Multi-Node Testing](#manual-multi-node-testing)
5. [Stress Testing](#stress-testing)
6. [Testing Scenarios](#testing-scenarios)
7. [Troubleshooting](#troubleshooting)

## Architecture Overview

### Distributed Components

The distributed queue consists of these key components:

1. **QueueServer** - Network server that runs on each node
2. **ClusterManager** - Handles node discovery, health monitoring, and leader election
3. **QueueClient** - Remote client for connecting to queue servers
4. **HeartbeatManager** - Sends periodic heartbeats to maintain cluster health
5. **PartitionAssignmentStrategy** - Distributes partitions across nodes using consistent hashing

### Communication Protocol

Nodes communicate using a JSON-based RPC protocol over TCP sockets. Supported operations:

- `PRODUCE` - Send messages to queue
- `CONSUME` - Retrieve messages from queue
- `ACKNOWLEDGE` - Confirm message processing
- `JOIN_CLUSTER` - Node joins cluster
- `HEARTBEAT` - Health check
- `PARTITION_ASSIGNMENTS` - Get partition distribution
- `CLUSTER_INFO` - Query cluster state

## Node Coordination Implementation

### Cluster Formation

```
Node 1 (Seed)               Node 2                     Node 3
     |                          |                          |
     |<------ JOIN_CLUSTER -----|                          |
     |------- CLUSTER_INFO ---->|                          |
     |                          |<------ JOIN_CLUSTER -----|
     |                          |------- CLUSTER_INFO ---->|
     |<------ HEARTBEAT --------|<------ HEARTBEAT --------|
```

**Implementation Details:**

1. **Leader Election** - Simple deterministic: lowest node ID becomes leader
   - See: `ClusterManager.electLeader()` in src/main/java/com/dqueue/cluster/ClusterManager.java:130

2. **Health Monitoring** - Nodes exchange heartbeats every 3 seconds
   - Timeout: 15 seconds
   - See: `HeartbeatManager.start()` in src/main/java/com/dqueue/network/HeartbeatManager.java:35

3. **Partition Assignment** - Consistent hashing with 150 virtual nodes per physical node
   - Minimizes rebalancing when nodes join/leave
   - See: `ConsistentHashingAssignmentStrategy.assign()` in src/main/java/com/dqueue/cluster/ConsistentHashingAssignmentStrategy.java:20

### Message Routing

Messages are routed to partitions using MD5 hashing of the message key:

```
partition_id = hash(message.key) % partition_count
```

Implementation: `PartitionRouter.route()` in src/main/java/com/dqueue/core/PartitionRouter.java:24

## Running Integration Tests

### Automated Integration Tests

The `DistributedIntegrationTest` automatically spins up a 3-node cluster and runs comprehensive tests.

```bash
# Run integration tests
mvn test -Dtest=DistributedIntegrationTest

# Run with verbose logging
mvn test -Dtest=DistributedIntegrationTest -Dorg.slf4j.simpleLogger.defaultLogLevel=debug
```

**Test Coverage:**

1. ✓ Cluster formation and node discovery
2. ✓ Leader election
3. ✓ Distributed produce and consume
4. ✓ Partition distribution with consistent hashing
5. ✓ Concurrent producers from multiple clients
6. ✓ Node-to-node communication
7. ✓ Load balancing across nodes

### Test Output Example

```
[INFO] TEST: Cluster Formation
[INFO] Node node-1 sees cluster size: 3
[INFO] Cluster leader: node-1

[INFO] TEST: Distributed Produce and Consume
[INFO] Producing 100 messages to node-1
[INFO] All 100 messages produced successfully
[INFO] Consumed 100 messages from node-2

[INFO] TEST: Partition Distribution
[INFO] Partition assignments:
[INFO]   node-1: 33 partitions
[INFO]   node-2: 34 partitions
[INFO]   node-3: 33 partitions
```

## Manual Multi-Node Testing

### Starting a Cluster Manually

**Terminal 1 - Start Node 1 (Seed Node):**
```bash
mvn compile exec:java -Dexec.mainClass="com.dqueue.examples.MultiNodeClusterExample" -Dexec.args="1"
```

**Terminal 2 - Start Node 2:**
```bash
mvn compile exec:java -Dexec.mainClass="com.dqueue.examples.MultiNodeClusterExample" -Dexec.args="2 localhost:9001"
```

**Terminal 3 - Start Node 3:**
```bash
mvn compile exec:java -Dexec.mainClass="com.dqueue.examples.MultiNodeClusterExample" -Dexec.args="3 localhost:9001"
```

### Interactive Commands

Each node provides an interactive menu:

```
COMMANDS:
  1. Produce message         - Send a message to the queue
  2. Consume messages        - Read messages from the queue
  3. Show cluster info       - Display cluster status
  4. Show partition stats    - View partition distribution
  5. Produce batch           - Send 100 messages
  6. Test remote node        - Connect to another node
  0. Exit                    - Shutdown node
```

### Testing Scenarios

#### Scenario 1: Basic Cluster Operations

1. Start 3 nodes as shown above
2. On Node 1, select option `3` to see cluster info
3. Verify all 3 nodes are visible
4. On Node 1, select option `5` to produce 100 messages
5. On Node 2, select option `2` to consume messages
6. Verify messages are distributed across nodes

#### Scenario 2: Partition Distribution

1. Start 3-node cluster
2. On any node, select option `4` to see partition stats
3. Produce messages with different keys
4. Observe how partitions are distributed using consistent hashing

#### Scenario 3: Node Failure and Recovery

1. Start 3-node cluster
2. Note the current leader (option `3`)
3. Kill the leader node (Ctrl+C)
4. On remaining nodes, verify:
   - New leader is elected
   - Partitions are rebalanced
   - Messages remain accessible

#### Scenario 4: Cross-Node Communication

1. Start 2-node cluster
2. On Node 1, select option `6` and enter `2`
3. This tests remote connectivity to Node 2
4. Verify successful ping and cluster info retrieval

## Stress Testing

### Running Stress Tests

The `DistributedStressTest` simulates high load across multiple nodes.

**Start Cluster:**
```bash
# Terminal 1
mvn exec:java -Dexec.mainClass="com.dqueue.examples.MultiNodeClusterExample" -Dexec.args="1"

# Terminal 2
mvn exec:java -Dexec.mainClass="com.dqueue.examples.MultiNodeClusterExample" -Dexec.args="2 localhost:9001"

# Terminal 3
mvn exec:java -Dexec.mainClass="com.dqueue.examples.MultiNodeClusterExample" -Dexec.args="3 localhost:9001"
```

**Run Stress Test:**
```bash
# Terminal 4 - 10 producers, 10K messages each, 3 nodes
mvn exec:java -Dexec.mainClass="com.dqueue.examples.DistributedStressTest" \
  -Dexec.args="10 10000 9001 9002 9003"
```

**Expected Output:**
```
==========================================================
DISTRIBUTED QUEUE STRESS TEST
==========================================================
Configuration:
  Producers: 10
  Messages per producer: 10000
  Total messages: 100000
  Cluster nodes: 3
  Node ports: [9001, 9002, 9003]
==========================================================

Progress: 10000/100000 (10.0%)
Progress: 20000/100000 (20.0%)
...

==========================================================
STRESS TEST RESULTS
==========================================================
Total Messages Sent: 100,000
Failed Messages: 0
Success Rate: 100.00%
Duration: 12.45 seconds
Throughput: 8,032 messages/second
Average Latency: 1.245 ms/message
==========================================================
```

### Performance Benchmarks

Target performance (per node on modern hardware):

- **Throughput**: 50K-100K messages/second
- **Latency**: <10ms p99
- **Connections**: 10K+ concurrent clients
- **Cluster**: 3-10 nodes optimal

## Testing Scenarios

### 1. Cluster Formation Test

**What it tests:** Node discovery, cluster membership, leader election

**Steps:**
```bash
# Start nodes one by one
mvn exec:java -Dexec.mainClass="com.dqueue.examples.MultiNodeClusterExample" -Dexec.args="1"
# Wait 5 seconds
mvn exec:java -Dexec.mainClass="com.dqueue.examples.MultiNodeClusterExample" -Dexec.args="2 localhost:9001"
# Wait 5 seconds
mvn exec:java -Dexec.mainClass="com.dqueue.examples.MultiNodeClusterExample" -Dexec.args="3 localhost:9001"
```

**Expected:**
- All nodes see cluster size = 3
- One node is elected as leader
- Partitions are distributed evenly

### 2. Message Distribution Test

**What it tests:** Consistent hashing, partition routing

**Steps:**
1. Start 3-node cluster
2. Produce 1000 messages with keys: "key-0" through "key-999"
3. Check partition distribution on each node

**Expected:**
- Messages evenly distributed across partitions
- Same key always routes to same partition
- No message loss

### 3. Concurrent Access Test

**What it tests:** Virtual threads, concurrent message handling

**Steps:**
```bash
# Run stress test with many producers
mvn exec:java -Dexec.mainClass="com.dqueue.examples.DistributedStressTest" \
  -Dexec.args="50 1000 9001 9002 9003"
```

**Expected:**
- All messages successfully produced
- No deadlocks or race conditions
- Linear scalability with producer count

### 4. Node Failure Test

**What it tests:** Fault tolerance, leader re-election

**Steps:**
1. Start 3-node cluster
2. Identify leader (option `3`)
3. Kill leader node
4. Wait 20 seconds
5. Check cluster status on remaining nodes

**Expected:**
- New leader elected within 15 seconds
- Partitions rebalanced to surviving nodes
- Queue remains operational
- Messages remain accessible

### 5. Network Partition Test

**What it tests:** Split-brain scenarios, heartbeat timeout

**Steps:**
1. Start 3-node cluster
2. Block network traffic between nodes (firewall rules)
3. Observe cluster behavior

**Expected:**
- Nodes detect missing heartbeats
- Failed nodes removed from cluster after timeout
- Surviving nodes continue operating

### 6. Load Balancing Test

**What it tests:** Request distribution across nodes

**Steps:**
1. Start 3-node cluster
2. Create clients connecting to different nodes
3. Produce messages round-robin across clients
4. Verify even distribution

**Expected:**
- Messages distributed evenly across nodes
- No single node hotspot
- Consistent performance across all nodes

## Troubleshooting

### Issue: Nodes can't find each other

**Symptoms:**
- Cluster size shows 1
- JOIN_CLUSTER fails

**Solutions:**
- Check firewall settings
- Verify ports are not already in use: `netstat -an | grep 900`
- Ensure nodes use correct host/port for seed node
- Check logs for connection errors

### Issue: Leader election not working

**Symptoms:**
- No leader or multiple leaders

**Solutions:**
- Verify all nodes have unique IDs
- Check system clocks are synchronized
- Review ClusterManager logs
- Ensure heartbeats are being sent/received

### Issue: Messages not distributed

**Symptoms:**
- All messages go to one partition
- Uneven distribution

**Solutions:**
- Verify message keys are varied
- Check partition count configuration
- Review PartitionRouter logic
- Ensure consistent hashing is working

### Issue: High latency

**Symptoms:**
- Slow message produce/consume
- Timeouts

**Solutions:**
- Check system resources (CPU, memory)
- Reduce batch size or flush interval
- Increase virtual thread pool size
- Profile with JFR: `java -XX:StartFlightRecording ...`

### Issue: Connection refused

**Symptoms:**
- Clients can't connect to servers
- IOException: Connection refused

**Solutions:**
- Verify server is running: `curl -v telnet://localhost:9001`
- Check server logs for startup errors
- Ensure port is not blocked by firewall
- Try binding to 0.0.0.0 instead of localhost

## Advanced Testing

### Custom Test Scenarios

Create custom tests by extending the examples:

```java
// Connect to cluster
QueueClient client = new QueueClient("localhost", 9001);

// Your test logic here
for (int i = 0; i < 1000; i++) {
    Message msg = Message.builder()
        .key("test-" + i)
        .payload(("Custom test " + i).getBytes())
        .build();
    client.produce(msg).get();
}

// Verify results
List<Message> consumed = client.consumeAll(0, 1000).get();
assert consumed.size() == 1000;

client.close();
```

### Monitoring and Metrics

Enable detailed metrics:

```java
QueueMetrics metrics = new QueueMetrics();
MetricsCollector collector = new MetricsCollector(metrics, 5); // Report every 5s

// Metrics automatically reported:
// - Messages produced/consumed
// - Throughput (msg/sec)
// - Latency (avg, p95, p99)
// - Queue size
```

### Chaos Engineering

Test resilience by introducing failures:

1. Random node kills
2. Network delays (tc, netem)
3. CPU throttling (cgroups)
4. Disk I/O limits
5. Memory pressure

## Summary

The distributed queue system provides robust coordination through:

- **Cluster Management**: Automatic node discovery and health monitoring
- **Leader Election**: Deterministic leader selection
- **Consistent Hashing**: Minimal data movement during rebalancing
- **Virtual Threads**: Massive concurrency with minimal resources
- **Heartbeat Protocol**: Fast failure detection
- **RPC Communication**: Simple JSON-based protocol

All features are thoroughly tested via integration tests and can be manually verified using the provided examples.
