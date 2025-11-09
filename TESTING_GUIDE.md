# Quick Testing Guide - Distributed Queue

This guide shows you how to quickly test all distributed features.

## Prerequisites

```bash
# Ensure Java 21 is installed
java -version  # Should show version 21+

# Build the project
mvn clean compile
```

## 1. Unit Tests (5 minutes)

Test core functionality without networking:

```bash
mvn test -Dtest=DistributedQueueTest
```

**What it tests:**
- Message produce/consume
- Batch operations
- Partitioning
- Priority ordering
- Message expiry
- Concurrent producers

## 2. Integration Tests (2 minutes)

Automated tests that spin up a 3-node cluster:

```bash
mvn test -Dtest=DistributedIntegrationTest
```

**What it tests:**
- ✓ Cluster formation (3 nodes)
- ✓ Leader election
- ✓ Distributed produce/consume
- ✓ Partition distribution
- ✓ Concurrent producers
- ✓ Node-to-node communication
- ✓ Load balancing

**Expected Output:**
```
[INFO] TEST: Cluster Formation
[INFO] Node node-1 sees cluster size: 3
[INFO] Cluster leader: node-1

[INFO] TEST: Distributed Produce and Consume
[INFO] Producing 100 messages to node-1
[INFO] Consumed 100 messages from node-2

[INFO] TEST: Partition Distribution
[INFO] Partition assignments:
[INFO]   node-1: 33 partitions
[INFO]   node-2: 34 partitions
[INFO]   node-3: 33 partitions

Tests run: 7, Failures: 0, Errors: 0, Skipped: 0
```

## 3. Manual Multi-Node Testing (10 minutes)

### Option A: Use the Script (Easiest)

```bash
# Start 3-node cluster
./run-cluster.sh 3

# Logs will be in node-1.log, node-2.log, node-3.log
tail -f node-1.log
```

### Option B: Manual Start (More Control)

Open 3 terminals:

**Terminal 1 - Start Node 1 (Seed):**
```bash
mvn exec:java -Dexec.mainClass="com.dqueue.examples.MultiNodeClusterExample" -Dexec.args="1"
```

**Terminal 2 - Start Node 2:**
```bash
mvn exec:java -Dexec.mainClass="com.dqueue.examples.MultiNodeClusterExample" -Dexec.args="2 localhost:9001"
```

**Terminal 3 - Start Node 3:**
```bash
mvn exec:java -Dexec.mainClass="com.dqueue.examples.MultiNodeClusterExample" -Dexec.args="3 localhost:9001"
```

### Interactive Testing

Once nodes are running, you'll see this menu on each:

```
COMMANDS:
  1. Produce message
  2. Consume messages
  3. Show cluster info
  4. Show partition stats
  5. Produce batch (100 messages)
  6. Test remote node (requires node ID)
  0. Exit
```

**Try These Scenarios:**

1. **Cluster Info** - Enter `3` on any node
   - Should show 3 nodes in cluster
   - Should show a leader

2. **Produce on Node 1** - Enter `5` on Node 1
   - Produces 100 messages

3. **Consume on Node 2** - Enter `2` on Node 2
   - Should retrieve messages produced on Node 1

4. **Cross-Node Test** - Enter `6` on Node 1, then `2`
   - Tests communication with Node 2

5. **Partition Stats** - Enter `4` on any node
   - Shows how messages are distributed

## 4. Stress Testing (5 minutes)

Test high load across multiple nodes:

```bash
# Terminal 1-3: Start 3-node cluster (see above)

# Terminal 4: Run stress test
./run-stress-test.sh 10 10000 9001 9002 9003
```

This runs:
- 10 concurrent producers
- 10,000 messages per producer
- Total: 100,000 messages
- Distributed across 3 nodes

**Expected Output:**
```
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

## 5. Testing Node Coordination

### Test Leader Election

1. Start 3-node cluster
2. Note the leader (use option `3`)
3. Kill the leader node (Ctrl+C)
4. Wait 15 seconds
5. Check remaining nodes - new leader should be elected

**What to verify:**
- New leader elected within 15 seconds
- Remaining nodes still operational
- Messages still accessible

### Test Partition Rebalancing

1. Start 2-node cluster
2. Check partition assignments (option `3`)
3. Start Node 3 to join
4. Check assignments again
5. Partitions should be redistributed

**What to verify:**
- Partitions redistributed evenly
- No message loss
- System remains available during rebalancing

### Test Heartbeat Protocol

1. Start 3-node cluster
2. Watch logs: `tail -f node-1.log | grep -i heartbeat`
3. Should see periodic heartbeat messages
4. Kill a node
5. Should see failure detection after ~15 seconds

## 6. Performance Testing

### Single Node Performance

```bash
mvn exec:java -Dexec.mainClass="com.dqueue.examples.HighThroughputExample"
```

Expected: 100K-1M msg/sec depending on hardware

### Distributed Performance

```bash
# 3-node cluster with 50 producers
./run-stress-test.sh 50 10000 9001 9002 9003
```

Expected: Linear scaling with node count

## 7. Client API Testing

### Remote Client Connection

```java
// Connect to any node
QueueClient client = new QueueClient("localhost", 9001);

// Produce message
Message msg = Message.builder()
    .key("test")
    .payload("Hello".getBytes())
    .build();

Long offset = client.produce(msg).get();
System.out.println("Produced at offset: " + offset);

// Consume messages
List<Message> messages = client.consumeAll(0, 10).get();
System.out.println("Consumed: " + messages.size());

// Get cluster info
List<Node> nodes = client.getClusterInfo().get();
System.out.println("Cluster has " + nodes.size() + " nodes");

client.close();
```

## Common Test Scenarios

### Scenario 1: Basic Cluster Operations
```bash
1. Start 3-node cluster
2. Produce 1000 messages on Node 1
3. Consume from Node 2
4. Verify all messages received
```

### Scenario 2: Load Distribution
```bash
1. Start 3-node cluster
2. Run: ./run-stress-test.sh 30 1000 9001 9002 9003
3. Check partition stats on each node
4. Verify even distribution
```

### Scenario 3: Fault Tolerance
```bash
1. Start 3-node cluster
2. Produce 1000 messages
3. Kill Node 2
4. Verify Node 1 and 3 continue operating
5. Consume messages from Node 1
6. Verify all 1000 messages still available
```

### Scenario 4: Concurrent Access
```bash
1. Start 3-node cluster
2. Open 3 terminals
3. On each, produce 1000 messages concurrently
4. Total: 3000 messages
5. Verify count on any node
```

## Verification Checklist

After testing, verify:

- [ ] All unit tests pass
- [ ] All integration tests pass
- [ ] Cluster forms with multiple nodes
- [ ] Leader is elected
- [ ] Partitions are distributed evenly
- [ ] Messages can be produced to any node
- [ ] Messages can be consumed from any node
- [ ] Heartbeats are working (check logs)
- [ ] Node failure is detected
- [ ] New leader elected after failure
- [ ] Stress test completes successfully
- [ ] No message loss
- [ ] Performance meets expectations

## Troubleshooting

**Problem: Tests fail with "Connection refused"**
- Solution: Check if port is already in use: `netstat -an | grep 900`

**Problem: Nodes don't see each other**
- Solution: Check firewall, verify seed node address is correct

**Problem: Leader not elected**
- Solution: Check all nodes have unique IDs, review logs

**Problem: Low throughput**
- Solution: Increase batch size, reduce flush interval, check CPU/memory

## Next Steps

For more detailed testing scenarios, see:
- [DISTRIBUTED_TESTING.md](DISTRIBUTED_TESTING.md) - Comprehensive guide
- [README.md](README.md) - Full documentation
- [DESIGN.md](DESIGN.md) - Architecture details

## Quick Reference

```bash
# Build
mvn clean compile

# Run all tests
mvn test

# Integration tests
mvn test -Dtest=DistributedIntegrationTest

# Start cluster
./run-cluster.sh 3

# Stress test
./run-stress-test.sh 10 10000 9001 9002 9003

# Single example
mvn exec:java -Dexec.mainClass="com.dqueue.examples.SimpleProducerConsumerExample"
```
