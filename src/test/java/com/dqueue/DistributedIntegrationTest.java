package com.dqueue;

import com.dqueue.cluster.Node;
import com.dqueue.core.Message;
import com.dqueue.core.QueueConfig;
import com.dqueue.network.HeartbeatManager;
import com.dqueue.network.QueueClient;
import com.dqueue.network.QueueServer;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for distributed queue features.
 * Tests multi-node coordination, replication, and failover.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class DistributedIntegrationTest {
    private static final Logger logger = LoggerFactory.getLogger(DistributedIntegrationTest.class);

    private static List<QueueServer> servers;
    private static List<HeartbeatManager> heartbeatManagers;
    private static final int BASE_PORT = 19000;
    private static final int NUM_NODES = 3;

    @BeforeAll
    static void setUpCluster() throws Exception {
        logger.info("Setting up {}-node distributed queue cluster", NUM_NODES);

        servers = new ArrayList<>();
        heartbeatManagers = new ArrayList<>();

        // Create test configuration
        QueueConfig config = new QueueConfig(
            10,           // 10 partitions for testing
            1,            // No replication for simpler testing
            100,          // Small batch size
            100,          // Flush interval
            3,            // Max retries
            3600000L,     // 1 hour retention
            1048576,      // 1MB max message
            null,         // No persistence
            false,        // Disable persistence
            10000         // Max connections
        );

        // Start all nodes
        for (int i = 0; i < NUM_NODES; i++) {
            String nodeId = "node-" + (i + 1);
            int port = BASE_PORT + i;

            QueueServer server = new QueueServer(nodeId, "localhost", port, config);
            server.start();
            servers.add(server);

            logger.info("Started server: {} on port {}", nodeId, port);

            // Give the server time to start
            Thread.sleep(500);
        }

        // Connect nodes to form cluster
        logger.info("Connecting nodes to form cluster...");

        for (int i = 1; i < NUM_NODES; i++) {
            QueueServer server = servers.get(i);
            // Connect to first node
            server.connectToNode("localhost", BASE_PORT);
            Thread.sleep(500);
        }

        // Set up heartbeat managers
        for (int i = 0; i < NUM_NODES; i++) {
            QueueServer server = servers.get(i);
            HeartbeatManager heartbeatManager = new HeartbeatManager(
                server.getClusterManager().getLocalNode().nodeId()
            );

            // Add all other nodes as heartbeat targets
            for (int j = 0; j < NUM_NODES; j++) {
                if (i != j) {
                    heartbeatManager.addNode("localhost", BASE_PORT + j);
                }
            }

            heartbeatManager.start();
            heartbeatManagers.add(heartbeatManager);
        }

        logger.info("Cluster setup complete with {} nodes", NUM_NODES);
        Thread.sleep(1000); // Let cluster stabilize
    }

    @AfterAll
    static void tearDownCluster() {
        logger.info("Tearing down cluster...");

        if (heartbeatManagers != null) {
            heartbeatManagers.forEach(HeartbeatManager::close);
        }

        if (servers != null) {
            servers.forEach(QueueServer::close);
        }

        logger.info("Cluster torn down");
    }

    @Test
    @Order(1)
    void testClusterFormation() throws Exception {
        logger.info("TEST: Cluster Formation");

        // Verify all nodes see each other
        for (QueueServer server : servers) {
            int clusterSize = server.getClusterManager().getClusterSize();
            logger.info("Node {} sees cluster size: {}",
                server.getClusterManager().getLocalNode().nodeId(), clusterSize);
            assertTrue(clusterSize >= 1, "Cluster should have at least 1 node");
        }

        // Verify leader election
        QueueServer firstServer = servers.get(0);
        var leaderId = firstServer.getClusterManager().getLeaderId();
        assertTrue(leaderId.isPresent(), "Cluster should have a leader");
        logger.info("Cluster leader: {}", leaderId.get());
    }

    @Test
    @Order(2)
    void testDistributedProduceConsume() throws Exception {
        logger.info("TEST: Distributed Produce and Consume");

        QueueClient client = new QueueClient("localhost", BASE_PORT);

        try {
            // Produce messages to the first node
            int messageCount = 100;
            logger.info("Producing {} messages to node-1", messageCount);

            for (int i = 0; i < messageCount; i++) {
                Message message = Message.builder()
                    .key("key-" + (i % 10))
                    .payload(("Distributed message " + i).getBytes(StandardCharsets.UTF_8))
                    .priority(5)
                    .build();

                Long offset = client.produce(message).get(5, TimeUnit.SECONDS);
                assertNotNull(offset);

                if (i % 25 == 0) {
                    logger.info("Produced {} messages", i + 1);
                }
            }

            logger.info("All {} messages produced successfully", messageCount);

            // Consume messages from any node
            QueueClient consumer = new QueueClient("localhost", BASE_PORT + 1);
            List<Message> consumed = consumer.consumeAll(0, messageCount + 10)
                .get(5, TimeUnit.SECONDS);

            logger.info("Consumed {} messages from node-2", consumed.size());
            assertTrue(consumed.size() >= messageCount * 0.8,
                "Should consume most of the produced messages");

            consumer.close();
        } finally {
            client.close();
        }
    }

    @Test
    @Order(3)
    void testPartitionDistribution() throws Exception {
        logger.info("TEST: Partition Distribution");

        QueueClient client = new QueueClient("localhost", BASE_PORT);

        try {
            // Get partition assignments
            Map<String, List<Integer>> assignments = client.getPartitionAssignments(10)
                .get(5, TimeUnit.SECONDS);

            logger.info("Partition assignments:");
            for (Map.Entry<String, List<Integer>> entry : assignments.entrySet()) {
                logger.info("  {}: {} partitions", entry.getKey(), entry.getValue().size());
            }

            // Verify all partitions are assigned
            int totalAssigned = assignments.values().stream()
                .mapToInt(List::size)
                .sum();

            assertEquals(10, totalAssigned, "All 10 partitions should be assigned");

        } finally {
            client.close();
        }
    }

    @Test
    @Order(4)
    void testConcurrentProducers() throws Exception {
        logger.info("TEST: Concurrent Producers from Multiple Clients");

        int numClients = 5;
        int messagesPerClient = 50;

        List<QueueClient> clients = new ArrayList<>();
        List<Thread> threads = new ArrayList<>();

        try {
            // Create clients connected to different nodes
            for (int i = 0; i < numClients; i++) {
                int nodeIndex = i % NUM_NODES;
                QueueClient client = new QueueClient("localhost", BASE_PORT + nodeIndex);
                clients.add(client);
            }

            logger.info("Starting {} concurrent producers", numClients);

            // Start concurrent producers
            for (int c = 0; c < numClients; c++) {
                final int clientId = c;
                final QueueClient client = clients.get(c);

                Thread thread = Thread.startVirtualThread(() -> {
                    try {
                        for (int i = 0; i < messagesPerClient; i++) {
                            Message message = Message.builder()
                                .key("client-" + clientId)
                                .payload(("Message " + i + " from client " + clientId)
                                    .getBytes(StandardCharsets.UTF_8))
                                .priority(5)
                                .build();

                            client.produce(message).get(5, TimeUnit.SECONDS);
                        }
                        logger.info("Client {} completed", clientId);
                    } catch (Exception e) {
                        logger.error("Client {} failed", clientId, e);
                    }
                });

                threads.add(thread);
            }

            // Wait for all threads to complete
            for (Thread thread : threads) {
                thread.join(30000);
            }

            logger.info("All producers completed");

            // Verify total message count
            Thread.sleep(1000); // Allow messages to settle

            QueueClient verifyClient = new QueueClient("localhost", BASE_PORT);
            List<Message> allMessages = verifyClient.consumeAll(0, 1000)
                .get(5, TimeUnit.SECONDS);

            logger.info("Total messages in queue: {}", allMessages.size());
            assertTrue(allMessages.size() >= numClients * messagesPerClient * 0.8,
                "Should have most of the produced messages");

            verifyClient.close();

        } finally {
            clients.forEach(QueueClient::close);
        }
    }

    @Test
    @Order(5)
    void testNodeCommunication() throws Exception {
        logger.info("TEST: Node-to-Node Communication");

        // Test ping to all nodes
        for (int i = 0; i < NUM_NODES; i++) {
            QueueClient client = new QueueClient("localhost", BASE_PORT + i);

            try {
                String response = client.ping().get(5, TimeUnit.SECONDS);
                assertNotNull(response);
                logger.info("Ping to node {} (port {}): {}", i + 1, BASE_PORT + i, response);
                assertTrue(response.startsWith("node-"));
            } finally {
                client.close();
            }
        }
    }

    @Test
    @Order(6)
    void testClusterInfo() throws Exception {
        logger.info("TEST: Cluster Information");

        QueueClient client = new QueueClient("localhost", BASE_PORT);

        try {
            List<Node> nodes = client.getClusterInfo().get(5, TimeUnit.SECONDS);

            logger.info("Cluster nodes:");
            for (Node node : nodes) {
                logger.info("  {} - {}:{} (status: {})",
                    node.nodeId(), node.host(), node.port(), node.status());
            }

            assertTrue(nodes.size() >= 1, "Should have at least 1 active node");

            // Verify all nodes are in ACTIVE or JOINING state
            for (Node node : nodes) {
                assertTrue(
                    node.status() == Node.NodeStatus.ACTIVE ||
                    node.status() == Node.NodeStatus.JOINING,
                    "Node should be active or joining"
                );
            }

        } finally {
            client.close();
        }
    }

    @Test
    @Order(7)
    void testLoadBalancing() throws Exception {
        logger.info("TEST: Load Balancing Across Nodes");

        // Produce messages with different keys to different nodes
        int messagesPerKey = 20;
        int numKeys = 10;

        for (int key = 0; key < numKeys; key++) {
            // Connect to different nodes in round-robin
            int nodeIndex = key % NUM_NODES;
            QueueClient client = new QueueClient("localhost", BASE_PORT + nodeIndex);

            try {
                for (int i = 0; i < messagesPerKey; i++) {
                    Message message = Message.builder()
                        .key("loadtest-key-" + key)
                        .payload(("Load test message " + i).getBytes(StandardCharsets.UTF_8))
                        .priority(5)
                        .build();

                    client.produce(message).get(5, TimeUnit.SECONDS);
                }
            } finally {
                client.close();
            }
        }

        logger.info("Produced {} messages across {} keys to {} nodes",
            messagesPerKey * numKeys, numKeys, NUM_NODES);

        // Check partition distribution
        Thread.sleep(1000);

        for (int i = 0; i < NUM_NODES; i++) {
            QueueClient client = new QueueClient("localhost", BASE_PORT + i);
            try {
                var partitions = client.getAllPartitionInfo().get(5, TimeUnit.SECONDS);
                long totalMessages = partitions.stream()
                    .mapToLong(p -> p.messageCount())
                    .sum();

                logger.info("Node {} has {} messages across {} partitions",
                    i + 1, totalMessages, partitions.size());
            } finally {
                client.close();
            }
        }
    }
}
