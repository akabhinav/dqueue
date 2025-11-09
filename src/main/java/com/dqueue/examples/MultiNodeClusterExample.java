package com.dqueue.examples;

import com.dqueue.cluster.Node;
import com.dqueue.core.Message;
import com.dqueue.core.QueueConfig;
import com.dqueue.network.HeartbeatManager;
import com.dqueue.network.QueueClient;
import com.dqueue.network.QueueServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

/**
 * Example demonstrating a multi-node distributed queue cluster.
 * Run multiple instances of this program with different node IDs to test coordination.
 *
 * Usage:
 *   Node 1: java MultiNodeClusterExample 1
 *   Node 2: java MultiNodeClusterExample 2 localhost:9001
 *   Node 3: java MultiNodeClusterExample 3 localhost:9001
 */
public class MultiNodeClusterExample {
    private static final Logger logger = LoggerFactory.getLogger(MultiNodeClusterExample.class);
    private static final int BASE_PORT = 9000;

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.out.println("Usage: java MultiNodeClusterExample <nodeId> [seedNode]");
            System.out.println("  nodeId: Integer (1, 2, 3, ...)");
            System.out.println("  seedNode: Optional host:port to join existing cluster");
            System.out.println();
            System.out.println("Examples:");
            System.out.println("  Start first node:  java MultiNodeClusterExample 1");
            System.out.println("  Join cluster:      java MultiNodeClusterExample 2 localhost:9001");
            return;
        }

        int nodeId = Integer.parseInt(args[0]);
        String seedNode = args.length > 1 ? args[1] : null;

        System.out.println("=".repeat(60));
        System.out.println("DISTRIBUTED QUEUE - MULTI-NODE CLUSTER");
        System.out.println("=".repeat(60));
        System.out.println();

        // Configure queue
        QueueConfig config = new QueueConfig(
            50,              // 50 partitions
            2,               // Replication factor 2
            1000,            // Batch size
            100,             // Flush interval
            3,               // Max retries
            86400000L,       // 1 day retention
            1048576,         // 1MB max message
            "./data/node-" + nodeId,  // Storage path per node
            false,           // Disable persistence for demo
            100000           // Max connections
        );

        String nodeIdStr = "node-" + nodeId;
        int port = BASE_PORT + nodeId;

        // Start server
        System.out.println("Starting queue server...");
        System.out.println("  Node ID: " + nodeIdStr);
        System.out.println("  Port: " + port);
        System.out.println();

        QueueServer server = new QueueServer(nodeIdStr, "localhost", port, config);
        server.start();

        // Join existing cluster if seed node provided
        if (seedNode != null) {
            String[] parts = seedNode.split(":");
            String seedHost = parts[0];
            int seedPort = Integer.parseInt(parts[1]);

            System.out.println("Joining cluster via seed node: " + seedNode);
            server.connectToNode(seedHost, seedPort);

            Thread.sleep(1000);

            // Set up heartbeats to all cluster nodes
            HeartbeatManager heartbeatManager = new HeartbeatManager(nodeIdStr);

            QueueClient client = new QueueClient(seedHost, seedPort);
            List<Node> clusterNodes = client.getClusterInfo().get();
            client.close();

            for (Node node : clusterNodes) {
                if (!node.nodeId().equals(nodeIdStr)) {
                    heartbeatManager.addNode(node.host(), node.port());
                }
            }

            heartbeatManager.start();
            System.out.println("Heartbeat manager started for " + clusterNodes.size() + " nodes");
        }

        Thread.sleep(2000);

        // Display cluster info
        displayClusterInfo(server);

        // Interactive menu
        runInteractiveMenu(server);

        // Cleanup
        server.close();
        System.out.println("\nNode shut down. Goodbye!");
    }

    private static void displayClusterInfo(QueueServer server) {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("CLUSTER STATUS");
        System.out.println("=".repeat(60));

        var clusterManager = server.getClusterManager();
        var localNode = clusterManager.getLocalNode();

        System.out.println("Local Node: " + localNode.nodeId());
        System.out.println("Cluster Size: " + clusterManager.getClusterSize());
        System.out.println("Is Leader: " + (clusterManager.isLeader() ? "YES" : "NO"));

        clusterManager.getLeaderId().ifPresent(leaderId ->
            System.out.println("Cluster Leader: " + leaderId)
        );

        System.out.println("\nActive Nodes:");
        for (Node node : clusterManager.getActiveNodes()) {
            System.out.printf("  - %s (%s:%d) [%s]%n",
                node.nodeId(), node.host(), node.port(), node.status());
        }

        System.out.println("\nPartition Assignments:");
        Map<String, List<Integer>> assignments = clusterManager.getPartitionAssignments(50);
        for (Map.Entry<String, List<Integer>> entry : assignments.entrySet()) {
            System.out.printf("  %s: %d partitions%n", entry.getKey(), entry.getValue().size());
        }

        System.out.println("=".repeat(60));
    }

    private static void runInteractiveMenu(QueueServer server) throws Exception {
        Scanner scanner = new Scanner(System.in);

        while (true) {
            System.out.println("\n" + "-".repeat(60));
            System.out.println("COMMANDS:");
            System.out.println("  1. Produce message");
            System.out.println("  2. Consume messages");
            System.out.println("  3. Show cluster info");
            System.out.println("  4. Show partition stats");
            System.out.println("  5. Produce batch (100 messages)");
            System.out.println("  6. Test remote node (requires node ID)");
            System.out.println("  0. Exit");
            System.out.println("-".repeat(60));
            System.out.print("Enter command: ");

            String input = scanner.nextLine().trim();

            try {
                switch (input) {
                    case "1" -> {
                        System.out.print("Enter message text: ");
                        String text = scanner.nextLine();

                        Message message = Message.builder()
                            .key("user-message")
                            .payload(text.getBytes(StandardCharsets.UTF_8))
                            .priority(5)
                            .build();

                        Long offset = server.getQueue().produce(message).get();
                        System.out.println("✓ Message produced at offset: " + offset);
                    }

                    case "2" -> {
                        List<Message> messages = server.getQueue().consumeAll(0, 10).get();
                        System.out.println("\nConsumed " + messages.size() + " messages:");
                        for (Message msg : messages) {
                            System.out.println("  - " + new String(msg.payload(), StandardCharsets.UTF_8));
                        }
                    }

                    case "3" -> displayClusterInfo(server);

                    case "4" -> {
                        System.out.println("\nPartition Statistics:");
                        var partitions = server.getQueue().getAllPartitionInfo();
                        long totalMessages = 0;
                        int nonEmpty = 0;

                        for (var partition : partitions) {
                            if (partition.messageCount() > 0) {
                                System.out.printf("  Partition %d: offset=%d, messages=%d%n",
                                    partition.partitionId(),
                                    partition.currentOffset(),
                                    partition.messageCount());
                                totalMessages += partition.messageCount();
                                nonEmpty++;
                            }
                        }

                        System.out.println("\nSummary:");
                        System.out.println("  Total messages: " + totalMessages);
                        System.out.println("  Non-empty partitions: " + nonEmpty + "/" + partitions.size());
                    }

                    case "5" -> {
                        System.out.println("Producing 100 messages...");
                        for (int i = 0; i < 100; i++) {
                            Message message = Message.builder()
                                .key("batch-key-" + (i % 10))
                                .payload(("Batch message " + i).getBytes(StandardCharsets.UTF_8))
                                .priority(5)
                                .build();
                            server.getQueue().produce(message).get();

                            if ((i + 1) % 25 == 0) {
                                System.out.println("  Produced " + (i + 1) + " messages...");
                            }
                        }
                        System.out.println("✓ Batch production complete!");
                    }

                    case "6" -> {
                        System.out.print("Enter remote node ID (e.g., 2 for node-2): ");
                        int remoteNodeId = Integer.parseInt(scanner.nextLine().trim());
                        int remotePort = BASE_PORT + remoteNodeId;

                        System.out.println("Connecting to node-" + remoteNodeId + " on port " + remotePort + "...");

                        QueueClient client = new QueueClient("localhost", remotePort);
                        try {
                            String pong = client.ping().get();
                            System.out.println("✓ Ping successful: " + pong);

                            List<Node> nodes = client.getClusterInfo().get();
                            System.out.println("Remote node sees " + nodes.size() + " nodes in cluster");
                        } finally {
                            client.close();
                        }
                    }

                    case "0" -> {
                        System.out.println("Shutting down...");
                        return;
                    }

                    default -> System.out.println("Invalid command. Try again.");
                }
            } catch (Exception e) {
                System.err.println("Error: " + e.getMessage());
                logger.error("Command execution failed", e);
            }
        }
    }
}
