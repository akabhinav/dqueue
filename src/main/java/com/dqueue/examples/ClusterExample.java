package com.dqueue.examples;

import com.dqueue.cluster.ClusterManager;
import com.dqueue.cluster.Node;

import java.util.List;
import java.util.Map;

/**
 * Example demonstrating cluster management and partition assignment.
 */
public class ClusterExample {

    public static void main(String[] args) throws Exception {
        System.out.println("Starting Cluster Management Example...\n");

        // Create cluster manager for node 1
        ClusterManager manager1 = new ClusterManager("node-1", "localhost", 9001);

        // Simulate other nodes joining
        Node node2 = Node.create("node-2", "localhost", 9002);
        Node node3 = Node.create("node-3", "localhost", 9003);

        manager1.joinNode(node2);
        manager1.joinNode(node3);

        System.out.println("Cluster formed with 3 nodes");
        System.out.println();

        // Show active nodes
        List<Node> activeNodes = manager1.getActiveNodes();
        System.out.println("Active Nodes:");
        for (Node node : activeNodes) {
            System.out.printf("  %s (%s:%d) - Status: %s%n",
                node.nodeId(), node.host(), node.port(), node.status());
        }
        System.out.println();

        // Show leader
        manager1.getLeaderId().ifPresent(leaderId ->
            System.out.println("Current Leader: " + leaderId)
        );
        System.out.println();

        // Get partition assignments
        int totalPartitions = 100;
        Map<String, List<Integer>> assignments = manager1.getPartitionAssignments(totalPartitions);

        System.out.println("Partition Assignments (using consistent hashing):");
        for (Map.Entry<String, List<Integer>> entry : assignments.entrySet()) {
            System.out.printf("  %s: %d partitions%n", entry.getKey(), entry.getValue().size());
            if (entry.getValue().size() <= 10) {
                System.out.println("    Partitions: " + entry.getValue());
            } else {
                System.out.println("    First 10: " + entry.getValue().subList(0, 10) + "...");
            }
        }
        System.out.println();

        // Simulate node failure
        System.out.println("Simulating node-3 failure...");
        manager1.removeNode("node-3");

        Thread.sleep(1000);

        // Show updated assignments
        Map<String, List<Integer>> newAssignments = manager1.getPartitionAssignments(totalPartitions);
        System.out.println("\nUpdated Partition Assignments:");
        for (Map.Entry<String, List<Integer>> entry : newAssignments.entrySet()) {
            System.out.printf("  %s: %d partitions%n", entry.getKey(), entry.getValue().size());
        }

        // Cleanup
        manager1.close();

        System.out.println("\nExample completed!");
    }
}
