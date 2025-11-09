package com.dqueue.cluster;

import java.time.Instant;
import java.util.Objects;

/**
 * Represents a node in the distributed queue cluster.
 */
public record Node(
    String nodeId,
    String host,
    int port,
    NodeStatus status,
    long lastHeartbeat
) {

    public Node {
        Objects.requireNonNull(nodeId, "Node ID cannot be null");
        Objects.requireNonNull(host, "Host cannot be null");
        Objects.requireNonNull(status, "Status cannot be null");
    }

    public enum NodeStatus {
        JOINING,    // Node is joining the cluster
        ACTIVE,     // Node is active and healthy
        SUSPECTED,  // Node is suspected to be down
        LEAVING,    // Node is gracefully leaving
        DOWN        // Node is down
    }

    /**
     * Check if the node is healthy based on heartbeat timeout.
     */
    public boolean isHealthy(long timeoutMs) {
        long now = Instant.now().toEpochMilli();
        return (now - lastHeartbeat) < timeoutMs;
    }

    /**
     * Create a new node with updated heartbeat.
     */
    public Node withHeartbeat() {
        return new Node(nodeId, host, port, status, Instant.now().toEpochMilli());
    }

    /**
     * Create a new node with updated status.
     */
    public Node withStatus(NodeStatus newStatus) {
        return new Node(nodeId, host, port, newStatus, lastHeartbeat);
    }

    /**
     * Create a new node instance.
     */
    public static Node create(String nodeId, String host, int port) {
        return new Node(
            nodeId,
            host,
            port,
            NodeStatus.JOINING,
            Instant.now().toEpochMilli()
        );
    }
}
