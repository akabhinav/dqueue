package com.dqueue.cluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Manages the cluster of distributed queue nodes.
 * Handles node discovery, health monitoring, and partition assignment.
 */
public class ClusterManager implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(ClusterManager.class);
    private static final long HEARTBEAT_INTERVAL_MS = 5000;
    private static final long HEARTBEAT_TIMEOUT_MS = 15000;

    private final Node localNode;
    private final ConcurrentHashMap<String, Node> nodes;
    private final AtomicReference<String> leaderId;
    private final ScheduledExecutorService scheduler;
    private final PartitionAssignmentStrategy assignmentStrategy;

    public ClusterManager(String nodeId, String host, int port) {
        this.localNode = Node.create(nodeId, host, port);
        this.nodes = new ConcurrentHashMap<>();
        this.leaderId = new AtomicReference<>();
        this.scheduler = Executors.newScheduledThreadPool(2);
        this.assignmentStrategy = new ConsistentHashingAssignmentStrategy();

        // Add local node to cluster
        nodes.put(localNode.nodeId(), localNode.withStatus(Node.NodeStatus.ACTIVE));

        startHeartbeatTask();
        startHealthCheckTask();

        logger.info("Cluster manager started for node {}", nodeId);
    }

    /**
     * Join a node to the cluster.
     */
    public void joinNode(Node node) {
        nodes.put(node.nodeId(), node.withStatus(Node.NodeStatus.ACTIVE));
        logger.info("Node {} joined the cluster", node.nodeId());
        electLeader();
    }

    /**
     * Remove a node from the cluster.
     */
    public void removeNode(String nodeId) {
        Node removed = nodes.remove(nodeId);
        if (removed != null) {
            logger.info("Node {} removed from cluster", nodeId);
            electLeader();
        }
    }

    /**
     * Update heartbeat for a node.
     */
    public void updateHeartbeat(String nodeId) {
        nodes.computeIfPresent(nodeId, (id, node) -> node.withHeartbeat());
    }

    /**
     * Get all active nodes in the cluster.
     */
    public List<Node> getActiveNodes() {
        return nodes.values().stream()
            .filter(node -> node.status() == Node.NodeStatus.ACTIVE)
            .sorted(Comparator.comparing(Node::nodeId))
            .toList();
    }

    /**
     * Get the current leader node ID.
     */
    public Optional<String> getLeaderId() {
        return Optional.ofNullable(leaderId.get());
    }

    /**
     * Check if the local node is the leader.
     */
    public boolean isLeader() {
        return localNode.nodeId().equals(leaderId.get());
    }

    /**
     * Get partition assignments for all nodes.
     */
    public Map<String, List<Integer>> getPartitionAssignments(int totalPartitions) {
        List<Node> activeNodes = getActiveNodes();
        return assignmentStrategy.assign(activeNodes, totalPartitions);
    }

    /**
     * Elect a leader using simple deterministic selection (lowest node ID).
     */
    private void electLeader() {
        List<Node> activeNodes = getActiveNodes();
        if (!activeNodes.isEmpty()) {
            String newLeader = activeNodes.get(0).nodeId();
            String oldLeader = leaderId.getAndSet(newLeader);
            if (!newLeader.equals(oldLeader)) {
                logger.info("New leader elected: {}", newLeader);
            }
        }
    }

    /**
     * Start heartbeat task to send periodic heartbeats.
     */
    private void startHeartbeatTask() {
        scheduler.scheduleAtFixedRate(() -> {
            try {
                updateHeartbeat(localNode.nodeId());
                logger.debug("Heartbeat sent from node {}", localNode.nodeId());
            } catch (Exception e) {
                logger.error("Error sending heartbeat", e);
            }
        }, 0, HEARTBEAT_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    /**
     * Start health check task to monitor node health.
     */
    private void startHealthCheckTask() {
        scheduler.scheduleAtFixedRate(() -> {
            try {
                List<String> unhealthyNodes = new ArrayList<>();

                for (Map.Entry<String, Node> entry : nodes.entrySet()) {
                    Node node = entry.getValue();
                    // Skip local node
                    if (node.nodeId().equals(localNode.nodeId())) {
                        continue;
                    }

                    if (!node.isHealthy(HEARTBEAT_TIMEOUT_MS)) {
                        unhealthyNodes.add(node.nodeId());
                    }
                }

                // Remove unhealthy nodes
                for (String nodeId : unhealthyNodes) {
                    logger.warn("Node {} detected as unhealthy, removing from cluster", nodeId);
                    removeNode(nodeId);
                }

            } catch (Exception e) {
                logger.error("Error during health check", e);
            }
        }, HEARTBEAT_TIMEOUT_MS, HEARTBEAT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() {
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            scheduler.shutdownNow();
        }
        logger.info("Cluster manager shut down");
    }

    /**
     * Get the local node.
     */
    public Node getLocalNode() {
        return localNode;
    }

    /**
     * Get cluster size.
     */
    public int getClusterSize() {
        return (int) nodes.values().stream()
            .filter(node -> node.status() == Node.NodeStatus.ACTIVE)
            .count();
    }
}
