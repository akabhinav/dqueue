package com.dqueue.network;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.*;

/**
 * Manages heartbeats to remote nodes.
 */
public class HeartbeatManager implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(HeartbeatManager.class);
    private static final long HEARTBEAT_INTERVAL_MS = 3000;

    private final String nodeId;
    private final List<NodeConnection> remoteNodes;
    private final ScheduledExecutorService scheduler;
    private final ConcurrentHashMap<String, QueueClient> clients;

    public HeartbeatManager(String nodeId) {
        this.nodeId = nodeId;
        this.remoteNodes = new CopyOnWriteArrayList<>();
        this.scheduler = Executors.newScheduledThreadPool(1);
        this.clients = new ConcurrentHashMap<>();
    }

    /**
     * Add a remote node to send heartbeats to.
     */
    public void addNode(String host, int port) {
        NodeConnection connection = new NodeConnection(host, port);
        remoteNodes.add(connection);
        clients.put(host + ":" + port, new QueueClient(host, port));
        logger.info("Added remote node for heartbeat: {}:{}", host, port);
    }

    /**
     * Start sending heartbeats.
     */
    public void start() {
        scheduler.scheduleAtFixedRate(() -> {
            for (NodeConnection node : remoteNodes) {
                try {
                    QueueClient client = clients.get(node.host() + ":" + node.port());
                    if (client != null) {
                        client.sendHeartbeat(nodeId).get(2, TimeUnit.SECONDS);
                        logger.debug("Heartbeat sent to {}:{}", node.host(), node.port());
                    }
                } catch (Exception e) {
                    logger.warn("Failed to send heartbeat to {}:{}: {}",
                        node.host(), node.port(), e.getMessage());
                }
            }
        }, 0, HEARTBEAT_INTERVAL_MS, TimeUnit.MILLISECONDS);

        logger.info("Heartbeat manager started");
    }

    @Override
    public void close() {
        scheduler.shutdown();
        clients.values().forEach(QueueClient::close);
        logger.info("Heartbeat manager closed");
    }

    private record NodeConnection(String host, int port) {}
}
