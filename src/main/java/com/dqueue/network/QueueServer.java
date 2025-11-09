package com.dqueue.network;

import com.dqueue.cluster.ClusterManager;
import com.dqueue.cluster.Node;
import com.dqueue.core.DistributedQueue;
import com.dqueue.core.Message;
import com.dqueue.core.QueueConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Network server for distributed queue nodes.
 * Handles incoming connections and coordinates with other nodes.
 */
public class QueueServer implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(QueueServer.class);

    private final String nodeId;
    private final int port;
    private final DistributedQueue queue;
    private final ClusterManager clusterManager;
    private final ServerSocket serverSocket;
    private final ExecutorService virtualThreadExecutor;
    private final ObjectMapper objectMapper;
    private final AtomicBoolean running;

    public QueueServer(String nodeId, String host, int port, QueueConfig config) throws IOException {
        this.nodeId = nodeId;
        this.port = port;
        this.queue = new DistributedQueue(config);
        this.clusterManager = new ClusterManager(nodeId, host, port);
        this.serverSocket = new ServerSocket(port);
        this.virtualThreadExecutor = Executors.newVirtualThreadPerTaskExecutor();
        this.objectMapper = new ObjectMapper();
        this.running = new AtomicBoolean(false);

        logger.info("Queue server initialized: {} on {}:{}", nodeId, host, port);
    }

    /**
     * Start the server and listen for connections.
     */
    public void start() {
        if (running.compareAndSet(false, true)) {
            logger.info("Starting queue server on port {}", port);

            // Accept connections in a virtual thread
            virtualThreadExecutor.submit(() -> {
                while (running.get()) {
                    try {
                        Socket clientSocket = serverSocket.accept();
                        logger.debug("Accepted connection from {}", clientSocket.getRemoteSocketAddress());

                        // Handle each connection in its own virtual thread
                        virtualThreadExecutor.submit(() -> handleClient(clientSocket));
                    } catch (IOException e) {
                        if (running.get()) {
                            logger.error("Error accepting connection", e);
                        }
                    }
                }
            });

            logger.info("Queue server started successfully");
        }
    }

    /**
     * Handle client connection.
     */
    private void handleClient(Socket socket) {
        try (socket;
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {

            String requestLine;
            while ((requestLine = in.readLine()) != null) {
                try {
                    ServerRequest request = objectMapper.readValue(requestLine, ServerRequest.class);
                    ServerResponse response = processRequest(request);
                    String responseJson = objectMapper.writeValueAsString(response);
                    out.println(responseJson);
                } catch (Exception e) {
                    logger.error("Error processing request", e);
                    ServerResponse errorResponse = new ServerResponse("ERROR", null, e.getMessage());
                    out.println(objectMapper.writeValueAsString(errorResponse));
                }
            }
        } catch (IOException e) {
            logger.debug("Client connection closed: {}", e.getMessage());
        }
    }

    /**
     * Process incoming request.
     */
    private ServerResponse processRequest(ServerRequest request) throws Exception {
        logger.debug("Processing request: {}", request.command());

        return switch (request.command()) {
            case "PRODUCE" -> {
                Message message = objectMapper.convertValue(request.payload(), Message.class);
                Long offset = queue.produce(message).get();
                yield new ServerResponse("OK", offset, null);
            }
            case "CONSUME" -> {
                @SuppressWarnings("unchecked")
                var params = (java.util.Map<String, Object>) request.payload();
                int partitionId = (Integer) params.get("partitionId");
                long fromOffset = ((Number) params.get("fromOffset")).longValue();
                int maxMessages = (Integer) params.get("maxMessages");

                List<Message> messages = queue.consume(partitionId, fromOffset, maxMessages).get();
                yield new ServerResponse("OK", messages, null);
            }
            case "CONSUME_ALL" -> {
                @SuppressWarnings("unchecked")
                var params = (java.util.Map<String, Object>) request.payload();
                long fromOffset = ((Number) params.get("fromOffset")).longValue();
                int maxMessages = (Integer) params.get("maxMessages");

                List<Message> messages = queue.consumeAll(fromOffset, maxMessages).get();
                yield new ServerResponse("OK", messages, null);
            }
            case "ACKNOWLEDGE" -> {
                @SuppressWarnings("unchecked")
                var params = (java.util.Map<String, Object>) request.payload();
                int partitionId = (Integer) params.get("partitionId");
                long upToOffset = ((Number) params.get("upToOffset")).longValue();

                queue.acknowledge(partitionId, upToOffset).get();
                yield new ServerResponse("OK", null, null);
            }
            case "PARTITION_INFO" -> {
                int partitionId = (Integer) request.payload();
                var info = queue.getPartitionInfo(partitionId);
                yield new ServerResponse("OK", info, null);
            }
            case "ALL_PARTITIONS_INFO" -> {
                var info = queue.getAllPartitionInfo();
                yield new ServerResponse("OK", info, null);
            }
            case "JOIN_CLUSTER" -> {
                Node node = objectMapper.convertValue(request.payload(), Node.class);
                clusterManager.joinNode(node);
                yield new ServerResponse("OK", null, null);
            }
            case "CLUSTER_INFO" -> {
                var nodes = clusterManager.getActiveNodes();
                yield new ServerResponse("OK", nodes, null);
            }
            case "HEARTBEAT" -> {
                String nodeIdToUpdate = (String) request.payload();
                clusterManager.updateHeartbeat(nodeIdToUpdate);
                yield new ServerResponse("OK", null, null);
            }
            case "PARTITION_ASSIGNMENTS" -> {
                int totalPartitions = (Integer) request.payload();
                var assignments = clusterManager.getPartitionAssignments(totalPartitions);
                yield new ServerResponse("OK", assignments, null);
            }
            case "PING" -> new ServerResponse("PONG", nodeId, null);
            default -> new ServerResponse("ERROR", null, "Unknown command: " + request.command());
        };
    }

    /**
     * Connect to another node and join its cluster.
     */
    public void connectToNode(String host, int port) throws IOException {
        logger.info("Connecting to node at {}:{}", host, port);

        try (Socket socket = new Socket(host, port);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {

            // Send join request
            Node localNode = clusterManager.getLocalNode();
            ServerRequest joinRequest = new ServerRequest("JOIN_CLUSTER", localNode);
            out.println(objectMapper.writeValueAsString(joinRequest));

            // Read response
            String responseLine = in.readLine();
            ServerResponse response = objectMapper.readValue(responseLine, ServerResponse.class);

            if ("OK".equals(response.status())) {
                logger.info("Successfully joined cluster at {}:{}", host, port);

                // Get cluster info
                ServerRequest clusterInfoRequest = new ServerRequest("CLUSTER_INFO", null);
                out.println(objectMapper.writeValueAsString(clusterInfoRequest));

                responseLine = in.readLine();
                response = objectMapper.readValue(responseLine, ServerResponse.class);

                if ("OK".equals(response.status())) {
                    @SuppressWarnings("unchecked")
                    List<Node> nodes = objectMapper.convertValue(
                        response.data(),
                        objectMapper.getTypeFactory().constructCollectionType(List.class, Node.class)
                    );

                    // Add all nodes to local cluster
                    for (Node node : nodes) {
                        if (!node.nodeId().equals(nodeId)) {
                            clusterManager.joinNode(node);
                        }
                    }

                    logger.info("Cluster synchronized with {} nodes", nodes.size());
                }
            } else {
                logger.error("Failed to join cluster: {}", response.error());
            }
        }
    }

    /**
     * Get the cluster manager.
     */
    public ClusterManager getClusterManager() {
        return clusterManager;
    }

    /**
     * Get the distributed queue.
     */
    public DistributedQueue getQueue() {
        return queue;
    }

    /**
     * Get server info.
     */
    public ServerInfo getInfo() {
        return new ServerInfo(
            nodeId,
            port,
            clusterManager.getClusterSize(),
            clusterManager.isLeader(),
            queue.getAllPartitionInfo().size()
        );
    }

    @Override
    public void close() {
        if (running.compareAndSet(true, false)) {
            logger.info("Shutting down queue server...");

            try {
                serverSocket.close();
            } catch (IOException e) {
                logger.error("Error closing server socket", e);
            }

            virtualThreadExecutor.shutdown();
            clusterManager.close();
            queue.close();

            logger.info("Queue server shut down successfully");
        }
    }

    /**
     * Server request record.
     */
    public record ServerRequest(String command, Object payload) {}

    /**
     * Server response record.
     */
    public record ServerResponse(String status, Object data, String error) {}

    /**
     * Server info record.
     */
    public record ServerInfo(
        String nodeId,
        int port,
        int clusterSize,
        boolean isLeader,
        int partitionCount
    ) {}
}
