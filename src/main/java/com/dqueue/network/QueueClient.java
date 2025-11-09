package com.dqueue.network;

import com.dqueue.cluster.Node;
import com.dqueue.core.DistributedQueue;
import com.dqueue.core.Message;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.Socket;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Client for connecting to remote queue servers.
 * Provides the same API as DistributedQueue but operates over the network.
 */
public class QueueClient implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(QueueClient.class);

    private final String host;
    private final int port;
    private final ObjectMapper objectMapper;
    private final ExecutorService executor;

    public QueueClient(String host, int port) {
        this.host = host;
        this.port = port;
        this.objectMapper = new ObjectMapper();
        this.executor = Executors.newVirtualThreadPerTaskExecutor();
    }

    /**
     * Send a request to the server and get response.
     */
    private CompletableFuture<QueueServer.ServerResponse> sendRequest(QueueServer.ServerRequest request) {
        return CompletableFuture.supplyAsync(() -> {
            try (Socket socket = new Socket(host, port);
                 BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                 PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {

                // Send request
                String requestJson = objectMapper.writeValueAsString(request);
                out.println(requestJson);

                // Read response
                String responseLine = in.readLine();
                QueueServer.ServerResponse response = objectMapper.readValue(
                    responseLine,
                    QueueServer.ServerResponse.class
                );

                if (!"OK".equals(response.status()) && !"PONG".equals(response.status())) {
                    throw new RuntimeException("Server error: " + response.error());
                }

                return response;
            } catch (IOException e) {
                throw new RuntimeException("Network error: " + e.getMessage(), e);
            }
        }, executor);
    }

    /**
     * Produce a message to the remote queue.
     */
    public CompletableFuture<Long> produce(Message message) {
        QueueServer.ServerRequest request = new QueueServer.ServerRequest("PRODUCE", message);
        return sendRequest(request).thenApply(response ->
            objectMapper.convertValue(response.data(), Long.class)
        );
    }

    /**
     * Consume messages from a specific partition.
     */
    public CompletableFuture<List<Message>> consume(int partitionId, long fromOffset, int maxMessages) {
        Map<String, Object> params = Map.of(
            "partitionId", partitionId,
            "fromOffset", fromOffset,
            "maxMessages", maxMessages
        );

        QueueServer.ServerRequest request = new QueueServer.ServerRequest("CONSUME", params);
        return sendRequest(request).thenApply(response ->
            objectMapper.convertValue(
                response.data(),
                objectMapper.getTypeFactory().constructCollectionType(List.class, Message.class)
            )
        );
    }

    /**
     * Consume messages from all partitions.
     */
    public CompletableFuture<List<Message>> consumeAll(long fromOffset, int maxMessages) {
        Map<String, Object> params = Map.of(
            "fromOffset", fromOffset,
            "maxMessages", maxMessages
        );

        QueueServer.ServerRequest request = new QueueServer.ServerRequest("CONSUME_ALL", params);
        return sendRequest(request).thenApply(response ->
            objectMapper.convertValue(
                response.data(),
                objectMapper.getTypeFactory().constructCollectionType(List.class, Message.class)
            )
        );
    }

    /**
     * Acknowledge messages up to offset.
     */
    public CompletableFuture<Void> acknowledge(int partitionId, long upToOffset) {
        Map<String, Object> params = Map.of(
            "partitionId", partitionId,
            "upToOffset", upToOffset
        );

        QueueServer.ServerRequest request = new QueueServer.ServerRequest("ACKNOWLEDGE", params);
        return sendRequest(request).thenApply(response -> null);
    }

    /**
     * Get partition info.
     */
    public CompletableFuture<DistributedQueue.PartitionInfo> getPartitionInfo(int partitionId) {
        QueueServer.ServerRequest request = new QueueServer.ServerRequest("PARTITION_INFO", partitionId);
        return sendRequest(request).thenApply(response ->
            objectMapper.convertValue(response.data(), DistributedQueue.PartitionInfo.class)
        );
    }

    /**
     * Get all partition info.
     */
    public CompletableFuture<List<DistributedQueue.PartitionInfo>> getAllPartitionInfo() {
        QueueServer.ServerRequest request = new QueueServer.ServerRequest("ALL_PARTITIONS_INFO", null);
        return sendRequest(request).thenApply(response ->
            objectMapper.convertValue(
                response.data(),
                objectMapper.getTypeFactory().constructCollectionType(
                    List.class,
                    DistributedQueue.PartitionInfo.class
                )
            )
        );
    }

    /**
     * Get cluster info.
     */
    public CompletableFuture<List<Node>> getClusterInfo() {
        QueueServer.ServerRequest request = new QueueServer.ServerRequest("CLUSTER_INFO", null);
        return sendRequest(request).thenApply(response ->
            objectMapper.convertValue(
                response.data(),
                objectMapper.getTypeFactory().constructCollectionType(List.class, Node.class)
            )
        );
    }

    /**
     * Send heartbeat for a node.
     */
    public CompletableFuture<Void> sendHeartbeat(String nodeId) {
        QueueServer.ServerRequest request = new QueueServer.ServerRequest("HEARTBEAT", nodeId);
        return sendRequest(request).thenApply(response -> null);
    }

    /**
     * Get partition assignments.
     */
    public CompletableFuture<Map<String, List<Integer>>> getPartitionAssignments(int totalPartitions) {
        QueueServer.ServerRequest request = new QueueServer.ServerRequest("PARTITION_ASSIGNMENTS", totalPartitions);
        return sendRequest(request).thenApply(response ->
            objectMapper.convertValue(
                response.data(),
                objectMapper.getTypeFactory().constructMapType(
                    Map.class,
                    String.class,
                    objectMapper.getTypeFactory().constructCollectionType(List.class, Integer.class)
                )
            )
        );
    }

    /**
     * Ping the server.
     */
    public CompletableFuture<String> ping() {
        QueueServer.ServerRequest request = new QueueServer.ServerRequest("PING", null);
        return sendRequest(request).thenApply(response ->
            objectMapper.convertValue(response.data(), String.class)
        );
    }

    @Override
    public void close() {
        executor.shutdown();
        logger.info("Queue client closed");
    }
}
