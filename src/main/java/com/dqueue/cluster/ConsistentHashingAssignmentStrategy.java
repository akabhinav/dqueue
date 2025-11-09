package com.dqueue.cluster;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

/**
 * Consistent hashing-based partition assignment strategy.
 */
public class ConsistentHashingAssignmentStrategy implements PartitionAssignmentStrategy {
    private static final int VIRTUAL_NODES = 150; // Virtual nodes per physical node

    @Override
    public Map<String, List<Integer>> assign(List<Node> nodes, int totalPartitions) {
        if (nodes.isEmpty()) {
            return Map.of();
        }

        // Build consistent hash ring
        TreeMap<Long, String> ring = new TreeMap<>();
        MessageDigest md5;
        try {
            md5 = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("MD5 not available", e);
        }

        // Add virtual nodes to the ring
        for (Node node : nodes) {
            for (int i = 0; i < VIRTUAL_NODES; i++) {
                String virtualNodeKey = node.nodeId() + "#" + i;
                long hash = hash(md5, virtualNodeKey);
                ring.put(hash, node.nodeId());
            }
        }

        // Assign partitions to nodes
        Map<String, List<Integer>> assignments = new HashMap<>();
        for (Node node : nodes) {
            assignments.put(node.nodeId(), new ArrayList<>());
        }

        for (int partition = 0; partition < totalPartitions; partition++) {
            String partitionKey = "partition-" + partition;
            long hash = hash(md5, partitionKey);

            // Find the node responsible for this partition
            Map.Entry<Long, String> entry = ring.ceilingEntry(hash);
            if (entry == null) {
                entry = ring.firstEntry();
            }

            String assignedNode = entry.getValue();
            assignments.get(assignedNode).add(partition);
        }

        return assignments;
    }

    private long hash(MessageDigest md5, String key) {
        byte[] digest = md5.digest(key.getBytes(StandardCharsets.UTF_8));
        long hash = 0;
        for (int i = 0; i < 8; i++) {
            hash = (hash << 8) | (digest[i] & 0xFF);
        }
        return hash;
    }
}
