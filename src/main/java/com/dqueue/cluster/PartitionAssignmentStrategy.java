package com.dqueue.cluster;

import java.util.List;
import java.util.Map;

/**
 * Strategy for assigning partitions to nodes.
 */
public interface PartitionAssignmentStrategy {
    /**
     * Assign partitions to nodes.
     * @param nodes List of active nodes
     * @param totalPartitions Total number of partitions
     * @return Map of node ID to list of assigned partition IDs
     */
    Map<String, List<Integer>> assign(List<Node> nodes, int totalPartitions);
}
