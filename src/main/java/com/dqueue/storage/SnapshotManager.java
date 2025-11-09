package com.dqueue.storage;

import com.dqueue.core.Message;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Manages snapshots of queue state for faster recovery.
 */
public class SnapshotManager {
    private static final Logger logger = LoggerFactory.getLogger(SnapshotManager.class);
    private static final String SNAPSHOT_PREFIX = "snapshot-";
    private static final String SNAPSHOT_SUFFIX = ".json.gz";

    private final Path snapshotDirectory;
    private final ObjectMapper objectMapper;

    public SnapshotManager(String baseDirectory) throws IOException {
        this.snapshotDirectory = Paths.get(baseDirectory, "snapshots");
        this.objectMapper = new ObjectMapper();

        // Create directory if it doesn't exist
        Files.createDirectories(snapshotDirectory);

        logger.info("Snapshot manager initialized at {}", snapshotDirectory);
    }

    /**
     * Create a snapshot of partition state.
     */
    public void createSnapshot(int partitionId, long offset, List<Message> messages) throws IOException {
        Path snapshotFile = getSnapshotFilePath(partitionId, offset);

        Snapshot snapshot = new Snapshot(partitionId, offset, System.currentTimeMillis(), messages);

        try (OutputStream fos = Files.newOutputStream(snapshotFile);
             GZIPOutputStream gzos = new GZIPOutputStream(fos)) {

            objectMapper.writeValue(gzos, snapshot);
        }

        logger.info("Created snapshot for partition {} at offset {} with {} messages",
            partitionId, offset, messages.size());

        // Clean up old snapshots
        cleanupOldSnapshots(partitionId, 5);
    }

    /**
     * Load the latest snapshot for a partition.
     */
    public Optional<Snapshot> loadLatestSnapshot(int partitionId) throws IOException {
        List<Path> snapshots = findSnapshots(partitionId);

        if (snapshots.isEmpty()) {
            return Optional.empty();
        }

        // Get the latest snapshot (by offset)
        Path latestSnapshot = snapshots.get(snapshots.size() - 1);

        try (InputStream fis = Files.newInputStream(latestSnapshot);
             GZIPInputStream gzis = new GZIPInputStream(fis)) {

            Snapshot snapshot = objectMapper.readValue(gzis, Snapshot.class);
            logger.info("Loaded snapshot for partition {} at offset {} with {} messages",
                partitionId, snapshot.offset(), snapshot.messages().size());

            return Optional.of(snapshot);
        }
    }

    /**
     * Find all snapshots for a partition, sorted by offset.
     */
    private List<Path> findSnapshots(int partitionId) throws IOException {
        String prefix = SNAPSHOT_PREFIX + partitionId + "-";

        List<Path> snapshots = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(snapshotDirectory,
                prefix + "*" + SNAPSHOT_SUFFIX)) {
            for (Path entry : stream) {
                snapshots.add(entry);
            }
        }

        // Sort by offset (extracted from filename)
        snapshots.sort(Comparator.comparing(this::extractOffsetFromFilename));

        return snapshots;
    }

    /**
     * Clean up old snapshots, keeping only the most recent N.
     */
    private void cleanupOldSnapshots(int partitionId, int keepCount) throws IOException {
        List<Path> snapshots = findSnapshots(partitionId);

        if (snapshots.size() > keepCount) {
            int deleteCount = snapshots.size() - keepCount;
            for (int i = 0; i < deleteCount; i++) {
                Files.delete(snapshots.get(i));
                logger.debug("Deleted old snapshot: {}", snapshots.get(i).getFileName());
            }
        }
    }

    /**
     * Get the path to a snapshot file.
     */
    private Path getSnapshotFilePath(int partitionId, long offset) {
        String filename = SNAPSHOT_PREFIX + partitionId + "-" + offset + SNAPSHOT_SUFFIX;
        return snapshotDirectory.resolve(filename);
    }

    /**
     * Extract offset from snapshot filename.
     */
    private long extractOffsetFromFilename(Path path) {
        String filename = path.getFileName().toString();
        String offsetStr = filename
            .replace(SNAPSHOT_PREFIX, "")
            .replace(SNAPSHOT_SUFFIX, "")
            .split("-")[1];
        return Long.parseLong(offsetStr);
    }

    /**
     * Snapshot record.
     */
    public record Snapshot(
        int partitionId,
        long offset,
        long timestamp,
        List<Message> messages
    ) {}
}
