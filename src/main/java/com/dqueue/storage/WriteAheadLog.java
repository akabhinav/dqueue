package com.dqueue.storage;

import com.dqueue.core.Message;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Write-Ahead Log (WAL) for message persistence.
 * Ensures durability by writing messages to disk before acknowledging.
 */
public class WriteAheadLog implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(WriteAheadLog.class);
    private static final String WAL_FILE_PREFIX = "wal-";
    private static final String WAL_FILE_SUFFIX = ".log";

    private final Path walDirectory;
    private final int partitionId;
    private final ObjectMapper objectMapper;
    private final ReadWriteLock lock;
    private BufferedWriter writer;
    private long currentOffset;

    public WriteAheadLog(String baseDirectory, int partitionId) throws IOException {
        this.partitionId = partitionId;
        this.walDirectory = Paths.get(baseDirectory, "partition-" + partitionId);
        this.objectMapper = new ObjectMapper();
        this.lock = new ReentrantReadWriteLock();
        this.currentOffset = 0;

        // Create directory if it doesn't exist
        Files.createDirectories(walDirectory);

        // Open or create WAL file
        Path walFile = getWalFilePath();
        this.writer = Files.newBufferedWriter(
            walFile,
            StandardOpenOption.CREATE,
            StandardOpenOption.APPEND
        );

        // Recover offset from existing log
        recoverOffset();

        logger.info("WAL initialized for partition {} at {}", partitionId, walDirectory);
    }

    /**
     * Append a message to the WAL.
     */
    public void append(long offset, Message message) throws IOException {
        lock.writeLock().lock();
        try {
            WalEntry entry = new WalEntry(offset, message);
            String json = objectMapper.writeValueAsString(entry);

            writer.write(json);
            writer.newLine();
            writer.flush(); // Ensure durability

            currentOffset = offset;

            logger.debug("Appended message {} at offset {} to WAL", message.id(), offset);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Read all entries from the WAL.
     */
    public List<WalEntry> readAll() throws IOException {
        lock.readLock().lock();
        try {
            List<WalEntry> entries = new ArrayList<>();
            Path walFile = getWalFilePath();

            if (!Files.exists(walFile)) {
                return entries;
            }

            try (BufferedReader reader = Files.newBufferedReader(walFile)) {
                String line;
                while ((line = reader.readLine()) != null) {
                    if (!line.trim().isEmpty()) {
                        WalEntry entry = objectMapper.readValue(line, WalEntry.class);
                        entries.add(entry);
                    }
                }
            }

            logger.info("Read {} entries from WAL", entries.size());
            return entries;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Compact the WAL by removing entries before the given offset.
     */
    public void compact(long beforeOffset) throws IOException {
        lock.writeLock().lock();
        try {
            List<WalEntry> entries = readAll();
            Path walFile = getWalFilePath();
            Path tempFile = walDirectory.resolve("wal-temp.log");

            // Write entries after the offset to temp file
            try (BufferedWriter tempWriter = Files.newBufferedWriter(tempFile)) {
                for (WalEntry entry : entries) {
                    if (entry.offset() >= beforeOffset) {
                        String json = objectMapper.writeValueAsString(entry);
                        tempWriter.write(json);
                        tempWriter.newLine();
                    }
                }
            }

            // Replace old WAL with compacted version
            writer.close();
            Files.delete(walFile);
            Files.move(tempFile, walFile);

            // Reopen writer
            writer = Files.newBufferedWriter(
                walFile,
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND
            );

            logger.info("Compacted WAL, removed entries before offset {}", beforeOffset);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Recover the last offset from the WAL.
     */
    private void recoverOffset() throws IOException {
        List<WalEntry> entries = readAll();
        if (!entries.isEmpty()) {
            currentOffset = entries.get(entries.size() - 1).offset();
            logger.info("Recovered offset {} from WAL", currentOffset);
        }
    }

    /**
     * Get the path to the WAL file.
     */
    private Path getWalFilePath() {
        return walDirectory.resolve(WAL_FILE_PREFIX + partitionId + WAL_FILE_SUFFIX);
    }

    /**
     * Get the current offset.
     */
    public long getCurrentOffset() {
        return currentOffset;
    }

    @Override
    public void close() throws IOException {
        lock.writeLock().lock();
        try {
            if (writer != null) {
                writer.close();
            }
            logger.info("WAL closed for partition {}", partitionId);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * WAL entry record.
     */
    public record WalEntry(long offset, Message message) {}
}
