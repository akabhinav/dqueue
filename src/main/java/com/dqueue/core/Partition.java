package com.dqueue.core;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A partition of the distributed queue.
 * Each partition maintains its own message queue and offset tracking.
 */
public class Partition {
    private final int partitionId;
    private final ConcurrentSkipListSet<MessageEntry> messages;
    private final AtomicLong offset;
    private final AtomicLong messagesCount;
    private final ReadWriteLock lock;
    private final QueueConfig config;

    /**
     * Internal wrapper for messages with offset.
     */
    private record MessageEntry(long offset, Message message) implements Comparable<MessageEntry> {
        @Override
        public int compareTo(MessageEntry other) {
            // First compare by message priority and timestamp
            int msgCompare = message.compareTo(other.message);
            if (msgCompare != 0) {
                return msgCompare;
            }
            // Then by offset to ensure uniqueness
            return Long.compare(this.offset, other.offset);
        }
    }

    public Partition(int partitionId, QueueConfig config) {
        this.partitionId = partitionId;
        this.messages = new ConcurrentSkipListSet<>();
        this.offset = new AtomicLong(0);
        this.messagesCount = new AtomicLong(0);
        this.lock = new ReentrantReadWriteLock();
        this.config = config;
    }

    /**
     * Add a message to the partition.
     * Returns the offset assigned to the message.
     */
    public long append(Message message) {
        lock.writeLock().lock();
        try {
            // Check if message has expired
            if (message.isExpired()) {
                throw new IllegalArgumentException("Message has expired");
            }

            long messageOffset = offset.incrementAndGet();
            MessageEntry entry = new MessageEntry(messageOffset, message);
            messages.add(entry);
            messagesCount.incrementAndGet();
            return messageOffset;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Poll messages from the partition starting from the given offset.
     * Returns up to maxMessages.
     */
    public List<Message> poll(long fromOffset, int maxMessages) {
        lock.readLock().lock();
        try {
            List<Message> result = new ArrayList<>();
            int count = 0;

            for (MessageEntry entry : messages) {
                if (entry.offset > fromOffset && count < maxMessages) {
                    // Skip expired messages
                    if (!entry.message.isExpired()) {
                        result.add(entry.message);
                        count++;
                    }
                }
                if (count >= maxMessages) {
                    break;
                }
            }

            return result;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Acknowledge messages up to the given offset (remove from queue).
     */
    public void acknowledge(long upToOffset) {
        lock.writeLock().lock();
        try {
            messages.removeIf(entry -> entry.offset <= upToOffset);
            messagesCount.set(messages.size());
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Clean up expired messages.
     */
    public int cleanupExpired() {
        lock.writeLock().lock();
        try {
            int removedCount = 0;
            var iterator = messages.iterator();
            while (iterator.hasNext()) {
                MessageEntry entry = iterator.next();
                if (entry.message.isExpired()) {
                    iterator.remove();
                    removedCount++;
                }
            }
            messagesCount.set(messages.size());
            return removedCount;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Get the current offset (last assigned offset).
     */
    public long getCurrentOffset() {
        return offset.get();
    }

    /**
     * Get the number of messages in the partition.
     */
    public long getMessageCount() {
        return messagesCount.get();
    }

    /**
     * Get the partition ID.
     */
    public int getPartitionId() {
        return partitionId;
    }

    /**
     * Clear all messages from the partition.
     */
    public void clear() {
        lock.writeLock().lock();
        try {
            messages.clear();
            messagesCount.set(0);
        } finally {
            lock.writeLock().unlock();
        }
    }
}
