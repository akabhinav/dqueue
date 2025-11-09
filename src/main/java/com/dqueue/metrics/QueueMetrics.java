package com.dqueue.metrics;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Metrics collection for the distributed queue.
 * Thread-safe counters for monitoring queue performance.
 */
public class QueueMetrics {
    // Message counters
    private final LongAdder messagesProduced = new LongAdder();
    private final LongAdder messagesConsumed = new LongAdder();
    private final LongAdder messagesFailed = new LongAdder();
    private final LongAdder messagesExpired = new LongAdder();

    // Byte counters
    private final LongAdder bytesProduced = new LongAdder();
    private final LongAdder bytesConsumed = new LongAdder();

    // Latency tracking
    private final AtomicLong totalProduceLatencyMs = new AtomicLong(0);
    private final AtomicLong totalConsumeLatencyMs = new AtomicLong(0);
    private final LongAdder produceOperations = new LongAdder();
    private final LongAdder consumeOperations = new LongAdder();

    // Current state
    private final AtomicLong currentQueueSize = new AtomicLong(0);
    private final AtomicLong peakQueueSize = new AtomicLong(0);

    /**
     * Record a produced message.
     */
    public void recordProduced(int messageSize, long latencyMs) {
        messagesProduced.increment();
        bytesProduced.add(messageSize);
        totalProduceLatencyMs.addAndGet(latencyMs);
        produceOperations.increment();

        long newSize = currentQueueSize.incrementAndGet();
        updatePeakQueueSize(newSize);
    }

    /**
     * Record a consumed message.
     */
    public void recordConsumed(int messageSize, long latencyMs) {
        messagesConsumed.increment();
        bytesConsumed.add(messageSize);
        totalConsumeLatencyMs.addAndGet(latencyMs);
        consumeOperations.increment();
        currentQueueSize.decrementAndGet();
    }

    /**
     * Record a failed message.
     */
    public void recordFailed() {
        messagesFailed.increment();
    }

    /**
     * Record expired messages.
     */
    public void recordExpired(int count) {
        messagesExpired.add(count);
    }

    /**
     * Get total messages produced.
     */
    public long getMessagesProduced() {
        return messagesProduced.sum();
    }

    /**
     * Get total messages consumed.
     */
    public long getMessagesConsumed() {
        return messagesConsumed.sum();
    }

    /**
     * Get total messages failed.
     */
    public long getMessagesFailed() {
        return messagesFailed.sum();
    }

    /**
     * Get total messages expired.
     */
    public long getMessagesExpired() {
        return messagesExpired.sum();
    }

    /**
     * Get total bytes produced.
     */
    public long getBytesProduced() {
        return bytesProduced.sum();
    }

    /**
     * Get total bytes consumed.
     */
    public long getBytesConsumed() {
        return bytesConsumed.sum();
    }

    /**
     * Get average produce latency in milliseconds.
     */
    public double getAverageProduceLatency() {
        long ops = produceOperations.sum();
        return ops > 0 ? (double) totalProduceLatencyMs.get() / ops : 0.0;
    }

    /**
     * Get average consume latency in milliseconds.
     */
    public double getAverageConsumeLatency() {
        long ops = consumeOperations.sum();
        return ops > 0 ? (double) totalConsumeLatencyMs.get() / ops : 0.0;
    }

    /**
     * Get current queue size.
     */
    public long getCurrentQueueSize() {
        return Math.max(0, currentQueueSize.get());
    }

    /**
     * Get peak queue size.
     */
    public long getPeakQueueSize() {
        return peakQueueSize.get();
    }

    /**
     * Get throughput in messages per second.
     */
    public double getThroughput(long durationMs) {
        if (durationMs <= 0) return 0.0;
        long totalMessages = messagesProduced.sum() + messagesConsumed.sum();
        return (double) totalMessages / durationMs * 1000.0;
    }

    /**
     * Update peak queue size.
     */
    private void updatePeakQueueSize(long newSize) {
        long currentPeak = peakQueueSize.get();
        while (newSize > currentPeak) {
            if (peakQueueSize.compareAndSet(currentPeak, newSize)) {
                break;
            }
            currentPeak = peakQueueSize.get();
        }
    }

    /**
     * Reset all metrics.
     */
    public void reset() {
        messagesProduced.reset();
        messagesConsumed.reset();
        messagesFailed.reset();
        messagesExpired.reset();
        bytesProduced.reset();
        bytesConsumed.reset();
        totalProduceLatencyMs.set(0);
        totalConsumeLatencyMs.set(0);
        produceOperations.reset();
        consumeOperations.reset();
        currentQueueSize.set(0);
        peakQueueSize.set(0);
    }

    /**
     * Get a snapshot of all metrics.
     */
    public MetricsSnapshot snapshot() {
        return new MetricsSnapshot(
            getMessagesProduced(),
            getMessagesConsumed(),
            getMessagesFailed(),
            getMessagesExpired(),
            getBytesProduced(),
            getBytesConsumed(),
            getAverageProduceLatency(),
            getAverageConsumeLatency(),
            getCurrentQueueSize(),
            getPeakQueueSize()
        );
    }

    /**
     * Immutable snapshot of metrics.
     */
    public record MetricsSnapshot(
        long messagesProduced,
        long messagesConsumed,
        long messagesFailed,
        long messagesExpired,
        long bytesProduced,
        long bytesConsumed,
        double avgProduceLatencyMs,
        double avgConsumeLatencyMs,
        long currentQueueSize,
        long peakQueueSize
    ) {
        @Override
        public String toString() {
            return String.format("""
                Queue Metrics:
                  Messages: produced=%d, consumed=%d, failed=%d, expired=%d
                  Bytes: produced=%d, consumed=%d
                  Latency: produce=%.2fms, consume=%.2fms
                  Queue: current=%d, peak=%d
                """,
                messagesProduced, messagesConsumed, messagesFailed, messagesExpired,
                bytesProduced, bytesConsumed,
                avgProduceLatencyMs, avgConsumeLatencyMs,
                currentQueueSize, peakQueueSize
            );
        }
    }
}
