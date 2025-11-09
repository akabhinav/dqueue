package com.dqueue.metrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Collects and reports metrics periodically.
 */
public class MetricsCollector implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(MetricsCollector.class);

    private final QueueMetrics metrics;
    private final ScheduledExecutorService scheduler;
    private final long reportIntervalSeconds;
    private final long startTime;

    public MetricsCollector(QueueMetrics metrics, long reportIntervalSeconds) {
        this.metrics = metrics;
        this.reportIntervalSeconds = reportIntervalSeconds;
        this.startTime = System.currentTimeMillis();
        this.scheduler = Executors.newScheduledThreadPool(1);

        startReporting();
    }

    /**
     * Start periodic metrics reporting.
     */
    private void startReporting() {
        scheduler.scheduleAtFixedRate(() -> {
            try {
                report();
            } catch (Exception e) {
                logger.error("Error reporting metrics", e);
            }
        }, reportIntervalSeconds, reportIntervalSeconds, TimeUnit.SECONDS);

        logger.info("Metrics reporting started (interval: {}s)", reportIntervalSeconds);
    }

    /**
     * Report current metrics.
     */
    public void report() {
        QueueMetrics.MetricsSnapshot snapshot = metrics.snapshot();
        long uptime = System.currentTimeMillis() - startTime;
        double uptimeSeconds = uptime / 1000.0;

        logger.info("\n" + "=".repeat(60));
        logger.info("QUEUE METRICS REPORT (uptime: {:.2f}s)", uptimeSeconds);
        logger.info("=".repeat(60));
        logger.info(snapshot.toString());
        logger.info("Throughput: {:.2f} msg/s", calculateThroughput(snapshot, uptime));
        logger.info("=".repeat(60));
    }

    /**
     * Calculate overall throughput.
     */
    private double calculateThroughput(QueueMetrics.MetricsSnapshot snapshot, long uptimeMs) {
        if (uptimeMs <= 0) return 0.0;
        long totalMessages = snapshot.messagesProduced() + snapshot.messagesConsumed();
        return (double) totalMessages / uptimeMs * 1000.0;
    }

    /**
     * Get the metrics instance.
     */
    public QueueMetrics getMetrics() {
        return metrics;
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

        // Final report
        logger.info("Final metrics report:");
        report();

        logger.info("Metrics collector shut down");
    }
}
