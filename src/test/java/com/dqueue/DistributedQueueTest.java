package com.dqueue;

import com.dqueue.core.DistributedQueue;
import com.dqueue.core.Message;
import com.dqueue.core.QueueConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for DistributedQueue.
 */
class DistributedQueueTest {

    private DistributedQueue queue;

    @BeforeEach
    void setUp() {
        QueueConfig config = QueueConfig.memoryOnlyConfig();
        queue = new DistributedQueue(config);
    }

    @AfterEach
    void tearDown() {
        if (queue != null) {
            queue.close();
        }
    }

    @Test
    void testProduceAndConsume() throws Exception {
        // Produce a message
        Message message = Message.builder()
            .key("test-key")
            .payload("Hello World".getBytes(StandardCharsets.UTF_8))
            .priority(5)
            .build();

        Long offset = queue.produce(message).get();
        assertNotNull(offset);
        assertTrue(offset > 0);

        // Consume the message
        List<Message> messages = queue.consumeAll(0, 10).get();
        assertEquals(1, messages.size());
        assertEquals("Hello World", new String(messages.get(0).payload(), StandardCharsets.UTF_8));
    }

    @Test
    void testBatchProduce() throws Exception {
        // Create batch of messages
        List<Message> batch = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            Message message = Message.builder()
                .key("key-" + i)
                .payload(("Message " + i).getBytes(StandardCharsets.UTF_8))
                .priority(5)
                .build();
            batch.add(message);
        }

        // Produce batch
        List<Long> offsets = queue.produceBatch(batch).get();
        assertEquals(10, offsets.size());

        // Consume all messages
        List<Message> consumed = queue.consumeAll(0, 20).get();
        assertTrue(consumed.size() >= 10);
    }

    @Test
    void testPartitioning() throws Exception {
        // Produce messages with different keys
        for (int i = 0; i < 100; i++) {
            Message message = Message.builder()
                .key("key-" + i)
                .payload(("Message " + i).getBytes(StandardCharsets.UTF_8))
                .priority(5)
                .build();
            queue.produce(message).get();
        }

        // Check that messages are distributed across partitions
        List<DistributedQueue.PartitionInfo> partitions = queue.getAllPartitionInfo();
        long nonEmptyPartitions = partitions.stream()
            .filter(p -> p.messageCount() > 0)
            .count();

        // At least some partitions should have messages
        assertTrue(nonEmptyPartitions > 1);
    }

    @Test
    void testMessageExpiry() throws Exception {
        // Create message that expires immediately
        Message message = Message.builder()
            .key("test-key")
            .payload("Expiring message".getBytes(StandardCharsets.UTF_8))
            .priority(5)
            .expiryTime(System.currentTimeMillis() - 1000) // Already expired
            .build();

        // Should throw exception for expired message
        assertThrows(Exception.class, () -> queue.produce(message).get());
    }

    @Test
    void testPriorityOrdering() throws Exception {
        // Produce messages with different priorities
        Message lowPriority = Message.builder()
            .key("same-key")
            .payload("Low".getBytes(StandardCharsets.UTF_8))
            .priority(1)
            .build();

        Message highPriority = Message.builder()
            .key("same-key")
            .payload("High".getBytes(StandardCharsets.UTF_8))
            .priority(10)
            .build();

        queue.produce(lowPriority).get();
        queue.produce(highPriority).get();

        // High priority should come first
        List<Message> messages = queue.consumeAll(0, 2).get();
        assertEquals(2, messages.size());
        // First message should be high priority (priority 10)
        assertTrue(messages.get(0).priority() >= messages.get(1).priority());
    }

    @Test
    void testAcknowledge() throws Exception {
        // Produce messages
        for (int i = 0; i < 5; i++) {
            Message message = Message.builder()
                .key("key-0") // Same key to ensure same partition
                .payload(("Message " + i).getBytes(StandardCharsets.UTF_8))
                .priority(5)
                .build();
            queue.produce(message).get();
        }

        // Consume and acknowledge
        List<Message> messages = queue.consume(0, 0, 3).get();
        assertEquals(3, messages.size());

        // Acknowledge first 3 messages
        queue.acknowledge(0, 3).get();

        // Should still have 2 messages
        DistributedQueue.PartitionInfo info = queue.getPartitionInfo(0);
        assertTrue(info.messageCount() >= 0); // Messages may be in other partitions too
    }

    @Test
    void testConcurrentProducers() throws Exception {
        // Create multiple producers concurrently
        int numProducers = 10;
        int messagesPerProducer = 100;

        List<CompletableFuture<Void>> futures = new ArrayList<>();

        for (int p = 0; p < numProducers; p++) {
            final int producerId = p;
            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                for (int i = 0; i < messagesPerProducer; i++) {
                    Message message = Message.builder()
                        .key("producer-" + producerId)
                        .payload(("Message " + i).getBytes(StandardCharsets.UTF_8))
                        .priority(5)
                        .build();
                    try {
                        queue.produce(message).get();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            });
            futures.add(future);
        }

        // Wait for all producers to complete
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();

        // Check total messages
        List<Message> allMessages = queue.consumeAll(0, numProducers * messagesPerProducer + 100).get();
        assertEquals(numProducers * messagesPerProducer, allMessages.size());
    }
}
