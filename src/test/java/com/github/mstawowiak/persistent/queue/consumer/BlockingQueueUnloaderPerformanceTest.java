package com.github.mstawowiak.persistent.queue.consumer;

import com.github.mstawowiak.persistent.queue.BerkeleyDbQueue;
import com.github.mstawowiak.persistent.queue.Queue;
import com.github.mstawowiak.persistent.queue.data.DoNothingTestPayloadConsumer;
import com.github.mstawowiak.persistent.queue.data.SimplePayload;
import com.github.mstawowiak.persistent.queue.data.TestPayload;
import java.math.BigInteger;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

public class BlockingQueueUnloaderPerformanceTest {

    private static final Mode MODE = Mode.DEQUEUE;
    private static final int THREADS_COUNT = 10;
    private static final int BATCH_SIZE = 100;

    private enum Mode {ENQUEUE, DEQUEUE}
    private static final AtomicLong counter = new AtomicLong(0);
    private static volatile long timestamp;

    private static Queue<TestPayload> queue;

    public static void main(String[] args) throws InterruptedException {
        queue = berkeleyDbQueue();

        System.out.println("-----------------------------------------------------------");
        System.out.println("Start performance tests of persistent-queue");
        System.out.println(String.format("Mode: %s, Threads count: %d, Batch size: %d", MODE, THREADS_COUNT, BATCH_SIZE));
        System.out.println(String.format("Queue size: %,d", queue.size()));
        System.out.println("-----------------------------------------------------------");

        timestamp = System.currentTimeMillis();

        switch(MODE) {
            case ENQUEUE:
                enqueue();
                break;
            case DEQUEUE:
                dequeue();
                break;
        }
    }

    private static Queue<TestPayload> berkeleyDbQueue() {
        final String queueName = BlockingQueueUnloaderPerformanceTest.class.getSimpleName();
        final String queueDirName = "build/" + queueName;

        return new BerkeleyDbQueue<>(queueDirName, queueName, BATCH_SIZE);
    }

    private static QueueUnloader queueUnloader() {
        return new BlockingQueueUnloader<>(queue,
                new QueueUnloaderConfig.Builder<TestPayload>()
                        .numOfThreads(THREADS_COUNT)
                        .consumer(new DoNothingTestPayloadConsumer())
                        //.consumer(new RandomErrorTestPayloadConsumer())
                        .build());
    }

    private static void enqueue() {
        ExecutorService executor = Executors.newFixedThreadPool(THREADS_COUNT);

        executor.submit(() -> {
            while (true) {
                long numberOfEnqueued = counter.incrementAndGet();

                queue.push(new SimplePayload("t", (int) numberOfEnqueued, BigInteger.valueOf(numberOfEnqueued)));

                if (numberOfEnqueued % 10000 == 0) {
                    long tps = 10000 * 1000 / (System.currentTimeMillis() - timestamp);
                    System.out.println("TPS [enqueue]: " + tps);
                    timestamp = System.currentTimeMillis();
                }
                if (numberOfEnqueued % 100000 == 0) {
                    System.out.println("Queue size: " + queue.size());
                }
            }
        });
    }

    private static void dequeue() throws InterruptedException {
        QueueUnloader unloader = queueUnloader();
        unloader.start();
        Runtime.getRuntime().addShutdownHook(new Thread(unloader::stop));

        long lastQueueSize = queue.size();
        while (!queue.isEmpty()) {
            Thread.sleep(1000);
            System.out.println("Queue size: " + queue.size() + ", TPS [dequeue]: " + ((lastQueueSize - queue.size())));
            lastQueueSize = queue.size();
        }
    }

}