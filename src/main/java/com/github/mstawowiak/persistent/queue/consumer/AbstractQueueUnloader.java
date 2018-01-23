package com.github.mstawowiak.persistent.queue.consumer;

import com.github.mstawowiak.commons.logging.ContextLogger;
import com.github.mstawowiak.commons.logging.ContextLoggerFactory;
import com.github.mstawowiak.persistent.queue.Payload;
import com.github.mstawowiak.persistent.queue.Queue;
import com.github.mstawowiak.persistent.queue.strategy.WaitStrategy;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public abstract class AbstractQueueUnloader<P extends Payload> implements QueueUnloader {

    protected static final ContextLogger LOGGER = ContextLoggerFactory.getLogger();

    private final ExecutorService startThread;
    protected final ExecutorService consumerThreadPool;

    protected final Queue<P> queue;
    protected final Consumer<P> consumer;
    protected final WaitStrategy waitStrategy;

    public AbstractQueueUnloader(Queue<P> queue, QueueUnloaderConfig<P> config) {
        this.queue = queue;
        this.consumer = config.getConsumer();
        this.waitStrategy = config.getWaitStrategy();

        this.startThread = Executors.newSingleThreadExecutor(config.getThreadFactory());
        this.consumerThreadPool = Executors.newFixedThreadPool(config.getNumOfThreads(), config.getThreadFactory());
    }

    protected abstract void process();

    @Override
    public final void start() {
        LOGGER.info(queue.name(), "Queue unloading started [size={}]", queue.size());

        startThread.execute(() -> {
            // infinite loop
            while (!startThread.isShutdown()) {
                try {
                    queue.getSemaphore().acquire();
                    process();
                } catch (Exception ex) {
                    LOGGER.warn(queue.name(), "The start thread was interrupted", ex);
                }
            }
        });
    }

    /**
     * Initialization of stop procedure in order to finish executing already submitted tasks.
     * No new tasks will be accepted.
     */
    @Override
    public final void stop() {
        LOGGER.info(queue.name(), "Stop unloading nad closing queue");

        stop(10, TimeUnit.SECONDS);
        closeQueue();
    }

    private void stop(int timeout, TimeUnit timeUnit) {
        queue.getSemaphore().release();
        startThread.shutdown();
        try {
            if (!startThread.awaitTermination(timeout, timeUnit)) {
                LOGGER.info(queue.name(), "Timeout to stop 'startThread' has been exceeded");
                startThread.shutdownNow();
            }
        } catch (InterruptedException e) {
            LOGGER.warn(queue.name(), "Error occurred during waiting for "
                    + "'startThread' thread pool termination", e);
        }

        consumerThreadPool.shutdown();
        try {
            if (!consumerThreadPool.awaitTermination(timeout, TimeUnit.SECONDS)) {
                LOGGER.info(queue.name(), "Timeout to stop 'consumerThreadPool' has been exceeded");
                consumerThreadPool.shutdownNow();
            }
        } catch (InterruptedException e) {
            LOGGER.warn(queue.name(), "Error occurred during waiting for "
                    + "'consumerThreadPool' thread pool termination", e);
        }
    }

    private void closeQueue() {
        long countBeforeClose = queue.size();
        try {
            queue.close();
        } catch (IllegalStateException ex) {
            LOGGER.warn(queue.name(), "Error occurred during closing queue", ex);
            queue.close();
        } finally {
            LOGGER.info(queue.name(), "Queue closed [size={}]", countBeforeClose);
        }
    }

}
