package com.github.mstawowiak.persistent.queue.consumer;

import com.github.mstawowiak.persistent.queue.Payload;
import com.github.mstawowiak.persistent.queue.strategy.WaitStrategy;
import com.github.mstawowiak.persistent.queue.strategy.WaitStrategyFactory;
import com.github.mstawowiak.persistent.queue.util.Preconditions;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public class QueueUnloaderConfig<P extends Payload> {

    private final int numOfThreads;
    private final ThreadFactory threadFactory;
    private final WaitStrategy waitStrategy;
    private final Consumer<P> consumer;

    private QueueUnloaderConfig(Builder<P> builder) {
        this.numOfThreads = builder.numOfThreads;
        this.threadFactory = builder.threadFactory;
        this.waitStrategy = builder.waitStrategy;
        this.consumer = builder.consumer;
    }

    public static class Builder<P extends Payload> {

        private int numOfThreads = 1;
        private ThreadFactory threadFactory = Executors.defaultThreadFactory();
        private WaitStrategy waitStrategy = defaultWaitStrategy();
        private Consumer<P> consumer;

        public Builder<P> numOfThreads(int numOfThreads) {
            this.numOfThreads = numOfThreads;
            return this;
        }

        public Builder<P> threadFactory(ThreadFactory threadFactory) {
            this.threadFactory = threadFactory;
            return this;
        }

        public Builder<P> waitStrategy(WaitStrategy waitStrategy) {
            this.waitStrategy = waitStrategy;
            return this;
        }

        public Builder<P> consumer(Consumer<P> consumer) {
            this.consumer = consumer;
            return this;
        }

        private WaitStrategy defaultWaitStrategy() {
            return WaitStrategyFactory.incrementingWait(
                    5, TimeUnit.MILLISECONDS,
                    100, TimeUnit.MILLISECONDS,
                    5, TimeUnit.SECONDS);
        }

        @SuppressWarnings("PMD.AccessorClassGeneration")
        public QueueUnloaderConfig<P> build() {
            Preconditions.checkArgument(consumer != null, "Consumer may not be null");

            return new QueueUnloaderConfig<>(this);
        }
    }

    public int getNumOfThreads() {
        return numOfThreads;
    }

    public ThreadFactory getThreadFactory() {
        return threadFactory;
    }

    public WaitStrategy getWaitStrategy() {
        return waitStrategy;
    }

    public Consumer<P> getConsumer() {
        return consumer;
    }
}
