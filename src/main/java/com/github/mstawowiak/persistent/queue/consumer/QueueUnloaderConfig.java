package com.github.mstawowiak.persistent.queue.consumer;

import com.github.mstawowiak.persistent.queue.Payload;
import com.github.mstawowiak.persistent.queue.util.Preconditions;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public class QueueUnloaderConfig<P extends Payload> {

    private final int numOfThreads;
    private final ThreadFactory threadFactory;
    private final Consumer<P> consumer;

    private QueueUnloaderConfig(Builder<P> builder) {
        this.numOfThreads = builder.numOfThreads;
        this.threadFactory = builder.threadFactory;
        this.consumer = builder.consumer;
    }

    public static class Builder<P extends Payload> {

        private int numOfThreads = 1;
        private ThreadFactory threadFactory = Executors.defaultThreadFactory();
        private Consumer<P> consumer;

        public Builder<P> numOfThreads(int numOfThreads) {
            this.numOfThreads = numOfThreads;
            return this;
        }

        public Builder<P> threadFactory(ThreadFactory threadFactory) {
            this.threadFactory = threadFactory;
            return this;
        }

        public Builder<P> consumer(Consumer<P> consumer) {
            this.consumer = consumer;
            return this;
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

    public Consumer<P> getConsumer() {
        return consumer;
    }
}
