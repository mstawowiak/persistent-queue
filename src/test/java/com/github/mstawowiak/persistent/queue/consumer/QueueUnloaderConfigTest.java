package com.github.mstawowiak.persistent.queue.consumer;

import com.github.mstawowiak.persistent.queue.data.DoNothingTestPayloadConsumer;
import com.github.mstawowiak.persistent.queue.data.SimplePayload;
import com.github.mstawowiak.persistent.queue.data.TestPayload;
import com.github.mstawowiak.persistent.queue.strategy.WaitStrategyFactory;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

/**
 * Tests for {@link QueueUnloaderConfig}
 */
public class QueueUnloaderConfigTest {

    @Test
    public void shouldBuildDefaultConfig() {
        QueueUnloaderConfig<TestPayload> config
                = new QueueUnloaderConfig.Builder<TestPayload>()
                .consumer(new DoNothingTestPayloadConsumer())
                .build();

        assertNotNull(config);
        assertEquals(1, config.getNumOfThreads());
        assertNotNull(config.getThreadFactory());
        assertNotNull(config.getConsumer());
        assertNotNull(config.getWaitStrategy());
    }

    @Test
    public void shouldBuildConfigWithAllParameters() {
        QueueUnloaderConfig<TestPayload> config
                = new QueueUnloaderConfig.Builder<TestPayload>()
                .numOfThreads(15)
                .threadFactory(new NamedThreadFactory("unit-test"))
                .consumer(new DoNothingTestPayloadConsumer())
                .waitStrategy(WaitStrategyFactory.exponentialWait(5, TimeUnit.MINUTES))
                .build();

        assertNotNull(config);
        assertEquals(15, config.getNumOfThreads());
        assertTrue(config.getThreadFactory() instanceof  NamedThreadFactory);
        assertTrue(config.getConsumer() instanceof DoNothingTestPayloadConsumer);
        assertNotNull(config.getWaitStrategy());
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenNoConsumer() {
        new QueueUnloaderConfig.Builder<SimplePayload>()
                .build();
    }

    private static class NamedThreadFactory implements ThreadFactory {

        private final AtomicInteger threadNumber = new AtomicInteger(1);
        private final String baseName;

        public NamedThreadFactory(String name) {
            this.baseName = name;
        }

        @Override
        public Thread newThread(Runnable run) {
            String name = baseName + "-" + threadNumber.getAndIncrement();

            return new Thread(run, name);
        }

    }
}
