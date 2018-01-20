package com.github.mstawowiak.persistent.queue.consumer;

import com.github.mstawowiak.persistent.queue.BerkeleyDbQueue;
import com.github.mstawowiak.persistent.queue.Queue;
import com.github.mstawowiak.persistent.queue.data.DoNothingSimplePayloadConsumer;
import com.github.mstawowiak.persistent.queue.data.SimplePayload;
import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import org.apache.commons.io.FileUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * Tests for {@link BlockingQueueUnloader}
 */
public class BlockingQueueUnloaderTest {

    private static final String queueName = BlockingQueueUnloaderTest.class.getSimpleName();
    private static final String queueDirName = "build/" + queueName;
    private static final File queueDir = new File(queueDirName);

    private Queue<SimplePayload> berkeleyDbQueue() {
        return new BerkeleyDbQueue<>(queueDirName, queueName);
    }

    @BeforeClass
    public static void beforeClass() throws IOException {
        if (queueDir.exists()) {
            FileUtils.forceDelete(queueDir);
        }
    }

    @AfterClass
    public static void afterClass() throws IOException {
        if (queueDir.exists()) {
            FileUtils.forceDeleteOnExit(queueDir);
        }
    }

    @Test
    public void shouldUnloadAllPayloads() throws InterruptedException {
        Queue<SimplePayload> queue = berkeleyDbQueue();

        QueueUnloader unloader = new BlockingQueueUnloader<>(queue,
                new QueueUnloaderConfig.Builder<SimplePayload>()
                        .consumer(new DoNothingSimplePayloadConsumer())
                        .build());

        assertEquals(queue.size(), 0);
        for (int i = 0; i < 100; i++) {
            queue.push(new SimplePayload("test" + i, i, BigInteger.valueOf(i)));
        }
        assertEquals(queue.size(), 100);

        unloader.start();

        while (!queue.isEmpty()) {
            Thread.sleep(1000);
        }

        assertEquals(queue.size(), 0);

        unloader.stop();
    }

}
