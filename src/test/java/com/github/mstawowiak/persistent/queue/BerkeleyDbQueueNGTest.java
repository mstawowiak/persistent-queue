package com.github.mstawowiak.persistent.queue;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.util.NoSuchElementException;
import org.apache.commons.io.FileUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

/**
 * Tests for {@link BerkeleyDbQueue}
 */
public class BerkeleyDbQueueNGTest {

    private Queue<SimplePayload> queue;
    private String queueName = "unit_tests";

    private static final String queueDirName = "./testBerkeleyDbQueue";
    private static final File queueDir = new File(queueDirName);

    private Queue<SimplePayload> berkeleyDbQueue() {
        return new BerkeleyDbQueue<>(queueDirName, queueName, 1);
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

    @AfterMethod
    public void after() throws IOException {
        queue.close();
    }

    @Test
    public void shouldReturnInfoAboutQueue() {
        queue = berkeleyDbQueue();

        assertEquals(queue.name(), queueName);
        assertEquals(queue.size(), 0);
        assertTrue(queue.isEmpty());
    }

    @Test(dependsOnMethods = {"shouldReturnInfoAboutQueue"})
    public void shouldPollAndPeekReturnNullForEmptyQueue() {
        queue = berkeleyDbQueue();

        assertNull(queue.peek());
        assertNull(queue.poll());
    }

    @Test(dependsOnMethods = {"shouldPollAndPeekReturnNullForEmptyQueue"}, expectedExceptions = NoSuchElementException.class)
    public void shouldElementThrowExceptionForEmptyQueue() {
        queue = berkeleyDbQueue();

        assertNull(queue.element());
    }

    @Test(dependsOnMethods = {"shouldElementThrowExceptionForEmptyQueue"}, expectedExceptions = NoSuchElementException.class)
    public void shouldRemoveThrowExceptionForEmptyQueue() {
        queue = berkeleyDbQueue();

        assertNull(queue.remove());
    }

    @Test(dependsOnMethods = {"shouldRemoveThrowExceptionForEmptyQueue"})
    public void shouldPush() {
        queue = berkeleyDbQueue();

        assertEquals(queue.size(), 0);
        for (int i = 0; i < 100; i++) {
            queue.push(new SimplePayload("test" + i, i, BigInteger.valueOf(i)));
        }
        assertEquals(queue.size(), 100);
    }

    @Test(dependsOnMethods = {"shouldPush"})
    public void shouldPoll() {
        queue = berkeleyDbQueue();

        assertEquals(queue.size(), 100);

        long size = queue.size();
        for (int i = 0; i < size; i++) {
            queue.poll();
        }

        assertEquals(queue.size(), 0);
    }

    @Test(dependsOnMethods = {"shouldPoll"})
    public void shouldRemove() {
        queue = berkeleyDbQueue();

        assertEquals(queue.size(), 0);
        for (int i = 0; i < 100; i++) {
            queue.push(new SimplePayload("test" + i, i, BigInteger.valueOf(i)));
        }
        assertEquals(queue.size(), 100);

        long size = queue.size();
        for (int i = 0; i < size; i++) {
            queue.remove();
        }

        assertEquals(queue.size(), 0);
    }

    @Test(dependsOnMethods = {"shouldRemove"})
    public void shouldPeek() {
        queue = berkeleyDbQueue();

        queue.push(new SimplePayload("testABC", 234, BigInteger.valueOf(234)));
        SimplePayload payloadPeeked = queue.peek();
        SimplePayload payloadPolled = queue.poll();

        assertEquals(payloadPeeked.getName(), payloadPolled.getName());
        assertEquals(payloadPeeked.getNumber(), payloadPolled.getNumber());
        assertEquals(payloadPeeked.getBigNumber(), payloadPolled.getBigNumber());
    }

    @Test(dependsOnMethods = {"shouldPeek"})
    public void shouldElement() {
        queue = berkeleyDbQueue();

        queue.push(new SimplePayload("testXYZ", 235, BigInteger.valueOf(235)));
        SimplePayload payloadElement = queue.element();
        SimplePayload payloadPolled = queue.poll();

        assertEquals(payloadElement.getName(), payloadPolled.getName());
        assertEquals(payloadElement.getNumber(), payloadPolled.getNumber());
        assertEquals(payloadElement.getBigNumber(), payloadPolled.getBigNumber());
    }

    @Test(dependsOnMethods = {"shouldElement"})
    public void shouldPushConcurrent() throws InterruptedException {
        queue = berkeleyDbQueue();

        Runnable runnable = () -> {
            int i = 500;
            while (i-- > 0) {
                queue.push(new SimplePayload("simplePayload", 1, BigInteger.ONE));
            }
        };

        Thread threadAdd1 = new Thread(runnable);
        Thread threadAdd2 = new Thread(runnable);
        Thread threadAdd3 = new Thread(runnable);

        threadAdd1.start();
        threadAdd2.start();
        threadAdd3.start();

        threadAdd1.join();
        threadAdd2.join();
        threadAdd3.join();

        assertEquals(queue.size(), 1500);
    }

    @Test(dependsOnMethods = {"shouldPushConcurrent"})
    public void shouldPollConcurrent() throws InterruptedException {
        queue = berkeleyDbQueue();

        Runnable runnable = () -> {
            while (!queue.isEmpty()) {
                queue.poll();
            }
        };

        Thread threadAdd1 = new Thread(runnable);
        Thread threadAdd2 = new Thread(runnable);
        Thread threadAdd3 = new Thread(runnable);

        threadAdd1.start();
        threadAdd2.start();
        threadAdd3.start();

        threadAdd1.join();
        threadAdd2.join();
        threadAdd3.join();

        assertEquals(queue.size(), 0);
    }

    @Test(dependsOnMethods = {"shouldPollConcurrent"})
    public void shouldPushAndPoll() {
        queue = berkeleyDbQueue();

        int i = 800;
        while (i-- > 0) {
            queue.push(new SimplePayload("simplePayload", 1, BigInteger.ONE));
        }
        queue.close();

        queue = berkeleyDbQueue();
        while (!queue.isEmpty()) {
            queue.poll();
        }
        assertTrue(queue.isEmpty());
    }
}
