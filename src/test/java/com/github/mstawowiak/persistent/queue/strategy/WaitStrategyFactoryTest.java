package com.github.mstawowiak.persistent.queue.strategy;

import java.util.concurrent.TimeUnit;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * Tests for @{link {@link WaitStrategyFactory}}
 */
public class WaitStrategyFactoryTest {

    @Test
    public void testNoWait() {
        WaitStrategy noWait = WaitStrategyFactory.noWait();
        assertEquals(0L, noWait.computeSleepTime(18));
    }

    @Test
    public void testFixedWait() {
        WaitStrategy fixedWait = WaitStrategyFactory.fixedWait(1000L, TimeUnit.MILLISECONDS);
        assertEquals(1000L, fixedWait.computeSleepTime(12));
    }

    @Test
    public void testIncrementingWait() {
        WaitStrategy incrementingWait = WaitStrategyFactory.incrementingWait(
                500L, TimeUnit.MILLISECONDS,
                100L, TimeUnit.MILLISECONDS,
                1, TimeUnit.SECONDS);

        assertEquals(500L, incrementingWait.computeSleepTime(1));
        assertEquals(600L, incrementingWait.computeSleepTime(2));
        assertEquals(700L, incrementingWait.computeSleepTime(3));
        assertEquals(800L, incrementingWait.computeSleepTime(4));
        assertEquals(900L, incrementingWait.computeSleepTime(5));
        assertEquals(1000L, incrementingWait.computeSleepTime(6));
        assertEquals(1000L, incrementingWait.computeSleepTime(7));
        assertEquals(1000L, incrementingWait.computeSleepTime(100));
    }

    @Test
    public void testExponentialWithMaximumWait() {
        WaitStrategy exponentialWait = WaitStrategyFactory.exponentialWait(40, TimeUnit.MILLISECONDS);
        assertTrue(exponentialWait.computeSleepTime(1) == 2);
        assertTrue(exponentialWait.computeSleepTime(2) == 4);
        assertTrue(exponentialWait.computeSleepTime(3) == 8);
        assertTrue(exponentialWait.computeSleepTime(4) == 16);
        assertTrue(exponentialWait.computeSleepTime(5) == 32);
        assertTrue(exponentialWait.computeSleepTime(6) == 40);
        assertTrue(exponentialWait.computeSleepTime(7) == 40);
        assertTrue(exponentialWait.computeSleepTime(100) == 40);
    }

    @Test
    public void testExponentialWithMultiplierAndMaximumWait() {
        WaitStrategy exponentialWait = WaitStrategyFactory.exponentialWait(1000, 50000, TimeUnit.MILLISECONDS);
        assertTrue(exponentialWait.computeSleepTime(1) == 2000);
        assertTrue(exponentialWait.computeSleepTime(2) == 4000);
        assertTrue(exponentialWait.computeSleepTime(3) == 8000);
        assertTrue(exponentialWait.computeSleepTime(4) == 16000);
        assertTrue(exponentialWait.computeSleepTime(5) == 32000);
        assertTrue(exponentialWait.computeSleepTime(6) == 50000);
        assertTrue(exponentialWait.computeSleepTime(7) == 50000);
        assertTrue(exponentialWait.computeSleepTime(100) == 50000);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void fixedWaitNullTimeUnit() {
        WaitStrategyFactory.fixedWait(0, null);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void fixedWaitSleepTimeLTZero() {
        WaitStrategyFactory.fixedWait(-1, TimeUnit.MILLISECONDS);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void incrementingWaitNullInitialSleepTimeUnit() {
        WaitStrategyFactory.incrementingWait(0, null, 0, TimeUnit.MILLISECONDS, 0, TimeUnit.MILLISECONDS);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void incrementingWaitNullIncrementTimeUnit() {
        WaitStrategyFactory.incrementingWait(0, TimeUnit.MILLISECONDS, 0, null, 0, TimeUnit.MILLISECONDS);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void incrementingWaitNullMaximumTimeUnit() {
        WaitStrategyFactory.incrementingWait(0, TimeUnit.MILLISECONDS, 0, TimeUnit.MILLISECONDS, 0, null);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void incrementingWaitInitialSleepTimeLTZero() {
        WaitStrategyFactory.incrementingWait(-1, TimeUnit.MILLISECONDS, 0, TimeUnit.MILLISECONDS, 0, TimeUnit.MILLISECONDS);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void incrementingWaitIncrementTimeLTZero() {
        WaitStrategyFactory.incrementingWait(0, TimeUnit.MILLISECONDS, -1, TimeUnit.MILLISECONDS, 0, TimeUnit.MILLISECONDS);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void incrementingWaitMaximumTimeLTZero() {
        WaitStrategyFactory.incrementingWait(0, TimeUnit.MILLISECONDS, 0, TimeUnit.MILLISECONDS, -1, TimeUnit.MILLISECONDS);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void exponentialWaitNullMaximumTimeUnit() {
        WaitStrategyFactory.exponentialWait(0, null);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void exponentialWaitMaximumTimeLTZero() {
        WaitStrategyFactory.exponentialWait(-1, TimeUnit.MILLISECONDS);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void exponentialWaitMultiplierLTZero() {
        WaitStrategyFactory.exponentialWait(-1, 10, TimeUnit.MILLISECONDS);
    }
}
