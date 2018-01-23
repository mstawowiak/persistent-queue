package com.github.mstawowiak.persistent.queue.strategy;

import java.util.concurrent.TimeUnit;

import static com.github.mstawowiak.persistent.queue.util.Preconditions.checkArgument;

/**
 * Factory class for instances of {@link WaitStrategy}
 * Based on: https://github.com/rholder/guava-retrying
 */
@SuppressWarnings("PMD.AvoidLiteralsInIfCondition")
public final class WaitStrategyFactory {

    private static final WaitStrategy NO_WAIT_STRATEGY = new FixedWaitStrategy(0L);

    private WaitStrategyFactory() {
    }

    /**
     * Returns a wait strategy that doesn't sleep at all before retrying.
     *
     * @return a wait strategy that doesn't wait between retries
     */
    public static WaitStrategy noWait() {
        return NO_WAIT_STRATEGY;
    }

    /**
     * Returns a wait strategy that sleeps a fixed amount of time before retrying.
     *
     * @param sleepTime the time to sleep
     * @param timeUnit  the unit of the time to sleep
     * @return a wait strategy that sleeps a fixed amount of time
     * @throws IllegalStateException if the sleep time is &lt; 0
     */
    public static WaitStrategy fixedWait(long sleepTime, TimeUnit timeUnit) throws IllegalStateException {
        checkArgument(timeUnit != null, "The time unit may not be null");

        return new FixedWaitStrategy(timeUnit.toMillis(sleepTime));
    }

    /**
     * Returns a strategy that sleeps a fixed amount of time after the first failed attempt and in
     * incrementing amounts of time after each additional failed attempt.
     *
     * @param initialSleepTime     the time to sleep before retrying the first time
     * @param initialSleepTimeUnit the unit of the initial sleep time
     * @param increment            the increment added to the previous sleep time after each failed attempt
     * @param incrementTimeUnit    the unit of the increment
     * @param maximumWait          the maximum time to sleep
     * @param maximumTimeUnit      the unit of the maximum time
     * @return a wait strategy that incrementally sleeps an additional fixed time after each failed
     * attempt
     */
    public static WaitStrategy incrementingWait(
            long initialSleepTime, TimeUnit initialSleepTimeUnit,
            long increment, TimeUnit incrementTimeUnit,
            long maximumWait, TimeUnit maximumTimeUnit) {

        checkArgument(initialSleepTimeUnit != null, "The initial sleep time unit may not be null");
        checkArgument(incrementTimeUnit != null, "The increment time unit may not be null");
        checkArgument(maximumTimeUnit != null, "The maximum time unit may not be null");

        return new IncrementingWaitStrategy(initialSleepTimeUnit.toMillis(initialSleepTime),
                incrementTimeUnit.toMillis(increment), maximumTimeUnit.toMillis(maximumWait));
    }

    /**
     * Returns a strategy which sleeps for an exponential amount of time after the first failed
     * attempt, and in exponentially incrementing amounts after each failed attempt up to the
     * maximumTime.
     *
     * @param maximumTime     the maximum time to sleep
     * @param maximumTimeUnit the unit of the maximum time
     * @return a wait strategy that increments with each failed attempt using exponential backoff
     */
    public static WaitStrategy exponentialWait(long maximumTime, TimeUnit maximumTimeUnit) {
        checkArgument(maximumTimeUnit != null, "The maximum time unit may not be null");

        return new ExponentialWaitStrategy(1, maximumTimeUnit.toMillis(maximumTime));
    }

    /**
     * Returns a strategy which sleeps for an exponential amount of time after the first failed
     * attempt, and in exponentially incrementing amounts after each failed attempt up to the
     * maximumTime. The wait time between the retries can be controlled by the multiplier.
     * nextWaitTime = exponentialIncrement * {@code multiplier}.
     *
     * @param multiplier      multiply the wait time calculated by this
     * @param maximumTime     the maximum time to sleep
     * @param maximumTimeUnit the unit of the maximum time
     * @return a wait strategy that increments with each failed attempt using exponential backoff
     */
    public static WaitStrategy exponentialWait(long multiplier, long maximumTime, TimeUnit maximumTimeUnit) {
        checkArgument(maximumTimeUnit != null, "The maximum time unit may not be null");

        return new ExponentialWaitStrategy(multiplier, maximumTimeUnit.toMillis(maximumTime));
    }

    private static final class FixedWaitStrategy implements WaitStrategy {

        private final long sleepTime;

        public FixedWaitStrategy(long sleepTime) {
            if (sleepTime < 0L) {
                throw new IllegalArgumentException("sleepTime must be >= 0 but is " + sleepTime);
            }
            this.sleepTime = sleepTime;
        }

        @Override
        public long computeSleepTime(long attemptNumber) {
            return sleepTime;
        }
    }

    private static final class IncrementingWaitStrategy implements WaitStrategy {

        private final long initialSleepTime;
        private final long increment;
        private final long maximumWait;

        public IncrementingWaitStrategy(long initialSleepTime, long increment, long maximumWait) {
            checkArgument(initialSleepTime >= 0L, "initialSleepTime must be >= 0 but is " + initialSleepTime);
            checkArgument(increment >= 0L, "increment must be >= 0 but is " + increment);
            checkArgument(maximumWait >= 0L, "maximumWait must be >= 0 but is " + maximumWait);

            this.initialSleepTime = initialSleepTime;
            this.increment = increment;
            this.maximumWait = maximumWait;
        }

        @Override
        public long computeSleepTime(long attemptNumber) {
            long result = initialSleepTime + (increment * (attemptNumber - 1));
            if (result > maximumWait) {
                result = maximumWait;
            }
            return result >= 0L ? result : 0L;
        }
    }

    private static final class ExponentialWaitStrategy implements WaitStrategy {

        private final long multiplier;
        private final long maximumWait;

        public ExponentialWaitStrategy(long multiplier, long maximumWait) {
            checkArgument(multiplier >= 0L, "multiplier must be >= 0 but is " + multiplier);
            checkArgument(maximumWait >= 0L, "maximumWait must be >= 0 but is " + maximumWait);
            checkArgument(multiplier < maximumWait, "multiplier must be < maximumWait but is " + multiplier);

            this.multiplier = multiplier;
            this.maximumWait = maximumWait;
        }

        @Override
        public long computeSleepTime(long attemptNumber) {
            double exp = Math.pow(2, attemptNumber);
            long result = Math.round(multiplier * exp);
            if (result > maximumWait) {
                result = maximumWait;
            }
            return result >= 0L ? result : 0L;
        }
    }
}