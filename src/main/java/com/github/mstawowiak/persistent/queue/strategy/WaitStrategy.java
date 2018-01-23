package com.github.mstawowiak.persistent.queue.strategy;

/**
 * A strategy used to decide how long to sleep before retrying after a failed attempt
 */
public interface WaitStrategy {

    /**
     * Returns the time, in milliseconds, to sleep before retrying
     *
     * @param attemptNumber number of attempt for which we compute sleep time
     * @return the sleep time before next attempt
     */
    long computeSleepTime(long attemptNumber);
}

