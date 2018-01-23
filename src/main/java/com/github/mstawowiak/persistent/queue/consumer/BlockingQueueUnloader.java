package com.github.mstawowiak.persistent.queue.consumer;

import com.github.mstawowiak.persistent.queue.Payload;
import com.github.mstawowiak.persistent.queue.Queue;
import com.github.mstawowiak.persistent.queue.exception.SerializationException;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicLong;

public class BlockingQueueUnloader<P extends Payload> extends AbstractQueueUnloader<P> {

    private static final int NO_EXCEPTIONS = 0;
    private final AtomicLong failedCounterInARow = new AtomicLong(0);

    public BlockingQueueUnloader(Queue<P> queue, QueueUnloaderConfig<P> config) {
        super(queue, config);
    }

    protected void process() {
        P payload;
        try {
            payload = queue.remove();
        } catch (SerializationException | NoSuchElementException ex) {
            LOGGER.warn(queue.name(), "Skipping payload. Reason: '{}'", ex.getMessage());
            return;
        }

        if (failedCounterInARow.get() > NO_EXCEPTIONS) {
            //sleep to prevent log killing
            sleepSafe(waitStrategy.computeSleepTime(failedCounterInARow.get()));
        }

        try {
            consumerThreadPool.submit(() -> {
                try {
                    consumer.consume(payload);

                    if (failedCounterInARow.get() > NO_EXCEPTIONS) {
                        LOGGER.debug(queue.name(), "Reset repeat delay");
                        failedCounterInARow.set(NO_EXCEPTIONS);
                    }
                } catch (Exception ex) {
                    LOGGER.warn(queue.name(), "Unable to consume payload", ex);

                    failedCounterInARow.incrementAndGet();
                    queue.push(payload);
                }
            }).get();
        } catch (Exception ex) {
            LOGGER.warn(queue.name(), "Unable to consume payload", ex);

            failedCounterInARow.incrementAndGet();
            queue.push(payload);
        }
    }

    private static void sleepSafe(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException iex) {
        }
    }

}
