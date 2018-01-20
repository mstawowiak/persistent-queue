package com.github.mstawowiak.persistent.queue.consumer;

import com.github.mstawowiak.persistent.queue.Payload;
import com.github.mstawowiak.persistent.queue.Queue;
import com.github.mstawowiak.persistent.queue.exception.SerializationException;
import java.util.NoSuchElementException;

public class BlockingQueueUnloader<P extends Payload> extends AbstractQueueUnloader<P> {

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

        try {
            consumerThreadPool.submit(() -> {
                try {
                    consumer.consume(payload);
                } catch (Exception ex) {
                    LOGGER.warn(queue.name(), "Unable to consume payload", ex);

                    queue.push(payload);
                }
            }).get();
        } catch (Exception ex) {
            LOGGER.warn(queue.name(), "Unable to consume payload", ex);

            queue.push(payload);
        }
    }
}
