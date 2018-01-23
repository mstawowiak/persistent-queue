package com.github.mstawowiak.persistent.queue.data;

import com.github.mstawowiak.persistent.queue.consumer.Consumer;

public class RandomErrorTestPayloadConsumer implements Consumer<TestPayload> {

    private static final int DEFAULT_FREQUENCY = 2018;

    private final int frequency;

    public RandomErrorTestPayloadConsumer() {
        this.frequency = DEFAULT_FREQUENCY;
    }

    public RandomErrorTestPayloadConsumer(int frequency) {
        this.frequency = frequency;
    }

    @Override
    public void consume(TestPayload payload) {
        if (System.currentTimeMillis() % frequency == 0) {
            System.out.println("Consumer error: " + payload.getNumber());
            throw new RuntimeException("Consumer error - payload no: " + payload.getNumber());
        }
    }
}
