package com.github.mstawowiak.persistent.queue.data;

import com.github.mstawowiak.persistent.queue.consumer.Consumer;

public class DoNothingTestPayloadConsumer implements Consumer<TestPayload> {

    @Override
    public void consume(TestPayload element) {
        //do nothing
    }
}