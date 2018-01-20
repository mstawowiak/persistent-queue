package com.github.mstawowiak.persistent.queue.data;

import com.github.mstawowiak.persistent.queue.consumer.Consumer;

public class DoNothingSimplePayloadConsumer implements Consumer<SimplePayload> {

    @Override
    public void consume(SimplePayload element) {
        //do nothing
    }
}