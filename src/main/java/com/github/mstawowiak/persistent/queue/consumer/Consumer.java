package com.github.mstawowiak.persistent.queue.consumer;

import com.github.mstawowiak.persistent.queue.Payload;

public interface Consumer<P extends Payload> {

    void consume(P payload);
}
