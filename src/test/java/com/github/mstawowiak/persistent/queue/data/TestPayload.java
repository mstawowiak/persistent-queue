package com.github.mstawowiak.persistent.queue.data;

import com.github.mstawowiak.persistent.queue.Payload;

public interface TestPayload extends Payload {

    String getName();

    Integer getNumber();
}
