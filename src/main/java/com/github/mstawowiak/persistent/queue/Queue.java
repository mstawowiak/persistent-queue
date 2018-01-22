package com.github.mstawowiak.persistent.queue;

/**
 * A collection designed for holding elements prior to processing.
 *
 * @param <P> the type of payload held in this collection
 */
public interface Queue<P extends Payload> {

    void push(P payload);

    P poll();

    P remove();

    P peek();

    P element();

    String name();

    long size();

    boolean isEmpty();

    void close();

}
