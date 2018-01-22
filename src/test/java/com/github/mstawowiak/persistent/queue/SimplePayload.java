package com.github.mstawowiak.persistent.queue;

import java.math.BigInteger;

public class SimplePayload implements Payload {

    private static final long serialVersionUID = 8222352509404340736L;

    private String name;
    private Integer number;
    private BigInteger bigNumber;

    public SimplePayload(String name, Integer number, BigInteger bigNumber) {
        this.name = name;
        this.number = number;
        this.bigNumber = bigNumber;
    }

    public String getName() {
        return name;
    }

    public Integer getNumber() {
        return number;
    }

    public BigInteger getBigNumber() {
        return bigNumber;
    }
}
