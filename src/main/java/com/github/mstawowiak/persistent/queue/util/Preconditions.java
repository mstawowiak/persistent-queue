package com.github.mstawowiak.persistent.queue.util;

public final class Preconditions {

    public static void checkArgument(boolean expression, String errorMessage) {
        if (!expression) {
            throw new IllegalArgumentException(String.valueOf(errorMessage));
        }
    }

    private Preconditions() {
    }
}
