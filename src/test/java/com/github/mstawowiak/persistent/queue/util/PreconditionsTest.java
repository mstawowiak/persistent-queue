package com.github.mstawowiak.persistent.queue.util;

import org.testng.annotations.Test;

/**
 * Tests for {@link Preconditions}
 */
public final class PreconditionsTest {

    @Test
    public void shouldDoNothingForTrueExpressionWhenCheckArgument() {
        Preconditions.checkArgument(true, "Cannot be false");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void shouldThrowExceptionForFalseExpressionWhenCheckArgument() {
        Preconditions.checkArgument(false, "Cannot be false");
    }
}

