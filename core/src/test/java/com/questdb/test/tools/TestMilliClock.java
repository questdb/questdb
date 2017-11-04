package com.questdb.test.tools;

import com.questdb.std.clock.Clock;

public class TestMilliClock implements Clock {
    private final long increment;
    private long millis;

    public TestMilliClock(long millis, long increment) {
        this.millis = millis;
        this.increment = increment;
    }

    @Override
    public long getTicks() {
        long result = millis;
        millis += increment;
        return result;
    }
}