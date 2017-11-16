package com.questdb.test.tools;

import com.questdb.std.microtime.MicrosecondClock;

public class TestMicroClock implements MicrosecondClock {
    private final long increment;
    private long micros;

    public TestMicroClock(long micros, long increment) {
        this.micros = micros;
        this.increment = increment * 1000;
    }

    @Override
    public long getTicks() {
        long result = micros;
        micros += increment;
        return result;
    }
}