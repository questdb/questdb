package com.questdb.std.time;

public class FixedTimeZoneRule implements TimeZoneRules {
    private final long offset;

    public FixedTimeZoneRule(long offset) {
        this.offset = offset;
    }

    @Override
    public long getOffset(long millis, int year, boolean leap) {
        return offset;
    }
}
