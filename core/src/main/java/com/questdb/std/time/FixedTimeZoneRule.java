package com.questdb.std.time;

public class FixedTimeZoneRule implements TimeZoneRules {
    private final long offset;
    private final String id;

    public FixedTimeZoneRule(String id, long offset) {
        this.id = id;
        this.offset = offset;
    }

    @Override
    public long getOffset(long millis) {
        return offset;
    }

    @Override
    public long getOffset(long millis, int year, boolean leap) {
        return offset;
    }

    @Override
    public String getId() {
        return id;
    }
}
