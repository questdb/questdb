package com.questdb.std.time;

public interface TimeZoneRules {
    long getOffset(long millis);
    long getOffset(long millis, int year, boolean leap);
}
