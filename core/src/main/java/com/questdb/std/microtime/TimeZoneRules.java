package com.questdb.std.microtime;

public interface TimeZoneRules {
    String getId();

    long getOffset(long millis, int year, boolean leap);

    long getOffset(long millis);
}
