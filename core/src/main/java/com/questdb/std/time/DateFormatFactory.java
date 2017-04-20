package com.questdb.std.time;

import com.questdb.std.ConcurrentHashMap;

public class DateFormatFactory {
    private final static ThreadLocal<DateFormatCompiler> tlCompiler = ThreadLocal.withInitial(DateFormatCompiler::new);
    private final ConcurrentHashMap<CharSequence, DateFormat> cache = new ConcurrentHashMap<>();

    /**
     * Retrieves cached data format, if already exists of creates and caches new one. Concurrent behaviour is
     * backed by ConcurrentHashMap, making method calls thread-safe and largely non-blocking.
     * <p>
     * Input pattern does not have to be a string, but it does have to implement hashCode/equals methods
     * correctly. No new objects created when pattern already exists.
     *
     * @param pattern can be mutable and is not stored if same pattern already in cache.
     * @return compiled implementation of DateFormat
     */
    public DateFormat get(CharSequence pattern) {
        return cache.computeIfAbsent(pattern, p -> tlCompiler.get().compile(p, false));
    }
}
