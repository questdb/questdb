package io.questdb.metrics;

import io.questdb.std.str.BorrowableUtf8Sink;

public class NullAtomicCounter implements AtomicCounter {
    public static final NullAtomicCounter INSTANCE = new NullAtomicCounter();

    @Override
    public boolean compareAndSet(long expectedValue, long newValue) {
        return true;
    }

    @Override
    public void dec() {
    }

    @Override
    public long get() {
        return 0;
    }

    @Override
    public CharSequence getName() {
        return null;
    }

    @Override
    public void inc() {
    }

    @Override
    public void scrapeIntoPrometheus(BorrowableUtf8Sink sink) {
    }
}
