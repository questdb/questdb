package io.questdb.metrics;

public interface AtomicCounter extends Target {
    boolean compareAndSet(long expectedValue, long newValue);

    void dec();

    long get();

    CharSequence getName();

    void inc();

    void reset();
}
