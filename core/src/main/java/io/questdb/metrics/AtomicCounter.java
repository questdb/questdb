package io.questdb.metrics;

public interface AtomicCounter extends Scrapable {
    boolean compareAndSet(long expectedValue, long newValue);

    void dec();

    long get();

    CharSequence getName();

    void inc();
}
