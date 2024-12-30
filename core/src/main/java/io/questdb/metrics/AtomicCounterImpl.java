package io.questdb.metrics;

import io.questdb.std.str.BorrowableUtf8Sink;
import io.questdb.std.str.CharSink;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.atomic.AtomicLong;

public class AtomicCounterImpl implements AtomicCounter {
    private final AtomicLong counter;
    private final CharSequence name;

    public AtomicCounterImpl(CharSequence name) {
        this.name = name;
        this.counter = new AtomicLong();
    }

    @Override
    public boolean compareAndSet(long expectedValue, long newValue) {
        return counter.compareAndSet(expectedValue, newValue);
    }

    @Override
    public void dec() {
        counter.decrementAndGet();
    }

    @Override
    public long get() {
        return counter.get();
    }

    @Override
    public CharSequence getName() {
        return name;
    }

    @Override
    public void inc() {
        counter.incrementAndGet();
    }

    @Override
    public void scrapeIntoPrometheus(@NotNull BorrowableUtf8Sink sink) {
        appendType(sink);
        appendMetricName(sink);
        PrometheusFormatUtils.appendSampleLineSuffix(sink, counter.get());
        PrometheusFormatUtils.appendNewLine(sink);
    }

    private void appendMetricName(CharSink<?> sink) {
        sink.putAscii(PrometheusFormatUtils.METRIC_NAME_PREFIX);
        sink.put(name);
    }

    private void appendType(CharSink<?> sink) {
        sink.putAscii(PrometheusFormatUtils.TYPE_PREFIX);
        sink.put(name);
        sink.putAscii(" counter\n");
    }
}
