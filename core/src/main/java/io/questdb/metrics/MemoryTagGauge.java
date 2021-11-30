package io.questdb.metrics;

import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.CharSink;

/**
 * MemoryTag statistic dressed up as prometheus gauge .
 */
public class MemoryTagGauge implements Gauge {

    private static final String MEMORY_TAG_PREFIX = "memory_tag_";

    private final int memoryTag;

    public MemoryTagGauge(int memoryTag) {
        assert memoryTag >= 0 && memoryTag < MemoryTag.SIZE;
        this.memoryTag = memoryTag;
    }

    @Override
    public void inc() {
        //do nothing as this gauge is RO view of memory tag stats  
    }

    @Override
    public void dec() {
        //do nothing as this gauge is RO view of memory tag stats
    }

    public String getName() {
        return MemoryTag.nameOf(memoryTag);
    }

    private long getValue() {
        return Unsafe.getMemUsedByTag(memoryTag);
    }

    @Override
    public void scrapeIntoPrometheus(CharSink sink) {
        appendType(sink);
        appendMetricName(sink);
        PrometheusFormatUtils.appendSampleLineSuffix(sink, getValue());
        PrometheusFormatUtils.appendNewLine(sink);
    }

    private void appendType(CharSink sink) {
        sink.put(PrometheusFormatUtils.TYPE_PREFIX);
        sink.put(MEMORY_TAG_PREFIX);
        sink.put(getName());
        sink.put(" gauge\n");
    }

    private void appendMetricName(CharSink sink) {
        sink.put(PrometheusFormatUtils.METRIC_NAME_PREFIX);
        sink.put(MEMORY_TAG_PREFIX);
        sink.put(getName());
    }
}
