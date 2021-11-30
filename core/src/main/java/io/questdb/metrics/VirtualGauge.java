package io.questdb.metrics;

import io.questdb.std.str.CharSink;

/**
 * Read Only gauge used to expose various stats .
 */
public class VirtualGauge implements Gauge {

    private final CharSequence name;
    private final StatProvider provider;

    public VirtualGauge(CharSequence name, StatProvider statProvider) {
        this.name = name;
        this.provider = statProvider;
    }

    public long getValue() {
        return provider.getValue();
    }

    @Override
    public void inc() {
        //do nothing as this gauge is RO view of some stat
    }

    @Override
    public void dec() {
        //do nothing as this gauge is RO view of some stat
    }

    private CharSequence getName() {
        return this.name;
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
        sink.put(getName());
        sink.put(" gauge\n");
    }

    private void appendMetricName(CharSink sink) {
        sink.put(PrometheusFormatUtils.METRIC_NAME_PREFIX);
        sink.put(getName());
    }

    @FunctionalInterface
    public interface StatProvider {
        long getValue();
    }
}
