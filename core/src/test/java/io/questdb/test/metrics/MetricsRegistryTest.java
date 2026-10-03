/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.test.metrics;

import io.questdb.metrics.Counter;
import io.questdb.metrics.CounterWithOneLabel;
import io.questdb.metrics.CounterWithTwoLabels;
import io.questdb.metrics.LongGauge;
import io.questdb.metrics.MetricSnapshotVisitor;
import io.questdb.metrics.MetricType;
import io.questdb.metrics.MetricsRegistry;
import io.questdb.metrics.MetricsRegistryImpl;
import io.questdb.metrics.NullMetricsRegistry;
import io.questdb.metrics.Target;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class MetricsRegistryTest {

    @Test
    public void testCounterWithOneLabel() {
        MetricsRegistry metricsRegistry = new MetricsRegistryImpl();
        CounterWithOneLabel counter = metricsRegistry.newCounter("counter", "label0", new CharSequence[]{"A", "B", "C"});

        counter.inc((short) 0);
        counter.inc((short) 0);
        counter.inc((short) 2);

        String expected = "# TYPE questdb_counter_total counter\n" +
                "questdb_counter_total{label0=\"A\"} 2\n" +
                "questdb_counter_total{label0=\"B\"} 0\n" +
                "questdb_counter_total{label0=\"C\"} 1\n" +
                "\n";
        assertScrapable(counter, expected);
    }

    @Test
    public void testCounterWithTwoLabels() {
        MetricsRegistry metricsRegistry = new MetricsRegistryImpl();
        CounterWithTwoLabels counter = metricsRegistry.newCounter("counter",
                "label0", new CharSequence[]{"A", "B", "C"},
                "label1", new CharSequence[]{"X", "Y", "Z"}
        );

        counter.inc((short) 0, (short) 1);
        counter.inc((short) 0, (short) 1);
        counter.inc((short) 2, (short) 1);

        String expected = "# TYPE questdb_counter_total counter\n" +
                "questdb_counter_total{label0=\"A\",label1=\"X\"} 0\n" +
                "questdb_counter_total{label0=\"A\",label1=\"Y\"} 2\n" +
                "questdb_counter_total{label0=\"A\",label1=\"Z\"} 0\n" +
                "questdb_counter_total{label0=\"B\",label1=\"X\"} 0\n" +
                "questdb_counter_total{label0=\"B\",label1=\"Y\"} 0\n" +
                "questdb_counter_total{label0=\"B\",label1=\"Z\"} 0\n" +
                "questdb_counter_total{label0=\"C\",label1=\"X\"} 0\n" +
                "questdb_counter_total{label0=\"C\",label1=\"Y\"} 1\n" +
                "questdb_counter_total{label0=\"C\",label1=\"Z\"} 0\n" +
                "\n";
        assertScrapable(counter, expected);
    }

    @Test
    public void testCounterWithoutLabels() {
        MetricsRegistry metricsRegistry = new MetricsRegistryImpl();
        Counter counter = metricsRegistry.newCounter("counter");

        counter.inc();

        String expected = "# TYPE questdb_counter_total counter\n" +
                "questdb_counter_total 1\n" +
                "\n";
        assertScrapable(counter, expected);
    }

    @Test
    public void testGauge() {
        MetricsRegistry metricsRegistry = new MetricsRegistryImpl();
        LongGauge gauge = metricsRegistry.newLongGauge("gauge");

        gauge.inc();
        gauge.inc();

        String expected1 = "# TYPE questdb_gauge gauge\n" +
                "questdb_gauge 2\n" +
                "\n";
        assertScrapable(gauge, expected1);

        gauge.dec();

        String expected2 = "# TYPE questdb_gauge gauge\n" +
                "questdb_gauge 1\n" +
                "\n";
        assertScrapable(gauge, expected2);
    }

    @Test
    public void testEveryRegisteredMetricIsEnumerable() {
        MetricsRegistry metricsRegistry = new MetricsRegistryImpl();
        metricsRegistry.newAtomicLongGauge("atomic").setValue(1);
        metricsRegistry.newCounter("counter").add(2);
        CounterWithOneLabel oneLabel = metricsRegistry.newCounter(
                "one_label",
                "label",
                new CharSequence[]{"a", "b"}
        );
        oneLabel.inc((short) 1);
        CounterWithTwoLabels twoLabels = metricsRegistry.newCounter(
                "two_labels",
                "label0",
                new CharSequence[]{"a"},
                "label1",
                new CharSequence[]{"x", "y"}
        );
        twoLabels.inc((short) 0, (short) 1);
        metricsRegistry.newDoubleGauge("double").setValue(3.5);
        metricsRegistry.newLongGauge("long").setValue(4);
        metricsRegistry.newVirtualGauge("virtual", () -> 5);

        final List<String> values = new ArrayList<>();
        metricsRegistry.snapshot(new MetricSnapshotVisitor() {
            @Override
            public void visitDouble(CharSequence name, double value) {
                values.add(name + ":DOUBLE_GAUGE:" + value);
            }

            @Override
            public void visitLong(CharSequence name, MetricType type, long value) {
                values.add(name + ":" + type + ":" + value);
            }

            @Override
            public void visitLong(CharSequence name, MetricType type, CharSequence labelValue0, long value) {
                values.add(name + "__" + labelValue0 + ":" + type + ":" + value);
            }

            @Override
            public void visitLong(
                    CharSequence name,
                    MetricType type,
                    CharSequence labelValue0,
                    CharSequence labelValue1,
                    long value
            ) {
                values.add(name + "__" + labelValue0 + "__" + labelValue1 + ":" + type + ":" + value);
            }
        });

        Assert.assertEquals(List.of(
                "atomic:LONG_GAUGE:1",
                "counter:COUNTER:2",
                "one_label__a:COUNTER:0",
                "one_label__b:COUNTER:1",
                "two_labels__a__x:COUNTER:0",
                "two_labels__a__y:COUNTER:1",
                "double:DOUBLE_GAUGE:3.5",
                "long:LONG_GAUGE:4",
                "virtual:VIRTUAL_LONG_GAUGE:5"
        ), values);
    }

    @Test
    public void testNullCounter() {
        MetricsRegistry metricsRegistry = new NullMetricsRegistry();
        Counter counter = metricsRegistry.newCounter("counter");

        counter.inc();
        assetNull(counter);
    }

    @Test
    public void testNullGauge() {
        MetricsRegistry metricsRegistry = new NullMetricsRegistry();
        LongGauge gauge = metricsRegistry.newLongGauge("gauge");

        gauge.inc();
        gauge.inc();
        assetNull(gauge);

        gauge.dec();
        assetNull(gauge);
    }

    private static void assertScrapable(Target target, CharSequence expected) {
        try (DirectUtf8Sink sink = new DirectUtf8Sink(32)) {
            target.scrapeIntoPrometheus(sink);
            TestUtils.assertEquals(expected, sink.toString());
        }
    }

    private static void assetNull(Target target) {
        try (DirectUtf8Sink sink = new DirectUtf8Sink(32)) {
            target.scrapeIntoPrometheus(sink);
            TestUtils.assertEquals("", sink.toString());
        }
    }
}
