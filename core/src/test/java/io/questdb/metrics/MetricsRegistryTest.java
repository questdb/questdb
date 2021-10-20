/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.metrics;

import io.questdb.std.str.CharSink;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

public class MetricsRegistryTest {

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
    public void testNullCounter() {
        MetricsRegistry metricsRegistry = new NullMetricsRegistry();
        Counter counter = metricsRegistry.newCounter("counter");

        counter.inc();
        assetNull(counter);
    }

    @Test
    public void testGauge() {
        MetricsRegistry metricsRegistry = new MetricsRegistryImpl();
        Gauge gauge = metricsRegistry.newGauge("gauge");

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
    public void testNullGauge() {
        MetricsRegistry metricsRegistry = new NullMetricsRegistry();
        Gauge gauge = metricsRegistry.newGauge("gauge");

        gauge.inc();
        gauge.inc();
        assetNull(gauge);

        gauge.dec();
        assetNull(gauge);
    }

    private static void assertScrapable(Scrapable scrapable, CharSequence expected) {
        CharSink sink = new StringSink();
        scrapable.scrapeIntoPrometheus(sink);
        TestUtils.assertEquals(expected, (CharSequence) sink);
    }

    private static void assetNull(Scrapable scrapable) {
        CharSink sink = new StringSink();
        scrapable.scrapeIntoPrometheus(sink);
        TestUtils.assertEquals("", (CharSequence) sink);
    }
}
