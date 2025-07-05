/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.test.griffin.engine.table;

import io.questdb.metrics.Counter;
import io.questdb.metrics.CounterWithOneLabel;
import io.questdb.metrics.CounterWithTwoLabels;
import io.questdb.metrics.LongGauge;
import io.questdb.metrics.MetricsRegistry;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class PrometheusMetricsTest extends AbstractCairoTest {

    private void assertPrometheusMetrics(String expected) throws Exception {
        assertQuery(
                expected,
                "prometheus_metrics()",
                null,
                null,
                false,
                false
        );
    }

    @Test
    public void testCounterWithOneLabel() throws Exception {
        // Clear existing metrics and create synthetic ones
        MetricsRegistry metricsRegistry = engine.getMetrics().getRegistry();
        metricsRegistry.clear();
        
        CounterWithOneLabel counter = metricsRegistry.newCounter("counter", "label0", new CharSequence[]{"A", "B", "C"});

        counter.inc((short) 0);
        counter.inc((short) 0);
        counter.inc((short) 2);

        assertPrometheusMetrics("name\ttype\tlong_value\tdouble_value\tkind\tlabels\n" +
                        "questdb_counter_total\tcounter\t2\tnull\tLONG\t{ \"label0\" : \"A\" }\n" +
                        "questdb_counter_total\tcounter\t0\tnull\tLONG\t{ \"label0\" : \"B\" }\n" +
                        "questdb_counter_total\tcounter\t1\tnull\tLONG\t{ \"label0\" : \"C\" }\n");
    }

    @Test
    public void testCounterWithTwoLabels() throws Exception {
        MetricsRegistry metricsRegistry = engine.getMetrics().getRegistry();
        metricsRegistry.clear();
        
        CounterWithTwoLabels counter = metricsRegistry.newCounter("counter",
                "label0", new CharSequence[]{"A", "B", "C"},
                "label1", new CharSequence[]{"X", "Y", "Z"}
        );

        counter.inc(0, 1);
        counter.inc(0, 1);
        counter.inc(2, 1);

        assertPrometheusMetrics(
                "name\ttype\tlong_value\tdouble_value\tkind\tlabels\n" +
                        "questdb_counter_total\tcounter\t0\tnull\tLONG\t{ \"label0\" : \"A\", \"label1\" : \"X\" }\n" +
                        "questdb_counter_total\tcounter\t2\tnull\tLONG\t{ \"label0\" : \"A\", \"label1\" : \"Y\" }\n" +
                        "questdb_counter_total\tcounter\t0\tnull\tLONG\t{ \"label0\" : \"A\", \"label1\" : \"Z\" }\n" +
                        "questdb_counter_total\tcounter\t0\tnull\tLONG\t{ \"label0\" : \"B\", \"label1\" : \"X\" }\n" +
                        "questdb_counter_total\tcounter\t0\tnull\tLONG\t{ \"label0\" : \"B\", \"label1\" : \"Y\" }\n" +
                        "questdb_counter_total\tcounter\t0\tnull\tLONG\t{ \"label0\" : \"B\", \"label1\" : \"Z\" }\n" +
                        "questdb_counter_total\tcounter\t0\tnull\tLONG\t{ \"label0\" : \"C\", \"label1\" : \"X\" }\n" +
                        "questdb_counter_total\tcounter\t1\tnull\tLONG\t{ \"label0\" : \"C\", \"label1\" : \"Y\" }\n" +
                        "questdb_counter_total\tcounter\t0\tnull\tLONG\t{ \"label0\" : \"C\", \"label1\" : \"Z\" }\n");
    }

    @Test
    public void testCounterWithoutLabels() throws Exception {
        MetricsRegistry metricsRegistry = engine.getMetrics().getRegistry();
        metricsRegistry.clear();
        
        Counter counter = metricsRegistry.newCounter("counter");

        counter.inc();

        assertPrometheusMetrics("name\ttype\tlong_value\tdouble_value\tkind\tlabels\n" +
                        "questdb_counter_total\tcounter\t1\tnull\tLONG\t\n");
    }

    @Test
    public void testGauge() throws Exception {
        MetricsRegistry metricsRegistry = engine.getMetrics().getRegistry();
        metricsRegistry.clear();
        
        LongGauge gauge = metricsRegistry.newLongGauge("gauge");

        gauge.inc();
        gauge.inc();

        assertPrometheusMetrics("name\ttype\tlong_value\tdouble_value\tkind\tlabels\n" +
                        "questdb_gauge\tgauge\t2\tnull\tLONG\t\n");

        gauge.dec();

        assertPrometheusMetrics("name\ttype\tlong_value\tdouble_value\tkind\tlabels\n" +
                        "questdb_gauge\tgauge\t1\tnull\tLONG\t\n");
    }

    @Test
    public void testMetricsAreDisabled() throws Exception {
        configuration.getMetrics().disable();
        assertException("prometheus_metrics();", 0, "metrics are disabled! try setting `metrics.enabled=true` in `server.conf`");
        configuration.getMetrics().clear();
    }

    @Test
    public void testNullCounter() throws Exception {
        // Clear the engine's registry to simulate no metrics being registered
        MetricsRegistry engineRegistry = engine.getMetrics().getRegistry();
        engineRegistry.clear();
        
        // Test that when no metrics are registered, only headers are returned
        assertPrometheusMetrics("name\ttype\tlong_value\tdouble_value\tkind\tlabels\n");
    }

    @Test
    public void testNullGauge() throws Exception {
        // Clear the engine's registry to simulate no metrics being registered
        MetricsRegistry engineRegistry = engine.getMetrics().getRegistry();
        engineRegistry.clear();
        
        // Test that when no metrics are registered, only headers are returned
        assertPrometheusMetrics("name\ttype\tlong_value\tdouble_value\tkind\tlabels\n");
    }

    @Test
    public void testPlan() throws Exception {
        assertMemoryLeak(() -> assertPlanNoLeakCheck("prometheus_metrics();", "prometheus_metrics\n"));
    }
}
