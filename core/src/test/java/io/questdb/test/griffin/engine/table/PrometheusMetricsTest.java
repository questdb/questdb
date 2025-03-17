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

import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.table.PrometheusMetricsRecordCursorFactory;
import io.questdb.metrics.Counter;
import io.questdb.metrics.CounterWithOneLabel;
import io.questdb.metrics.CounterWithTwoLabels;
import io.questdb.metrics.LongGauge;
import io.questdb.metrics.MetricsRegistry;
import io.questdb.metrics.MetricsRegistryImpl;
import io.questdb.metrics.NullMetricsRegistry;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

public class PrometheusMetricsTest extends AbstractCairoTest {

    @Test
    public void testCoverage() throws Exception {
        assertMemoryLeak(() -> {
            RecordCursorFactory ignore = select("prometheus_metrics();");
            ignore.close();
        });
    }

    @Test
    public void testPlan() throws Exception {
        assertMemoryLeak(() -> assertPlanNoLeakCheck("prometheus_metrics();", "prometheus_metrics\n"));
    }

    @Test
    public void testCounterWithOneLabel() {
        MetricsRegistry metricsRegistry = new MetricsRegistryImpl();
        CounterWithOneLabel counter = metricsRegistry.newCounter("counter", "label0", new CharSequence[]{"A", "B", "C"});

        counter.inc((short) 0);
        counter.inc((short) 0);
        counter.inc((short) 2);

        assertPrometheusMetrics("name\ttype\tvalue\tkind\tlabels\n" +
                        "questdb_counter_total\tcounter\t2\tLONG\t{ \"label0\" : \"A\" }\n" +
                        "questdb_counter_total\tcounter\t0\tLONG\t{ \"label0\" : \"B\" }\n" +
                        "questdb_counter_total\tcounter\t1\tLONG\t{ \"label0\" : \"C\" }\n",
                metricsRegistry);
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

        assertPrometheusMetrics("name\ttype\tvalue\tkind\tlabels\n" +
                        "questdb_counter_total\tcounter\t0\tLONG\t{ \"label0\" : \"A\", \"label1\" : \"X\" }\n" +
                        "questdb_counter_total\tcounter\t2\tLONG\t{ \"label0\" : \"A\", \"label1\" : \"Y\" }\n" +
                        "questdb_counter_total\tcounter\t0\tLONG\t{ \"label0\" : \"A\", \"label1\" : \"Z\" }\n" +
                        "questdb_counter_total\tcounter\t0\tLONG\t{ \"label0\" : \"B\", \"label1\" : \"X\" }\n" +
                        "questdb_counter_total\tcounter\t0\tLONG\t{ \"label0\" : \"B\", \"label1\" : \"Y\" }\n" +
                        "questdb_counter_total\tcounter\t0\tLONG\t{ \"label0\" : \"B\", \"label1\" : \"Z\" }\n" +
                        "questdb_counter_total\tcounter\t0\tLONG\t{ \"label0\" : \"C\", \"label1\" : \"X\" }\n" +
                        "questdb_counter_total\tcounter\t1\tLONG\t{ \"label0\" : \"C\", \"label1\" : \"Y\" }\n" +
                        "questdb_counter_total\tcounter\t0\tLONG\t{ \"label0\" : \"C\", \"label1\" : \"Z\" }\n",
                metricsRegistry);
    }

    @Test
    public void testCounterWithoutLabels() {
        MetricsRegistry metricsRegistry = new MetricsRegistryImpl();
        Counter counter = metricsRegistry.newCounter("counter");

        counter.inc();

        assertPrometheusMetrics("name\ttype\tvalue\tkind\tlabels\n" +
                        "questdb_counter_total\tcounter\t1\tLONG\t\n",
                metricsRegistry);
    }

    @Test
    public void testGauge() {
        MetricsRegistry metricsRegistry = new MetricsRegistryImpl();
        LongGauge gauge = metricsRegistry.newLongGauge("gauge");

        gauge.inc();
        gauge.inc();

        assertPrometheusMetrics("name\ttype\tvalue\tkind\tlabels\n" +
                        "gauge\tgauge\t2\tLONG\t\n",
                metricsRegistry);

        gauge.dec();

        assertPrometheusMetrics("name\ttype\tvalue\tkind\tlabels\n" +
                        "gauge\tgauge\t1\tLONG\t\n",
                metricsRegistry);
    }

    @Test
    public void testNullCounter() {
        MetricsRegistry metricsRegistry = new NullMetricsRegistry();
        Counter counter = metricsRegistry.newCounter("counter");

        counter.inc();
        assertPrometheusMetrics("name\ttype\tvalue\tkind\tlabels\n",
                metricsRegistry);
    }

    @Test
    public void testNullGauge() {
        MetricsRegistry metricsRegistry = new NullMetricsRegistry();
        LongGauge gauge = metricsRegistry.newLongGauge("gauge");

        gauge.inc();
        gauge.inc();
        assertPrometheusMetrics("name\ttype\tvalue\tkind\tlabels\n",
                metricsRegistry);

        gauge.dec();
        assertPrometheusMetrics("name\ttype\tvalue\tkind\tlabels\n",
                metricsRegistry);
    }

    public void assertPrometheusMetrics(CharSequence expected, MetricsRegistry metricsRegistry) {
        try (PrometheusMetricsRecordCursorFactory.PrometheusMetricsCursor cursor = new PrometheusMetricsRecordCursorFactory.PrometheusMetricsCursor()) {
            cursor.of(metricsRegistry);
            printSql(cursor, PrometheusMetricsRecordCursorFactory.METADATA);
        } catch (SqlException e) {
            throw new RuntimeException(e);
        }
        TestUtils.assertEquals(expected, sink);
    }
}
