/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb.test;

import io.questdb.Metrics;
import io.questdb.metrics.*;
import io.questdb.std.MemoryTag;
import io.questdb.std.str.BorrowableUtf8Sink;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MetricsTest {

    @Test
    public void testGetMetricsRegistry() {
        final NullMetricsRegistry registry = new NullMetricsRegistry();
        final Metrics metrics = new Metrics(true, registry);
        Assert.assertNotNull(metrics.getRegistry());
        Assert.assertSame(registry, metrics.getRegistry());
    }

    @Test
    public void testLabelNames() {
        Pattern labelNamePattern = Pattern.compile("[a-zA-Z0-9_]*");
        SpyingMetricsRegistry metricsRegistry = new SpyingMetricsRegistry();
        new Metrics(true, metricsRegistry);
        for (CharSequence name : metricsRegistry.getLabelNames()) {
            Matcher matcher = labelNamePattern.matcher(name);
            Assert.assertTrue("Invalid label name: " + name, matcher.matches());
        }
    }

    @Test
    public void testLabelUniqueness() {
        SpyingMetricsRegistry metricsRegistry = new SpyingMetricsRegistry();
        new Metrics(true, metricsRegistry);
        Set<CharSequence> metricsWithNotUniqueLabels = metricsRegistry.getMetricsWithNotUniqueLabels();
        Assert.assertTrue("Metrics with non-unique labels: " + metricsWithNotUniqueLabels, metricsWithNotUniqueLabels.isEmpty());
    }

    @Test
    public void testMetricNames() {
        Pattern metricNamePattern = Pattern.compile("[a-zA-Z0-9_]*");
        SpyingMetricsRegistry metricsRegistry = new SpyingMetricsRegistry();
        new Metrics(true, metricsRegistry);
        for (CharSequence name : metricsRegistry.getMetricNames()) {
            Matcher matcher = metricNamePattern.matcher(name);
            Assert.assertTrue("Invalid metric name: " + name, matcher.matches());
        }
    }

    @Test
    public void testMetricNamesContainAllMemoryTags() {
        SpyingMetricsRegistry metricsRegistry = new SpyingMetricsRegistry();
        new Metrics(true, metricsRegistry);

        for (int i = 0; i < MemoryTag.SIZE; i++) {
            Assert.assertTrue(metricsRegistry.getMetricNames().contains("memory_tag_" + MemoryTag.nameOf(i)));
        }

        Assert.assertTrue(metricsRegistry.getMetricNames().contains("memory_free_count"));
        Assert.assertTrue(metricsRegistry.getMetricNames().contains("memory_mem_used"));
        Assert.assertTrue(metricsRegistry.getMetricNames().contains("memory_malloc_count"));
        Assert.assertTrue(metricsRegistry.getMetricNames().contains("memory_jvm_free"));
        Assert.assertTrue(metricsRegistry.getMetricNames().contains("memory_jvm_total"));
        Assert.assertTrue(metricsRegistry.getMetricNames().contains("memory_jvm_max"));
    }

    @Test
    public void testMetricNamesContainGCMetrics() {
        final Metrics metrics = Metrics.enabled();

        final DirectUtf8Sink sink = new DirectUtf8Sink(32);
        metrics.scrapeIntoPrometheus(sink);

        final String encoded = sink.toString();
        TestUtils.assertContains(encoded, "jvm_major_gc_count");
        TestUtils.assertContains(encoded, "jvm_major_gc_time");
        TestUtils.assertContains(encoded, "jvm_minor_gc_count");
        TestUtils.assertContains(encoded, "jvm_minor_gc_time");
        TestUtils.assertContains(encoded, "jvm_unknown_gc_count");
        TestUtils.assertContains(encoded, "jvm_unknown_gc_time");
    }

    @Test
    public void testMetricUniqueness() {
        SpyingMetricsRegistry metricsRegistry = new SpyingMetricsRegistry();
        new Metrics(true, metricsRegistry);
        Set<CharSequence> notUniqueMetrics = metricsRegistry.getNotUniqueMetrics();
        Assert.assertTrue("Metrics with non-unique names: " + notUniqueMetrics, notUniqueMetrics.isEmpty());
    }

    private static class SpyingMetricsRegistry implements MetricsRegistry {
        private final MetricsRegistry delegate = new NullMetricsRegistry();
        private final Set<CharSequence> labelNames = new HashSet<>();
        private final Set<CharSequence> metricNames = new HashSet<>();
        private final Set<CharSequence> metricsWithNotUniqueLabels = new HashSet<>();
        private final Set<CharSequence> notUniqueMetrics = new HashSet<>();

        @Override
        public void addScrapable(Scrapable scrapable) {
            delegate.addScrapable(scrapable);
        }

        public Set<CharSequence> getLabelNames() {
            return labelNames;
        }

        public Set<CharSequence> getMetricNames() {
            return metricNames;
        }

        public Set<CharSequence> getMetricsWithNotUniqueLabels() {
            return metricsWithNotUniqueLabels;
        }

        public Set<CharSequence> getNotUniqueMetrics() {
            return notUniqueMetrics;
        }

        @Override
        public Counter newCounter(CharSequence name) {
            addMetricName(name);
            return delegate.newCounter(name);
        }

        @Override
        public CounterWithOneLabel newCounter(CharSequence name, CharSequence labelName0, CharSequence[] labelValues0) {
            addMetricName(name);
            addLabelNames(name, Collections.singletonList(labelName0));
            return delegate.newCounter(name, labelName0, labelValues0);
        }

        @Override
        public CounterWithTwoLabels newCounter(
                CharSequence name,
                CharSequence labelName0, CharSequence[] labelValues0,
                CharSequence labelName1, CharSequence[] labelValues1
        ) {
            addMetricName(name);
            addLabelNames(name, Arrays.asList(labelName0, labelName1));
            return delegate.newCounter(name, labelName0, labelValues0, labelName1, labelValues1);
        }

        @Override
        public DoubleGauge newDoubleGauge(CharSequence name) {
            addMetricName(name);
            return delegate.newDoubleGauge(name);
        }

        @Override
        public LongGauge newLongGauge(CharSequence name) {
            addMetricName(name);
            return delegate.newLongGauge(name);
        }

        @Override
        public LongGauge newLongGauge(int memoryTag) {
            addMetricName("memory_tag_" + MemoryTag.nameOf(memoryTag));
            return delegate.newLongGauge(memoryTag);
        }

        @Override
        public LongGauge newVirtualGauge(CharSequence name, VirtualLongGauge.StatProvider provider) {
            addMetricName(name);
            return delegate.newVirtualGauge(name, provider);
        }

        @Override
        public void scrapeIntoPrometheus(@NotNull BorrowableUtf8Sink sink) {
            delegate.scrapeIntoPrometheus(sink);
        }

        private void addLabelNames(CharSequence metricName, List<CharSequence> labels) {
            Set<CharSequence> uniqueLabels = new HashSet<>();
            for (CharSequence label : labels) {
                if (uniqueLabels.contains(label)) {
                    metricsWithNotUniqueLabels.add(metricName);
                } else {
                    uniqueLabels.add(label);
                }
                labelNames.add(label);
            }
        }

        private void addMetricName(CharSequence name) {
            if (metricNames.contains(name)) {
                notUniqueMetrics.add(name);
            } else {
                metricNames.add(name);
            }
        }
    }
}
