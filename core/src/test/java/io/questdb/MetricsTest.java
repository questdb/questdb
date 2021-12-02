/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb;

import io.questdb.metrics.*;
import io.questdb.std.MemoryTag;
import io.questdb.std.str.CharSink;
import org.hamcrest.MatcherAssert;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.hamcrest.CoreMatchers.*;

public class MetricsTest {

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
            MatcherAssert.assertThat(metricsRegistry.getMetricNames(), hasItem("memory_tag_" + MemoryTag.nameOf(i)));
        }

        MatcherAssert.assertThat(metricsRegistry.getMetricNames(), hasItem("memory_free_count"));
        MatcherAssert.assertThat(metricsRegistry.getMetricNames(), hasItem("memory_mem_used"));
        MatcherAssert.assertThat(metricsRegistry.getMetricNames(), hasItem("memory_malloc_count"));
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
    public void testMetricUniqueness() {
        SpyingMetricsRegistry metricsRegistry = new SpyingMetricsRegistry();
        new Metrics(true, metricsRegistry);
        Set<CharSequence> notUniqueMetrics = metricsRegistry.getNotUniqueMetrics();
        Assert.assertTrue("Metrics with non-unique names: " + notUniqueMetrics, notUniqueMetrics.isEmpty());
    }

    @Test
    public void testLabelUniqueness() {
        SpyingMetricsRegistry metricsRegistry = new SpyingMetricsRegistry();
        new Metrics(true, metricsRegistry);
        Set<CharSequence> metricsWithNotUniqueLabels = metricsRegistry.getMetricsWithNotUniqueLabels();
        Assert.assertTrue("Metrics with non-unique labels: " + metricsWithNotUniqueLabels, metricsWithNotUniqueLabels.isEmpty());
    }

    private static class SpyingMetricsRegistry implements MetricsRegistry {
        private final MetricsRegistry delegate = new NullMetricsRegistry();
        private final Set<CharSequence> metricNames = new HashSet<>();
        private final Set<CharSequence> labelNames = new HashSet<>();
        private final Set<CharSequence> metricsWithNotUniqueLabels = new HashSet<>();
        private final Set<CharSequence> notUniqueMetrics = new HashSet<>();

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
        public CounterWithTwoLabels newCounter(CharSequence name,
                                               CharSequence labelName0, CharSequence[] labelValues0,
                                               CharSequence labelName1, CharSequence[] labelValues1) {
            addMetricName(name);
            addLabelNames(name, Arrays.asList(labelName0, labelName1));
            return delegate.newCounter(name, labelName0, labelValues0, labelName1, labelValues1);
        }

        @Override
        public Gauge newGauge(CharSequence name) {
            addMetricName(name);
            return delegate.newGauge(name);
        }

        @Override
        public Gauge newGauge(int memoryTag) {
            addMetricName("memory_tag_" + MemoryTag.nameOf(memoryTag));
            Gauge gauge = delegate.newGauge(memoryTag);
            return gauge;
        }

        @Override
        public Gauge newVirtualGauge(CharSequence name, VirtualGauge.StatProvider provider) {
            addMetricName(name);
            return delegate.newVirtualGauge(name, provider);
        }

        @Override
        public void scrapeIntoPrometheus(CharSink sink) {
            delegate.scrapeIntoPrometheus(sink);
        }

        public Set<CharSequence> getMetricNames() {
            return metricNames;
        }

        public Set<CharSequence> getLabelNames() {
            return labelNames;
        }

        public Set<CharSequence> getMetricsWithNotUniqueLabels() {
            return metricsWithNotUniqueLabels;
        }

        public Set<CharSequence> getNotUniqueMetrics() {
            return notUniqueMetrics;
        }

        private void addMetricName(CharSequence name) {
            if (metricNames.contains(name)) {
                notUniqueMetrics.add(name);
            } else {
                metricNames.add(name);
            }
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
    }
}
