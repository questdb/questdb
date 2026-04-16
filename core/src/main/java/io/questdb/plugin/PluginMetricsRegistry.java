/*******************************************************************************
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

package io.questdb.plugin;

import io.questdb.metrics.Counter;
import io.questdb.metrics.CounterWithOneLabel;
import io.questdb.metrics.CounterWithTwoLabels;
import io.questdb.metrics.DoubleGauge;
import io.questdb.metrics.LongGauge;

/**
 * Plugin-scoped metrics registry. Metrics created through this registry
 * are automatically prefixed with the plugin name and scraped alongside
 * QuestDB's built-in metrics at the {@code /metrics} endpoint.
 * <p>
 * All metrics become no-ops after the plugin is unloaded: they stop
 * appearing in scrape output, but holding a stale reference does not
 * cause errors.
 */
public interface PluginMetricsRegistry {

    /**
     * Creates a new counter metric.
     *
     * @param name the metric name (will be prefixed with the plugin name)
     * @return the counter
     */
    Counter newCounter(CharSequence name);

    /**
     * Creates a new counter metric with one label dimension.
     *
     * @param name         the metric name (will be prefixed with the plugin name)
     * @param labelName0   the label name
     * @param labelValues0 the possible label values
     * @return the labeled counter
     */
    CounterWithOneLabel newCounter(CharSequence name, CharSequence labelName0, CharSequence[] labelValues0);

    /**
     * Creates a new counter metric with two label dimensions.
     *
     * @param name         the metric name (will be prefixed with the plugin name)
     * @param labelName0   first label name
     * @param labelValues0 first label's possible values
     * @param labelName1   second label name
     * @param labelValues1 second label's possible values
     * @return the labeled counter
     */
    CounterWithTwoLabels newCounter(
            CharSequence name,
            CharSequence labelName0,
            CharSequence[] labelValues0,
            CharSequence labelName1,
            CharSequence[] labelValues1
    );

    /**
     * Creates a new double gauge metric.
     *
     * @param name the metric name (will be prefixed with the plugin name)
     * @return the gauge
     */
    DoubleGauge newDoubleGauge(CharSequence name);

    /**
     * Creates a new long gauge metric.
     *
     * @param name the metric name (will be prefixed with the plugin name)
     * @return the gauge
     */
    LongGauge newLongGauge(CharSequence name);
}
