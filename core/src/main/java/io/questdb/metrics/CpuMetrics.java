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

package io.questdb.metrics;

import com.sun.management.OperatingSystemMXBean;
import io.questdb.std.str.BorrowableUtf8Sink;
import io.questdb.std.str.CharSink;
import org.jetbrains.annotations.NotNull;

import java.lang.management.ManagementFactory;

/**
 * CPU usage of the whole machine and of the QuestDB process, in percent (0-100) of all
 * CPUs. The JVM computes each value over the time since the previous read, so it is
 * the average since the last scrape or snapshot. NaN when the platform does not
 * report it.
 */
public class CpuMetrics implements Target {
    private static final OperatingSystemMXBean OS_BEAN = ManagementFactory.getPlatformMXBean(OperatingSystemMXBean.class);

    @Override
    public void scrapeIntoPrometheus(@NotNull BorrowableUtf8Sink sink) {
        appendGauge(sink, "cpu_box_percent", getBoxPercent());
        appendGauge(sink, "cpu_questdb_percent", getQuestdbPercent());
    }

    @Override
    public void snapshot(MetricSnapshotVisitor visitor) {
        visitor.visitDouble("cpu_box_percent", getBoxPercent());
        visitor.visitDouble("cpu_questdb_percent", getQuestdbPercent());
    }

    private static void appendGauge(CharSink<?> sink, String name, double value) {
        sink.putAscii(PrometheusFormatUtils.TYPE_PREFIX);
        sink.put(name);
        sink.putAscii(" gauge\n");
        sink.putAscii(PrometheusFormatUtils.METRIC_NAME_PREFIX);
        sink.put(name);
        PrometheusFormatUtils.appendSampleLineSuffix(sink, value);
        PrometheusFormatUtils.appendNewLine(sink);
    }

    private static double getBoxPercent() {
        return OS_BEAN != null ? toPercent(OS_BEAN.getCpuLoad()) : Double.NaN;
    }

    private static double getQuestdbPercent() {
        return OS_BEAN != null ? toPercent(OS_BEAN.getProcessCpuLoad()) : Double.NaN;
    }

    private static double toPercent(double load) {
        return load < 0 ? Double.NaN : load * 100;
    }
}
