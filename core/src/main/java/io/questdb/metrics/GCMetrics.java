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

package io.questdb.metrics;

import io.questdb.griffin.engine.table.PrometheusMetricsRecordCursorFactory.PrometheusMetricsRecord;
import io.questdb.std.CharSequenceHashSet;
import io.questdb.std.LongList;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.str.BorrowableUtf8Sink;
import io.questdb.std.str.CharSink;
import org.jetbrains.annotations.NotNull;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;

/**
 * GC metrics don't rely on MetricsRegistry to be able to obtain and write all metrics
 * to the sink in one go.
 */
public class GCMetrics implements Target, Mutable {

    private static final int MAJOR_COUNT = 0;
    private static final int MAJOR_TIME = 1;
    private static final int MINOR_COUNT = 2;
    private static final int MINOR_TIME = 3;
    private static final int UNKNOWN_COUNT = 4;
    private static final int UNKNOWN_TIME = 5;
    private static final CharSequenceHashSet majorGCNames = new CharSequenceHashSet();
    private static final LongList metrics = new LongList(6);
    private static final CharSequenceHashSet minorGCNames = new CharSequenceHashSet();
    private static final ObjList<String> names = new ObjList<>("jvm_major_gc_count", "jvm_major_gc_time", "jvm_minor_gc_count", "jvm_minor_gc_time", "jvm_unknown_gc_count", "jvm_unknown_gc_time");


    @Override
    public void clear() {
    }

    @Override
    public void scrapeIntoPrometheus(@NotNull BorrowableUtf8Sink sink) {
        long majorCount = 0;
        long majorTime = 0;
        long minorCount = 0;
        long minorTime = 0;
        long unknownCount = 0;
        long unknownTime = 0;

        for (GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans()) {
            long count = gc.getCollectionCount();
            if (count > -1) {
                if (majorGCNames.contains(gc.getName())) {
                    majorCount += count;
                    majorTime += gc.getCollectionTime();
                } else if (minorGCNames.contains(gc.getName())) {
                    minorCount += count;
                    minorTime += gc.getCollectionTime();
                } else {
                    unknownCount += count;
                    unknownTime += gc.getCollectionTime();
                }
            }
        }

        appendCounter(sink, majorCount, "jvm_major_gc_count");
        appendCounter(sink, majorTime, "jvm_major_gc_time");
        appendCounter(sink, minorCount, "jvm_minor_gc_count");
        appendCounter(sink, minorTime, "jvm_minor_gc_time");
        appendCounter(sink, unknownCount, "jvm_unknown_gc_count");
        appendCounter(sink, unknownTime, "jvm_unknown_gc_time");
    }

    @Override
    public void scrapeIntoRecord(PrometheusMetricsRecord record, int label) {
        record
                .setCounterName(names.getQuick(label))
                .setType("counter")
                .setValue(metrics.getQuick(label))
                .setKind("LONG");
    }

    // Uses a separate impl to avoid interfering concurrently with the metrics endpoint
    // This can race if multiple `prometheus_metrics()` queries scrape GCMetrics at the same time.
    // However, it being fast may be preferable to adding locking behaviour
    @Override
    public int scrapeIntoRecord(PrometheusMetricsRecord record) {
        snapshotGcMetrics();
        scrapeIntoRecord(record, 0);
        return metrics.size();
    }

    public synchronized void snapshotGcMetrics() {
        clearMetrics();
        for (GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans()) {
            long count = gc.getCollectionCount();
            if (count > -1) {
                if (majorGCNames.contains(gc.getName())) {
                    metrics.increment(MAJOR_COUNT, count);
                    metrics.increment(MAJOR_TIME, gc.getCollectionTime());
                } else if (minorGCNames.contains(gc.getName())) {
                    metrics.increment(MINOR_COUNT, count);
                    metrics.increment(MINOR_TIME, gc.getCollectionTime());
                } else {
                    metrics.increment(UNKNOWN_COUNT, count);
                    metrics.increment(UNKNOWN_TIME, gc.getCollectionTime());
                }
            }
        }
    }

    private void appendCounter(CharSink<?> sink, long value, String name) {
        PrometheusFormatUtils.appendCounterType(name, sink);
        PrometheusFormatUtils.appendCounterNamePrefix(name, sink);
        PrometheusFormatUtils.appendSampleLineSuffix(sink, value);
        PrometheusFormatUtils.appendNewLine(sink);
    }

    private void clearMetrics() {
        metrics.setAll(6, 0);
    }

    static {
        // Hotspot
        majorGCNames.add("PS MarkSweep");
        majorGCNames.add("ConcurrentMarkSweep");
        majorGCNames.add("G1 Old Generation");
        majorGCNames.add("G1 Mixed Generation");
        majorGCNames.add("MarkSweepCompact");
        majorGCNames.add("Shenandoah Pauses");
        // OpenJ9
        majorGCNames.add("global");
        majorGCNames.add("global garbage collect");

        // Hotspot
        minorGCNames.add("PS Scavenge");
        minorGCNames.add("ParNew");
        minorGCNames.add("G1 Young Generation");
        minorGCNames.add("Copy");
        minorGCNames.add("ZGC");
        minorGCNames.add("Shenandoah Cycles");
        // OpenJ9
        minorGCNames.add("partial gc");
        minorGCNames.add("scavenge");
    }
}
