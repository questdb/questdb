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

import io.questdb.mp.continuation.FiberRuntime;
import io.questdb.mp.continuation.LaunchResult;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.str.BorrowableUtf8Sink;
import org.jetbrains.annotations.NotNull;

public final class FiberMetrics implements Target, Mutable {
    private static final String BUDGET_EXHAUSTION_NAME = "worker_pool_fiber_mount_budget_exhaustion";
    private static final String CREATED_NAME = "worker_pool_fiber_created";
    private static final String FINALIZING_NAME = "worker_pool_fiber_finalizing";
    private static final String INLINE_SUSPEND_VIOLATION_NAME = "worker_pool_fiber_inline_suspend_violation";
    private static final String LABEL_RESULT = "result";
    private static final String LABEL_ROUTE = "route";
    private static final String LABEL_SOURCE = "source";
    private static final String LABEL_WORKER_POOL = "worker_pool";
    private static final String LAUNCH_NAME = "worker_pool_fiber_launch";
    private static final String LIVE_NAME = "worker_pool_fiber_live";
    private static final String MAX_LIVE_NAME = "worker_pool_fiber_max_live";
    private static final String MOUNT_NAME = "worker_pool_fiber_mount";
    private static final String MOUNTED_NAME = "worker_pool_fiber_mounted";
    private static final String ORPHAN_RECOVERY_NAME = "worker_pool_fiber_orphan_recovery";
    private static final String ORPHANED_SHARD_NAME = "worker_pool_fiber_orphaned_shard";
    private static final String OUTSTANDING_NAME = "worker_pool_fiber_outstanding";
    private static final String PARKED_NAME = "worker_pool_fiber_parked";
    private static final String QUEUED_NAME = "worker_pool_fiber_queued";
    private static final String RETAINED_NAME = "worker_pool_fiber_retained";
    private static final String RETIRED_NAME = "worker_pool_fiber_retired";
    private static final String SATURATION_NAME = "worker_pool_fiber_saturation";
    private static final String SCHEDULER_PUBLICATION_NAME = "worker_pool_fiber_scheduler_publication";
    private static final String SCHEDULER_SELECTION_NAME = "worker_pool_fiber_scheduler_selection";
    private static final String WAKE_NAME = "worker_pool_fiber_wake";
    private final ObjList<Entry> entries = new ObjList<>();

    @Override
    public synchronized void clear() {
        for (int i = 0, n = entries.size(); i < n; i++) {
            entries.getQuick(i).resetBaselines();
        }
    }

    public synchronized void register(String poolName, FiberRuntime runtime) {
        for (int i = 0, n = entries.size(); i < n; i++) {
            if (entries.getQuick(i).runtime == runtime) {
                return;
            }
        }
        entries.add(new Entry(poolName, runtime));
    }

    @Override
    public synchronized void scrapeIntoPrometheus(@NotNull BorrowableUtf8Sink sink) {
        if (entries.size() == 0) {
            return;
        }

        appendGaugeType(sink, LIVE_NAME);
        for (int i = 0, n = entries.size(); i < n; i++) {
            final Entry entry = entries.getQuick(i);
            appendGaugeSample(sink, LIVE_NAME, entry.poolName, entry.runtime.getLiveFiberCount());
        }
        PrometheusFormatUtils.appendNewLine(sink);

        appendGaugeType(sink, MAX_LIVE_NAME);
        for (int i = 0, n = entries.size(); i < n; i++) {
            final Entry entry = entries.getQuick(i);
            appendGaugeSample(sink, MAX_LIVE_NAME, entry.poolName, entry.runtime.getMaxLiveFiberCount());
        }
        PrometheusFormatUtils.appendNewLine(sink);

        appendGaugeType(sink, OUTSTANDING_NAME);
        for (int i = 0, n = entries.size(); i < n; i++) {
            final Entry entry = entries.getQuick(i);
            appendGaugeSample(sink, OUTSTANDING_NAME, entry.poolName, entry.runtime.getOutstandingTaskCount());
        }
        PrometheusFormatUtils.appendNewLine(sink);

        appendGaugeType(sink, QUEUED_NAME);
        for (int i = 0, n = entries.size(); i < n; i++) {
            final Entry entry = entries.getQuick(i);
            appendGaugeSample(sink, QUEUED_NAME, entry.poolName, entry.runtime.getQueuedCount());
        }
        PrometheusFormatUtils.appendNewLine(sink);

        appendGaugeType(sink, MOUNTED_NAME);
        for (int i = 0, n = entries.size(); i < n; i++) {
            final Entry entry = entries.getQuick(i);
            appendGaugeSample(sink, MOUNTED_NAME, entry.poolName, entry.runtime.getMountedCount());
        }
        PrometheusFormatUtils.appendNewLine(sink);

        appendGaugeType(sink, FINALIZING_NAME);
        for (int i = 0, n = entries.size(); i < n; i++) {
            final Entry entry = entries.getQuick(i);
            appendGaugeSample(sink, FINALIZING_NAME, entry.poolName, entry.runtime.getFinalizerCount());
        }
        PrometheusFormatUtils.appendNewLine(sink);

        appendGaugeType(sink, PARKED_NAME);
        for (int i = 0, n = entries.size(); i < n; i++) {
            final Entry entry = entries.getQuick(i);
            appendGaugeSample(sink, PARKED_NAME, entry.poolName, entry.runtime.getParkedFiberCount());
        }
        PrometheusFormatUtils.appendNewLine(sink);

        appendGaugeType(sink, RETAINED_NAME);
        for (int i = 0, n = entries.size(); i < n; i++) {
            final Entry entry = entries.getQuick(i);
            appendGaugeSample(sink, RETAINED_NAME, entry.poolName, entry.runtime.getRetainedFiberCount());
        }
        PrometheusFormatUtils.appendNewLine(sink);

        PrometheusFormatUtils.appendCounterType(CREATED_NAME, sink);
        for (int i = 0, n = entries.size(); i < n; i++) {
            final Entry entry = entries.getQuick(i);
            appendCounterSample(
                    sink,
                    CREATED_NAME,
                    entry.poolName,
                    entry.runtime.getCreatedFiberCount() - entry.createdBaseline
            );
        }
        PrometheusFormatUtils.appendNewLine(sink);

        PrometheusFormatUtils.appendCounterType(RETIRED_NAME, sink);
        for (int i = 0, n = entries.size(); i < n; i++) {
            final Entry entry = entries.getQuick(i);
            appendCounterSample(
                    sink,
                    RETIRED_NAME,
                    entry.poolName,
                    entry.runtime.getRetiredFiberCount() - entry.retiredBaseline
            );
        }
        PrometheusFormatUtils.appendNewLine(sink);

        PrometheusFormatUtils.appendCounterType(MOUNT_NAME, sink);
        for (int i = 0, n = entries.size(); i < n; i++) {
            final Entry entry = entries.getQuick(i);
            appendCounterSample(
                    sink,
                    MOUNT_NAME,
                    entry.poolName,
                    entry.runtime.getMountCount() - entry.mountBaseline
            );
        }
        PrometheusFormatUtils.appendNewLine(sink);

        PrometheusFormatUtils.appendCounterType(SATURATION_NAME, sink);
        for (int i = 0, n = entries.size(); i < n; i++) {
            final Entry entry = entries.getQuick(i);
            appendCounterSample(
                    sink,
                    SATURATION_NAME,
                    entry.poolName,
                    entry.runtime.getSaturationCount() - entry.saturationBaseline
            );
        }
        PrometheusFormatUtils.appendNewLine(sink);

        PrometheusFormatUtils.appendCounterType(BUDGET_EXHAUSTION_NAME, sink);
        for (int i = 0, n = entries.size(); i < n; i++) {
            final Entry entry = entries.getQuick(i);
            appendCounterSample(
                    sink,
                    BUDGET_EXHAUSTION_NAME,
                    entry.poolName,
                    entry.runtime.getBudgetExhaustionCount() - entry.budgetExhaustionBaseline
            );
        }
        PrometheusFormatUtils.appendNewLine(sink);

        PrometheusFormatUtils.appendCounterType(INLINE_SUSPEND_VIOLATION_NAME, sink);
        for (int i = 0, n = entries.size(); i < n; i++) {
            final Entry entry = entries.getQuick(i);
            appendCounterSample(
                    sink,
                    INLINE_SUSPEND_VIOLATION_NAME,
                    entry.poolName,
                    entry.runtime.getInlineSuspendViolationCount() - entry.inlineSuspendViolationBaseline
            );
        }
        PrometheusFormatUtils.appendNewLine(sink);

        PrometheusFormatUtils.appendCounterType(SCHEDULER_PUBLICATION_NAME, sink);
        for (int i = 0, n = entries.size(); i < n; i++) {
            final Entry entry = entries.getQuick(i);
            appendLabeledCounterSample(
                    sink,
                    SCHEDULER_PUBLICATION_NAME,
                    entry.poolName,
                    LABEL_ROUTE,
                    "owner_local",
                    entry.runtime.getLocalPublicationCount() - entry.localPublicationBaseline
            );
            appendLabeledCounterSample(
                    sink,
                    SCHEDULER_PUBLICATION_NAME,
                    entry.poolName,
                    LABEL_ROUTE,
                    "global",
                    entry.runtime.getGlobalPublicationCount() - entry.globalPublicationBaseline
            );
            appendLabeledCounterSample(
                    sink,
                    SCHEDULER_PUBLICATION_NAME,
                    entry.poolName,
                    LABEL_ROUTE,
                    "local_fallback",
                    entry.runtime.getLocalFallbackPublicationCount() - entry.localFallbackPublicationBaseline
            );
        }
        PrometheusFormatUtils.appendNewLine(sink);

        PrometheusFormatUtils.appendCounterType(SCHEDULER_SELECTION_NAME, sink);
        for (int i = 0, n = entries.size(); i < n; i++) {
            final Entry entry = entries.getQuick(i);
            appendLabeledCounterSample(
                    sink,
                    SCHEDULER_SELECTION_NAME,
                    entry.poolName,
                    LABEL_SOURCE,
                    "owner_local",
                    entry.runtime.getLocalSelectionCount() - entry.localSelectionBaseline
            );
            appendLabeledCounterSample(
                    sink,
                    SCHEDULER_SELECTION_NAME,
                    entry.poolName,
                    LABEL_SOURCE,
                    "global",
                    entry.runtime.getGlobalSelectionCount() - entry.globalSelectionBaseline
            );
            appendLabeledCounterSample(
                    sink,
                    SCHEDULER_SELECTION_NAME,
                    entry.poolName,
                    LABEL_SOURCE,
                    "stolen_local",
                    entry.runtime.getStolenSelectionCount() - entry.stolenSelectionBaseline
            );
        }
        PrometheusFormatUtils.appendNewLine(sink);

        PrometheusFormatUtils.appendCounterType(WAKE_NAME, sink);
        for (int i = 0, n = entries.size(); i < n; i++) {
            final Entry entry = entries.getQuick(i);
            appendCounterSample(
                    sink,
                    WAKE_NAME,
                    entry.poolName,
                    entry.runtime.getWakeClaimCount() - entry.wakeBaseline
            );
        }
        PrometheusFormatUtils.appendNewLine(sink);

        PrometheusFormatUtils.appendCounterType(ORPHANED_SHARD_NAME, sink);
        for (int i = 0, n = entries.size(); i < n; i++) {
            final Entry entry = entries.getQuick(i);
            appendCounterSample(
                    sink,
                    ORPHANED_SHARD_NAME,
                    entry.poolName,
                    entry.runtime.getOrphanedShardTransitionCount() - entry.orphanedShardBaseline
            );
        }
        PrometheusFormatUtils.appendNewLine(sink);

        PrometheusFormatUtils.appendCounterType(ORPHAN_RECOVERY_NAME, sink);
        for (int i = 0, n = entries.size(); i < n; i++) {
            final Entry entry = entries.getQuick(i);
            appendCounterSample(
                    sink,
                    ORPHAN_RECOVERY_NAME,
                    entry.poolName,
                    entry.runtime.getOrphanedEntryRecoveryCount() - entry.orphanRecoveryBaseline
            );
        }
        PrometheusFormatUtils.appendNewLine(sink);

        PrometheusFormatUtils.appendCounterType(LAUNCH_NAME, sink);
        for (int i = 0, n = entries.size(); i < n; i++) {
            final Entry entry = entries.getQuick(i);
            for (int resultIndex = 0; resultIndex < LaunchResult.COUNT; resultIndex++) {
                final LaunchResult result = LaunchResult.get(resultIndex);
                appendLaunchCounterSample(
                        sink,
                        entry.poolName,
                        result.getMetricLabel(),
                        entry.runtime.getLaunchCount(result) - entry.launchBaselines[resultIndex]
                );
            }
        }
        PrometheusFormatUtils.appendNewLine(sink);
    }

    public synchronized void unregister(FiberRuntime runtime) {
        for (int i = 0, n = entries.size(); i < n; i++) {
            if (entries.getQuick(i).runtime == runtime) {
                entries.remove(i);
                return;
            }
        }
    }

    private static void appendCounterSample(
            BorrowableUtf8Sink sink,
            CharSequence name,
            CharSequence poolName,
            long value
    ) {
        PrometheusFormatUtils.appendCounterNamePrefix(name, sink);
        appendPoolLabel(sink, poolName);
        PrometheusFormatUtils.appendSampleLineSuffix(sink, value);
    }

    private static void appendGaugeSample(
            BorrowableUtf8Sink sink,
            CharSequence name,
            CharSequence poolName,
            long value
    ) {
        sink.putAscii(PrometheusFormatUtils.METRIC_NAME_PREFIX);
        sink.putAscii(name);
        appendPoolLabel(sink, poolName);
        PrometheusFormatUtils.appendSampleLineSuffix(sink, value);
    }

    private static void appendGaugeType(BorrowableUtf8Sink sink, CharSequence name) {
        sink.putAscii(PrometheusFormatUtils.TYPE_PREFIX);
        sink.putAscii(name);
        sink.putAscii(" gauge\n");
    }

    private static void appendLaunchCounterSample(
            BorrowableUtf8Sink sink,
            CharSequence poolName,
            CharSequence result,
            long value
    ) {
        PrometheusFormatUtils.appendCounterNamePrefix(LAUNCH_NAME, sink);
        sink.putAscii('{');
        PrometheusFormatUtils.appendLabel(sink, LABEL_WORKER_POOL, poolName);
        sink.putAscii(',');
        PrometheusFormatUtils.appendLabel(sink, LABEL_RESULT, result);
        sink.putAscii('}');
        PrometheusFormatUtils.appendSampleLineSuffix(sink, value);
    }

    private static void appendLabeledCounterSample(
            BorrowableUtf8Sink sink,
            CharSequence name,
            CharSequence poolName,
            CharSequence labelName,
            CharSequence labelValue,
            long value
    ) {
        PrometheusFormatUtils.appendCounterNamePrefix(name, sink);
        sink.putAscii('{');
        PrometheusFormatUtils.appendLabel(sink, LABEL_WORKER_POOL, poolName);
        sink.putAscii(',');
        PrometheusFormatUtils.appendLabel(sink, labelName, labelValue);
        sink.putAscii('}');
        PrometheusFormatUtils.appendSampleLineSuffix(sink, value);
    }

    private static void appendPoolLabel(BorrowableUtf8Sink sink, CharSequence poolName) {
        sink.putAscii('{');
        PrometheusFormatUtils.appendLabel(sink, LABEL_WORKER_POOL, poolName);
        sink.putAscii('}');
    }

    private static final class Entry {
        private long budgetExhaustionBaseline;
        private long createdBaseline;
        private long globalPublicationBaseline;
        private long globalSelectionBaseline;
        private long inlineSuspendViolationBaseline;
        private final long[] launchBaselines = new long[LaunchResult.COUNT];
        private long localFallbackPublicationBaseline;
        private long localPublicationBaseline;
        private long localSelectionBaseline;
        private long mountBaseline;
        private long orphanRecoveryBaseline;
        private long orphanedShardBaseline;
        private final String poolName;
        private long retiredBaseline;
        private final FiberRuntime runtime;
        private long saturationBaseline;
        private long stolenSelectionBaseline;
        private long wakeBaseline;

        private Entry(String poolName, FiberRuntime runtime) {
            this.poolName = poolName;
            this.runtime = runtime;
            resetBaselines();
        }

        private void resetBaselines() {
            budgetExhaustionBaseline = runtime.getBudgetExhaustionCount();
            createdBaseline = runtime.getCreatedFiberCount();
            globalPublicationBaseline = runtime.getGlobalPublicationCount();
            globalSelectionBaseline = runtime.getGlobalSelectionCount();
            inlineSuspendViolationBaseline = runtime.getInlineSuspendViolationCount();
            for (int i = 0; i < LaunchResult.COUNT; i++) {
                launchBaselines[i] = runtime.getLaunchCount(LaunchResult.get(i));
            }
            localFallbackPublicationBaseline = runtime.getLocalFallbackPublicationCount();
            localPublicationBaseline = runtime.getLocalPublicationCount();
            localSelectionBaseline = runtime.getLocalSelectionCount();
            mountBaseline = runtime.getMountCount();
            orphanRecoveryBaseline = runtime.getOrphanedEntryRecoveryCount();
            orphanedShardBaseline = runtime.getOrphanedShardTransitionCount();
            retiredBaseline = runtime.getRetiredFiberCount();
            saturationBaseline = runtime.getSaturationCount();
            stolenSelectionBaseline = runtime.getStolenSelectionCount();
            wakeBaseline = runtime.getWakeClaimCount();
        }
    }
}
