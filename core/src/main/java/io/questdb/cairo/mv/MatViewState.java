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

package io.questdb.cairo.mv;

import io.questdb.cairo.file.AppendableBlock;
import io.questdb.cairo.file.BlockFileWriter;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.RecordToRowCopier;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static io.questdb.TelemetryEvent.*;

/**
 * Mat view refresh state serves the purpose of synchronizing and coordinating
 * {@link MatViewRefreshJob}s.
 * <p>
 * Unlike {@link MatViewStateReader}, it doesn't include invalidation reason
 * string as that field is not needed for refresh jobs.
 */
public class MatViewState implements QuietCloseable {
    // Cold-start gap-width threshold used when no scan/commit samples have been
    // observed yet. Units match the base table's timestamp column (us for
    // TIMESTAMP, ns for TIMESTAMP_NS). The value 2_000_000 corresponds to 2
    // seconds in microseconds and 2 milliseconds in nanoseconds -- both are
    // mild defaults that bias toward minimum merging until measured cost data
    // arrives. Subsequent refreshes will override via the EMA-derived
    // threshold computed from observed commit and scan latencies.
    public static final long COLD_START_GAP_THRESHOLD_TS_UNITS = 2_000_000L;
    public static final String MAT_VIEW_STATE_FILE_NAME = "_mv.s";
    public static final int MAT_VIEW_STATE_FORMAT_EXTRA_INTERVALS_MSG_TYPE = 3;
    public static final int MAT_VIEW_STATE_FORMAT_EXTRA_PERIOD_MSG_TYPE = 2;
    public static final int MAT_VIEW_STATE_FORMAT_EXTRA_TS_MSG_TYPE = 1;
    public static final int MAT_VIEW_STATE_FORMAT_MSG_TYPE = 0;
    // EMA smoothing inverse: new = (prev * (N-1) + sample) / N. Larger N is
    // smoother but slower to adapt. 8 gives ~6 samples to reach halfway to a
    // new steady state, which is a reasonable balance.
    static final int EMA_ALPHA_INV = 8;
    // Hard cap on incoming sample relative to current average, to prevent a
    // single outlier (GC pause, O3 partition rewrite) from poisoning the EMA
    // for the next several refreshes.
    static final int EMA_OUTLIER_MULTIPLIER = 5;
    // Used to avoid concurrent refresh runs.
    private final AtomicBoolean latch = new AtomicBoolean(false);
    // Protected by this.latch.
    // Holds cached txn intervals read from WAL transactions (_event files) of the base table.
    // Lets WalPurgeJob to make progress and delete applied WAL segments of a base table without
    // having to wait for all dependent mat views to be refreshed.
    private final LongList refreshIntervals = new LongList();
    // Incremented each time there's a base table transaction(s).
    // Used by MatViewTimerJob to avoid queueing redundant WAL txn intervals caching tasks.
    private final AtomicLong refreshIntervalsSeq = new AtomicLong();
    // Incremented each time an incremental/full refresh finishes.
    // Used by MatViewTimerJob to avoid queueing redundant refresh tasks.
    private final AtomicLong refreshSeq = new AtomicLong();
    private final MatViewTelemetryFacade telemetryFacade;
    // Exponential moving average of one REPLACE_RANGE commit, in nanoseconds.
    // Used together with avgScanNanosPerTsUnit to derive a timestamp-width
    // threshold below which clustering two adjacent cached refresh intervals
    // is cheaper than paying for an extra commit.
    // Writes protected by this.latch; reads may be racy (used for display
    // and cost-model decisions, never for correctness).
    private volatile long avgCommitNanos;
    // Exponential moving average of base-table scan latency per unit of the
    // base table's timestamp column (us for TIMESTAMP, ns for TIMESTAMP_NS).
    // This is the rate at which the refresh cursor processes wall-clock time
    // width regardless of underlying row density -- the cost model wants
    // "how long to scan a gap of N ts-units", which this directly answers.
    // Same locking discipline as avgCommitNanos.
    private volatile long avgScanNanosPerTsUnit;
    // Protected by this.latch.
    private RecordCursorFactory cursorFactory;
    private volatile boolean dropped;
    private volatile boolean invalid;
    // Stands for last successful period (range) refresh high boundary.
    // Increases monotonically as long as mat view stays valid.
    private volatile long lastPeriodHi = Numbers.LONG_NULL;
    // Stands for last successful incremental refresh base table reader txn.
    // Increases monotonically as long as mat view stays valid.
    private volatile long lastRefreshBaseTxn = -1;
    private volatile long lastRefreshFinishTimestampUs = Numbers.LONG_NULL;
    private volatile long lastRefreshStartTimestampUs = Numbers.LONG_NULL;
    private volatile boolean pendingInvalidation;
    // Protected by this.latch.
    private long recordRowCopierMetadataVersion;
    // Protected by this.latch.
    private RecordToRowCopier recordToRowCopier;
    // Protected by this.latch.
    // Base table txn that corresponds to refreshIntervals.
    private volatile long refreshIntervalsBaseTxn = -1;
    private volatile MatViewDefinition viewDefinition;

    public MatViewState(
            @NotNull MatViewDefinition viewDefinition,
            MatViewTelemetryFacade telemetryFacade
    ) {
        this.viewDefinition = viewDefinition;
        this.telemetryFacade = telemetryFacade;
    }

    public static void append(
            long lastRefreshTimestamp,
            long lastRefreshBaseTxn,
            boolean invalid,
            @Nullable CharSequence invalidationReason,
            long lastPeriodHi,
            @Nullable LongList refreshIntervals,
            long refreshIntervalsBaseTxn,
            @NotNull BlockFileWriter writer
    ) {
        AppendableBlock block = writer.append();
        appendState(lastRefreshBaseTxn, invalid, invalidationReason, block);
        block.commit(MAT_VIEW_STATE_FORMAT_MSG_TYPE);
        block = writer.append();
        appendTs(lastRefreshTimestamp, block);
        block.commit(MAT_VIEW_STATE_FORMAT_EXTRA_TS_MSG_TYPE);
        block = writer.append();
        appendPeriodHi(lastPeriodHi, block);
        block.commit(MAT_VIEW_STATE_FORMAT_EXTRA_PERIOD_MSG_TYPE);
        block = writer.append();
        appendRefreshIntervals(refreshIntervals, refreshIntervalsBaseTxn, block);
        block.commit(MAT_VIEW_STATE_FORMAT_EXTRA_INTERVALS_MSG_TYPE);
        writer.commit();
    }

    // refreshState can be null, in this case "default" record will be written
    public static void append(@Nullable MatViewStateReader refreshState, @NotNull BlockFileWriter writer) {
        if (refreshState != null) {
            append(
                    refreshState.getLastRefreshTimestampUs(),
                    refreshState.getLastRefreshBaseTxn(),
                    refreshState.isInvalid(),
                    refreshState.getInvalidationReason(),
                    refreshState.getLastPeriodHi(),
                    refreshState.getRefreshIntervals(),
                    refreshState.getRefreshIntervalsBaseTxn(),
                    writer
            );
        } else {
            append(
                    Numbers.LONG_NULL,
                    -1,
                    false,
                    null,
                    Numbers.LONG_NULL,
                    null,
                    -1,
                    writer
            );
        }
    }

    // kept public for tests
    public static void appendPeriodHi(long periodHi, @NotNull AppendableBlock block) {
        block.putLong(periodHi);
    }

    // kept public for tests
    public static void appendRefreshIntervals(
            @Nullable LongList refreshIntervals,
            long refreshIntervalsBaseTxn,
            @NotNull AppendableBlock block
    ) {
        block.putLong(refreshIntervalsBaseTxn);
        if (refreshIntervals != null) {
            block.putInt(refreshIntervals.size());
            for (int i = 0, n = refreshIntervals.size(); i < n; i++) {
                block.putLong(refreshIntervals.getQuick(i));
            }
        } else {
            block.putInt(-1);
        }
    }

    // kept public for tests
    public static void appendState(
            long lastRefreshBaseTxn,
            boolean invalid,
            @Nullable CharSequence invalidationReason,
            @NotNull AppendableBlock block
    ) {
        block.putBool(invalid);
        block.putLong(lastRefreshBaseTxn);
        block.putStr(invalidationReason);
    }

    // kept public for tests
    public static void appendTs(long lastRefreshTimestamp, @NotNull AppendableBlock block) {
        block.putLong(lastRefreshTimestamp);
    }

    /**
     * Folds a positive sample into a positive EMA, applying the outlier
     * multiplier cap and recovering safely from arithmetic overflow.
     * <p>
     * The pre-overflow form is {@code (prev*(N-1) + min(sample, prev*K)) / N}.
     * For values near {@code Long.MAX_VALUE / 7} the {@code prev*(N-1)} step
     * wraps -- in that case we fall back to a 50/50 blend of {@code prev} and
     * the capped sample, which keeps the EMA monotonic in sign and prevents
     * a single outlier from latching the average to a corrupted value.
     */
    private static long foldEma(long prev, long sample) {
        if (prev == 0) {
            return sample;
        }
        // Cap outlier samples relative to prev. multiplyExact guards against
        // overflow if prev itself is enormous; on overflow we treat the cap
        // as Long.MAX_VALUE (i.e. effectively no cap, which is fine because
        // we only cap when prev is small enough to make 5*prev finite).
        long cap;
        try {
            cap = Math.multiplyExact(prev, (long) EMA_OUTLIER_MULTIPLIER);
        } catch (ArithmeticException overflow) {
            cap = Long.MAX_VALUE;
        }
        final long capped = Math.min(sample, cap);
        // Standard EMA: (prev * (N-1) + capped) / N. Guard the multiply and
        // addition; on overflow fall back to a 50/50 blend.
        try {
            final long weighted = Math.multiplyExact(prev, (long) (EMA_ALPHA_INV - 1));
            final long sum = Math.addExact(weighted, capped);
            return sum / EMA_ALPHA_INV;
        } catch (ArithmeticException overflow) {
            return (prev / 2) + (capped / 2);
        }
    }

    public RecordCursorFactory acquireRecordFactory() {
        assert latch.get();
        RecordCursorFactory factory = cursorFactory;
        cursorFactory = null;
        return factory;
    }

    @Override
    public void close() {
        cursorFactory = Misc.free(cursorFactory);
    }

    public long getAvgCommitNanos() {
        return avgCommitNanos;
    }

    public long getAvgScanNanosPerTsUnit() {
        return avgScanNanosPerTsUnit;
    }

    /**
     * Returns the gap-width threshold below which two adjacent cached refresh
     * intervals should be merged into one cluster. Units match the base
     * table's timestamp column (us for TIMESTAMP, ns for TIMESTAMP_NS), the
     * same as the cached interval values themselves. Derived from the recent
     * moving averages: the threshold is the timestamp-range width whose scan
     * cost equals one extra REPLACE_RANGE commit -- below it, merging beats
     * splitting.
     * <p>
     * Returns the cold-start default ({@value #COLD_START_GAP_THRESHOLD_TS_UNITS}
     * ts-units) when no samples have been recorded yet. Returns 0 when the cost
     * model says merging is never worth it (commit cheaper than scanning one
     * ts unit of gap); {@link MatViewRefreshJob#clusterIntervals} interprets a
     * zero threshold as "gap-based merging disabled" because
     * {@link io.questdb.griffin.model.IntervalUtils#unionInPlace} already
     * guarantees adjacent intervals are at least one ts unit apart, so no gap
     * can satisfy {@code gap < 0}. Operators reading the catalogue see 0
     * instead of a misleading 1 that would suggest the cost model is active.
     */
    public long getCommitGapThresholdTsUnits() {
        final long commit = avgCommitNanos;
        final long scanPerTsUnit = avgScanNanosPerTsUnit;
        if (commit <= 0 || scanPerTsUnit <= 0) {
            return COLD_START_GAP_THRESHOLD_TS_UNITS;
        }
        return commit / scanPerTsUnit;
    }

    /**
     * Returns high boundary for all complete time periods up to which a period
     * mat view is refreshed.
     * <p>
     * A period view is a mat view that has REFRESH ... PERIOD (LENGTH ...) clause.
     * <p>
     * Each time when a period, e.g. a day, is complete, a range refresh is triggered
     * on the period mat view (unless it has manual refresh type). This range refresh
     * runs for the [lastPeriodHi, completePeriodHi) interval. If the refresh is successful,
     * completePeriodHi is stored as the new lastPeriodHi value.
     */
    public long getLastPeriodHi() {
        return lastPeriodHi;
    }

    /**
     * Returns base table txn read by the last incremental or full refresh.
     * Subsequent incremental refreshes should only refresh base table intervals
     * (think, slices of table partitions) that correspond to later txns.
     */
    public long getLastRefreshBaseTxn() {
        return lastRefreshBaseTxn;
    }

    public long getLastRefreshFinishTimestampUs() {
        return lastRefreshFinishTimestampUs;
    }

    public long getLastRefreshStartTimestampUs() {
        return lastRefreshStartTimestampUs;
    }

    public long getRecordRowCopierMetadataVersion() {
        return recordRowCopierMetadataVersion;
    }

    public RecordToRowCopier getRecordToRowCopier() {
        return recordToRowCopier;
    }

    public LongList getRefreshIntervals() {
        return refreshIntervals;
    }

    public long getRefreshIntervalsBaseTxn() {
        return refreshIntervalsBaseTxn;
    }

    public long getRefreshIntervalsSeq() {
        return refreshIntervalsSeq.get();
    }

    public long getRefreshSeq() {
        return refreshSeq.get();
    }

    // The view definition may change at any time as a result of ALTER MATERIALIZED VIEW SET REFRESH.
    // Avoid making chained calls, e.g. viewState.getViewDefinition().getRefreshType().
    public @NotNull MatViewDefinition getViewDefinition() {
        return viewDefinition;
    }

    public void incrementRefreshIntervalsSeq() {
        refreshIntervalsSeq.incrementAndGet();
    }

    public void incrementRefreshSeq() {
        refreshSeq.incrementAndGet();
    }

    public void init() {
        telemetryFacade.store(MAT_VIEW_CREATE, viewDefinition.getMatViewToken(), Numbers.LONG_NULL, null, 0);
    }

    public void initFromReader(MatViewStateReader reader) {
        this.invalid = reader.isInvalid();
        this.lastRefreshBaseTxn = reader.getLastRefreshBaseTxn();
        this.lastRefreshFinishTimestampUs = reader.getLastRefreshTimestampUs();
        this.lastPeriodHi = reader.getLastPeriodHi();
        this.refreshIntervalsBaseTxn = reader.getRefreshIntervalsBaseTxn();
        refreshIntervals.clear();
        refreshIntervals.addAll(reader.getRefreshIntervals());
    }

    public boolean isDropped() {
        return dropped;
    }

    public boolean isInvalid() {
        return invalid;
    }

    public boolean isLocked() {
        return latch.get();
    }

    public boolean isPendingInvalidation() {
        return pendingInvalidation;
    }

    public void markAsDropped() {
        dropped = true;
        telemetryFacade.store(MAT_VIEW_DROP, viewDefinition.getMatViewToken(), Numbers.LONG_NULL, null, 0);
    }

    public void markAsInvalid(CharSequence invalidationReason) {
        if (!invalid) {
            telemetryFacade.store(MAT_VIEW_INVALIDATE, viewDefinition.getMatViewToken(), Numbers.LONG_NULL, invalidationReason, 0);
        }
        this.invalid = true;
    }

    public void markAsPendingInvalidation() {
        pendingInvalidation = true;
    }

    public void markAsValid() {
        this.invalid = false;
        this.pendingInvalidation = false;
    }

    public void rangeRefreshSuccess(
            RecordCursorFactory factory,
            RecordToRowCopier copier,
            long recordRowCopierMetadataVersion,
            long refreshFinishedTimestampUs,
            long refreshTriggeredTimestampUs,
            long periodHi
    ) {
        assert latch.get();
        this.cursorFactory = factory;
        this.recordToRowCopier = copier;
        this.recordRowCopierMetadataVersion = recordRowCopierMetadataVersion;
        this.lastRefreshFinishTimestampUs = refreshFinishedTimestampUs;
        this.lastPeriodHi = periodHi;
        telemetryFacade.store(
                MAT_VIEW_REFRESH_SUCCESS,
                viewDefinition.getMatViewToken(),
                -1,
                null,
                refreshFinishedTimestampUs - refreshTriggeredTimestampUs
        );
    }

    /**
     * Folds a sampled REPLACE_RANGE commit latency into the rolling average used
     * for {@link #getCommitGapThresholdTsUnits()}. Samples in nanoseconds.
     * <p>
     * Samples larger than {@value #EMA_OUTLIER_MULTIPLIER}x the current average
     * are capped before folding, so a single GC pause or O3 partition rewrite
     * doesn't contaminate the cost model for the next several refreshes.
     */
    public void recordCommitNanos(long sampleNanos) {
        assert latch.get();
        if (sampleNanos <= 0) {
            return;
        }
        avgCommitNanos = foldEma(avgCommitNanos, sampleNanos);
    }

    /**
     * Folds a sampled base-table scan into the rolling per-ts-unit scan
     * latency. {@code rangeTsUnits} is the timestamp range width that the cursor
     * scanned (i.e. {@code iteratorHi - iteratorLo}) in the base table's
     * timestamp unit (us for TIMESTAMP, ns for TIMESTAMP_NS), not row counts.
     * Ignores zero-range samples so the rate isn't pulled to infinity, and
     * ignores sub-resolution samples (where {@code sampleNanos < rangeTsUnits})
     * because integer division would floor the per-ts-unit rate to zero and the
     * subsequent {@code Math.max(1, ...)} clamp would lock the EMA into a
     * regime orders of magnitude above the true rate -- which would in turn
     * collapse {@link #getCommitGapThresholdTsUnits()} below the cold-start
     * default. Letting the cold-start guard keep firing is preferable to
     * folding in a sample with no usable signal.
     * <p>
     * Samples larger than {@value #EMA_OUTLIER_MULTIPLIER}x the current average
     * are capped before folding.
     */
    public void recordScanMetrics(long sampleNanos, long rangeTsUnits) {
        assert latch.get();
        if (sampleNanos <= 0 || rangeTsUnits <= 0 || sampleNanos < rangeTsUnits) {
            return;
        }
        avgScanNanosPerTsUnit = foldEma(avgScanNanosPerTsUnit, sampleNanos / rangeTsUnits);
    }

    public void refreshFail(long refreshTimestamp, CharSequence errorMessage) {
        assert latch.get();
        this.lastRefreshFinishTimestampUs = refreshTimestamp;
        markAsInvalid(errorMessage);
        telemetryFacade.store(MAT_VIEW_REFRESH_FAIL, viewDefinition.getMatViewToken(), Numbers.LONG_NULL, errorMessage, 0);
    }

    /**
     * Clears the rolling commit and scan latency averages, returning the cost
     * model to its cold-start state. Used by {@code REFRESH MATERIALIZED VIEW
     * ... STATS} when an operator wants to force the auto-tune to re-learn
     * (e.g. after a workload shape change or a hardware migration).
     */
    public void refreshStats() {
        assert latch.get();
        this.avgCommitNanos = 0;
        this.avgScanNanosPerTsUnit = 0;
    }

    public void refreshSuccess(
            RecordCursorFactory factory,
            RecordToRowCopier copier,
            long recordRowCopierMetadataVersion,
            long refreshFinishedTimestamp,
            long refreshTriggeredTimestamp,
            long baseTableTxn,
            long periodHi
    ) {
        assert latch.get();
        this.cursorFactory = factory;
        this.recordToRowCopier = copier;
        this.recordRowCopierMetadataVersion = recordRowCopierMetadataVersion;
        this.lastRefreshFinishTimestampUs = refreshFinishedTimestamp;
        this.lastRefreshBaseTxn = baseTableTxn;
        this.lastPeriodHi = periodHi;
        // Successful incremental refresh means that cached intervals were applied and should be evicted.
        this.refreshIntervalsBaseTxn = -1;
        refreshIntervals.clear();
        telemetryFacade.store(
                MAT_VIEW_REFRESH_SUCCESS,
                viewDefinition.getMatViewToken(),
                baseTableTxn,
                null,
                refreshFinishedTimestamp - refreshTriggeredTimestamp
        );
    }

    public void refreshSuccessNoRows(
            long refreshFinishedTimestampUs,
            long refreshTriggeredTimestampUs,
            long baseTableTxn,
            long periodHi
    ) {
        assert latch.get();
        this.lastRefreshFinishTimestampUs = refreshFinishedTimestampUs;
        this.lastRefreshBaseTxn = baseTableTxn;
        this.lastPeriodHi = periodHi;
        // Successful incremental refresh means that cached intervals were applied and should be evicted.
        this.refreshIntervalsBaseTxn = -1;
        refreshIntervals.clear();
        telemetryFacade.store(
                MAT_VIEW_REFRESH_SUCCESS,
                viewDefinition.getMatViewToken(),
                baseTableTxn,
                null,
                refreshFinishedTimestampUs - refreshTriggeredTimestampUs
        );
    }

    public void setLastPeriodHi(long lastPeriodHi) {
        this.lastPeriodHi = lastPeriodHi;
    }

    public void setLastRefreshBaseTableTxn(long txn) {
        lastRefreshBaseTxn = txn;
    }

    public void setLastRefreshStartTimestampUs(long timestampUs) {
        lastRefreshStartTimestampUs = timestampUs;
    }

    public void setLastRefreshTimestampUs(long timestampUs) {
        this.lastRefreshFinishTimestampUs = timestampUs;
    }

    public void setRefreshIntervals(LongList refreshIntervals) {
        this.refreshIntervals.clear();
        this.refreshIntervals.addAll(refreshIntervals);
    }

    public void setRefreshIntervalsBaseTxn(long refreshIntervalsBaseTxn) {
        this.refreshIntervalsBaseTxn = refreshIntervalsBaseTxn;
    }

    /**
     * Test-only seeder for the rolling averages. Lets tests drive the
     * clustering cost model into a known regime without having to predict the
     * exact scan-time of a warmup refresh on the running hardware. Production
     * callers must use the {@code recordCommitNanos} / {@code recordScanMetrics}
     * pair instead.
     */
    @TestOnly
    public void setRefreshMetricsForTesting(long commitNanos, long scanNanosPerTsUnit) {
        assert latch.get();
        this.avgCommitNanos = Math.max(0, commitNanos);
        this.avgScanNanosPerTsUnit = Math.max(0, scanNanosPerTsUnit);
    }

    public void setViewDefinition(MatViewDefinition viewDefinition) {
        this.viewDefinition = viewDefinition;
    }

    public void tryCloseIfDropped() {
        if (dropped && tryLock()) {
            try {
                close();
            } finally {
                unlock();
            }
        }
    }

    public boolean tryLock() {
        return latch.compareAndSet(false, true);
    }

    public void unlock() {
        if (!latch.compareAndSet(true, false)) {
            throw new IllegalStateException("cannot unlock, not locked");
        }
    }
}
