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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.EntityColumnFilter;
import io.questdb.cairo.EntryUnavailableException;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TableWriterAPI;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.RecordToRowCopier;
import io.questdb.griffin.RecordToRowCopierUtils;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.groupby.TimestampSampler;
import io.questdb.griffin.engine.groupby.TimestampSamplerFactory;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.log.LogRecord;
import io.questdb.mp.Job;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTracker;
import io.questdb.std.MemoryTrackerWorkload;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import io.questdb.std.datetime.CommonUtils;
import io.questdb.std.datetime.MicrosecondClock;
import io.questdb.std.datetime.TimeZoneRules;
import io.questdb.std.str.Path;
import io.questdb.std.str.Sinkable;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.locks.Lock;

import static io.questdb.cairo.wal.WalUtils.WAL_DEDUP_MODE_REPLACE_RANGE;

public class MatViewRefreshJob implements Job, QuietCloseable {
    private static final Log LOG = LogFactory.getLog(MatViewRefreshJob.class);
    private final ObjList<TableToken> childViewSink = new ObjList<>();
    private final ObjList<TableToken> childViewSink2 = new ObjList<>();
    // Scratch list for the post-cluster working copy of refresh intervals.
    // Decouples downstream cluster/period/limit mutations from the live
    // viewState.refreshIntervals reference -- mutating the cache would persist
    // lossy clusters on the next cache write and survive across restarts.
    private final LongList clusteredIntervals = new LongList();
    private final EntityColumnFilter columnFilter = new EntityColumnFilter();
    private final long busyRetryTimeoutUs;
    private final CairoConfiguration configuration;
    private final CairoEngine engine;
    private final StringSink errorMsgSink = new StringSink();
    private final FixedOffsetIntervalIterator fixedOffsetIterator = new FixedOffsetIntervalIterator();
    private final MatViewGraph graph;
    private final LongList intervals = new LongList();
    private final int maxRefreshRetryAttempts;
    private final MicrosecondClock microsecondClock;
    private final RefreshContext refreshContext = new RefreshContext();
    private final MatViewRefreshSqlExecutionContext refreshSqlExecutionContext;
    private final MatViewRefreshTask refreshTask = new MatViewRefreshTask();
    private final int sharedQueryWorkerCount;
    private final MatViewStateStore stateStore;
    private final TimeZoneIntervalIterator timeZoneIterator = new TimeZoneIntervalIterator();
    private final WalTxnRangeLoader txnRangeLoader;

    public MatViewRefreshJob(int workerId, CairoEngine engine, int sharedQueryWorkerCount) {
        // workerId is accepted for source-compatibility; the rotation framework
        // makes the per-worker invariant a per-cont-snapshot invariant instead.
        this(engine, sharedQueryWorkerCount);
    }

    public MatViewRefreshJob(CairoEngine engine, int sharedQueryWorkerCount) {
        try {
            this.engine = engine;
            this.sharedQueryWorkerCount = sharedQueryWorkerCount;
            this.refreshSqlExecutionContext = new MatViewRefreshSqlExecutionContext(engine, sharedQueryWorkerCount);
            this.graph = engine.getMatViewGraph();
            this.stateStore = engine.getMatViewStateStore();
            this.configuration = engine.getConfiguration();
            this.txnRangeLoader = new WalTxnRangeLoader(configuration);
            this.microsecondClock = configuration.getMicrosecondClock();
            this.busyRetryTimeoutUs = configuration.getMatViewRefreshBusyRetryTimeout() * 1000;
            this.maxRefreshRetryAttempts = configuration.getMatViewRefreshBusyRetryLimit();
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    /**
     * Estimates how many buckets are needed to contain a target number of rows.
     * <p>
     * Formula: bucketsForRows = (totalBuckets * targetRows) / tableRows
     * where totalBuckets = (partitionDuration / bucket) * partitionCount
     */
    // kept public for testing
    public static long estimateBucketsForRows(long targetRows, long tableRows, long bucket, long partitionDuration, int partitionCount) {
        if (partitionCount > 0 && tableRows > 0) {
            // totalBuckets = (partitionDuration / bucket) * partitionCount
            // bucketsForRows = totalBuckets * targetRows / tableRows
            // Reorder to avoid overflow: (partitionDuration * partitionCount * targetRows) / (bucket * tableRows)
            // Use double to handle large values without overflow
            final double totalBuckets = ((double) partitionDuration / bucket) * partitionCount;
            return Math.max(1, (long) ((totalBuckets * targetRows) / tableRows));
        }
        return 1;
    }

    @Override
    public Job cloneInstance() {
        return new MatViewRefreshJob(engine, sharedQueryWorkerCount);
    }

    @Override
    public void close() {
        LOG.debug().$("materialized view refresh job closing").$();
        Misc.free(refreshSqlExecutionContext);
        Misc.free(txnRangeLoader);
    }

    @Override
    public void closeInstance() {
        // cloneInstance() mints a fresh job per generation, so the pool frees
        // each instance's native resources through this hook at halt. Misc.free
        // nulls the fields, keeping the call idempotent.
        close();
    }

    @Override
    public void recycleInstance() {
        // Per-iteration scratch is overwritten on entry to each refresh task.
        // Clearing here is defensive against stale state surviving into the
        // snapshot's next reuse.
        childViewSink.clear();
        childViewSink2.clear();
        errorMsgSink.clear();
        intervals.clear();
    }

    @Override
    public boolean run(@NotNull WorkerContext workerContext) {
        return processNotifications();
    }

    private static long approxStepDuration(long step, long approxBucketSize) {
        try {
            return Math.multiplyExact(step, approxBucketSize);
        } catch (ArithmeticException ignore) {
            return Long.MAX_VALUE;
        }
    }

    /**
     * Fills {@code out} with one step per cached interval (cluster): the
     * smaller of {@code naturalStep} and the cluster's width in buckets.
     * <p>
     * Goes together with {@link #clusterIntervals}: each list entry is one
     * cluster after auto-tuning. A per-cluster step lets the iterator fit
     * each step-group inside a single cluster -- a narrow cluster does not
     * shrink the step on wider clusters, and a wide cluster does not enlarge
     * the step on narrow ones. The iterator's gap-skip then excises the gaps
     * between clusters without dragging unchanged buckets into the cursor's
     * scan range.
     * <p>
     * On overflow or malformed entries the per-cluster step falls back to
     * {@code naturalStep} for that cluster.
     */
    // kept public for testing
    public static void computePerClusterSteps(
            @Nullable LongList intervals,
            long approxBucketSize,
            long naturalStep,
            @NotNull LongList out
    ) {
        out.clear();
        if (intervals == null || intervals.size() < 2) {
            return;
        }
        for (int i = 0, n = intervals.size(); i < n; i += 2) {
            if (approxBucketSize <= 0) {
                out.add(naturalStep);
                continue;
            }
            final long widthTsUnits;
            try {
                widthTsUnits = Math.subtractExact(intervals.getQuick(i + 1), intervals.getQuick(i));
            } catch (ArithmeticException overflow) {
                // Pathological interval spans almost the full long range; we
                // can't say anything useful about its width, fall back to
                // naturalStep so the iterator at least makes progress.
                out.add(naturalStep);
                continue;
            }
            if (widthTsUnits < 0) {
                // Malformed (hi < lo) -- be defensive, fall back to naturalStep.
                out.add(naturalStep);
                continue;
            }
            final long widthBuckets = Math.max(1, widthTsUnits / approxBucketSize + 1);
            out.add(Math.min(naturalStep, widthBuckets));
        }
    }

    /**
     * Copies {@code src} into {@code dst} and merges adjacent intervals when the
     * timestamp gap between them is cheaper to scan than to pay for an extra
     * REPLACE_RANGE commit. {@code gapThresholdTsUnits} comes from
     * {@link MatViewState#getCommitGapThresholdTsUnits()} and represents the
     * gap-width below which merging beats splitting; it is derived from the
     * rolling commit and per-unit scan latencies the refresh job records on
     * every iteration. The unit is the base table's timestamp resolution
     * (microseconds for TIMESTAMP, nanoseconds for TIMESTAMP_NS) and is
     * consistent with the cached interval values. A threshold of 0 means
     * gap-based merging is disabled (the cost model has determined that a
     * fresh commit is cheaper than scanning a single ts unit of gap); only
     * the {@code maxClusters} safety cap can still fold intervals together.
     * <p>
     * {@code src} must be sorted and disjoint on entry (as produced by
     * {@link IntervalUtils#unionInPlace}); on return {@code dst} is also sorted
     * and disjoint with at most {@code maxClusters} entries. {@code src} is not
     * mutated -- the separate destination is required so that the cached union
     * of unprocessed WAL ranges (a live {@link MatViewState#getRefreshIntervals()}
     * reference) stays loss-free across refresh retries and restarts.
     *
     * @return number of clusters in the final list (i.e. {@code dst.size() / 2}).
     */
    // kept public for testing
    public static int clusterIntervals(
            @NotNull LongList src,
            @NotNull LongList dst,
            long gapThresholdTsUnits,
            int maxClusters
    ) {
        dst.clear();
        dst.addAll(src);
        final int initialSize = dst.size();
        if (initialSize <= 2) {
            return initialSize / 2;
        }
        assert (initialSize & 1) == 0 : "intervals must contain [lo, hi] pairs";
        if (maxClusters < 1) {
            maxClusters = 1;
        }
        if (gapThresholdTsUnits < 0) {
            gapThresholdTsUnits = 0;
        }
        int write = 2;
        long prevHi = dst.getQuick(1);
        for (int read = 2; read < initialSize; read += 2) {
            final long lo = dst.getQuick(read);
            final long hi = dst.getQuick(read + 1);
            assert lo >= prevHi : "intervals must be sorted and disjoint";
            // Compute gap via subtractExact; a positive gap that overflows is
            // by definition larger than any possible threshold, so we treat
            // it as not mergeable.
            boolean isMergeable;
            try {
                final long gapTsUnits = Math.subtractExact(lo, prevHi);
                isMergeable = gapTsUnits < gapThresholdTsUnits;
            } catch (ArithmeticException overflow) {
                isMergeable = false;
            }
            // Force a merge if we've already produced maxClusters clusters --
            // the safety cap prevents pathological refreshes from producing
            // hundreds of tiny commits.
            if (!isMergeable && (write / 2) >= maxClusters) {
                isMergeable = true;
            }
            if (isMergeable) {
                if (hi > prevHi) {
                    dst.setQuick(write - 1, hi);
                    prevHi = hi;
                }
            } else {
                dst.setQuick(write, lo);
                dst.setQuick(write + 1, hi);
                write += 2;
                prevHi = hi;
            }
        }
        dst.setPos(write);
        return write / 2;
    }

    /**
     * Estimates how many buckets are needed to contain a target number of rows.
     */
    private static long estimateBucketsForRows(long targetRows, @NotNull TimestampDriver driver, @NotNull TableReader baseTableReader, long bucket) {
        final long tableRows = baseTableReader.size();
        final long partitionDuration = driver.approxPartitionDuration(baseTableReader.getPartitionedBy());
        final int partitionCount = baseTableReader.getPartitionCount();
        return estimateBucketsForRows(targetRows, tableRows, bucket, partitionDuration, partitionCount);
    }

    private static void intersectIntervals(LongList intervals, long lo, long hi) {
        if (intervals != null && intervals.size() > 0) {
            intervals.add(lo, hi);
            IntervalUtils.intersectInPlace(intervals, intervals.size() - 2);
        }
    }

    // Recognizes the "table is suspended" refusal that CairoEngine.getWalWriter raises for a hard-suspended
    // view when cairo.wal.apply.suspended.write.denied is on. It backstops the up-front isViewWriteSuspended
    // gate: a view can be suspended in the narrow window between that gate and the getWalWriter acquire, in
    // which case the throw lands in the refresh path's outer catch. The refresh job must treat it as a skip
    // (leave the view valid, do not re-enqueue) rather than a refresh failure -- invalidating a suspended
    // view is wrong (resume recovers it) and re-enqueueing would busy-loop the worker until resume.
    private static boolean isTableSuspendedError(Throwable th) {
        return th instanceof CairoException ce && ce.isTableSuspended();
    }

    private static void unionIntervals(LongList intervals, long lo, long hi) {
        if (intervals != null) {
            intervals.add(lo, hi);
            IntervalUtils.unionInPlace(intervals, intervals.size() - 2);
        }
    }

    private boolean checkIfBaseTableDropped(MatViewRefreshTask refreshTask) {
        final TableToken baseTableToken = refreshTask.baseTableToken;
        final TableToken viewToken = refreshTask.matViewToken;
        if (viewToken == null) {
            assert baseTableToken != null;
            try {
                engine.verifyTableToken(baseTableToken);
            } catch (CairoException | TableReferenceOutOfDateException ignore) {
                invalidateDependentViews(baseTableToken, "base table is dropped or renamed");
                return true;
            }
        }
        return false;
    }

    private void commitMatView(
            @NotNull MatViewState viewState,
            @NotNull WalWriter walWriter,
            @NotNull RefreshContext refreshContext,
            @NotNull RecordCursorFactory factory,
            @NotNull RecordToRowCopier copier,
            long refreshTriggerTimestampUs,
            long replacementTimestampLo,
            long replacementTimestampHi
    ) {
        final long recordRowCopierMetadataVersion = walWriter.getMetadata().getMetadataVersion();
        final long refreshFinishTimestampUs = microsecondClock.getTicks();
        final long commitPeriodHi = refreshContext.periodHi != Numbers.LONG_NULL ? refreshContext.periodHi : viewState.getLastPeriodHi();
        if (refreshContext.toBaseTxn == -1) {
            // It's a range refresh.
            // It comes in two flavors:
            //   1. Range refresh run by the user via REFRESH SQL
            //   2. Period range refresh triggered by period timer

            // First, do a range replace commit.
            fencedMatViewCommit(() -> walWriter.commitWithParams(
                    replacementTimestampLo,
                    replacementTimestampHi,
                    WAL_DEDUP_MODE_REPLACE_RANGE
            ));
            // Second, if it's a period range refresh, we need to persist state
            // with the new lastPeriodHi, but the same base txn and cached txn intervals.
            // If we did a mat view data commit, we'd unintentionally reset the cached intervals.
            if (refreshContext.periodHi != Numbers.LONG_NULL) {
                fencedMatViewCommit(() -> walWriter.resetMatViewState(
                        viewState.getLastRefreshBaseTxn(),
                        refreshFinishTimestampUs,
                        false,
                        null,
                        commitPeriodHi,
                        viewState.getRefreshIntervals(),
                        viewState.getRefreshIntervalsBaseTxn()
                ));
            }
            viewState.rangeRefreshSuccess(
                    factory,
                    copier,
                    recordRowCopierMetadataVersion,
                    refreshFinishTimestampUs,
                    refreshTriggerTimestampUs,
                    commitPeriodHi
            );
        } else {
            // It's an incremental/full refresh.
            // Easy job: first commit data along with the mat view state and then update the in-memory state.
            // The mat view data commit will reset cached txn intervals since we want to evict them.
            fencedMatViewCommit(() -> walWriter.commitMatView(
                    refreshContext.toBaseTxn,
                    refreshFinishTimestampUs,
                    commitPeriodHi,
                    replacementTimestampLo,
                    replacementTimestampHi
            ));
            viewState.refreshSuccess(
                    factory,
                    copier,
                    recordRowCopierMetadataVersion,
                    refreshFinishTimestampUs,
                    refreshTriggerTimestampUs,
                    refreshContext.toBaseTxn,
                    commitPeriodHi
            );
        }
    }

    // The automatic refresh job runs on a worker pool an in-place primary-to-replica demote never halts:
    // it acquires the view WalWriter while PRIMARY, runs a long SELECT pump, then externalizes a replicated
    // seqTxn with no read-only re-check, and the demote drain rendezvouses only with the refresh task queue,
    // never with the in-flight worker. Two replicated-WAL mint families ride the held writer: the row-data
    // commit (commitWithParams / commitMatView) and the view-state mint (truncateSoft on a full refresh,
    // resetMatViewState for every refresh-state persist / invalidate). Either one that lands after the demote
    // flips the read-only flag mints a local-only seqTxn on the replicated view table the closing uploader
    // never ships, so the new primary never sees it. Route both families through this fence: hold the
    // role-switch READ lock across an authoritative in-lock isReadOnlyMode() re-check and the mint, so the
    // mint is atomic against the role flip: either the flip ran first (refuse -- the refresh is abandoned,
    // and a materialized view is derived state so the new primary recomputes forward) or the mint lands fully
    // as PRIMARY while the flip's WRITE acquire waits for this read hold and replicates. This fences the WAL
    // externalization only -- the MatViewState.closed flag, the refresh latch and the state-store redirect
    // that defend the native cursor are untouched. The fence is a strict no-op for non-replicating
    // deployments: the read lock is uncontended and the read-only flag is static.
    private void fencedMatViewCommit(Runnable commit) {
        if (engine.isReadOnlyMode()) {
            throw CairoException.readOnlyAccess();
        }
        final Lock lock = engine.getRoleSwitchReadLock();
        lock.lock();
        try {
            if (engine.isReadOnlyMode()) {
                throw CairoException.readOnlyAccess();
            }
            engine.fireRoleSwitchMintObserver();
            commit.run();
        } finally {
            lock.unlock();
        }
    }

    private void enqueueInvalidateDependentViews(TableToken viewToken, String invalidationReason) {
        childViewSink2.clear();
        graph.getDependentViews(viewToken, childViewSink2);
        for (int v = 0, n = childViewSink2.size(); v < n; v++) {
            stateStore.enqueueInvalidate(childViewSink2.get(v), invalidationReason);
        }
    }

    private RefreshContext findRefreshIntervals(
            @NotNull TableReader baseTableReader,
            @NotNull MatViewDefinition viewDefinition,
            @NotNull MatViewState viewState,
            @NotNull WalWriter walWriter,
            long lastRefreshTxn
    ) throws SqlException {
        return findRefreshIntervals(
                baseTableReader,
                viewDefinition,
                viewState,
                walWriter,
                lastRefreshTxn,
                Numbers.LONG_NULL,
                Numbers.LONG_NULL
        );
    }

    /**
     * Builds the {@link RefreshContext} (interval iterator + commit txn) for an
     * upcoming refresh of {@code viewState}.
     * <p>
     * Precondition: the caller must hold {@code viewState}'s refresh latch
     * (acquired via {@link MatViewState#tryLock()}). The method reads and
     * mutates view-state fields (cached intervals, EMA accessors via
     * {@link MatViewState#getCommitGapThresholdTsUnits()}) that are protected
     * by that latch.
     */
    private RefreshContext findRefreshIntervals(
            @NotNull TableReader baseTableReader,
            @NotNull MatViewDefinition viewDefinition,
            @NotNull MatViewState viewState,
            @NotNull WalWriter walWriter,
            long lastRefreshTxn,
            long rangeFrom,
            long rangeTo
    ) throws SqlException {
        assert viewState.isLocked();
        refreshContext.clear();

        final long lastTxn = baseTableReader.getSeqTxn();
        final TableToken baseTableToken = baseTableReader.getTableToken();
        final TableToken viewToken = viewDefinition.getMatViewToken();
        final TimestampDriver driver = viewDefinition.getBaseTableTimestampDriver();

        final long now = driver.getTicks();
        final boolean rangeRefresh = rangeTo != Numbers.LONG_NULL;
        final boolean incrementalRefresh = lastRefreshTxn != Numbers.LONG_NULL;

        LongList refreshIntervals = null;
        long minTs = Long.MAX_VALUE;
        long maxTs = Long.MIN_VALUE;
        if (incrementalRefresh) {
            // Incremental refresh. This means that there may be a data transaction in the base table
            // or someone has run REFRESH INCREMENTAL SQL.
            // Let's find min/max timestamps in the new WAL transactions.
            if (lastRefreshTxn > -1) {
                // It's a subsequent incremental refresh, so WalPurgeJob must be aware of us.
                final LongList cachedIntervals = updateRefreshIntervals0(lastTxn, baseTableToken, viewDefinition, viewState, walWriter);
                if (cachedIntervals != null) {
                    if (cachedIntervals.size() > 0) {
                        // Merge cached intervals into cost-aware clusters so a single far-back
                        // O3 write does not drag a wide envelope of unchanged buckets into the
                        // refresh. The iterator preserves the gap-skip between clusters; within
                        // a cluster the existing single-cursor-per-step-group behaviour stays.
                        // Threshold and intervals are both in the base table's timestamp unit
                        // (us for TIMESTAMP, ns for TIMESTAMP_NS) -- the comparison is
                        // unit-consistent so the clustering decision is correct on both.
                        // Output goes into a job-owned scratch list so the cached union of
                        // unprocessed WAL ranges stays pristine -- downstream period and
                        // refresh-limit adjustments operate on the scratch, not the cache.
                        final long gapThresholdTsUnits = viewState.getCommitGapThresholdTsUnits();
                        clusterIntervals(cachedIntervals, clusteredIntervals, gapThresholdTsUnits, configuration.getMatViewRefreshMaxClusters());
                        refreshIntervals = clusteredIntervals;
                        // BAU incremental refresh.
                        minTs = refreshIntervals.getQuick(0);
                        maxTs = refreshIntervals.getQuick(refreshIntervals.size() - 1);
                    } // else - no data transactions, nothing to refresh
                } else {
                    // Looks like we need a full refresh.
                    // We must have failed to read WAL transactions.
                    minTs = baseTableReader.getMinTimestamp();
                    maxTs = baseTableReader.getMaxTimestamp();
                }
            } else {
                // It's the first incremental refresh. WAL segments may be already purged,
                // so let's take min/max timestamps from the reader.
                minTs = baseTableReader.getMinTimestamp();
                maxTs = baseTableReader.getMaxTimestamp();
            }
            refreshContext.toBaseTxn = lastTxn;
        } else if (rangeRefresh) {
            // Range refresh. This means that the timer is triggered on a period mat view
            // or someone has run REFRESH RANGE SQL.
            // In both cases we have the range to be refreshed.
            if (rangeFrom == Numbers.LONG_NULL) {
                // Period timer has triggered.
                long periodLo = viewState.getLastPeriodHi();
                if (periodLo == Numbers.LONG_NULL) {
                    periodLo = baseTableReader.getMinTimestamp();
                }
                // Check the last refresh txn: -1 means that there was no initial refresh, and we can
                // ignore this range refresh. If we don't ignore it and later on the base table/view
                // gets any range replace txns, the subsequent initial refresh may leave some dangling
                // rows in the view since it only considers min/max timestamps from the table reader.
                if (periodLo < rangeTo && viewState.getLastRefreshBaseTxn() != -1) {
                    minTs = periodLo;
                    maxTs = rangeTo;
                    // Bump lastPeriodHi once we're done. Its value is exclusive.
                    refreshContext.periodHi = rangeTo + 1;
                }
            } else {
                // User-defined range refresh, e.g.
                // `REFRESH MATERIALIZED VIEW my_view RANGE FROM '2025-05-05T01:00' TO '2025-05-05T02:00';`
                // Consider actual min/max timestamps in the table data to avoid redundant query executions.
                minTs = Math.max(rangeFrom, baseTableReader.getMinTimestamp());
                maxTs = Math.min(rangeTo, baseTableReader.getMaxTimestamp());
            }
        } else {
            // Full refresh, i.e. someone has run REFRESH FULL SQL.
            // When the table is empty, min/max timestamps are set to Long.MAX_VALUE / LONG.MIN_VALUE,
            // so we end up skipping the refresh.
            minTs = baseTableReader.getMinTimestamp();
            maxTs = baseTableReader.getMaxTimestamp();
            refreshContext.toBaseTxn = lastTxn;
        }

        // In case of incremental or full refresh we may need to do the following:
        //   * remove incomplete periods from the refresh interval
        //   * include complete periods into the refresh interval if we never refreshed them (lastPeriodHi)
        //   * remove intervals older than the refresh limit
        if (!rangeRefresh) {
            // Check if we're doing incremental/full refresh on a period mat view.
            // If so, we may need to remove incomplete periods from the final refresh interval.
            if (viewDefinition.getPeriodLength() > 0) {
                TimestampSampler periodSampler = viewDefinition.getPeriodSampler();
                if (periodSampler == null) {
                    periodSampler = TimestampSamplerFactory.getInstance(
                            driver,
                            viewDefinition.getPeriodLength(),
                            viewDefinition.getPeriodLengthUnit(),
                            -1
                    );
                    viewDefinition.setPeriodSampler(periodSampler);
                }
                periodSampler.setStart(driver.fromMicros(viewDefinition.getTimerStartUs()));

                final long nowLocal = viewDefinition.getTimerTzRulesUs() != null
                        ? now + driver.fromMicros(viewDefinition.getTimerTzRulesUs().getOffset(driver.toMicros(now)))
                        : now;
                final long delay = driver.from(viewDefinition.getPeriodDelay(), viewDefinition.getPeriodDelayUnit());
                // Period hi is exclusive, but maxTs is inclusive, so we align them.
                final long periodHiLocal = periodSampler.round(nowLocal - delay) - 1;
                final long periodHi = viewDefinition.getTimerTzRulesUs() != null
                        ? periodHiLocal - driver.fromMicros(viewDefinition.getTimerTzRulesUs().getOffset(driver.toMicros(periodHiLocal)))
                        : periodHiLocal;

                // Remove incomplete periods from both txn intervals and refresh interval.
                intersectIntervals(refreshIntervals, Long.MIN_VALUE, periodHi);
                maxTs = Math.min(maxTs, periodHi);

                if (incrementalRefresh) {
                    // Incremental refresh on a period mat view works in a special way.
                    // We need to refresh whatever is in the txn intervals and all periods
                    // that became complete since the last refresh.
                    long periodLo = viewState.getLastPeriodHi();
                    if (periodLo == Numbers.LONG_NULL) {
                        periodLo = baseTableReader.getMinTimestamp();
                    }
                    if (periodLo < periodHi) {
                        if (refreshIntervals != null) {
                            unionIntervals(refreshIntervals, periodLo, periodHi);
                            minTs = refreshIntervals.getQuick(0);
                            maxTs = refreshIntervals.getQuick(refreshIntervals.size() - 1);
                        } else {
                            minTs = periodLo;
                            maxTs = periodHi;
                        }
                        // Bump lastPeriodHi once we're done.
                        // lastPeriodHi is exclusive, but the local periodHi value is inclusive.
                        refreshContext.periodHi = periodHi + 1;
                    }
                } else {
                    // It's a full refresh, so we'll need to bump lastPeriodHi once we're done.
                    refreshContext.periodHi = periodHi + 1;
                }
            }

            // Check if refresh limit should be applied.
            final int refreshLimitHoursOrMonths = viewDefinition.getRefreshLimitHoursOrMonths();
            if (refreshLimitHoursOrMonths != 0) {
                if (refreshLimitHoursOrMonths > 0) { // hours
                    minTs = Math.max(minTs, now - driver.fromHours(refreshLimitHoursOrMonths));
                } else { // months
                    minTs = Math.max(minTs, driver.addMonths(now, refreshLimitHoursOrMonths));
                }
                intersectIntervals(refreshIntervals, minTs, Long.MAX_VALUE);
            }
        }

        if (minTs <= maxTs) {
            final TimestampSampler timestampSampler = viewDefinition.getTimestampSampler();
            final long approxBucketSize = timestampSampler.getApproxBucketSize();
            final long rowsPerQuery = configuration.getMatViewRowsPerQueryEstimate();

            long step = estimateBucketsForRows(rowsPerQuery, driver, baseTableReader, approxBucketSize);
            final long maxStepDuration = driver.fromMicros(configuration.getMatViewMaxRefreshStepUs());
            while (step > 1 && approxStepDuration(step, approxBucketSize) > maxStepDuration) {
                // the step is too large, check the duration of a 2x smaller step;
                // that's to avoid overflows in the interval iterator
                step = Math.max(1, step / 2);
            }
            // Compute one step per cluster so a narrow cluster does not pin
            // the step on wider clusters (and vice versa). The iterator's
            // gap-skip then excises gaps between clusters cheaply, and each
            // step-group's cursor filter is tight to one cluster.
            //
            // Note: refreshIntervals may have been mutated since clustering by
            // the period-mat-view branch (unionIntervals/intersectIntervals
            // above). We compute per-cluster steps from the current list,
            // since that is what the iterator is about to receive.
            refreshContext.approxBucketSize = approxBucketSize;
            refreshContext.refreshIntervals = refreshIntervals;
            refreshContext.naturalStep = step;
            computePerClusterSteps(refreshIntervals, approxBucketSize, step, refreshContext.stepPerInterval);

            // there are no concurrent accesses to the sampler at this point as we've locked the state
            final SampleByIntervalIterator intervalIterator = intervalIterator(
                    driver,
                    timestampSampler,
                    viewDefinition.getTzRules(),
                    viewDefinition.getFixedOffset(),
                    viewDefinition.getSamplingIntervalUnit(),
                    refreshIntervals,
                    minTs,
                    maxTs,
                    step,
                    refreshContext.stepPerInterval
            );

            final long iteratorMinTs = intervalIterator.getMinTimestamp();
            final long iteratorMaxTs = intervalIterator.getMaxTimestamp();
            LOG.info().$("refreshing materialized view [view=").$(viewToken)
                    .$(", baseTable=").$(baseTableToken)
                    .$(", fromTxn=").$(lastRefreshTxn)
                    .$(", toTxn=").$(refreshContext.toBaseTxn)
                    .$(", periodHi=").$ts(driver, refreshContext.periodHi)
                    .$(", iteratorMinTs=").$ts(driver, iteratorMinTs)
                    .$(", iteratorMaxTs=").$ts(driver, iteratorMaxTs)
                    .$("), iteratorStep=").$(step)
                    .I$();

            refreshContext.intervalIterator = intervalIterator;
        } else {
            LOG.info().$("no intervals to refresh in materialized view [view=").$(viewToken)
                    .$(", baseTable=").$(baseTableToken)
                    .$(", fromTxn=").$(lastRefreshTxn)
                    .$(", toTxn=").$(refreshContext.toBaseTxn)
                    .$(", periodHi=").$ts(driver, refreshContext.periodHi)
                    .I$();
        }

        return refreshContext;
    }

    private boolean fullRefresh(MatViewRefreshTask refreshTask) {
        final TableToken viewToken = refreshTask.matViewToken;
        assert viewToken != null;
        final long refreshTriggerTimestamp = refreshTask.refreshTriggerTimestamp;

        final MatViewState viewState = stateStore.getViewState(viewToken);
        if (viewState == null || viewState.isDropped()) {
            return false;
        }

        if (isRefreshBlocked(viewToken)) {
            LOG.info().$("skipping materialized view full refresh, view is in the refresh block list [view=").$(viewToken).I$();
            return false;
        }

        if (isViewWriteSuspended(viewToken)) {
            LOG.debug().$("skipping full refresh, materialized view is suspended [view=").$(viewToken).I$();
            return false;
        }

        if (!viewState.tryLock()) {
            // Someone is refreshing the view, so we're going for another attempt.
            // Just mark the view invalid to prevent intermediate incremental refreshes and republish the task.
            LOG.debug().$("could not lock materialized view for full refresh, will retry [view=").$(viewToken).I$();
            viewState.markAsPendingInvalidation();
            stateStore.enqueueFullRefresh(viewToken);
            return false;
        }

        final MatViewDefinition viewDefinition = viewState.getViewDefinition();
        try (WalWriter walWriter = engine.getWalWriter(viewToken)) {
            final TableToken baseTableToken = verifyBaseTableToken(viewDefinition, viewState, walWriter);
            if (baseTableToken == null) {
                return false;
            }

            if (!baseTableToken.isWal()) {
                refreshFailState(viewDefinition, viewState, walWriter, "base table is not a WAL table");
                return false;
            }

            // Steps:
            // - truncate view
            // - compile view and insert as select on all base table partitions
            // - write the result set to WAL (or directly to table writer O3 area)
            // - apply resulting commit
            // - update applied to txn in MatViewStateStore
            try (TableReader baseTableReader = engine.getReader(baseTableToken)) {
                // Operate SQL on a fixed reader that has known max transaction visible. The reader
                // is used to initialize base table readers returned from the refreshExecutionContext.getReader()
                // call, so that all of them are at the same txn.
                engine.detachReader(baseTableReader);
                refreshSqlExecutionContext.of(baseTableReader);
                try {
                    fencedMatViewCommit(walWriter::truncateSoft);
                    resetInvalidState(viewState, walWriter);

                    final RefreshContext refreshContext = findRefreshIntervals(baseTableReader, viewDefinition, viewState, walWriter, Numbers.LONG_NULL);
                    insertAsSelect(viewDefinition, viewState, walWriter, refreshContext, refreshTriggerTimestamp);
                } finally {
                    refreshSqlExecutionContext.clearReader();
                    engine.attachReader(baseTableReader);
                }
            } catch (Throwable th) {
                // A demote that flips the read-only flag mid-refresh makes the commit fence refuse from
                // inside the pump; re-throw so the outer catch defers (retry-later) instead of invalidating.
                rethrowReadOnlyRefusal(th);
                LOG.error()
                        .$("could not perform full refresh [view=").$(viewToken)
                        .$(", baseTable=").$(baseTableToken)
                        .$(", ex=").$(th)
                        .I$();
                refreshFailState(viewDefinition, viewState, walWriter, th);
                return false;
            }
        } catch (Throwable th) {
            if (isTableSuspendedError(th)) {
                LOG.info().$("skipping full refresh, materialized view is suspended [view=").$(viewToken).I$();
                return false;
            }
            if (handleErrorRetryRefresh(th, viewToken, stateStore, refreshTask)) {
                // Full refresh is re-scheduled.
                return false;
            }
            // If we're here, we either couldn't obtain the WAL writer or the writer couldn't write
            // invalid state transaction. Update the in-memory state and call it a day.
            LOG.error()
                    .$("could not perform full refresh, unexpected error [view=").$(viewToken)
                    .$(", ex=").$(th)
                    .I$();
            refreshFailState(viewDefinition, viewState, null, th);
            return false;
        } finally {
            viewState.incrementRefreshSeq();
            viewState.unlock();
            viewState.tryCloseIfDropped();
            viewState.tryCloseIfClosed();
        }

        if (viewDefinition.getRefreshType() == MatViewDefinition.REFRESH_TYPE_IMMEDIATE) {
            // Kickstart incremental refresh.
            stateStore.enqueueIncrementalRefresh(viewToken);
        }
        return true;
    }

    private RecordToRowCopier getRecordToRowCopier(TableWriterAPI tableWriter, RecordCursorFactory factory, SqlCompiler compiler) throws SqlException {
        columnFilter.of(factory.getMetadata().getColumnCount());
        return RecordToRowCopierUtils.generateCopier(
                compiler.getAsm(),
                factory.getMetadata(),
                tableWriter.getMetadata(),
                columnFilter,
                configuration
        );
    }

    /**
     * Returns true if the throwable is a transient, retriable refresh error: a "table busy" error
     * (base table reader pool or the view's WAL writer pool exhausted), or an out-of-memory error
     * that survived the in-call interval step reduction. Such errors are non-critical and should be
     * deferred and retried later instead of invalidating the materialized view.
     */
    private static boolean isRetriableRefreshError(Throwable th) {
        return th instanceof EntryUnavailableException || CairoException.isCairoOomError(th);
    }

    private static CharSequence retriableReason(Throwable th) {
        if (th instanceof EntryUnavailableException) {
            return ((EntryUnavailableException) th).getReason();
        }
        return th instanceof CairoException ? ((CairoException) th).getFlyweightMessage() : th.getMessage();
    }

    /**
     * Schedules a deferred incremental refresh retry for a view that hit a transient, retriable error
     * (base table or WAL writer pool exhausted, or out-of-memory), instead of invalidating it.
     * {@link MatViewTimerJob} re-drives the refresh once the backoff elapses. Each consecutive
     * deferral bumps a per-view counter; once it exceeds the configured limit this method returns
     * false so the caller invalidates the view, which releases base-table WAL retention. A successful
     * refresh resets the counter (see {@link MatViewState#resetRefreshRetry()}).
     *
     * @return true if a retry was scheduled; false if the error is not retriable or the retry limit
     * was exceeded - in both cases the caller invalidates the view
     */
    private boolean tryScheduleRetry(MatViewState viewState, TableToken viewToken, Throwable th) {
        if (!isRetriableRefreshError(th)) {
            return false;
        }
        final int attempt = viewState.incrementRefreshRetryCount();
        if (attempt > maxRefreshRetryAttempts) {
            LOG.error().$("materialized view refresh retry limit exceeded, invalidating [view=").$(viewToken)
                    .$(", attempts=").$(attempt - 1)
                    .$(", limit=").$(maxRefreshRetryAttempts)
                    .$(", reason=").$safe(retriableReason(th))
                    .I$();
            // The caller invalidates next; clear the retry state so an invalid view carries no stale
            // backoff deadline.
            viewState.resetRefreshRetry();
            return false;
        }
        final long retryAfterMicros = microsecondClock.getTicks() + busyRetryTimeoutUs;
        viewState.scheduleRefreshRetry(retryAfterMicros);
        // Wake up the timer job at the retry deadline so it re-drives this view without
        // scanning the full view fleet on every tick.
        stateStore.notifyRefreshRetry(viewToken, retryAfterMicros);
        LOG.info().$("materialized view refresh deferred [view=").$(viewToken)
                .$(", reason=").$safe(retriableReason(th))
                .$(", attempt=").$(attempt)
                .$(", limit=").$(maxRefreshRetryAttempts)
                .$(", retryInMs=").$(busyRetryTimeoutUs / 1000)
                .I$();
        return true;
    }

    private boolean handleErrorRetryRefresh(
            Throwable th,
            TableToken viewToken,
            @Nullable MatViewStateStore stateStore,
            @Nullable MatViewRefreshTask refreshTask
    ) {
        if (th instanceof CairoException ex) {
            if (ex.isAuthorizationError()) {
                // A read-only refusal from the role gate (the node is, or just became, a replica):
                // the refresh job acquires its WalWriter via the read-only chokepoint, which throws
                // an authorization error on a replica. This is a transient role condition, NOT a
                // refresh failure -- so do NOT invalidate the view or its dependents (return true).
                // Re-enqueue the task so it retries after a re-promote, but ONLY when the node is not
                // already read-only: during a demote the lifecycle thread drains this same queue to
                // empty, and a re-enqueue here would self-feed that drain forever (the queue never
                // empties). A node that is read-only discards its refresh queue at the demote's NoOp
                // swap and rebuilds it from disk on the next promote, so re-enqueuing during the
                // read-only window is pure wasted work that only traps the quiesce drain. The refresh
                // job runs under the internal all-access context, so an authorization error here can
                // only be the read-only gate; a genuine ACL denial cannot reach this path.
                if (stateStore != null && !engine.isReadOnlyMode()) {
                    if (refreshTask == null || refreshTask.operation == MatViewRefreshTask.INCREMENTAL_REFRESH) {
                        stateStore.enqueueIncrementalRefresh(viewToken);
                    } else if (refreshTask.operation == MatViewRefreshTask.FULL_REFRESH) {
                        stateStore.enqueueFullRefresh(viewToken);
                    } else if (refreshTask.operation == MatViewRefreshTask.RANGE_REFRESH) {
                        stateStore.enqueueRangeRefresh(viewToken, refreshTask.rangeFrom, refreshTask.rangeTo);
                    } else if (refreshTask.operation == MatViewRefreshTask.UPDATE_REFRESH_INTERVALS) {
                        stateStore.enqueueUpdateRefreshIntervals(viewToken);
                    } else {
                        return false;
                    }
                }
                // Fires on EVERY read-only refusal, requeued or not: the contract is retry-later, never
                // invalidate, so always return true. When the node is already read-only the requeue above
                // is skipped (the work is rebuilt from disk on the next promote), but the refusal is still
                // a deferral, not a failure.
                LOG.debug().$("materialized view refresh deferred, node is read-only [view=").$(viewToken).I$();
                return true;
            }
            if (ex.isTableDoesNotExist()) {
                // Can be that the mat view underlying table is in the middle of being renamed at this moment,
                // do not invalidate the view in this case.
                TableToken updatedToken = engine.getUpdatedTableToken(viewToken);
                if (updatedToken != null && updatedToken != viewToken) {
                    // The table was renamed, so we need to update the state
                    if (stateStore != null) {
                        if (refreshTask == null || refreshTask.operation == MatViewRefreshTask.INCREMENTAL_REFRESH) {
                            stateStore.enqueueIncrementalRefresh(updatedToken);
                        } else if (refreshTask.operation == MatViewRefreshTask.FULL_REFRESH) {
                            stateStore.enqueueFullRefresh(updatedToken);
                        } else if (refreshTask.operation == MatViewRefreshTask.RANGE_REFRESH) {
                            stateStore.enqueueRangeRefresh(updatedToken, refreshTask.rangeFrom, refreshTask.rangeTo);
                        } else if (refreshTask.operation == MatViewRefreshTask.UPDATE_REFRESH_INTERVALS) {
                            stateStore.enqueueUpdateRefreshIntervals(updatedToken);
                        } else {
                            // Invalid task, we cannot retry it.
                            return false;
                        }
                    }
                    return true;
                }
            }
        }
        return false;
    }

    private boolean incrementalRefresh(MatViewRefreshTask refreshTask) {
        final TableToken baseTableToken = refreshTask.baseTableToken;
        final TableToken viewToken = refreshTask.matViewToken;
        final long refreshTriggerTimestamp = refreshTask.refreshTriggerTimestamp;
        if (viewToken == null) {
            return refreshDependentViewsIncremental(baseTableToken, graph, stateStore, refreshTriggerTimestamp);
        } else {
            return refreshIncremental(viewToken, stateStore, refreshTriggerTimestamp);
        }
    }

    private boolean insertAsSelect(
            @NotNull MatViewDefinition viewDefinition,
            @NotNull MatViewState viewState,
            @NotNull WalWriter walWriter,
            @NotNull RefreshContext refreshContext,
            long refreshTriggerTimestamp
    ) {
        assert viewState.isLocked();

        final int maxRetries = configuration.getMatViewMaxRefreshRetries();
        final long batchSize = configuration.getMatViewInsertAsSelectBatchSize();

        RecordCursorFactory factory = null;
        RecordToRowCopier copier;
        final long prevRefreshStartTimestamp = viewState.getLastRefreshStartTimestampUs();
        final long refreshStartTimestamp = microsecondClock.getTicks();
        viewState.setLastRefreshStartTimestampUs(refreshStartTimestamp);
        final TableToken viewTableToken = viewDefinition.getMatViewToken();
        final SampleByIntervalIterator intervalIterator = refreshContext.intervalIterator;

        if (refreshContext.hasTruncateBarrier) {
            // A truncate sits in the scanned range and interval planning already finalized the invalidation
            // inline. Do NOT commit any watermark advance here -- that would push lastRefreshBaseTxn past
            // the truncate and blind the load-time backstop if the in-memory invalidation is later lost.
            // This must run regardless of whether an interval iterator was built: a period mat-view
            // synthesizes a non-null iterator from its period bounds even when the barrier cleared the
            // incremental intervals, so the check sits above the intervalIterator == null branch to cover
            // that path too. Restore the in-memory start timestamp bumped above so the view does not report
            // "refreshing" forever, and end this run without advancing.
            viewState.setLastRefreshStartTimestampUs(prevRefreshStartTimestamp);
            return false;
        }

        // If we don't have intervals to query, we may still need to bump base table txn or last period hi.
        if (intervalIterator == null) {
            final long commitBaseTxn = refreshContext.toBaseTxn != -1 ? refreshContext.toBaseTxn : viewState.getLastRefreshBaseTxn();
            final long commitPeriodHi = refreshContext.periodHi != Numbers.LONG_NULL ? refreshContext.periodHi : viewState.getLastPeriodHi();
            // Only commit when the watermark actually advances. Committing an unchanged watermark
            // writes a no-op replace-range WAL transaction; when the base table apply lags, the
            // self-re-enqueueing refresh loop can emit millions of these, flooding the view's WAL
            // and stalling replicas that apply them.
            if (commitBaseTxn > viewState.getLastRefreshBaseTxn() || commitPeriodHi > viewState.getLastPeriodHi()) {
                refreshSuccessNoRows(
                        viewState,
                        walWriter,
                        microsecondClock.getTicks(),
                        refreshTriggerTimestamp,
                        commitBaseTxn,
                        commitPeriodHi
                );
                return true;
            }
            // The watermark did not advance, so we skip the no-op WAL commit, leaving the persisted
            // refresh finish timestamp behind the in-memory start timestamp bumped above. Since
            // materialized_views reads the start from memory and the finish from the persisted state
            // file, the view would report the "refreshing" status forever. Restore the start timestamp.
            viewState.setLastRefreshStartTimestampUs(prevRefreshStartTimestamp);
            return false;
        }

        try {
            factory = viewState.acquireRecordFactory();
            copier = viewState.getRecordToRowCopier();

            OUTER:
            for (int i = 0; i <= maxRetries; i++) {
                // One tracker per refresh attempt; the finally releases it before each retry.
                final MemoryTracker memoryTracker = engine.getMemoryTrackerProvider().acquire(
                        refreshSqlExecutionContext.getSecurityContext(),
                        viewTableToken.getTableId(),
                        MemoryTrackerWorkload.MAT_VIEW_REFRESH
                );
                refreshSqlExecutionContext.setMemoryTracker(memoryTracker);
                try {
                    if (factory == null) {
                        final String viewSql = viewDefinition.getMatViewSql();
                        try (SqlCompiler compiler = engine.getSqlCompiler()) {
                            LOG.info().$("compiling materialized view [view=").$(viewTableToken).$(", attempt=").$(i).I$();
                            final CompiledQuery compiledQuery = compiler.compile(viewSql, refreshSqlExecutionContext);
                            assert compiledQuery.getType() == CompiledQuery.SELECT;
                            factory = compiledQuery.getRecordCursorFactory();
                            if (copier == null || walWriter.getMetadata().getMetadataVersion() != viewState.getRecordRowCopierMetadataVersion()) {
                                copier = getRecordToRowCopier(walWriter, factory, compiler);
                            }
                        } catch (SqlException e) {
                            factory = Misc.free(factory);
                            LOG.error().$("could not compile materialized view [view=").$(viewTableToken)
                                    .$(", sql=").$(viewSql)
                                    .$(", errorPos=").$(e.getPosition())
                                    .$(", attempt=").$(i)
                                    .$(", error=").$safe(e.getFlyweightMessage())
                                    .I$();
                            refreshFailState(viewDefinition, viewState, walWriter, e);
                            return false;
                        }
                    }

                    assert factory != null;
                    assert copier != null;
                    final int timestampType = factory.getMetadata().getTimestampType();
                    if (timestampType != walWriter.getMetadata().getTimestampType()) {
                        throw CairoException.nonCritical().put("timestamp type mismatch between materialized view and query [view=")
                                .put(ColumnType.nameOf(walWriter.getMetadata().getTimestampType()))
                                .put(", query=")
                                .put(ColumnType.nameOf(timestampType))
                                .put(']');
                    }

                    final CharSequence timestampName = walWriter.getMetadata().getColumnName(walWriter.getMetadata().getTimestampIndex());
                    final int cursorTimestampIndex = factory.getMetadata().getColumnIndex(timestampName);
                    assert cursorTimestampIndex > -1;

                    long commitTarget = batchSize;
                    long rowCount = 0;
                    // Pending scan metrics accumulate across iterations and fold
                    // into the EMA only after a successful REPLACE_RANGE commit.
                    // Deferring keeps rolled-back work (retries on oversized
                    // batches, OOM, commit failure) out of the cost model: the
                    // EMA must reflect committed work, not wasted scans that
                    // get discarded with walWriter.rollback().
                    long pendingScanSampleNanos = 0L;
                    long pendingScanRangeTsUnits = 0L;

                    if (refreshContext.stepPerInterval.size() > 0) {
                        intervalIterator.toTop(refreshContext.stepPerInterval);
                    } else {
                        intervalIterator.toTop(refreshContext.naturalStep);
                    }
                    long replacementTimestampLo = Long.MIN_VALUE;
                    long replacementTimestampHi = Long.MIN_VALUE;

                    while (intervalIterator.next()) {
                        refreshSqlExecutionContext.setRange(
                                intervalIterator.getTimestampLo(),
                                intervalIterator.getTimestampHi(),
                                viewDefinition.getBaseTableTimestampDriver().getTimestampType()
                        );
                        if (replacementTimestampHi != intervalIterator.getTimestampLo()) {
                            if (replacementTimestampHi > replacementTimestampLo) {
                                // Gap in the refresh intervals, commit the previous batch
                                // so that the replacement interval does not span across the gap.
                                final long commitStart = System.nanoTime();
                                final long lo = replacementTimestampLo;
                                final long hi = replacementTimestampHi;
                                fencedMatViewCommit(() -> walWriter.commitWithParams(
                                        lo,
                                        hi,
                                        WAL_DEDUP_MODE_REPLACE_RANGE
                                ));
                                viewState.recordCommitNanos(System.nanoTime() - commitStart);
                                if (pendingScanRangeTsUnits > 0) {
                                    viewState.recordScanMetrics(pendingScanSampleNanos, pendingScanRangeTsUnits);
                                    pendingScanSampleNanos = 0L;
                                    pendingScanRangeTsUnits = 0L;
                                }
                                commitTarget = rowCount + batchSize;
                            }
                            replacementTimestampLo = intervalIterator.getTimestampLo();
                        }

                        // Interval high and replace range high are both exclusive
                        replacementTimestampHi = intervalIterator.getTimestampHi();

                        final long scanStart = System.nanoTime();
                        try (RecordCursor cursor = factory.getCursor(refreshSqlExecutionContext)) {
                            final Record record = cursor.getRecord();
                            final TimestampDriver driver = ColumnType.getTimestampDriver(timestampType);
                            long insertedRows = 0;
                            while (cursor.hasNext()) {
                                final long timestamp = record.getTimestamp(cursorTimestampIndex);
                                if (timestamp < replacementTimestampLo || timestamp > replacementTimestampHi) {
                                    throw CairoException.nonCritical()
                                            .put("timestamp out of replace range [expected=").ts(driver, replacementTimestampLo)
                                            .put(", ").ts(driver, replacementTimestampHi)
                                            .put(", actual=").ts(driver, timestamp)
                                            .put(']');
                                }
                                final TableWriter.Row row = walWriter.newRow(timestamp);
                                copier.copy(refreshSqlExecutionContext, record, row);
                                row.append();
                                insertedRows++;
                            }
                            final long scanNanos = System.nanoTime() - scanStart;
                            final long scanRangeTsUnits = intervalIterator.getTimestampHi() - intervalIterator.getTimestampLo();

                            // Check if we've inserted a lot of rows in a single iteration.
                            if (insertedRows > batchSize && i < maxRetries && refreshContext.naturalStep > 1) {
                                // Yes, the transaction is large, thus try once again with a proportionally smaller step.
                                final double transactionRatio = (double) batchSize / insertedRows;
                                refreshContext.naturalStep = Math.max((long) (transactionRatio * refreshContext.naturalStep) - 1, 1);
                                computePerClusterSteps(
                                        refreshContext.refreshIntervals,
                                        refreshContext.approxBucketSize,
                                        refreshContext.naturalStep,
                                        refreshContext.stepPerInterval
                                );
                                walWriter.rollback();
                                LOG.info().$("inserted too many rows in a single iteration, retrying with a reduced step [view=").$(viewTableToken)
                                        .$(", insertedRows=").$(insertedRows)
                                        .$(", batchSize=").$(batchSize)
                                        .$(", intervalStep=").$(refreshContext.naturalStep)
                                        .I$();
                                continue OUTER;
                            }

                            // Iteration is past the retry gate -- accumulate its
                            // scan metrics. They fold into the EMA only when the
                            // next commit succeeds; any subsequent rollback (OOM,
                            // commit failure) discards them via the for-loop
                            // restart, which re-declares these locals at zero.
                            pendingScanSampleNanos += scanNanos;
                            pendingScanRangeTsUnits += scanRangeTsUnits;

                            rowCount += insertedRows;
                            if (rowCount >= commitTarget) {
                                final long commitStart = System.nanoTime();
                                if (intervalIterator.isLast()) {
                                    commitMatView(
                                            viewState,
                                            walWriter,
                                            refreshContext,
                                            factory,
                                            copier,
                                            refreshTriggerTimestamp,
                                            replacementTimestampLo,
                                            replacementTimestampHi
                                    );
                                } else {
                                    final long lo = replacementTimestampLo;
                                    final long hi = replacementTimestampHi;
                                    fencedMatViewCommit(() -> walWriter.commitWithParams(
                                            lo,
                                            hi,
                                            WAL_DEDUP_MODE_REPLACE_RANGE
                                    ));
                                }
                                viewState.recordCommitNanos(System.nanoTime() - commitStart);
                                viewState.recordScanMetrics(pendingScanSampleNanos, pendingScanRangeTsUnits);
                                pendingScanSampleNanos = 0L;
                                pendingScanRangeTsUnits = 0L;

                                replacementTimestampLo = replacementTimestampHi;
                                commitTarget = rowCount + batchSize;
                            }
                        }
                    }

                    if (replacementTimestampHi > replacementTimestampLo) {
                        final long commitStart = System.nanoTime();
                        commitMatView(
                                viewState,
                                walWriter,
                                refreshContext,
                                factory,
                                copier,
                                refreshTriggerTimestamp,
                                replacementTimestampLo,
                                replacementTimestampHi
                        );
                        viewState.recordCommitNanos(System.nanoTime() - commitStart);
                        if (pendingScanRangeTsUnits > 0) {
                            viewState.recordScanMetrics(pendingScanSampleNanos, pendingScanRangeTsUnits);
                            pendingScanSampleNanos = 0L;
                            pendingScanRangeTsUnits = 0L;
                        }
                    }
                    break;
                } catch (TableReferenceOutOfDateException e) {
                    factory = Misc.free(factory);
                    walWriter.rollback();
                    if (i == maxRetries) {
                        LOG.info().$("base table is under heavy DDL changes, will retry refresh later [view=").$(viewTableToken)
                                .$(", totalAttempts=").$(maxRetries)
                                .$(", msg=").$safe(e.getFlyweightMessage())
                                .I$();
                        stateStore.enqueueIncrementalRefresh(viewTableToken);
                        return false;
                    }
                } catch (Throwable th) {
                    factory = Misc.free(factory);
                    walWriter.rollback();
                    if (th instanceof CairoException ce && CairoException.isCairoOomError(ce) && i < maxRetries && refreshContext.naturalStep > 1) {
                        refreshContext.naturalStep /= 2;
                        computePerClusterSteps(
                                refreshContext.refreshIntervals,
                                refreshContext.approxBucketSize,
                                refreshContext.naturalStep,
                                refreshContext.stepPerInterval
                        );
                        LOG.info().$("query failed with out-of-memory, retrying with a reduced step [view=").$(viewTableToken)
                                .$(", intervalStep=").$(refreshContext.naturalStep)
                                .$(", error=").$safe(ce.getFlyweightMessage())
                                .I$();
                        // Step reduction is the actual OOM mitigation; retry immediately without
                        // sleeping the refresh worker (which drains the whole queue on one thread).
                        // Once the step can no longer shrink, the throw below propagates and the
                        // incremental caller defers the retry to the timer.
                        continue;
                    }
                    throw th;
                } finally {
                    refreshSqlExecutionContext.setMemoryTracker(null);
                    memoryTracker.close();
                }
            }
        } catch (Throwable th) {
            Misc.free(factory);
            if (isRetriableRefreshError(th)) {
                // Transient: base table reader pool exhausted, or an out-of-memory error that survived
                // the in-call interval step reduction. The interval loop already rolled the WAL writer
                // back before rethrowing, so propagate to the caller: incremental and range refresh
                // schedule a deferred retry (up to the configured limit) instead of invalidating,
                // while full refresh invalidates as before (it truncates the view up front).
                throw (RuntimeException) th;
            }
            // A demote that flips the read-only flag after this refresh acquired its WalWriter makes the
            // commit fence refuse the mint from inside the pump; re-throw so the caller's outer catch defers
            // (retry-later) instead of invalidating the view (which would leave it sticky-invalid while the
            // on-disk state stays valid -- monitoring cannot see it).
            rethrowReadOnlyRefusal(th);
            int errno = Integer.MIN_VALUE;
            if (th instanceof CairoException e) {
                if (e.isInterruption() && engine.isClosing()) {
                    // The query was cancelled, because a questdb shutdown.
                    LOG.info().$("materialized view refresh cancelled on shutdown [view=").$(viewTableToken)
                            .$(", msg=").$safe(e.getFlyweightMessage())
                            .I$();
                    return false;
                } else {
                    errno = e.getErrno();
                }
            }

            LogRecord log = LOG.error()
                    .$("could not refresh materialized view [view=").$(viewTableToken)
                    .$(", ex=").$(th);

            if (errno != Integer.MIN_VALUE) {
                log.$(", errno=").$(errno);
            }
            log.I$();

            refreshFailState(viewDefinition, viewState, walWriter, th);
            return false;
        }

        return true;
    }

    private SampleByIntervalIterator intervalIterator(
            @NotNull TimestampDriver driver,
            @NotNull TimestampSampler sampler,
            @Nullable TimeZoneRules tzRules,
            long fixedOffset,
            char samplingIntervalUnit,
            @Nullable LongList refreshIntervals,
            long minTs,
            long maxTs,
            long naturalStep,
            @NotNull LongList stepPerInterval
    ) {
        // Per-cluster mode requires a non-null intervals list and a populated
        // step list. When either is absent, fall back to the natural step --
        // the iterator then walks a single envelope at one step.
        final boolean perCluster = refreshIntervals != null && stepPerInterval.size() > 0;
        if (tzRules == null || tzRules.hasFixedOffset()) {
            long fixedTzOffset = tzRules != null ? tzRules.getOffset(0) : 0;
            if (perCluster) {
                return fixedOffsetIterator.of(
                        sampler,
                        fixedOffset - fixedTzOffset,
                        refreshIntervals,
                        minTs,
                        maxTs,
                        stepPerInterval
                );
            }
            return fixedOffsetIterator.of(
                    sampler,
                    fixedOffset - fixedTzOffset,
                    refreshIntervals,
                    minTs,
                    maxTs,
                    naturalStep
            );
        }

        // For sub-day intervals, timestamp_floor_utc uses the standard (non-DST)
        // offset for UTC↔local conversion and bakes the user offset into the floor
        // anchor. Bucket key K covers raw data in [K, K + stride). The iterator
        // boundaries must include the user offset so they align with bucket keys.
        if (CommonUtils.isSubDayUnit(samplingIntervalUnit)) {
            long stdOff = CommonUtils.getFloorUtcTzOffset(tzRules, 0, samplingIntervalUnit);
            if (perCluster) {
                return fixedOffsetIterator.of(
                        sampler,
                        fixedOffset - stdOff,
                        refreshIntervals,
                        minTs,
                        maxTs,
                        stepPerInterval
                );
            }
            return fixedOffsetIterator.of(
                    sampler,
                    fixedOffset - stdOff,
                    refreshIntervals,
                    minTs,
                    maxTs,
                    naturalStep
            );
        }

        if (perCluster) {
            return timeZoneIterator.of(
                    driver,
                    sampler,
                    tzRules,
                    fixedOffset,
                    refreshIntervals,
                    minTs,
                    maxTs,
                    stepPerInterval
            );
        }
        return timeZoneIterator.of(
                driver,
                sampler,
                tzRules,
                fixedOffset,
                refreshIntervals,
                minTs,
                maxTs,
                naturalStep
        );
    }

    private void invalidate(MatViewRefreshTask refreshTask) {
        final String invalidationReason = refreshTask.invalidationReason;
        if (refreshTask.isBaseTableTask()) {
            invalidateDependentViews(refreshTask.baseTableToken, invalidationReason);
        } else {
            invalidateView(refreshTask.matViewToken, invalidationReason, true);
        }
    }

    private void invalidateDependentViews(TableToken baseTableToken, String invalidationReason) {
        childViewSink.clear();
        graph.getDependentViews(baseTableToken, childViewSink);
        for (int v = 0, n = childViewSink.size(); v < n; v++) {
            final TableToken viewToken = childViewSink.get(v);
            invalidateView(viewToken, invalidationReason, false);
        }
        stateStore.notifyBaseInvalidated(baseTableToken);
    }

    /**
     * Returns true if the materialized view is in the configured refresh block list
     * ({@code cairo.mat.view.refresh.block.list}). Blocked views are skipped by every refresh path -
     * incremental, full, and range. They may still be invalidated by a base-table/parent cascade or
     * an explicit INVALIDATE; this is safe because invalidation runs no view SQL (so it can't trigger
     * the crash the block list guards against) and it releases the base table's WAL retention. This
     * is an operator escape hatch for a view whose refresh keeps crashing the database: blocking it
     * lets the database start and stay up. The tradeoff is that a blocked view that is never
     * invalidated never advances its last refreshed base txn, so it can pin the base table's WAL
     * retention until it is dropped or removed from the block list.
     */
    private boolean isRefreshBlocked(TableToken viewToken) {
        return configuration.isMatViewRefreshBlocked(viewToken.getTableName());
    }

    private void invalidateView(TableToken viewToken, String invalidationReason, boolean force) {
        final MatViewState viewState = stateStore.getViewState(viewToken);
        // Known limitation (tracked follow-up): the pendingInvalidation term skips a view whose earlier
        // invalidation deferred (read-only node, or the lock was held by a concurrent refresh) and was
        // re-enqueued -- so a deferred enqueued INVALIDATE that loses the lock race can leave the view
        // pending in memory while its on-disk state stays valid until a restart, REFRESH FULL, or role
        // switch rebuilds the store. The truncate barrier no longer relies on this path (it invalidates
        // inline while holding the lock + writer); the residual is the apply-time INVALIDATE race.
        if (viewState != null && !viewState.isDropped() && !viewState.isInvalid() && !viewState.isPendingInvalidation()) {
            if (engine.isReadOnlyMode()) {
                // The node is, or just became, a replica: marking the view invalid acquires a WalWriter
                // through the read-only chokepoint, which throws an authorization error that would escape
                // the refresh worker's run() (a spurious CRITICAL plus a dropped invalidation, and a
                // halt-on-error pool would stop). Defer instead -- mark the view pending and re-enqueue so
                // the invalidation retries after a re-promote, mirroring the refresh face's read-only
                // deferral. A materialized view is derived state.
                viewState.markAsPendingInvalidation();
                stateStore.enqueueInvalidate(viewToken, invalidationReason);
                return;
            }
            if (isViewWriteSuspended(viewToken)) {
                // The view is hard-suspended and writes are denied. Acquiring its WAL writer to mint the
                // invalid state would throw tableSuspended and escape run() (handleErrorRetryRefresh does not
                // recognize it). Skip instead, mirroring the refresh paths: leave the view valid and do not
                // cascade -- the view's data is unchanged. Recovery is REFRESH ... FULL after RESUME WAL; a
                // rebased or dropped base is not picked up by a plain post-resume incremental refresh.
                LOG.debug().$("skipping materialized view invalidation, view is suspended [view=").$(viewToken).I$();
                return;
            }
            if (!viewState.tryLock()) {
                LOG.debug().$("skipping materialized view invalidation, locked by another refresh run [view=").$(viewToken).I$();
                viewState.markAsPendingInvalidation();
                stateStore.enqueueInvalidate(viewToken, invalidationReason);
                return;
            }

            try {
                // Mark the view invalid only if the operation is forced or the view was never refreshed.
                if (force || viewState.getLastRefreshBaseTxn() != -1) {
                    final long prevRefreshStartTimestampUs = viewState.getLastRefreshStartTimestampUs();
                    while (true) {
                        // Just in case the view is being concurrently renamed.
                        viewToken = engine.getUpdatedTableToken(viewToken);
                        try (WalWriter walWriter = engine.getWalWriter(viewToken)) {
                            final long invalidationTimestamp = microsecondClock.getTicks();
                            LOG.error().$("marking materialized view as invalid [view=").$(viewToken)
                                    .$(", reason=").$safe(invalidationReason)
                                    .$(", ts=").$ts(invalidationTimestamp)
                                    .I$();

                            setInvalidState(viewState, walWriter, invalidationReason, invalidationTimestamp);
                            break;
                        } catch (CairoException ex) {
                            if (isTableSuspendedError(ex)) {
                                // The view was suspended between the isViewWriteSuspended gate and the
                                // getWalWriter acquire. Skip without invalidating or cascading; RESUME WAL
                                // plus REFRESH ... FULL recovers it.
                                LOG.info().$("skipping materialized view invalidation, view is suspended [view=").$(viewToken).I$();
                                return;
                            }
                            if (ex.isAuthorizationError()) {
                                // The role flipped read-only after the top-of-method guard (a demote racing
                                // this in-flight invalidate). The refusal can come from the writer acquire
                                // (nothing mutated yet) or from the commit fence inside setInvalidState,
                                // which already flipped the in-memory invalid flag before the fence refused
                                // and persisted nothing -- leaving the view invalid in memory but valid on
                                // disk. Roll that flag back to valid before deferring so the in-memory state
                                // matches disk and the deferred invalidation is a clean pending retry, not a
                                // half-applied one the re-enqueued task would then skip. Defer the same way
                                // the top guard does -- mark pending and re-enqueue -- instead of looping on
                                // the refused acquire forever. The finally unlocks. setInvalidState also
                                // bumped the in-memory start timestamp before the fence refused; restore it
                                // so the catalogue does not report this valid view as "refreshing" forever
                                // (its in-memory start would otherwise sit ahead of the persisted finish).
                                viewState.markAsValid();
                                viewState.setLastRefreshStartTimestampUs(prevRefreshStartTimestampUs);
                                viewState.markAsPendingInvalidation();
                                stateStore.enqueueInvalidate(viewToken, invalidationReason);
                                return;
                            }
                            if (!handleErrorRetryRefresh(ex, viewToken, null, null)) {
                                throw ex;
                            }
                        }
                    }
                }
            } finally {
                viewState.unlock();
                viewState.tryCloseIfDropped();
                viewState.tryCloseIfClosed();
            }
            // Invalidate dependent views recursively.
            enqueueInvalidateDependentViews(viewToken, "base materialized view is invalidated");
        }
    }

    private boolean isViewWriteSuspended(TableToken viewToken) {
        return configuration.isWalApplySuspendedWriteDenied() && engine.isWalApplySuspended(viewToken);
    }

    private boolean processNotifications() {
        boolean refreshed = false;
        if (engine.isMatViewRefreshSuspended()) {
            // A role promote has hydrated the real store but not yet opened writes. Do not dequeue or
            // execute any task while the engine is still read-only -- executing here would refuse the
            // view WalWriter and drop or mis-handle the hydrate-enqueued catch-up work. The tasks stay
            // queued and run after the gate clears (writes open).
            return false;
        }
        while (stateStore.tryDequeueRefreshTask(refreshTask)) {
            // Re-read the suspend gate AFTER the dequeue. A promote can set the gate, swap in the real
            // store, and enqueue the hydrate kickstart between this pass's top-of-method gate read and
            // this dequeue. The dequeue synchronizes-with that enqueue, which the promoter ordered
            // after the gate-set, so this read is guaranteed to observe the set gate -- a re-check in
            // the while condition would NOT (it is ordered before the dequeue). Put the task back and
            // stop: executing it now would refuse the view WalWriter on the still-read-only engine and
            // drop it. It runs after the gate clears (writes open).
            if (engine.isMatViewRefreshSuspended()) {
                stateStore.reenqueueRefreshTask(refreshTask);
                break;
            }
            if (checkIfBaseTableDropped(refreshTask)) {
                continue;
            }

            final int operation = refreshTask.operation;
            switch (operation) {
                case MatViewRefreshTask.INCREMENTAL_REFRESH:
                    refreshed |= incrementalRefresh(refreshTask);
                    break;
                case MatViewRefreshTask.RANGE_REFRESH:
                    refreshed |= rangeRefresh(refreshTask);
                    break;
                case MatViewRefreshTask.FULL_REFRESH:
                    refreshed |= fullRefresh(refreshTask);
                    break;
                case MatViewRefreshTask.INVALIDATE:
                    invalidate(refreshTask);
                    break;
                case MatViewRefreshTask.UPDATE_REFRESH_INTERVALS:
                    updateRefreshIntervals(refreshTask);
                    break;
                default:
                    throw new RuntimeException("unexpected operation: " + operation);
            }
        }
        return refreshed;
    }

    private boolean rangeRefresh(MatViewRefreshTask refreshTask) {
        final TableToken viewToken = refreshTask.matViewToken;
        assert viewToken != null;
        final long refreshTriggerTimestamp = refreshTask.refreshTriggerTimestamp;
        final long rangeFrom = refreshTask.rangeFrom;
        final long rangeTo = refreshTask.rangeTo;
        // A period-timer-driven refresh leaves rangeFrom unset (the timer only knows the period's hi
        // boundary). A transient error on this path can be deferred and re-driven safely, because the
        // re-drive runs an incremental refresh that recomputes and re-includes every complete period.
        // A user-initiated REFRESH ... RANGE FROM .. TO .. sets rangeFrom and targets an arbitrary
        // window the incremental re-drive would NOT cover, so it keeps the legacy invalidate-on-error
        // behaviour rather than silently dropping the requested range.
        final boolean periodRefresh = rangeFrom == Numbers.LONG_NULL;

        final MatViewState viewState = stateStore.getViewState(viewToken);
        if (viewState == null || viewState.isPendingInvalidation() || viewState.isInvalid() || viewState.isDropped()) {
            return false;
        }

        if (isRefreshBlocked(viewToken)) {
            LOG.info().$("skipping materialized view range refresh, view is in the refresh block list [view=").$(viewToken).I$();
            return false;
        }

        if (isViewWriteSuspended(viewToken)) {
            LOG.debug().$("skipping range refresh, materialized view is suspended [view=").$(viewToken).I$();
            return false;
        }

        if (periodRefresh && !viewState.isRefreshDue(microsecondClock.getTicks())) {
            // Period view is in a transient-refresh backoff window (e.g. the base table was busy or
            // the refresh hit out-of-memory). Skip this pass; MatViewTimerJob re-drives an incremental
            // refresh once the backoff elapses, which re-includes every complete-but-unrefreshed
            // period, so the deferred range is not lost.
            return false;
        }

        final MatViewDefinition viewDefinition = viewState.getViewDefinition();
        final TimestampDriver driver = viewDefinition.getBaseTableTimestampDriver();

        if (!viewState.tryLock()) {
            // Someone is refreshing the view, so we're going for another attempt.
            LOG.debug().$("could not lock materialized view for range refresh, will retry [view=").$(viewToken)
                    .$(", from=").$ts(driver, rangeFrom)
                    .$(", to=").$ts(driver, rangeTo)
                    .I$();
            stateStore.enqueueRangeRefresh(viewToken, rangeFrom, rangeTo);
            return false;
        }

        try (WalWriter walWriter = engine.getWalWriter(viewToken)) {
            final TableToken baseTableToken;
            final String baseTableName = viewDefinition.getBaseTableName();
            try {
                baseTableToken = engine.verifyTableName(viewDefinition.getBaseTableName());
            } catch (CairoException e) {
                LOG.error().$("could not perform range refresh, could not verify base table [view=").$(viewToken)
                        .$(", from=").$ts(driver, rangeFrom)
                        .$(", to=").$ts(driver, rangeTo)
                        .$(", baseTableName=").$(baseTableName)
                        .$(", errno=").$(e.getErrno())
                        .$(", errorMsg=").$safe(e.getFlyweightMessage())
                        .I$();
                refreshFailState(viewDefinition, viewState, walWriter, e);
                return false;
            }

            if (!baseTableToken.isWal()) {
                refreshFailState(viewDefinition, viewState, walWriter, "base table is not a WAL table");
                return false;
            }

            try (TableReader baseTableReader = engine.getReader(baseTableToken)) {
                // Operate SQL on a fixed reader that has known max transaction visible. The reader
                // is used to initialize base table readers returned from the refreshExecutionContext.getReader()
                // call, so that all of them are at the same txn.
                engine.detachReader(baseTableReader);
                refreshSqlExecutionContext.of(baseTableReader);
                try {
                    final RefreshContext refreshContext = findRefreshIntervals(
                            baseTableReader,
                            viewDefinition,
                            viewState,
                            walWriter,
                            Numbers.LONG_NULL,
                            rangeFrom,
                            rangeTo
                    );
                    insertAsSelect(viewDefinition, viewState, walWriter, refreshContext, refreshTriggerTimestamp);
                    // Refresh completed without a retriable failure; clear any accumulated retry
                    // backoff and counter so a future transient error starts from a fresh budget.
                    viewState.resetRefreshRetry();
                } finally {
                    refreshSqlExecutionContext.clearReader();
                    engine.attachReader(baseTableReader);
                }
            } catch (Throwable th) {
                // A demote that flips the read-only flag mid-refresh makes the commit fence refuse from
                // inside the pump; re-throw so the outer catch defers (retry-later) instead of invalidating.
                rethrowReadOnlyRefusal(th);
                // The !viewState.isInvalid() guard mirrors the outer catch: a prior refreshFailState
                // inside insertAsSelect may have marked the view invalid in-memory and then had its
                // WAL write (resetMatViewState) throw a retriable error that propagates here. Never
                // arm a retry on an already-invalid view.
                if (periodRefresh && !viewState.isInvalid() && tryScheduleRetry(viewState, viewToken, th)) {
                    // Transient error (base table reader pool exhausted or out-of-memory) on a
                    // period refresh: defer instead of invalidating. MatViewTimerJob re-drives an
                    // incremental refresh once the backoff elapses; that incremental refresh
                    // recomputes and re-includes every complete-but-unrefreshed period, so the
                    // deferred period range is not lost.
                    return false;
                }
                LOG.error()
                        .$("could not perform range refresh [view=").$(viewToken)
                        .$(", baseTable=").$(baseTableToken)
                        .$(", ex=").$(th)
                        .I$();
                refreshFailState(viewDefinition, viewState, walWriter, th);
                return false;
            }
        } catch (Throwable th) {
            if (isTableSuspendedError(th)) {
                LOG.info().$("skipping range refresh, materialized view is suspended [view=").$(viewToken).I$();
                return false;
            }
            if (handleErrorRetryRefresh(th, viewToken, stateStore, refreshTask)) {
                // Range refresh is re-scheduled.
                return false;
            }
            // Don't arm a retry on a view that a prior refreshFailState already marked invalid
            // in-memory (e.g. its WAL write threw a retriable error after refreshFail ran): the
            // retry would be scheduled on an already-invalid view and only dropped later by the
            // timer job's isInvalid() guard, after needlessly bumping the retry counter.
            if (periodRefresh && !viewState.isInvalid() && tryScheduleRetry(viewState, viewToken, th)) {
                // The view's WAL writer pool was exhausted; retry later instead of invalidating.
                return false;
            }
            // If we're here, we either couldn't obtain the WAL writer or the writer couldn't write
            // invalid state transaction. Update the in-memory state and call it a day.
            LOG.error()
                    .$("could not perform range refresh, unexpected error [view=").$(viewToken)
                    .$(", ex=").$(th)
                    .I$();
            refreshFailState(viewDefinition, viewState, null, th);
            return false;
        } finally {
            viewState.unlock();
            viewState.tryCloseIfDropped();
            viewState.tryCloseIfClosed();
        }

        return true;
    }

    private boolean refreshDependentViewsIncremental(
            TableToken baseTableToken,
            MatViewGraph graph,
            MatViewStateStore stateStore,
            long refreshTriggerTimestamp
    ) {
        assert baseTableToken.isWal();

        boolean refreshed = false;
        final SeqTxnTracker baseSeqTracker = engine.getTableSequencerAPI().getTxnTracker(baseTableToken);
        // Safe floor: every view opens its reader after this sample, so each examines at least this txn.
        final long minRefreshToTxn = baseSeqTracker.getWriterTxn();
        // The minimum base txn examined across the refreshed views. Acknowledging this instead of the
        // pre-loop floor lets the clean/dirty handshake converge when the readable base txn advances
        // between the floor sample and a view opening its reader, while never claiming the base is
        // refreshed past what any view actually examined.
        long minExaminedToTxn = Numbers.LONG_NULL;

        childViewSink.clear();
        graph.getDependentViews(baseTableToken, childViewSink);
        for (int v = 0, n = childViewSink.size(); v < n; v++) {
            final TableToken viewToken = childViewSink.get(v);
            final MatViewState viewState = stateStore.getViewState(viewToken);
            if (viewState != null && !viewState.isPendingInvalidation() && !viewState.isInvalid() && !viewState.isDropped()) {
                if (isRefreshBlocked(viewToken)) {
                    // View is in the configured refresh block list (e.g. its refresh keeps crashing);
                    // skip it without invalidating. The base table WAL retention can stay pinned at
                    // this view's un-advanced last refreshed txn until it is dropped or unblocked.
                    LOG.debug().$("skipping materialized view refresh, view is in the refresh block list [view=").$(viewToken).I$();
                    continue;
                }
                if (isViewWriteSuspended(viewToken)) {
                    // The view is hard-suspended and writes are denied. Skip it instead of failing into
                    // invalidation (which would also cascade-invalidate its dependents). Leaving the view's
                    // lastRefreshBaseTxn untouched keeps the base WAL it still needs from being purged; the
                    // refresh resumes on the next base-table commit after RESUME WAL.
                    LOG.debug().$("skipping incremental refresh, materialized view is suspended [view=").$(viewToken).I$();
                    continue;
                }
                final MatViewDefinition viewDefinition = viewState.getViewDefinition();
                if (viewDefinition.getRefreshType() != MatViewDefinition.REFRESH_TYPE_IMMEDIATE) {
                    // The refresh is not immediate, i.e. it's either manual or timer.
                    // Increment the sequence, so that mat view timer job knows it should enqueue a caching task
                    // when the timer is triggered.
                    viewState.incrementRefreshIntervalsSeq();
                    continue;
                }

                if (!viewState.isRefreshDue(microsecondClock.getTicks())) {
                    // View is in a transient-refresh backoff window (e.g. base table was busy);
                    // skip it this pass. MatViewTimerJob re-drives it once the backoff elapses.
                    continue;
                }

                if (!viewState.tryLock()) {
                    LOG.debug().$("skipping materialized view refresh, locked by another refresh run [view=").$(viewToken).I$();
                    stateStore.enqueueIncrementalRefresh(viewToken);
                    continue;
                }

                try (WalWriter walWriter = engine.getWalWriter(viewToken)) {
                    try {
                        final long result = refreshIncremental0(baseTableToken, viewDefinition, viewState, walWriter, refreshTriggerTimestamp);
                        refreshed |= (result & 1L) != 0;
                        final long examinedBaseTxn = result >> 1;
                        minExaminedToTxn = minExaminedToTxn != Numbers.LONG_NULL
                                ? Math.min(minExaminedToTxn, examinedBaseTxn)
                                : examinedBaseTxn;
                    } catch (Throwable th) {
                        // A demote that flips the read-only flag mid-refresh makes the commit fence refuse
                        // from inside the pump; re-throw so the outer catch defers (retry-later) instead of
                        // invalidating.
                        rethrowReadOnlyRefusal(th);
                        // Never arm a retry on an already-invalid view: a prior refreshFailState
                        // (e.g. inside insertAsSelect) may have marked it invalid in-memory and then
                        // had its WAL write (resetMatViewState) throw a retriable error that
                        // propagates here. The isInvalid() short-circuit mirrors the outer catch's
                        // !isInvalid() guard, so such a retriable error fails the state instead of
                        // arming a retry on an invalid view.
                        if (viewState.isInvalid() || !tryScheduleRetry(viewState, viewToken, th)) {
                            refreshFailState(viewDefinition, viewState, walWriter, th);
                        }
                    }
                } catch (Throwable th) {
                    if (isTableSuspendedError(th)) {
                        // The view was suspended between the isViewWriteSuspended gate and the getWalWriter
                        // acquire. Skip without invalidating; resume re-triggers the refresh.
                        LOG.info().$("skipping incremental refresh, materialized view is suspended [view=").$(viewToken).I$();
                        continue;
                    }
                    if (handleErrorRetryRefresh(th, viewToken, stateStore, null)) {
                        // Incremental refresh is re-scheduled.
                        continue;
                    }
                    // Don't arm a retry on a view that a prior refreshFailState already marked invalid
                    // in-memory (e.g. its WAL write threw a retriable error after refreshFail ran): the
                    // retry would be scheduled on an already-invalid view and only dropped later by the
                    // timer job's isInvalid() guard, after needlessly bumping the retry counter.
                    if (!viewState.isInvalid() && tryScheduleRetry(viewState, viewToken, th)) {
                        // The view's WAL writer pool was exhausted; retry later instead of invalidating.
                        continue;
                    }
                    // If we're here, we either couldn't obtain the WAL writer or the writer couldn't write
                    // invalid state transaction. Update the in-memory state and call it a day.
                    LOG.error()
                            .$("could not perform incremental refresh, unexpected error [view=").$(viewToken)
                            .$(", ex=").$(th)
                            .I$();
                    refreshFailState(viewDefinition, viewState, null, th);
                } finally {
                    viewState.incrementRefreshSeq();
                    viewState.unlock();
                    viewState.tryCloseIfDropped();
                    viewState.tryCloseIfClosed();
                }
            }
        }
        refreshTask.clear();
        refreshTask.baseTableToken = baseTableToken;
        refreshTask.operation = MatViewRefreshTask.INCREMENTAL_REFRESH;
        // Fall back to the pre-loop floor when no view was refreshed (e.g. all skipped or timer-only).
        final long refreshedToTxn = minExaminedToTxn != Numbers.LONG_NULL ? minExaminedToTxn : minRefreshToTxn;
        stateStore.notifyBaseRefreshed(refreshTask, refreshedToTxn);

        if (refreshed) {
            LOG.info().$("refreshed materialized views dependent on [baseTable=").$(baseTableToken)
                    .$(", lastSeqTxn=").$(refreshedToTxn).I$();
        }
        return refreshed;
    }

    private void refreshFailState(
            @NotNull MatViewDefinition viewDefinition,
            @NotNull MatViewState viewState,
            @Nullable WalWriter walWriter,
            CharSequence errorMessage
    ) {
        viewState.refreshFail(microsecondClock.getTicks(), errorMessage);
        // Skip the WAL state reset when the node is already read-only: a demote that landed mid-refresh
        // refuses the data commit and routes here, and minting the invalid-state reset on a held pre-flip
        // writer would externalize a local-only seqTxn the closing uploader never ships -- the peer would
        // never see the invalidation, so the demoting node's view state would silently diverge. The
        // in-memory fail state above is enough to stop the refresh on this node; a materialized view is
        // derived state, so the new primary recomputes it forward.
        if (walWriter != null && !engine.isReadOnlyMode()) {
            try {
                fencedMatViewCommit(() -> walWriter.resetMatViewState(
                        viewState.getLastRefreshBaseTxn(),
                        viewState.getLastRefreshFinishTimestampUs(),
                        true,
                        errorMessage,
                        viewState.getLastPeriodHi(),
                        viewState.getRefreshIntervals(),
                        viewState.getRefreshIntervalsBaseTxn()
                ));
            } catch (CairoException refused) {
                // A demote landed between the eager check above and the fence's in-lock re-check, so the
                // fence refused the mint. This is the abandon-on-demote outcome -- swallow it here (this is
                // already the failure path) so the refresh ends cleanly on the now-read-only node.
                if (!refused.isAuthorizationError()) {
                    throw refused;
                }
            }
        }
        // Invalidate dependent views recursively.
        enqueueInvalidateDependentViews(viewDefinition.getMatViewToken(), "base materialized view refresh failed");
    }

    private void refreshFailState(
            @NotNull MatViewDefinition viewDefinition,
            @NotNull MatViewState viewState,
            @Nullable WalWriter walWriter,
            @NotNull Throwable th
    ) {
        errorMsgSink.clear();
        if (th instanceof Sinkable) {
            ((Sinkable) th).toSink(errorMsgSink);
        } else {
            errorMsgSink.put(th.getMessage());
        }
        refreshFailState(viewDefinition, viewState, walWriter, errorMsgSink);
    }

    private boolean refreshIncremental(@NotNull TableToken viewToken, MatViewStateStore stateStore, long refreshTriggerTimestamp) {
        final MatViewState viewState = stateStore.getViewState(viewToken);
        if (viewState == null || viewState.isPendingInvalidation() || viewState.isInvalid() || viewState.isDropped()) {
            return false;
        }

        if (isRefreshBlocked(viewToken)) {
            LOG.info().$("skipping materialized view incremental refresh, view is in the refresh block list [view=").$(viewToken).I$();
            return false;
        }

        if (isViewWriteSuspended(viewToken)) {
            // The view is hard-suspended and writes are denied. Skip rather than fail into invalidation;
            // the refresh resumes on the next base-table commit after RESUME WAL.
            LOG.debug().$("skipping incremental refresh, materialized view is suspended [view=").$(viewToken).I$();
            return false;
        }

        if (!viewState.isRefreshDue(microsecondClock.getTicks())) {
            // View is in a transient-refresh backoff window; skip. MatViewTimerJob will re-drive it.
            return false;
        }

        if (!viewState.tryLock()) {
            LOG.debug().$("could not lock materialized view for incremental refresh, will retry [view=").$(viewToken).I$();
            stateStore.enqueueIncrementalRefresh(viewToken);
            return false;
        }

        final MatViewDefinition viewDefinition = viewState.getViewDefinition();
        try (WalWriter walWriter = engine.getWalWriter(viewToken)) {
            final TableToken baseTableToken = verifyBaseTableToken(viewDefinition, viewState, walWriter);
            if (baseTableToken == null) {
                return false;
            }

            if (!baseTableToken.isWal()) {
                refreshFailState(viewDefinition, viewState, walWriter, "base table is not a WAL table");
                return false;
            }

            try {
                // The examined base txn is only needed when refreshing dependent views; here we just
                // return the "refreshed" flag from bit 0.
                final long result = refreshIncremental0(baseTableToken, viewDefinition, viewState, walWriter, refreshTriggerTimestamp);
                return (result & 1L) != 0;
            } catch (Throwable th) {
                // A demote that flips the read-only flag mid-refresh makes the commit fence refuse from
                // inside the pump; re-throw so the outer catch defers (retry-later) instead of invalidating.
                rethrowReadOnlyRefusal(th);
                // Never arm a retry on an already-invalid view (see refreshDependentViewsIncremental):
                // a prior refreshFailState inside insertAsSelect may have marked it invalid in-memory
                // and then had its WAL write (resetMatViewState) throw a retriable error that
                // propagates here. The !isInvalid() guard mirrors the outer catch.
                if (!viewState.isInvalid() && tryScheduleRetry(viewState, viewToken, th)) {
                    return false;
                }
                LOG.error()
                        .$("could not perform incremental refresh [view=").$(viewToken)
                        .$(", baseTableToken=").$(baseTableToken)
                        .$(", ex=").$(th)
                        .I$();
                refreshFailState(viewDefinition, viewState, walWriter, th);
                return false;
            }
        } catch (Throwable th) {
            if (isTableSuspendedError(th)) {
                // The view was suspended between the isViewWriteSuspended gate and the getWalWriter acquire.
                // Skip without invalidating; resume re-triggers the refresh.
                LOG.info().$("skipping incremental refresh, materialized view is suspended [view=").$(viewToken).I$();
                return false;
            }
            if (handleErrorRetryRefresh(th, viewToken, stateStore, null)) {
                // Incremental refresh is re-scheduled.
                return false;
            }
            // Don't arm a retry on a view that a prior refreshFailState already marked invalid
            // in-memory (e.g. its WAL write threw a retriable error after refreshFail ran): the retry
            // would be scheduled on an already-invalid view and only dropped later by the timer job's
            // isInvalid() guard, after needlessly bumping the retry counter.
            if (!viewState.isInvalid() && tryScheduleRetry(viewState, viewToken, th)) {
                // The view's WAL writer pool was exhausted; retry later instead of invalidating.
                return false;
            }

            // If we're here, we either couldn't obtain the WAL writer or the writer couldn't write
            // invalid state transaction. Update the in-memory state and call it a day.
            LOG.error()
                    .$("could not perform incremental refresh, unexpected error [view=").$(viewToken)
                    .$(", ex=").$(th)
                    .I$();
            refreshFailState(viewDefinition, viewState, null, th);
            return false;
        } finally {
            viewState.incrementRefreshSeq();
            viewState.unlock();
            viewState.tryCloseIfDropped();
            viewState.tryCloseIfClosed();
        }
    }

    // Returns a packed long: the examined base table txn (the reader's seqTxn) in bits 63..1 and the
    // "refreshed" flag in bit 0. The examined txn lets refreshDependentViewsIncremental() acknowledge
    // only the base txn that was actually examined. Every non-throwing return opens the reader first,
    // so the examined txn is always valid; the txn-sanity throw below is handled by the caller without
    // reading the result.
    private long refreshIncremental0(
            @NotNull TableToken baseTableToken,
            @NotNull MatViewDefinition viewDefinition,
            @NotNull MatViewState viewState,
            @NotNull WalWriter walWriter,
            long refreshTriggerTimestamp
    ) throws SqlException {
        assert viewState.isLocked();

        // Steps:
        // - compile view and execute with timestamp ranges from the unprocessed commits
        // - write the result set to WAL (or directly to table writer O3 area)
        // - apply resulting commit
        // - update applied to txn in MatViewStateStore

        try (TableReader baseTableReader = engine.getReader(baseTableToken)) {
            final long fromBaseTxn = viewState.getLastRefreshBaseTxn();
            final long toBaseTxn = baseTableReader.getSeqTxn();
            if (fromBaseTxn > toBaseTxn) {
                final TableToken viewToken = viewDefinition.getMatViewToken();
                throw CairoException.nonCritical().put("unexpected txn numbers, base table may have been renamed [view=").put(viewToken.getTableName())
                        .put(", fromBaseTxn=").put(fromBaseTxn)
                        .put(", toBaseTxn=").put(toBaseTxn)
                        .put(']');
            }
            if (viewDefinition.getPeriodLength() == 0 && fromBaseTxn > -1 && fromBaseTxn == toBaseTxn) {
                // Non-period mat view which is already up-to-date.
                viewState.resetRefreshRetry();
                return toBaseTxn << 1;
            }

            // Operate SQL on a fixed reader that has known max transaction visible. The reader
            // is used to initialize base table readers returned from the refreshExecutionContext.getReader()
            // call, so that all of them are at the same txn.
            engine.detachReader(baseTableReader);
            refreshSqlExecutionContext.of(baseTableReader);
            try {
                final RefreshContext refreshContext = findRefreshIntervals(baseTableReader, viewDefinition, viewState, walWriter, fromBaseTxn);
                final boolean refreshed = insertAsSelect(viewDefinition, viewState, walWriter, refreshContext, refreshTriggerTimestamp);
                // Reached only when the refresh completed without a retriable failure; clear any
                // accumulated retry backoff and counter so a future transient error starts fresh.
                viewState.resetRefreshRetry();
                return (toBaseTxn << 1) | (refreshed ? 1L : 0L);
            } finally {
                refreshSqlExecutionContext.clearReader();
                engine.attachReader(baseTableReader);
            }
        }
    }

    private void refreshSuccessNoRows(
            MatViewState viewState,
            @Nullable WalWriter walWriter,
            long refreshFinishedTimestamp,
            long refreshTriggeredTimestamp,
            long baseTableTxn,
            long periodHi
    ) {
        viewState.refreshSuccessNoRows(
                refreshFinishedTimestamp,
                refreshTriggeredTimestamp,
                baseTableTxn,
                periodHi
        );
        if (walWriter != null) {
            fencedMatViewCommit(() -> walWriter.resetMatViewState(
                    baseTableTxn,
                    refreshFinishedTimestamp,
                    false,
                    null,
                    periodHi,
                    null,
                    -1
            ));
        }
    }

    private void resetInvalidState(MatViewState viewState, WalWriter walWriter) {
        viewState.markAsValid();
        viewState.setLastRefreshBaseTableTxn(-1);
        viewState.setRefreshIntervalsBaseTxn(-1);
        viewState.getRefreshIntervals().clear();
        viewState.setLastRefreshTimestampUs(Numbers.LONG_NULL);
        viewState.setLastPeriodHi(Numbers.LONG_NULL);
        fencedMatViewCommit(() -> walWriter.resetMatViewState(
                viewState.getLastRefreshBaseTxn(),
                viewState.getLastRefreshFinishTimestampUs(),
                false,
                null,
                viewState.getLastPeriodHi(),
                null,
                -1
        ));
    }

    // Re-throws a read-only authorization refusal so the surrounding outer catch routes it through
    // handleErrorRetryRefresh (re-enqueue the same operation, never invalidate). A demote can flip the
    // read-only flag after the refresh acquired its WalWriter but before it commits, so the role-switch
    // commit fence refuses the mint from inside the refresh pump -- caught here by an inner catch rather
    // than by the outer getWalWriter acquire path. Treating that refusal as a refresh failure (invalidate)
    // leaves the view sticky-invalid while its on-disk state stays valid, which monitoring cannot see; the
    // correct reaction is retry-later, identical to the acquire-path refusal. A materialized view is derived
    // state, so the new primary recomputes it forward. Scoped to read-only authorization errors only: any
    // other failure falls through to the caller's refreshFailState (still invalidates). The refresh job runs
    // under the internal all-access context, so an authorization error here can only be the read-only gate.
    private static void rethrowReadOnlyRefusal(Throwable th) {
        if (th instanceof CairoException ce && ce.isAuthorizationError()) {
            throw ce;
        }
    }

    private void setInvalidState(MatViewState viewState, WalWriter walWriter, CharSequence invalidationReason, long invalidationTimestamp) {
        viewState.markAsInvalid(invalidationReason);
        viewState.setLastRefreshTimestampUs(invalidationTimestamp);
        viewState.setLastRefreshStartTimestampUs(invalidationTimestamp);
        fencedMatViewCommit(() -> walWriter.resetMatViewState(
                viewState.getLastRefreshBaseTxn(),
                viewState.getLastRefreshFinishTimestampUs(),
                true,
                invalidationReason,
                viewState.getLastPeriodHi(),
                viewState.getRefreshIntervals(),
                viewState.getRefreshIntervalsBaseTxn()
        ));
    }

    private void updateRefreshIntervals(@NotNull MatViewRefreshTask refreshTask) {
        assert refreshTask.matViewToken != null;

        final TableToken viewToken = refreshTask.matViewToken;
        final MatViewState viewState = stateStore.getViewState(viewToken);
        if (viewState != null && !viewState.isPendingInvalidation() && !viewState.isInvalid() && !viewState.isDropped()) {
            if (isViewWriteSuspended(viewToken)) {
                // The view is hard-suspended and writes are denied. Skip the cached-interval update (which
                // persists via a WAL state mint) rather than fail into invalidation; the intervals refresh
                // on the next base-table commit after RESUME WAL.
                LOG.debug().$("skipping refresh intervals update, materialized view is suspended [view=").$(viewToken).I$();
                return;
            }
            if (!viewState.tryLock()) {
                LOG.debug().$("skipping refresh intervals update, locked by a refresh run [view=").$(viewToken).I$();
                stateStore.enqueueUpdateRefreshIntervals(viewToken);
                return;
            }

            final MatViewDefinition viewDefinition = viewState.getViewDefinition();
            try (WalWriter walWriter = engine.getWalWriter(viewToken)) {
                final TableToken baseTableToken = verifyBaseTableToken(viewDefinition, viewState, walWriter);
                if (baseTableToken == null) {
                    return;
                }

                final SeqTxnTracker baseSeqTracker = engine.getTableSequencerAPI().getTxnTracker(baseTableToken);
                final long lastTxn = baseSeqTracker.getWriterTxn();
                updateRefreshIntervals0(lastTxn, baseTableToken, viewDefinition, viewState, walWriter);
            } catch (Throwable th) {
                if (isTableSuspendedError(th)) {
                    // The view was suspended between the isViewWriteSuspended gate and the getWalWriter
                    // acquire. Skip without invalidating; resume re-triggers the interval update.
                    LOG.info().$("skipping refresh intervals update, materialized view is suspended [view=").$(viewToken).I$();
                    return;
                }
                if (handleErrorRetryRefresh(th, viewToken, stateStore, refreshTask)) {
                    // A read-only refusal (a demote racing this interval update; the writer acquire or
                    // the inline-mint commit fence refuses) or an in-progress base-table rename is a
                    // transient condition, not a refresh failure. handleErrorRetryRefresh re-enqueues
                    // the task when still writable and returns true. Mirror the incremental path: do NOT
                    // invalidate. Without this the refusal routes to refreshFailState below, freezing the
                    // view invalid-in-memory / valid-on-disk and firing a spurious dependent cascade.
                    return;
                }
                // If we're here, we couldn't obtain the WAL writer or commit the interval state for a
                // non-transient reason. Update the in-memory state and call it a day.
                LOG.error()
                        .$("could not update refresh intervals, unexpected error [view=").$(viewToken)
                        .$(", ex=").$(th)
                        .I$();
                refreshFailState(viewDefinition, viewState, null, th);
            } finally {
                viewState.unlock();
                viewState.tryCloseIfDropped();
                viewState.tryCloseIfClosed();
            }
        }
    }

    private LongList updateRefreshIntervals0(
            long lastBaseTxn,
            @NotNull TableToken baseTableToken,
            @NotNull MatViewDefinition viewDefinition,
            @NotNull MatViewState viewState,
            @NotNull WalWriter walWriter
    ) {
        assert viewState.isLocked();

        // Reset the barrier signal at the top so a value set by a prior call cannot leak forward on the
        // shared refresh context. The standalone UPDATE_REFRESH_INTERVALS task path reaches this method
        // without going through findRefreshIntervals (which clears the context), and insertAsSelect now
        // reads this flag before the interval-iterator check, so a stale true would otherwise wrongly
        // abort an unrelated later refresh.
        refreshContext.hasTruncateBarrier = false;

        final TableToken viewToken = viewDefinition.getMatViewToken();
        final long lastRefreshTxn = Math.max(viewState.getLastRefreshBaseTxn(), viewState.getRefreshIntervalsBaseTxn());

        if (lastRefreshTxn > -1) {
            // lastBaseTxn may originate from a SeqTxnTracker in which case it may be behind
            // the readable base txn and the last refresh txn. If so or if the txn hasn't changed
            // since the last refresh, we don't need to update the intervals.
            if (lastRefreshTxn >= lastBaseTxn) {
                return viewState.getRefreshIntervals();
            }

            try {
                intervals.clear();
                txnRangeLoader.load(engine, Path.PATH.get(), baseTableToken, intervals, lastRefreshTxn, lastBaseTxn);
                if (txnRangeLoader.hasTruncate()) {
                    // The scanned base WAL range contains a TRUNCATE. The loader skips it as a non-data
                    // txn, so the data intervals alone look like an ordinary advance -- but a truncate
                    // invalidates the view (the same way ApplyWal2TableJob invalidates dependents on a
                    // truncate). Do NOT advance refreshIntervalsBaseTxn past the barrier; finalize the
                    // invalidation inline and stop this refresh. This run already holds the view lock AND
                    // the view's WalWriter on a primary (the only role that reaches here -- a replica's
                    // writer acquire already failed upstream), so the invalidation can mint here directly.
                    // Enqueuing it instead would hand the task to the shared refresh queue, where a second
                    // pool worker can dequeue it during this run's lock-hold window, fail the lock, and park
                    // the view as pending-invalidation -- a state the queued task can no longer clear,
                    // leaving the view silently valid with stale rows. Minting inline avoids that race.
                    // Mirror invalidateView's never-refreshed guard: only mint invalid for a view that has
                    // actually refreshed before (lastRefreshBaseTxn != -1). This guard is effectively always
                    // true at the barrier: refreshIntervalsBaseTxn only advances after a data refresh sets
                    // lastRefreshBaseTxn (set solely below, gated by lastRefreshTxn > -1), and
                    // MatViewStateReader resets it when a legacy state file omits the intervals block. Kept
                    // defensively; the invariant is pinned by testRefreshIntervalsDoNotAdvanceBeforeFirstDataRefresh
                    // and testReusedReaderResetsStaleStateWhenLaterBlocksAbsent. A never-materialized view has
                    // no stale rows anyway, so holding the watermark suffices.
                    if (viewState.getLastRefreshBaseTxn() != -1) {
                        final long prevRefreshStartTimestampUs = viewState.getLastRefreshStartTimestampUs();
                        final long invalidationTimestamp = microsecondClock.getTicks();
                        LOG.info().$("marking materialized view as invalid [view=").$(viewToken)
                                .$(", reason=truncate operation, ts=").$ts(invalidationTimestamp)
                                .I$();
                        try {
                            setInvalidState(viewState, walWriter, "truncate operation", invalidationTimestamp);
                        } catch (CairoException ex) {
                            // setInvalidState flips the in-memory invalid flag and bumps the in-memory start
                            // timestamp BEFORE the fenced commit. If that commit then fails -- a read-only
                            // fence refusal (a demote flipped the node read-only after the writer acquire) or a
                            // genuine WAL/IO write failure -- nothing was persisted, so roll both back so the
                            // in-memory state matches the unchanged on-disk state before re-throwing. The
                            // refresh's outer handler then decides: a read-only refusal defers (retry-later)
                            // and the load-time backstop re-detects the truncate on the next promote (the
                            // watermark never advanced past it); a non-auth failure falls back to a full
                            // refresh. Restoring the start timestamp keeps the catalogue from reporting this
                            // still-valid view as "refreshing".
                            viewState.markAsValid();
                            viewState.setLastRefreshStartTimestampUs(prevRefreshStartTimestampUs);
                            throw ex;
                        }
                        // Cascade to chained views the same way invalidateView does on a successful mint:
                        // a mat-view built on top of this one is now stale and must be invalidated too. The
                        // enqueued tasks target only child tokens, so this is safe while still holding this
                        // view's lock. Runs only on a successful invalidation (the fence-refusal path above
                        // re-throws before reaching here).
                        enqueueInvalidateDependentViews(viewToken, "base materialized view is invalidated");
                    }
                    // Signal the surrounding refresh to stop without committing a no-rows watermark advance.
                    refreshContext.hasTruncateBarrier = true;
                    intervals.clear();
                    return intervals;
                }
                if (intervals.size() > 0) {
                    final int dividerIndex = intervals.size();
                    intervals.addAll(viewState.getRefreshIntervals());
                    IntervalUtils.unionInPlace(intervals, dividerIndex);

                    final int cacheCapacity = configuration.getMatViewMaxRefreshIntervals() << 1;
                    if (intervals.size() > cacheCapacity) {
                        // Squash the latest intervals into a single one.
                        intervals.setQuick(cacheCapacity - 1, intervals.getQuick(intervals.size() - 1));
                        intervals.setPos(cacheCapacity);
                    }
                    viewState.setRefreshIntervals(intervals);
                }
                viewState.setRefreshIntervalsBaseTxn(lastBaseTxn);

                fencedMatViewCommit(() -> walWriter.resetMatViewState(
                        viewState.getLastRefreshBaseTxn(),
                        viewState.getLastRefreshFinishTimestampUs(),
                        false,
                        null,
                        viewState.getLastPeriodHi(),
                        viewState.getRefreshIntervals(),
                        viewState.getRefreshIntervalsBaseTxn()
                ));

                return viewState.getRefreshIntervals();
            } catch (CairoException ex) {
                if (ex.isAuthorizationError()) {
                    // A read-only refusal from a commit fence (the inline truncate invalidation, or the
                    // interval-state mint above) is a transient role condition, not a missing-WAL read
                    // failure. Re-throw so the refresh's outer read-only handler defers (retry-later)
                    // rather than clearing the cached intervals and forcing a full refresh.
                    throw ex;
                }
                if (configuration.isMatViewRefreshMissingWalFilesFatal()) {
                    LOG.critical().$("could not read WAL transactions, falling back to full refresh [view=").$(viewToken)
                            .$(", ex=").$safe(ex.getFlyweightMessage())
                            .$(", errno=").$(ex.getErrno())
                            .I$();
                    throw ex;
                }
                LOG.error().$("could not read WAL transactions, falling back to full refresh [view=").$(viewToken)
                        .$(", ex=").$safe(ex.getFlyweightMessage())
                        .$(", errno=").$(ex.getErrno())
                        .I$();
                viewState.getRefreshIntervals().clear();
                viewState.setRefreshIntervalsBaseTxn(-1);
            }
        }

        return null;
    }

    private @Nullable TableToken verifyBaseTableToken(@NotNull MatViewDefinition viewDefinition, @NotNull MatViewState viewState, @NotNull WalWriter walWriter) {
        final String baseTableName = viewDefinition.getBaseTableName();
        final TableToken baseTableToken;
        try {
            baseTableToken = engine.verifyTableName(baseTableName);
        } catch (CairoException e) {
            LOG.error()
                    .$("could not verify base table [view=").$(viewDefinition.getMatViewToken())
                    .$(", baseTableName=").$(baseTableName)
                    .$(", errno=").$(e.getErrno())
                    .$(", errorMsg=").$safe(e.getFlyweightMessage())
                    .I$();
            refreshFailState(viewDefinition, viewState, walWriter, e);
            return null;
        }
        return baseTableToken;
    }

    private static class RefreshContext implements Mutable {
        public final LongList stepPerInterval = new LongList();
        public long approxBucketSize;
        public boolean hasTruncateBarrier;
        public SampleByIntervalIterator intervalIterator;
        public long naturalStep;
        public long periodHi = Numbers.LONG_NULL;
        // Reference to the live refresh intervals list owned by the iterator.
        // Held here so the retry path can recompute stepPerInterval after
        // shrinking naturalStep, without needing to walk the iterator's
        // internals.
        public LongList refreshIntervals;
        public long toBaseTxn = -1;

        @Override
        public void clear() {
            approxBucketSize = 0;
            hasTruncateBarrier = false;
            intervalIterator = null;
            naturalStep = 0;
            periodHi = Numbers.LONG_NULL;
            refreshIntervals = null;
            stepPerInterval.clear();
            toBaseTxn = -1;
        }
    }
}
