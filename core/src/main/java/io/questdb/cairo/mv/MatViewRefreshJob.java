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
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.QuietCloseable;
import io.questdb.std.datetime.CommonUtils;
import io.questdb.std.datetime.MicrosecondClock;
import io.questdb.std.datetime.TimeZoneRules;
import io.questdb.std.str.Path;
import io.questdb.std.str.Sinkable;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

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
    private final CairoConfiguration configuration;
    private final CairoEngine engine;
    private final StringSink errorMsgSink = new StringSink();
    private final FixedOffsetIntervalIterator fixedOffsetIterator = new FixedOffsetIntervalIterator();
    private final MatViewGraph graph;
    private final LongList intervals = new LongList();
    private final MicrosecondClock microsecondClock;
    private final RefreshContext refreshContext = new RefreshContext();
    private final MatViewRefreshSqlExecutionContext refreshSqlExecutionContext;
    private final MatViewRefreshTask refreshTask = new MatViewRefreshTask();
    private final MatViewStateStore stateStore;
    private final TimeZoneIntervalIterator timeZoneIterator = new TimeZoneIntervalIterator();
    private final WalTxnRangeLoader txnRangeLoader;
    private final int workerId;

    public MatViewRefreshJob(int workerId, CairoEngine engine, int sharedQueryWorkerCount) {
        try {
            this.workerId = workerId;
            this.engine = engine;
            this.refreshSqlExecutionContext = new MatViewRefreshSqlExecutionContext(engine, sharedQueryWorkerCount);
            this.graph = engine.getMatViewGraph();
            this.stateStore = engine.getMatViewStateStore();
            this.configuration = engine.getConfiguration();
            this.txnRangeLoader = new WalTxnRangeLoader(configuration);
            this.microsecondClock = configuration.getMicrosecondClock();
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
    public void close() {
        LOG.debug().$("materialized view refresh job closing [workerId=").$(workerId).I$();
        Misc.free(refreshSqlExecutionContext);
        Misc.free(txnRangeLoader);
    }

    @Override
    public boolean run(int workerId, @NotNull RunStatus runStatus) {
        // there is job instance per thread, the worker id must never change for this job
        assert this.workerId == workerId;
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
            walWriter.commitWithParams(
                    replacementTimestampLo,
                    replacementTimestampHi,
                    WAL_DEDUP_MODE_REPLACE_RANGE
            );
            // Second, if it's a period range refresh, we need to persist state
            // with the new lastPeriodHi, but the same base txn and cached txn intervals.
            // If we did a mat view data commit, we'd unintentionally reset the cached intervals.
            if (refreshContext.periodHi != Numbers.LONG_NULL) {
                walWriter.resetMatViewState(
                        viewState.getLastRefreshBaseTxn(),
                        refreshFinishTimestampUs,
                        false,
                        null,
                        commitPeriodHi,
                        viewState.getRefreshIntervals(),
                        viewState.getRefreshIntervalsBaseTxn()
                );
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
            walWriter.commitMatView(
                    refreshContext.toBaseTxn,
                    refreshFinishTimestampUs,
                    commitPeriodHi,
                    replacementTimestampLo,
                    replacementTimestampHi
            );
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
        // Publish the staged frozen-zone boundary floor only after the
        // REPLACE_RANGE commit and view-state success update have landed. The
        // backfill validator's min(own, published) invariant relies on the
        // published floor corresponding to actual committed coverage; an orphan
        // floor from a refresh that aborted pre-commit would clamp the validator
        // to a region no REPLACE_RANGE covers.
        if (refreshContext.pendingFrozenBoundaryFloor != Numbers.LONG_NULL) {
            viewState.setLastRefreshFrozenBoundaryFloor(refreshContext.pendingFrozenBoundaryFloor);
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
                // Anchor the boundary on the high-water mark of the data frontier,
                // max(max(base_ts), max(view_ts), backfillFrontier), then cap by `now`.
                // max(view_ts) is the newest bucket the view has ever materialised; when the
                // latest base partition is dropped (or a re-ingestion lowers max(base_ts)),
                // max(base_ts) retreats but those view buckets stay put (orphans), so
                // max(view_ts) preserves the frontier and the boundary cannot retreat below it
                // and let a FULL refresh wipe frozen backfill. The frontier is monotonic
                // because max(view_ts) never goes backwards: a refresh adds higher buckets as
                // data grows and leaves older ones in place. (Routine TTL drops the OLDEST base
                // partitions and lowers min(base_ts), not max, so it never moves the boundary.)
                // backfillFrontier extends the same idea to user backfill accepted BEFORE the
                // first refresh: the backfilled row is older than max(view_ts) so max(view_ts)
                // cannot protect it, but the validator records the anchor it accepted against
                // (see MatViewState.advanceBackfillFrontier) and folding it in here keeps the
                // boundary from retreating over it. The first refresh of an empty view with no
                // backfill sees all three = MIN_VALUE and just uses max(base_ts) -- there is no
                // frozen data to preserve yet. Capping by `now` keeps a stale-ingestion gap or
                // future-dated base data from pushing the boundary past the present. The
                // wall-clock escape hatch restores the pre-frozen-zone anchor (wall clock only).
                final long boundaryAnchor;
                if (configuration.isMatViewRefreshLimitWallClockEnabled()) {
                    boundaryAnchor = now;
                } else {
                    long frontier = baseTableReader.getMaxTimestamp();
                    try (TableReader viewReader = engine.getReader(viewToken)) {
                        final long viewMaxTs = viewReader.getMaxTimestamp();
                        if (viewMaxTs != Long.MIN_VALUE && (frontier == Long.MIN_VALUE || viewMaxTs > frontier)) {
                            frontier = viewMaxTs;
                        }
                    }
                    // Fold in the backfill frontier: the largest anchor any accepted user
                    // backfill was validated against (see MatViewState.advanceBackfillFrontier).
                    // A backfilled row is older than max(view_ts), so max(view_ts) cannot keep
                    // the boundary from retreating over it after a max(base_ts) drop; this can.
                    final long backfillFrontier = viewState.getBackfillFrontier();
                    if (backfillFrontier != Long.MIN_VALUE && (frontier == Long.MIN_VALUE || backfillFrontier > frontier)) {
                        frontier = backfillFrontier;
                    }
                    // Empty base AND empty view both report Long.MIN_VALUE; fall back to now to avoid underflow.
                    boundaryAnchor = frontier == Long.MIN_VALUE ? now : Math.min(frontier, now);
                }
                // The interval iterators snap minTs down to the containing bucket floor
                // (FixedOffsetIntervalIterator.ofCommon and TimeZoneIntervalIterator.of),
                // so refresh always processes whole buckets even when the boundary lands
                // mid-bucket. The backfill validator (separate layer) replicates the same
                // snap so the managed/frozen split is bucket-aligned on both sides.
                // boundaryFromAnchor saturates to Long.MIN_VALUE on overflow (absurd limit)
                // instead of using a wrapped boundary -- see MatViewBackfillValidator. The
                // boundary is already non-retreating: the anchor above folds in max(view_ts)
                // and backfillFrontier, both monotonic, so round(rawBoundary) only ever
                // advances as the view materialises or accepts backfill. An EXTEND/SHRINK of
                // the limit is absorbed by the validator's min(own, published) clamp, so no
                // extra monotonic floor clamp is needed here -- and adding one would wrongly
                // pin the boundary across an EXTEND (which must move it down).
                final long rawBoundary = MatViewBackfillValidator.boundaryFromAnchor(driver, boundaryAnchor, refreshLimitHoursOrMonths);
                minTs = Math.max(minTs, rawBoundary);
                // Stage the snapped REPLACE_RANGE.lo on the refresh context. The backfill
                // validator and materialized_views().backfill_max_ts clamp their accepted
                // floor to min(own, published) so user backfill sits strictly below it and
                // survives subsequent REPLACE_RANGE commits even when ALTER SET REFRESH LIMIT
                // changes the current LIMIT between this publish and the user commit.
                // commitMatView publishes the staged floor only after the REPLACE_RANGE
                // commit lands, so a refresh that aborts pre-commit never leaves an orphan
                // floor that does not correspond to actual REPLACE_RANGE coverage.
                final TimestampSampler publishSampler = viewDefinition.getTimestampSampler();
                publishSampler.setOffset(viewDefinition.getFixedOffset());
                refreshContext.pendingFrozenBoundaryFloor = publishSampler.round(rawBoundary);
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
            // - truncate view (only when no REFRESH LIMIT is set)
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
                    // With REFRESH LIMIT set, FULL preserves the frozen zone: the
                    // per-interval REPLACE_RANGE commits inside insertAsSelect rewrite
                    // managed-zone buckets atomically while rows below the boundary stay
                    // put. Without REFRESH LIMIT there is no frozen zone, so the legacy
                    // wipe-then-reinsert is safe and avoids leaving orphan managed-zone
                    // buckets when base data has gone missing. The escape-hatch config
                    // also forces the legacy truncate so the whole frozen-zone feature
                    // reverts together. Users who want to discard the frozen-zone backfill
                    // on a view with REFRESH LIMIT set should DROP and recreate the view.
                    if (viewDefinition.getRefreshLimitHoursOrMonths() == 0
                            || configuration.isMatViewRefreshLimitWallClockEnabled()) {
                        walWriter.truncateSoft();
                    }
                    resetInvalidState(viewState, walWriter);

                    final RefreshContext refreshContext = findRefreshIntervals(baseTableReader, viewDefinition, viewState, walWriter, Numbers.LONG_NULL);
                    insertAsSelect(viewDefinition, viewState, walWriter, refreshContext, refreshTriggerTimestamp);
                } finally {
                    refreshSqlExecutionContext.clearReader();
                    engine.attachReader(baseTableReader);
                }
            } catch (Throwable th) {
                LOG.error()
                        .$("could not perform full refresh [view=").$(viewToken)
                        .$(", baseTable=").$(baseTableToken)
                        .$(", ex=").$(th)
                        .I$();
                refreshFailState(viewDefinition, viewState, walWriter, th);
                return false;
            }
        } catch (Throwable th) {
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

    private boolean handleErrorRetryRefresh(
            Throwable th,
            TableToken viewToken,
            @Nullable MatViewStateStore stateStore,
            @Nullable MatViewRefreshTask refreshTask
    ) {
        if (th instanceof CairoException ex) {
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
        final long oomRetryTimeout = configuration.getMatViewRefreshOomRetryTimeout();
        final long batchSize = configuration.getMatViewInsertAsSelectBatchSize();

        RecordCursorFactory factory = null;
        RecordToRowCopier copier;
        final long prevRefreshStartTimestamp = viewState.getLastRefreshStartTimestampUs();
        final long refreshStartTimestamp = microsecondClock.getTicks();
        viewState.setLastRefreshStartTimestampUs(refreshStartTimestamp);
        final TableToken viewTableToken = viewDefinition.getMatViewToken();
        final SampleByIntervalIterator intervalIterator = refreshContext.intervalIterator;

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
                                walWriter.commitWithParams(
                                        replacementTimestampLo,
                                        replacementTimestampHi,
                                        WAL_DEDUP_MODE_REPLACE_RANGE
                                );
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
                                    walWriter.commitWithParams(
                                            replacementTimestampLo,
                                            replacementTimestampHi,
                                            WAL_DEDUP_MODE_REPLACE_RANGE
                                    );
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
                    if (th instanceof CairoException && CairoException.isCairoOomError(th) && i < maxRetries && refreshContext.naturalStep > 1) {
                        refreshContext.naturalStep /= 2;
                        computePerClusterSteps(
                                refreshContext.refreshIntervals,
                                refreshContext.approxBucketSize,
                                refreshContext.naturalStep,
                                refreshContext.stepPerInterval
                        );
                        LOG.info().$("query failed with out-of-memory, retrying with a reduced step [view=").$(viewTableToken)
                                .$(", intervalStep=").$(refreshContext.naturalStep)
                                .$(", error=").$safe(((CairoException) th).getFlyweightMessage())
                                .I$();
                        Os.sleep(oomRetryTimeout);
                        continue;
                    }
                    throw th;
                }
            }
        } catch (Throwable th) {
            Misc.free(factory);
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

    private void invalidateView(TableToken viewToken, String invalidationReason, boolean force) {
        final MatViewState viewState = stateStore.getViewState(viewToken);
        if (viewState != null && !viewState.isDropped() && !viewState.isInvalid()) {
            if (!viewState.tryLock()) {
                LOG.debug().$("skipping materialized view invalidation, locked by another refresh run [view=").$(viewToken).I$();
                viewState.markAsPendingInvalidation();
                stateStore.enqueueInvalidate(viewToken, invalidationReason);
                return;
            }

            try {
                // Mark the view invalid only if the operation is forced or the view was never refreshed.
                if (force || viewState.getLastRefreshBaseTxn() != -1) {
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
                            if (!handleErrorRetryRefresh(ex, viewToken, null, null)) {
                                throw ex;
                            }
                        }
                    }
                }
            } finally {
                viewState.unlock();
                viewState.tryCloseIfDropped();
            }
            // Invalidate dependent views recursively.
            enqueueInvalidateDependentViews(viewToken, "base materialized view is invalidated");
        }
    }

    private boolean processNotifications() {
        boolean refreshed = false;
        while (stateStore.tryDequeueRefreshTask(refreshTask)) {
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

        final MatViewState viewState = stateStore.getViewState(viewToken);
        if (viewState == null || viewState.isPendingInvalidation() || viewState.isInvalid() || viewState.isDropped()) {
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
                } finally {
                    refreshSqlExecutionContext.clearReader();
                    engine.attachReader(baseTableReader);
                }
            } catch (Throwable th) {
                LOG.error()
                        .$("could not perform full refresh [view=").$(viewToken)
                        .$(", baseTable=").$(baseTableToken)
                        .$(", ex=").$(th)
                        .I$();
                refreshFailState(viewDefinition, viewState, walWriter, th);
                return false;
            }
        } catch (Throwable th) {
            if (handleErrorRetryRefresh(th, viewToken, stateStore, refreshTask)) {
                // Range refresh is re-scheduled.
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
                final MatViewDefinition viewDefinition = viewState.getViewDefinition();
                if (viewDefinition.getRefreshType() != MatViewDefinition.REFRESH_TYPE_IMMEDIATE) {
                    // The refresh is not immediate, i.e. it's either manual or timer.
                    // Increment the sequence, so that mat view timer job knows it should enqueue a caching task
                    // when the timer is triggered.
                    viewState.incrementRefreshIntervalsSeq();
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
                        refreshFailState(viewDefinition, viewState, walWriter, th);
                    }
                } catch (Throwable th) {
                    if (handleErrorRetryRefresh(th, viewToken, stateStore, null)) {
                        // Incremental refresh is re-scheduled.
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
        if (walWriter != null) {
            walWriter.resetMatViewState(
                    viewState.getLastRefreshBaseTxn(),
                    viewState.getLastRefreshFinishTimestampUs(),
                    true,
                    errorMessage,
                    viewState.getLastPeriodHi(),
                    viewState.getRefreshIntervals(),
                    viewState.getRefreshIntervalsBaseTxn()
            );
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
                LOG.error()
                        .$("could not perform incremental refresh [view=").$(viewToken)
                        .$(", baseTableToken=").$(baseTableToken)
                        .$(", ex=").$(th)
                        .I$();
                refreshFailState(viewDefinition, viewState, walWriter, th);
                return false;
            }
        } catch (Throwable th) {
            if (handleErrorRetryRefresh(th, viewToken, stateStore, null)) {
                // Incremental refresh is re-scheduled.
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
            walWriter.resetMatViewState(
                    baseTableTxn,
                    refreshFinishedTimestamp,
                    false,
                    null,
                    periodHi,
                    null,
                    -1
            );
        }
    }

    private void resetInvalidState(MatViewState viewState, WalWriter walWriter) {
        viewState.markAsValid();
        viewState.setLastRefreshBaseTableTxn(-1);
        viewState.setRefreshIntervalsBaseTxn(-1);
        viewState.getRefreshIntervals().clear();
        viewState.setLastRefreshTimestampUs(Numbers.LONG_NULL);
        viewState.setLastPeriodHi(Numbers.LONG_NULL);
        walWriter.resetMatViewState(
                viewState.getLastRefreshBaseTxn(),
                viewState.getLastRefreshFinishTimestampUs(),
                false,
                null,
                viewState.getLastPeriodHi(),
                null,
                -1
        );
    }

    private void setInvalidState(MatViewState viewState, WalWriter walWriter, CharSequence invalidationReason, long invalidationTimestamp) {
        viewState.markAsInvalid(invalidationReason);
        viewState.setLastRefreshTimestampUs(invalidationTimestamp);
        viewState.setLastRefreshStartTimestampUs(invalidationTimestamp);
        walWriter.resetMatViewState(
                viewState.getLastRefreshBaseTxn(),
                viewState.getLastRefreshFinishTimestampUs(),
                true,
                invalidationReason,
                viewState.getLastPeriodHi(),
                viewState.getRefreshIntervals(),
                viewState.getRefreshIntervalsBaseTxn()
        );
    }

    private void updateRefreshIntervals(@NotNull MatViewRefreshTask refreshTask) {
        assert refreshTask.matViewToken != null;

        final TableToken viewToken = refreshTask.matViewToken;
        final MatViewState viewState = stateStore.getViewState(viewToken);
        if (viewState != null && !viewState.isPendingInvalidation() && !viewState.isInvalid() && !viewState.isDropped()) {
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
                // If we're here, we couldn't obtain the WAL writer.
                // Update the in-memory state and call it a day.
                LOG.error()
                        .$("could not update refresh intervals, unexpected error [view=").$(viewToken)
                        .$(", ex=").$(th)
                        .I$();
                refreshFailState(viewDefinition, viewState, null, th);
            } finally {
                viewState.unlock();
                viewState.tryCloseIfDropped();
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

                walWriter.resetMatViewState(
                        viewState.getLastRefreshBaseTxn(),
                        viewState.getLastRefreshFinishTimestampUs(),
                        false,
                        null,
                        viewState.getLastPeriodHi(),
                        viewState.getRefreshIntervals(),
                        viewState.getRefreshIntervalsBaseTxn()
                );

                return viewState.getRefreshIntervals();
            } catch (CairoException ex) {
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
        public SampleByIntervalIterator intervalIterator;
        public long naturalStep;
        // Snapped REPLACE_RANGE.lo computed by findRefreshIntervals, staged for
        // publish via MatViewState.setLastRefreshFrozenBoundaryFloor only after
        // the REPLACE_RANGE commit actually lands. Publishing pre-commit would
        // leave the validator clamped to a floor that no REPLACE_RANGE covers if
        // the refresh aborts mid-flight.
        public long pendingFrozenBoundaryFloor = Numbers.LONG_NULL;
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
            intervalIterator = null;
            naturalStep = 0;
            pendingFrozenBoundaryFloor = Numbers.LONG_NULL;
            periodHi = Numbers.LONG_NULL;
            refreshIntervals = null;
            stepPerInterval.clear();
            toBaseTxn = -1;
        }
    }
}
