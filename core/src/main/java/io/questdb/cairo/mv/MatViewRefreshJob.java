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

package io.questdb.cairo.mv;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.EntityColumnFilter;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TableWriterAPI;
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
import io.questdb.mp.Job;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.QuietCloseable;
import io.questdb.std.datetime.TimeZoneRules;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.std.str.Path;
import io.questdb.std.str.Sinkable;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import static io.questdb.cairo.wal.WalUtils.WAL_DEDUP_MODE_REPLACE_RANGE;
import static io.questdb.cairo.wal.WalUtils.WAL_DEFAULT_LAST_REFRESH_TIMESTAMP;

public class MatViewRefreshJob implements Job, QuietCloseable {
    private static final Log LOG = LogFactory.getLog(MatViewRefreshJob.class);
    private final ObjList<TableToken> childViewSink = new ObjList<>();
    private final ObjList<TableToken> childViewSink2 = new ObjList<>();
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

    public MatViewRefreshJob(int workerId, CairoEngine engine, int workerCount, int sharedWorkerCount) {
        try {
            this.workerId = workerId;
            this.engine = engine;
            this.refreshSqlExecutionContext = new MatViewRefreshSqlExecutionContext(engine, workerCount, sharedWorkerCount);
            this.graph = engine.getMatViewGraph();
            this.stateStore = engine.getMatViewStateStore();
            this.configuration = engine.getConfiguration();
            this.txnRangeLoader = new WalTxnRangeLoader(configuration.getFilesFacade());
            this.microsecondClock = configuration.getMicrosecondClock();
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    @TestOnly
    public MatViewRefreshJob(int workerId, CairoEngine engine) {
        this(workerId, engine, 1, 1);
    }

    @Override
    public void close() {
        LOG.info().$("materialized view refresh job closing [workerId=").$(workerId).I$();
        Misc.free(refreshSqlExecutionContext);
        Misc.free(txnRangeLoader);
    }

    @Override
    public boolean run(int workerId, @NotNull RunStatus runStatus) {
        // there is job instance per thread, the worker id must never change for this job
        assert this.workerId == workerId;
        return processNotifications();
    }

    private static long approxPartitionMicros(int partitionBy) {
        switch (partitionBy) {
            case PartitionBy.HOUR:
                return Timestamps.HOUR_MICROS;
            case PartitionBy.DAY:
                return Timestamps.DAY_MICROS;
            case PartitionBy.WEEK:
                return Timestamps.WEEK_MICROS;
            case PartitionBy.MONTH:
                return Timestamps.MONTH_MICROS_APPROX;
            case PartitionBy.YEAR:
                return Timestamps.YEAR_MICROS_NONLEAP;
            default:
                throw new UnsupportedOperationException("unexpected partition by: " + partitionBy);
        }
    }

    /**
     * Estimates density of rows per SAMPLE BY bucket. The estimate is not very precise as
     * it doesn't use exact min/max timestamps for each partition, but it should do the job
     * of splitting large refresh table scans into multiple smaller scans.
     */
    private static long estimateRowsPerBucket(@NotNull TableReader baseTableReader, long bucketMicros) {
        final long rows = baseTableReader.size();
        final long partitionMicros = approxPartitionMicros(baseTableReader.getPartitionedBy());
        final int partitionCount = baseTableReader.getPartitionCount();
        if (partitionCount > 0) {
            return Math.max(1, (rows * bucketMicros) / (partitionMicros * partitionCount));
        }
        return 1;
    }

    private static void intersectTxnIntervals(LongList txnIntervals, long lo, long hi) {
        if (txnIntervals != null && txnIntervals.size() > 0) {
            txnIntervals.add(lo, hi);
            IntervalUtils.intersectInPlace(txnIntervals, txnIntervals.size() - 2);
        }
    }

    private static void unionTxnIntervals(LongList txnIntervals, long lo, long hi) {
        if (txnIntervals != null) {
            txnIntervals.add(lo, hi);
            IntervalUtils.unionInPlace(txnIntervals, txnIntervals.size() - 2);
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

    private void enqueueInvalidateDependentViews(TableToken viewToken, String invalidationReason) {
        childViewSink2.clear();
        graph.getDependentViews(viewToken, childViewSink2);
        for (int v = 0, n = childViewSink2.size(); v < n; v++) {
            stateStore.enqueueInvalidate(childViewSink2.get(v), invalidationReason);
        }
    }

    private RefreshContext findRefreshIntervals(
            @NotNull TableReader baseTableReader,
            @NotNull MatViewState viewState,
            long lastRefreshTxn
    ) throws SqlException {
        return findRefreshIntervals(baseTableReader, viewState, lastRefreshTxn, Numbers.LONG_NULL, Numbers.LONG_NULL);
    }

    private RefreshContext findRefreshIntervals(
            @NotNull TableReader baseTableReader,
            @NotNull MatViewState viewState,
            long lastRefreshTxn,
            long rangeFrom,
            long rangeTo
    ) throws SqlException {
        refreshContext.clear();

        final long lastTxn = baseTableReader.getSeqTxn();
        final TableToken baseTableToken = baseTableReader.getTableToken();
        final MatViewDefinition viewDefinition = viewState.getViewDefinition();
        final TableToken viewToken = viewDefinition.getMatViewToken();

        final long now = microsecondClock.getTicks();
        final boolean rangeRefresh = rangeTo != Numbers.LONG_NULL;
        final boolean incrementalRefresh = lastRefreshTxn != Numbers.LONG_NULL;

        LongList txnIntervals = null;
        long minTs = Long.MAX_VALUE;
        long maxTs = Long.MIN_VALUE;
        if (incrementalRefresh) {
            // Incremental refresh. This means that there may be a data transaction in the base table
            // or someone has run REFRESH INCREMENTAL SQL.
            // Let's find min/max timestamps in the new WAL transactions.
            if (lastRefreshTxn > -1) {
                // It's a subsequent incremental refresh, so WalPurgeJob must be aware of us.
                txnIntervals = intervals;
                txnIntervals.clear();
                try {
                    txnRangeLoader.load(engine, Path.PATH.get(), baseTableToken, txnIntervals, lastRefreshTxn, lastTxn);
                    minTs = txnRangeLoader.getMinTimestamp();
                    maxTs = txnRangeLoader.getMaxTimestamp();
                } catch (CairoException ex) {
                    LOG.error().$("could not read WAL transactions, falling back to full refresh [view=").$(viewToken)
                            .$(", ex=").$safe(ex.getFlyweightMessage())
                            .$(", errno=").$(ex.getErrno())
                            .I$();
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
                if (periodLo < rangeTo) {
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
                    periodSampler = TimestampSamplerFactory.getInstance(viewDefinition.getPeriodLength(), viewDefinition.getPeriodLengthUnit(), -1);
                    viewDefinition.setPeriodSampler(periodSampler);
                }
                periodSampler.setStart(viewDefinition.getTimerStart());

                final long delay = MatViewTimerJob.periodDelayMicros(viewDefinition.getPeriodDelay(), viewDefinition.getPeriodDelayUnit());
                long nowLocal = viewDefinition.getTimerTzRules() != null
                        ? now + viewDefinition.getTimerTzRules().getOffset(now)
                        : now;
                // Period hi is exclusive, but maxTs is inclusive, so we align them.
                final long periodHiLocal = periodSampler.round(nowLocal - delay) - 1;
                final long periodHi = viewDefinition.getTimerTzRules() != null
                        ? periodHiLocal - viewDefinition.getTimerTzRules().getOffset(periodHiLocal)
                        : periodHiLocal;

                // Remove incomplete periods from both txn intervals and refresh interval.
                intersectTxnIntervals(txnIntervals, Long.MIN_VALUE, periodHi);
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
                        if (txnIntervals != null) {
                            unionTxnIntervals(txnIntervals, periodLo, periodHi);
                            minTs = txnIntervals.getQuick(0);
                            maxTs = txnIntervals.getQuick(txnIntervals.size() - 1);
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
                    minTs = Math.max(minTs, now - Timestamps.HOUR_MICROS * refreshLimitHoursOrMonths);
                } else { // months
                    minTs = Math.max(minTs, Timestamps.addMonths(now, refreshLimitHoursOrMonths));
                }
                intersectTxnIntervals(txnIntervals, minTs, Long.MAX_VALUE);
            }
        }

        if (minTs <= maxTs) {
            TimestampSampler timestampSampler = viewDefinition.getTimestampSampler();
            // For small sampling intervals such as '10T' or '2s' the actual sampler is
            // chosen as a 10x multiple of the original interval. That's to speed up
            // iteration done by the interval iterator.
            final long minIntervalMicros = configuration.getMatViewMinRefreshInterval();
            long approxBucketSize = timestampSampler.getApproxBucketSize();
            long actualInterval = viewDefinition.getSamplingInterval();
            if (approxBucketSize < minIntervalMicros) {
                while (approxBucketSize < minIntervalMicros) {
                    approxBucketSize *= 10;
                    actualInterval *= 10;
                }
                timestampSampler = TimestampSamplerFactory.getInstance(
                        actualInterval,
                        viewDefinition.getSamplingIntervalUnit(),
                        0
                );
            }

            final long rowsPerBucket = estimateRowsPerBucket(baseTableReader, timestampSampler.getApproxBucketSize());
            final int rowsPerQuery = configuration.getMatViewRowsPerQueryEstimate();
            final int step = Math.max(1, (int) (rowsPerQuery / rowsPerBucket));

            // there are no concurrent accesses to the sampler at this point as we've locked the state
            final SampleByIntervalIterator intervalIterator = intervalIterator(
                    timestampSampler,
                    viewDefinition.getTzRules(),
                    viewDefinition.getFixedOffset(),
                    txnIntervals,
                    minTs,
                    maxTs,
                    step
            );

            final long iteratorMinTs = intervalIterator.getMinTimestamp();
            final long iteratorMaxTs = intervalIterator.getMaxTimestamp();
            LOG.info().$("refreshing materialized view [view=").$(viewToken)
                    .$(", baseTable=").$(baseTableToken)
                    .$(", fromTxn=").$(lastRefreshTxn)
                    .$(", toTxn=").$(refreshContext.toBaseTxn)
                    .$(", periodHi=").$ts(refreshContext.periodHi)
                    .$(", iteratorMinTs>=").$ts(iteratorMinTs)
                    .$(", iteratorMaxTs<").$ts(iteratorMaxTs)
                    .I$();

            refreshContext.intervalIterator = intervalIterator;
        } else {
            LOG.info().$("no intervals to refresh in materialized view [view=").$(viewToken)
                    .$(", baseTable=").$(baseTableToken)
                    .$(", fromTxn=").$(lastRefreshTxn)
                    .$(", toTxn=").$(refreshContext.toBaseTxn)
                    .$(", periodHi=").$ts(refreshContext.periodHi)
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
            final TableToken baseTableToken;
            final String baseTableName = viewState.getViewDefinition().getBaseTableName();
            try {
                baseTableToken = engine.verifyTableName(viewState.getViewDefinition().getBaseTableName());
            } catch (CairoException e) {
                LOG.error().$("could not perform full refresh, could not verify base table [view=").$(viewToken)
                        .$(", baseTableName=").$(baseTableName)
                        .$(", errno=").$(e.getErrno())
                        .$(", errorMsg=").$safe(e.getFlyweightMessage())
                        .I$();
                refreshFailState(viewState, walWriter, e);
                return false;
            }

            if (!baseTableToken.isWal()) {
                refreshFailState(viewState, walWriter, "base table is not a WAL table");
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
                    walWriter.truncateSoft();
                    resetInvalidState(viewState, walWriter);

                    final RefreshContext refreshContext = findRefreshIntervals(baseTableReader, viewState, Numbers.LONG_NULL);
                    insertAsSelect(viewState, walWriter, refreshContext, refreshTriggerTimestamp);
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
                refreshFailState(viewState, walWriter, th);
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
            refreshFailState(viewState, null, th);
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
                columnFilter
        );
    }

    private boolean handleErrorRetryRefresh(
            Throwable th,
            TableToken viewToken,
            @Nullable MatViewStateStore stateStore,
            @Nullable MatViewRefreshTask refreshTask
    ) {
        if (th instanceof CairoException) {
            CairoException ex = (CairoException) th;
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
            @NotNull MatViewState viewState,
            @NotNull WalWriter walWriter,
            @NotNull MatViewRefreshJob.RefreshContext refreshContext,
            long refreshTriggerTimestamp
    ) {
        assert viewState.isLocked();

        final int maxRetries = configuration.getMatViewMaxRefreshRetries();
        final long oomRetryTimeout = configuration.getMatViewRefreshOomRetryTimeout();
        final long batchSize = configuration.getMatViewInsertAsSelectBatchSize();

        RecordCursorFactory factory = null;
        RecordToRowCopier copier;
        final long refreshStartTimestamp = microsecondClock.getTicks();
        viewState.setLastRefreshStartTimestamp(refreshStartTimestamp);
        final MatViewDefinition viewDefinition = viewState.getViewDefinition();
        final TableToken viewTableToken = viewDefinition.getMatViewToken();
        final long commitBaseTxn = refreshContext.toBaseTxn != -1 ? refreshContext.toBaseTxn : viewState.getLastRefreshBaseTxn();
        final long commitPeriodHi = refreshContext.periodHi != Numbers.LONG_NULL ? refreshContext.periodHi : viewState.getLastPeriodHi();

        final SampleByIntervalIterator intervalIterator = refreshContext.intervalIterator;
        if (intervalIterator == null) {
            // We don't have intervals to query, but we may need to bump base table txn or last period hi.
            if (refreshContext.toBaseTxn != -1 || refreshContext.periodHi != Numbers.LONG_NULL) {
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
            return false;
        }

        long refreshFinishTimestamp = 0;
        int intervalStep = intervalIterator.getStep();
        try {
            factory = viewState.acquireRecordFactory();
            copier = viewState.getRecordToRowCopier();

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
                            refreshFailState(viewState, walWriter, e);
                            return false;
                        }
                    }

                    assert factory != null;
                    assert copier != null;

                    final CharSequence timestampName = walWriter.getMetadata().getColumnName(walWriter.getMetadata().getTimestampIndex());
                    final int cursorTimestampIndex = factory.getMetadata().getColumnIndex(timestampName);
                    assert cursorTimestampIndex > -1;

                    long commitTarget = batchSize;
                    long rowCount = 0;

                    intervalIterator.toTop(intervalStep);
                    long replacementTimestampLo = Long.MIN_VALUE;
                    long replacementTimestampHi = Long.MIN_VALUE;

                    while (intervalIterator.next()) {
                        refreshSqlExecutionContext.setRange(intervalIterator.getTimestampLo(), intervalIterator.getTimestampHi());
                        if (replacementTimestampHi != intervalIterator.getTimestampLo()) {
                            if (replacementTimestampHi > replacementTimestampLo) {
                                // Gap in the refresh intervals, commit the previous batch
                                // so that the replacement interval does not span across the gap.
                                walWriter.commitWithParams(
                                        replacementTimestampLo,
                                        replacementTimestampHi,
                                        WAL_DEDUP_MODE_REPLACE_RANGE
                                );
                                commitTarget = rowCount + batchSize;
                            }
                            replacementTimestampLo = intervalIterator.getTimestampLo();
                        }

                        // Interval high and replace range high are both exclusive
                        replacementTimestampHi = intervalIterator.getTimestampHi();

                        try (RecordCursor cursor = factory.getCursor(refreshSqlExecutionContext)) {
                            final Record record = cursor.getRecord();
                            while (cursor.hasNext()) {
                                long timestamp = record.getTimestamp(cursorTimestampIndex);
                                assert timestamp >= replacementTimestampLo && timestamp < replacementTimestampHi
                                        : "timestamp out of range [expected: " + Timestamps.toUSecString(replacementTimestampLo) + ", "
                                        + Timestamps.toUSecString(replacementTimestampHi) + "), actual: "
                                        + Timestamps.toUSecString(timestamp);
                                TableWriter.Row row = walWriter.newRow(timestamp);
                                copier.copy(record, row);
                                row.append();
                                rowCount++;
                            }

                            if (rowCount >= commitTarget) {
                                final boolean isLastInterval = intervalIterator.isLast();
                                refreshFinishTimestamp = isLastInterval ? microsecondClock.getTicks() : WAL_DEFAULT_LAST_REFRESH_TIMESTAMP;

                                if (isLastInterval) {
                                    refreshFinishTimestamp = microsecondClock.getTicks();
                                    walWriter.commitMatView(
                                            commitBaseTxn,
                                            refreshFinishTimestamp,
                                            commitPeriodHi,
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

                                replacementTimestampLo = replacementTimestampHi;
                                commitTarget = rowCount + batchSize;
                            }
                        }
                    }

                    if (replacementTimestampHi > replacementTimestampLo) {
                        refreshFinishTimestamp = microsecondClock.getTicks();
                        walWriter.commitMatView(
                                commitBaseTxn,
                                refreshFinishTimestamp,
                                commitPeriodHi,
                                replacementTimestampLo,
                                replacementTimestampHi
                        );
                    }
                    break;
                } catch (TableReferenceOutOfDateException e) {
                    factory = Misc.free(factory);
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
                    if (th instanceof CairoException && CairoException.isCairoOomError(th) && i < maxRetries && intervalStep > 1) {
                        intervalStep /= 2;
                        LOG.info().$("query failed with out-of-memory, retrying with a reduced intervalStep [view=").$(viewTableToken)
                                .$(", intervalStep=").$(intervalStep)
                                .$(", error=").$safe(((CairoException) th).getFlyweightMessage())
                                .I$();
                        Os.sleep(oomRetryTimeout);
                        continue;
                    }
                    throw th;
                }
            }

            final long recordRowCopierMetadataVersion = walWriter.getMetadata().getMetadataVersion();
            if (refreshContext.toBaseTxn == -1) {
                // It's a range refresh, so we don't bump last refresh base table txn.
                // Keep the last refresh txn as is and only update the finish timestamp.
                viewState.rangeRefreshSuccess(
                        factory,
                        copier,
                        recordRowCopierMetadataVersion,
                        refreshFinishTimestamp,
                        refreshTriggerTimestamp,
                        commitPeriodHi
                );
            } else {
                // It's an incremental/full refresh.
                viewState.refreshSuccess(
                        factory,
                        copier,
                        recordRowCopierMetadataVersion,
                        refreshFinishTimestamp,
                        refreshTriggerTimestamp,
                        commitBaseTxn,
                        commitPeriodHi
                );
            }
        } catch (Throwable th) {
            Misc.free(factory);
            LOG.error()
                    .$("could not refresh materialized view [view=").$(viewTableToken)
                    .$(", ex=").$(th)
                    .I$();
            refreshFailState(viewState, walWriter, th);
            return false;
        }

        return true;
    }

    private SampleByIntervalIterator intervalIterator(
            @NotNull TimestampSampler sampler,
            @Nullable TimeZoneRules tzRules,
            long fixedOffset,
            @Nullable LongList txnIntervals,
            long minTs,
            long maxTs,
            int step
    ) {
        if (tzRules == null || tzRules.hasFixedOffset()) {
            long fixedTzOffset = tzRules != null ? tzRules.getOffset(0) : 0;
            return fixedOffsetIterator.of(
                    sampler,
                    fixedOffset - fixedTzOffset,
                    txnIntervals,
                    minTs,
                    maxTs,
                    step
            );
        }

        return timeZoneIterator.of(
                sampler,
                tzRules,
                fixedOffset,
                txnIntervals,
                minTs,
                maxTs,
                step
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
        if (viewState != null && !viewState.isDropped()) {
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

        if (!viewState.tryLock()) {
            // Someone is refreshing the view, so we're going for another attempt.
            LOG.debug().$("could not lock materialized view for range refresh, will retry [view=").$(viewToken)
                    .$(", from=").$ts(rangeFrom)
                    .$(", to=").$ts(rangeTo)
                    .I$();
            stateStore.enqueueRangeRefresh(viewToken, rangeFrom, rangeTo);
            return false;
        }

        try (WalWriter walWriter = engine.getWalWriter(viewToken)) {
            final TableToken baseTableToken;
            final String baseTableName = viewState.getViewDefinition().getBaseTableName();
            try {
                baseTableToken = engine.verifyTableName(viewState.getViewDefinition().getBaseTableName());
            } catch (CairoException e) {
                LOG.error().$("could not perform range refresh, could not verify base table [view=").$(viewToken)
                        .$(", from=").$ts(rangeFrom)
                        .$(", to=").$ts(rangeTo)
                        .$(", baseTableName=").$(baseTableName)
                        .$(", errno=").$(e.getErrno())
                        .$(", errorMsg=").$safe(e.getFlyweightMessage())
                        .I$();
                refreshFailState(viewState, walWriter, e);
                return false;
            }

            if (!baseTableToken.isWal()) {
                refreshFailState(viewState, walWriter, "base table is not a WAL table");
                return false;
            }

            try (TableReader baseTableReader = engine.getReader(baseTableToken)) {
                // Operate SQL on a fixed reader that has known max transaction visible. The reader
                // is used to initialize base table readers returned from the refreshExecutionContext.getReader()
                // call, so that all of them are at the same txn.
                engine.detachReader(baseTableReader);
                refreshSqlExecutionContext.of(baseTableReader);
                try {
                    final RefreshContext refreshContext = findRefreshIntervals(baseTableReader, viewState, Numbers.LONG_NULL, rangeFrom, rangeTo);
                    insertAsSelect(viewState, walWriter, refreshContext, refreshTriggerTimestamp);
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
                refreshFailState(viewState, walWriter, th);
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
            refreshFailState(viewState, null, th);
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
        final long minRefreshToTxn = baseSeqTracker.getWriterTxn();

        childViewSink.clear();
        graph.getDependentViews(baseTableToken, childViewSink);
        for (int v = 0, n = childViewSink.size(); v < n; v++) {
            TableToken viewToken = childViewSink.get(v);
            final MatViewState viewState = stateStore.getViewState(viewToken);
            if (viewState != null && !viewState.isPendingInvalidation() && !viewState.isInvalid() && !viewState.isDropped()) {
                if (viewState.getViewDefinition().getRefreshType() != MatViewDefinition.REFRESH_TYPE_IMMEDIATE) {
                    continue;
                }

                if (!viewState.tryLock()) {
                    LOG.debug().$("skipping materialized view refresh, locked by another refresh run [view=").$(viewToken).I$();
                    stateStore.enqueueIncrementalRefresh(viewToken);
                    continue;
                }
                try (WalWriter walWriter = engine.getWalWriter(viewToken)) {
                    try {
                        refreshed |= refreshIncremental0(baseTableToken, viewState, walWriter, refreshTriggerTimestamp);
                    } catch (Throwable th) {
                        refreshFailState(viewState, walWriter, th);
                    }
                } catch (Throwable th) {
                    if (handleErrorRetryRefresh(th, viewToken, stateStore, null)) {
                        // Incremental refresh is re-scheduled.
                        continue;
                    }
                    // If we're here, we either couldn't obtain the WAL writer or the writer couldn't write
                    // invalid state transaction. Update the in-memory state and call it a day.
                    LOG.error()
                            .$("could not get table writer for view [view=").$(viewToken)
                            .$(", ex=").$(th)
                            .I$();
                    refreshFailState(viewState, null, th);
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
        stateStore.notifyBaseRefreshed(refreshTask, minRefreshToTxn);

        if (refreshed) {
            LOG.info().$("refreshed materialized views dependent on [baseTable=").$(baseTableToken).I$();
        }
        return refreshed;
    }

    private void refreshFailState(MatViewState viewState, @Nullable WalWriter walWriter, CharSequence errorMessage) {
        viewState.refreshFail(microsecondClock.getTicks(), errorMessage);
        if (walWriter != null) {
            walWriter.resetMatViewState(
                    viewState.getLastRefreshBaseTxn(),
                    viewState.getLastRefreshFinishTimestamp(),
                    true,
                    errorMessage,
                    viewState.getLastPeriodHi()
            );
        }
        // Invalidate dependent views recursively.
        enqueueInvalidateDependentViews(viewState.getViewDefinition().getMatViewToken(), "base materialized view refresh failed");
    }

    private void refreshFailState(MatViewState viewState, @Nullable WalWriter walWriter, Throwable th) {
        errorMsgSink.clear();
        if (th instanceof Sinkable) {
            ((Sinkable) th).toSink(errorMsgSink);
        } else {
            errorMsgSink.put(th.getMessage());
        }
        refreshFailState(viewState, walWriter, errorMsgSink);
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

        final String baseTableName = viewState.getViewDefinition().getBaseTableName();
        try (WalWriter walWriter = engine.getWalWriter(viewToken)) {
            final TableToken baseTableToken;
            try {
                baseTableToken = engine.verifyTableName(baseTableName);
            } catch (CairoException e) {
                LOG.error()
                        .$("could not perform incremental refresh, could not verify base table [view=").$(viewToken)
                        .$(", baseTableName=").$(baseTableName)
                        .$(", errno=").$(e.getErrno())
                        .$(", errorMsg=").$safe(e.getFlyweightMessage())
                        .I$();
                refreshFailState(viewState, walWriter, e);
                return false;
            }

            if (!baseTableToken.isWal()) {
                refreshFailState(viewState, walWriter, "base table is not a WAL table");
                return false;
            }

            try {
                return refreshIncremental0(baseTableToken, viewState, walWriter, refreshTriggerTimestamp);
            } catch (Throwable th) {
                LOG.error()
                        .$("could not perform incremental refresh [view=").$(viewToken)
                        .$(", baseTableToken=").$(baseTableToken)
                        .$(", ex=").$(th)
                        .I$();
                refreshFailState(viewState, walWriter, th);
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
            refreshFailState(viewState, null, th);
            return false;
        } finally {
            viewState.incrementRefreshSeq();
            viewState.unlock();
            viewState.tryCloseIfDropped();
        }
    }

    private boolean refreshIncremental0(
            @NotNull TableToken baseTableToken,
            @NotNull MatViewState viewState,
            @NotNull WalWriter walWriter,
            long refreshTriggerTimestamp
    ) throws SqlException {
        assert viewState.isLocked();

        final SeqTxnTracker baseSeqTracker = engine.getTableSequencerAPI().getTxnTracker(baseTableToken);
        final long toBaseTxn = baseSeqTracker.getWriterTxn();

        final long fromBaseTxn = viewState.getLastRefreshBaseTxn();
        if (viewState.getViewDefinition().getPeriodLength() == 0 && fromBaseTxn >= 0 && fromBaseTxn >= toBaseTxn) {
            // Non-period mat view which is already refreshed.
            return false;
        }

        // Steps:
        // - compile view and execute with timestamp ranges from the unprocessed commits
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
                final RefreshContext refreshContext = findRefreshIntervals(baseTableReader, viewState, fromBaseTxn);
                return insertAsSelect(viewState, walWriter, refreshContext, refreshTriggerTimestamp);
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
                    periodHi
            );
        }
    }

    private void resetInvalidState(MatViewState viewState, WalWriter walWriter) {
        viewState.markAsValid();
        viewState.setLastRefreshBaseTableTxn(-1);
        viewState.setLastRefreshTimestamp(Numbers.LONG_NULL);
        viewState.setLastPeriodHi(Numbers.LONG_NULL);
        walWriter.resetMatViewState(
                viewState.getLastRefreshBaseTxn(),
                viewState.getLastRefreshFinishTimestamp(),
                false,
                null,
                viewState.getLastPeriodHi()
        );
    }

    private void setInvalidState(MatViewState viewState, WalWriter walWriter, CharSequence invalidationReason, long invalidationTimestamp) {
        viewState.markAsInvalid(invalidationReason);
        viewState.setLastRefreshTimestamp(invalidationTimestamp);
        viewState.setLastRefreshStartTimestamp(invalidationTimestamp);
        walWriter.resetMatViewState(
                viewState.getLastRefreshBaseTxn(),
                viewState.getLastRefreshFinishTimestamp(),
                true,
                invalidationReason,
                viewState.getLastPeriodHi()
        );
    }

    private static class RefreshContext implements Mutable {
        public SampleByIntervalIterator intervalIterator;
        public long periodHi = Numbers.LONG_NULL;
        public long toBaseTxn = -1;

        @Override
        public void clear() {
            intervalIterator = null;
            periodHi = Numbers.LONG_NULL;
            toBaseTxn = -1;
        }
    }
}
