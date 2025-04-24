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
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.Job;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
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
    private final MatViewRefreshExecutionContext refreshExecutionContext;
    private final MatViewRefreshTask refreshTask = new MatViewRefreshTask();
    private final MatViewStateStore stateStore;
    private final TimeZoneIntervalIterator timeZoneIterator = new TimeZoneIntervalIterator();
    private final WalTxnRangeLoader txnRangeLoader;
    private final int workerId;

    public MatViewRefreshJob(int workerId, CairoEngine engine, int workerCount, int sharedWorkerCount) {
        try {
            this.workerId = workerId;
            this.engine = engine;
            this.refreshExecutionContext = new MatViewRefreshExecutionContext(engine, workerCount, sharedWorkerCount);
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
        Misc.free(refreshExecutionContext);
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

    private boolean checkIfBaseTableDropped(MatViewRefreshTask refreshTask) {
        final TableToken baseTableToken = refreshTask.baseTableToken;
        final TableToken matViewToken = refreshTask.matViewToken;
        if (matViewToken == null) {
            assert baseTableToken != null;
            try {
                engine.verifyTableToken(baseTableToken);
            } catch (CairoException | TableReferenceOutOfDateException e) {
                LOG.error().$("base table is dropped or renamed [table=").$(baseTableToken)
                        .$(", error=").$(e.getFlyweightMessage())
                        .I$();
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

    private SampleByIntervalIterator findSampleByIntervals(
            @NotNull TableReader baseTableReader,
            @NotNull MatViewDefinition viewDefinition,
            long lastRefreshTxn
    ) throws SqlException {
        final long lastTxn = baseTableReader.getSeqTxn();
        final TableToken baseTableToken = baseTableReader.getTableToken();
        final TableToken matViewToken = viewDefinition.getMatViewToken();

        LongList txnIntervals = null;
        long minTs;
        long maxTs;
        if (lastRefreshTxn > 0) {
            // Find min/max timestamps from WAL transactions.
            txnIntervals = intervals;
            txnIntervals.clear();
            txnRangeLoader.load(engine, Path.PATH.get(), baseTableToken, txnIntervals, lastRefreshTxn, lastTxn);
            minTs = txnRangeLoader.getMinTimestamp();
            maxTs = txnRangeLoader.getMaxTimestamp();
        } else {
            // Full table scan.
            // When the table is empty, min timestamp is set to Long.MAX_VALUE,
            // while max timestamp is Long.MIN_VALUE, so we end up skipping the refresh.
            minTs = baseTableReader.getMinTimestamp();
            maxTs = baseTableReader.getMaxTimestamp();
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

            LOG.info().$("refreshing materialized view [view=").$(matViewToken)
                    .$(", baseTable=").$(baseTableToken)
                    .$(", fromTxn=").$(lastRefreshTxn)
                    .$(", toTxn=").$(lastTxn)
                    .$(", iteratorMinTs>=").$ts(iteratorMinTs)
                    .$(", iteratorMaxTs<").$ts(iteratorMaxTs)
                    .I$();

            return intervalIterator;
        }

        return null;
    }

    private boolean fullRefresh(MatViewRefreshTask refreshTask) {
        final TableToken viewToken = refreshTask.matViewToken;
        assert viewToken != null;
        final long refreshTriggeredTimestamp = refreshTask.refreshTriggeredTimestamp;

        final MatViewState state = stateStore.getViewState(viewToken);
        if (state == null || state.isDropped()) {
            return false;
        }

        if (!state.tryLock()) {
            // Someone is refreshing the view, so we're going for another attempt.
            // Just mark the view invalid to prevent intermediate incremental refreshes and republish the task.
            LOG.debug().$("could not lock materialized view for full refresh, will retry [view=").$(viewToken).I$();
            state.markAsPendingInvalidation();
            stateStore.enqueueFullRefresh(viewToken);
            return false;
        }

        try (WalWriter walWriter = engine.getWalWriter(viewToken)) {
            final TableToken baseTableToken;
            final String baseTableName = state.getViewDefinition().getBaseTableName();
            try {
                baseTableToken = engine.verifyTableName(state.getViewDefinition().getBaseTableName());
            } catch (CairoException e) {
                LOG.error().$("could not perform full refresh, could not verify base table [view=").$(viewToken)
                        .$(", baseTableName=").$(baseTableName)
                        .$(", errno=").$(e.getErrno())
                        .$(", errorMsg=").$(e.getFlyweightMessage())
                        .I$();
                refreshFailState(state, walWriter, microsecondClock.getTicks(), e);
                return false;
            }

            if (!baseTableToken.isWal()) {
                refreshFailState(state, walWriter, microsecondClock.getTicks(), "base table is not a WAL table");
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
                refreshExecutionContext.of(baseTableReader);
                try {
                    walWriter.truncateSoft();
                    resetInvalidState(state, walWriter);

                    final long toBaseTxn = baseTableReader.getSeqTxn();
                    final MatViewDefinition viewDef = state.getViewDefinition();
                    // Specify -1 as the last refresh txn, so that we scan all partitions.
                    final SampleByIntervalIterator intervalIterator = findSampleByIntervals(baseTableReader, viewDef, -1);
                    if (intervalIterator != null) {
                        insertAsSelect(state, viewDef, walWriter, intervalIterator, toBaseTxn, refreshTriggeredTimestamp);
                    }
                } finally {
                    refreshExecutionContext.clearReader();
                    engine.attachReader(baseTableReader);
                }
            } catch (Throwable th) {
                LOG.error()
                        .$("could not perform full refresh [view=").$(viewToken)
                        .$(", baseTable=").$(baseTableToken)
                        .$(", ex=").$(th)
                        .I$();
                refreshFailState(state, walWriter, microsecondClock.getTicks(), th);
                return false;
            }
        } catch (Throwable th) {
            // If we're here, we either couldn't obtain the WAL writer or the writer couldn't write
            // invalid state transaction. Update the in-memory state and call it a day.
            LOG.error()
                    .$("could not perform incremental refresh, unexpected error [view=").$(viewToken)
                    .$(", ex=").$(th)
                    .I$();
            refreshFailState(state, null, microsecondClock.getTicks(), th);
            return false;
        } finally {
            state.unlock();
            state.tryCloseIfDropped();
        }

        // Kickstart incremental refresh.
        stateStore.enqueueIncrementalRefresh(viewToken);
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

    private boolean incrementalRefresh(MatViewRefreshTask refreshTask) {
        final TableToken baseTableToken = refreshTask.baseTableToken;
        final TableToken matViewToken = refreshTask.matViewToken;
        final long refreshTriggeredTimestamp = refreshTask.refreshTriggeredTimestamp;
        if (matViewToken == null) {
            return refreshDependentViewsIncremental(baseTableToken, graph, stateStore, refreshTriggeredTimestamp);
        } else {
            return refreshIncremental(matViewToken, stateStore, refreshTriggeredTimestamp);
        }
    }

    private boolean insertAsSelect(
            MatViewState state,
            MatViewDefinition viewDef,
            WalWriter walWriter,
            SampleByIntervalIterator intervalIterator,
            long baseTableTxn,
            long refreshTriggeredTimestamp
    ) {
        assert state.isLocked();

        final int maxRetries = configuration.getMatViewMaxRefreshRetries();
        final long oomRetryTimeout = configuration.getMatViewRefreshOomRetryTimeout();
        final long batchSize = configuration.getMatViewInsertAsSelectBatchSize();

        RecordCursorFactory factory = null;
        RecordToRowCopier copier;
        int intervalStep = intervalIterator.getStep();
        final long refreshTimestamp = microsecondClock.getTicks();
        final TableToken viewTableToken = viewDef.getMatViewToken();
        try {
            factory = state.acquireRecordFactory();
            copier = state.getRecordToRowCopier();

            for (int i = 0; i <= maxRetries; i++) {
                try {
                    if (factory == null) {
                        final String viewSql = viewDef.getMatViewSql();
                        try (SqlCompiler compiler = engine.getSqlCompiler()) {
                            LOG.info().$("compiling materialized view [view=").$(viewTableToken).$(", attempt=").$(i).I$();
                            final CompiledQuery compiledQuery = compiler.compile(viewSql, refreshExecutionContext);
                            assert compiledQuery.getType() == CompiledQuery.SELECT;
                            factory = compiledQuery.getRecordCursorFactory();
                            if (copier == null || walWriter.getMetadata().getMetadataVersion() != state.getRecordRowCopierMetadataVersion()) {
                                copier = getRecordToRowCopier(walWriter, factory, compiler);
                            }
                        } catch (SqlException e) {
                            factory = Misc.free(factory);
                            LOG.error().$("could not compile materialized view [view=").$(viewTableToken)
                                    .$(", sql=").$(viewSql)
                                    .$(", errorPos=").$(e.getPosition())
                                    .$(", attempt=").$(i)
                                    .$(", error=").$(e.getFlyweightMessage())
                                    .I$();
                            refreshFailState(state, walWriter, refreshTimestamp, e);
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
                    while (intervalIterator.next()) {
                        refreshExecutionContext.setRange(intervalIterator.getTimestampLo(), intervalIterator.getTimestampHi());
                        try (RecordCursor cursor = factory.getCursor(refreshExecutionContext)) {
                            final Record record = cursor.getRecord();
                            while (cursor.hasNext()) {
                                TableWriter.Row row = walWriter.newRow(record.getTimestamp(cursorTimestampIndex));
                                copier.copy(record, row);
                                row.append();
                                if (++rowCount >= commitTarget) {
                                    walWriter.commit();
                                    commitTarget = rowCount + batchSize;
                                }
                            }
                        }
                    }
                    break;
                } catch (TableReferenceOutOfDateException e) {
                    factory = Misc.free(factory);
                    if (i == maxRetries) {
                        LOG.info().$("base table is under heavy DDL changes, will retry refresh later [view=").$(viewTableToken)
                                .$(", totalAttempts=").$(maxRetries)
                                .$(", msg=").$(e.getFlyweightMessage())
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
                                .$(", error=").$(((CairoException) th).getFlyweightMessage())
                                .I$();
                        Os.sleep(oomRetryTimeout);
                        continue;
                    }
                    throw th;
                }
            }

            walWriter.commitMatView(baseTableTxn, refreshTimestamp);
            state.refreshSuccess(factory, copier, walWriter.getMetadata().getMetadataVersion(), refreshTimestamp, refreshTriggeredTimestamp, baseTableTxn);
            state.setLastRefreshBaseTableTxn(baseTableTxn);
        } catch (Throwable th) {
            Misc.free(factory);
            LOG.error()
                    .$("could not refresh materialized view [view=").$(viewTableToken)
                    .$(", ex=").$(th)
                    .I$();
            refreshFailState(state, walWriter, refreshTimestamp, th);
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
        final MatViewState state = stateStore.getViewState(viewToken);
        if (state != null && !state.isDropped()) {
            if (!state.tryLock()) {
                LOG.debug().$("skipping materialized view invalidation, locked by another refresh run [view=").$(viewToken).I$();
                state.markAsPendingInvalidation();
                stateStore.enqueueInvalidate(viewToken, invalidationReason);
                return;
            }

            try (WalWriter walWriter = engine.getWalWriter(viewToken)) {
                // Mark the view invalid only if the operation is forced or the view was never refreshed.
                if (force || state.getLastRefreshBaseTxn() != -1) {
                    final long invalidationTimestamp = microsecondClock.getTicks();
                    LOG.error().$("marking materialized view as invalid [view=").$(viewToken)
                            .$(", reason=").$(invalidationReason)
                            .$(", ts=").$ts(invalidationTimestamp)
                            .I$();
                    setInvalidState(state, walWriter, invalidationReason, invalidationTimestamp);
                }
            } finally {
                state.unlock();
                state.tryCloseIfDropped();
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

    private boolean refreshDependentViewsIncremental(
            TableToken baseTableToken,
            MatViewGraph graph,
            MatViewStateStore stateStore,
            long refreshTriggeredTimestamp
    ) {
        assert baseTableToken.isWal();

        boolean refreshed = false;
        final SeqTxnTracker baseSeqTracker = engine.getTableSequencerAPI().getTxnTracker(baseTableToken);
        final long minRefreshToTxn = baseSeqTracker.getWriterTxn();

        childViewSink.clear();
        graph.getDependentViews(baseTableToken, childViewSink);
        for (int v = 0, n = childViewSink.size(); v < n; v++) {
            final TableToken viewToken = childViewSink.get(v);
            final MatViewState state = stateStore.getViewState(viewToken);
            if (state != null && !state.isPendingInvalidation() && !state.isInvalid() && !state.isDropped()) {
                if (!state.tryLock()) {
                    LOG.debug().$("skipping materialized view refresh, locked by another refresh run [view=").$(viewToken).I$();
                    stateStore.enqueueIncrementalRefresh(viewToken);
                    continue;
                }
                try (WalWriter walWriter = engine.getWalWriter(viewToken)) {
                    try {
                        refreshed |= refreshIncremental0(state, baseTableToken, walWriter, refreshTriggeredTimestamp);
                    } catch (Throwable th) {
                        refreshFailState(state, walWriter, microsecondClock.getTicks(), th);
                    }
                } catch (Throwable th) {
                    // If we're here, we either couldn't obtain the WAL writer or the writer couldn't write
                    // invalid state transaction. Update the in-memory state and call it a day.
                    LOG.error()
                            .$("could not get table writer for view [view=").$(viewToken)
                            .$(", ex=").$(th)
                            .I$();
                    refreshFailState(state, null, microsecondClock.getTicks(), th);
                } finally {
                    state.unlock();
                    state.tryCloseIfDropped();
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

    private void refreshFailState(MatViewState state, @Nullable WalWriter walWriter, long refreshTimestamp, CharSequence errorMessage) {
        state.refreshFail(refreshTimestamp, errorMessage);
        if (walWriter != null) {
            walWriter.invalidateMatView(state.getLastRefreshBaseTxn(), state.getLastRefreshTimestamp(), true, errorMessage);
        }
        // Invalidate dependent views recursively.
        enqueueInvalidateDependentViews(state.getViewDefinition().getMatViewToken(), "base materialized view refresh failed");
    }

    private void refreshFailState(MatViewState state, @Nullable WalWriter walWriter, long refreshTimestamp, Throwable th) {
        errorMsgSink.clear();
        if (th instanceof Sinkable) {
            ((Sinkable) th).toSink(errorMsgSink);
        } else {
            errorMsgSink.put(th.getMessage());
        }
        refreshFailState(state, walWriter, refreshTimestamp, errorMsgSink);
    }

    private boolean refreshIncremental(@NotNull TableToken viewToken, MatViewStateStore stateStore, long refreshTriggeredTimestamp) {
        final MatViewState state = stateStore.getViewState(viewToken);
        if (state == null || state.isPendingInvalidation() || state.isInvalid() || state.isDropped()) {
            return false;
        }

        if (!state.tryLock()) {
            LOG.debug().$("could not lock materialized view for incremental refresh, will retry [view=").$(viewToken).I$();
            stateStore.enqueueIncrementalRefresh(viewToken);
            return false;
        }

        final String baseTableName = state.getViewDefinition().getBaseTableName();
        try (WalWriter walWriter = engine.getWalWriter(viewToken)) {
            final TableToken baseTableToken;
            try {
                baseTableToken = engine.verifyTableName(baseTableName);
            } catch (CairoException e) {
                LOG.error()
                        .$("could not perform incremental refresh, could not verify base table [view=").$(viewToken)
                        .$(", baseTableName=").$(baseTableName)
                        .$(", errno=").$(e.getErrno())
                        .$(", errorMsg=").$(e.getFlyweightMessage())
                        .I$();
                refreshFailState(state, walWriter, microsecondClock.getTicks(), e);
                return false;
            }

            if (!baseTableToken.isWal()) {
                refreshFailState(state, walWriter, microsecondClock.getTicks(), "base table is not a WAL table");
                return false;
            }

            try {
                return refreshIncremental0(state, baseTableToken, walWriter, refreshTriggeredTimestamp);
            } catch (Throwable th) {
                LOG.error()
                        .$("could not perform incremental refresh [view=").$(viewToken)
                        .$(", baseTableToken=").$(baseTableToken)
                        .$(", ex=").$(th)
                        .I$();
                refreshFailState(state, walWriter, microsecondClock.getTicks(), th);
                return false;
            }
        } catch (Throwable th) {
            // If we're here, we either couldn't obtain the WAL writer or the writer couldn't write
            // invalid state transaction. Update the in-memory state and call it a day.
            LOG.error()
                    .$("could not perform incremental refresh, unexpected error [view=").$(viewToken)
                    .$(", ex=").$(th)
                    .I$();
            refreshFailState(state, null, microsecondClock.getTicks(), th);
            return false;
        } finally {
            state.unlock();
            state.tryCloseIfDropped();
        }
    }

    private boolean refreshIncremental0(
            @NotNull MatViewState state,
            @NotNull TableToken baseTableToken,
            @NotNull WalWriter walWriter,
            long refreshTriggeredTimestamp
    ) throws SqlException {
        assert state.isLocked();

        final SeqTxnTracker baseSeqTracker = engine.getTableSequencerAPI().getTxnTracker(baseTableToken);
        long toBaseTxn = baseSeqTracker.getWriterTxn();

        final long fromBaseTxn = state.getLastRefreshBaseTxn();
        if (fromBaseTxn >= 0 && fromBaseTxn >= toBaseTxn) {
            // Already refreshed
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
            refreshExecutionContext.of(baseTableReader);
            try {
                final MatViewDefinition viewDef = state.getViewDefinition();
                final SampleByIntervalIterator intervalIterator = findSampleByIntervals(baseTableReader, viewDef, fromBaseTxn);
                if (intervalIterator != null) {
                    return insertAsSelect(state, viewDef, walWriter, intervalIterator, baseTableReader.getSeqTxn(), refreshTriggeredTimestamp);
                }
            } finally {
                refreshExecutionContext.clearReader();
                engine.attachReader(baseTableReader);
            }
        }
        return false;
    }

    private void resetInvalidState(MatViewState state, WalWriter walWriter) {
        state.markAsValid();
        state.setLastRefreshBaseTableTxn(-1);
        state.setLastRefreshTimestamp(Numbers.LONG_NULL);
        walWriter.invalidateMatView(state.getLastRefreshBaseTxn(), state.getLastRefreshTimestamp(), false, null);
    }

    private void setInvalidState(MatViewState state, WalWriter walWriter, CharSequence invalidationReason, long invalidationTimestamp) {
        state.markAsInvalid(invalidationReason);
        state.setLastRefreshTimestamp(invalidationTimestamp);
        walWriter.invalidateMatView(state.getLastRefreshBaseTxn(), state.getLastRefreshTimestamp(), true, invalidationReason);
    }
}
