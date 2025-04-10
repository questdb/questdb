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
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.Job;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import io.questdb.std.datetime.TimeZoneRules;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;

public class MatViewRefreshJob implements Job, QuietCloseable {
    private static final Log LOG = LogFactory.getLog(MatViewRefreshJob.class);
    private final ObjList<TableToken> childViewSink = new ObjList<>();
    private final ObjList<TableToken> childViewSink2 = new ObjList<>();
    private final EntityColumnFilter columnFilter = new EntityColumnFilter();
    private final CairoConfiguration configuration;
    private final CairoEngine engine;
    private final MatViewGraph graph;
    private final MicrosecondClock microsecondClock;
    private final MatViewRefreshExecutionContext refreshExecutionContext;
    private final MatViewRefreshTask refreshTask = new MatViewRefreshTask();
    private final MatViewStateStore stateStore;
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
    }

    @Override
    public boolean run(int workerId, @NotNull RunStatus runStatus) {
        // there is job instance per thread, the worker id must never change for this job
        assert this.workerId == workerId;
        return processNotifications();
    }

    private boolean checkIfBaseTableDropped(MatViewRefreshTask refreshTask) {
        final TableToken baseTableToken = refreshTask.baseTableToken;
        final TableToken matViewToken = refreshTask.matViewToken;
        if (matViewToken == null) {
            assert baseTableToken != null;
            try {
                engine.verifyTableToken(baseTableToken);
            } catch (CairoException | TableReferenceOutOfDateException e) {
                LOG.info().$("base table is dropped or renamed [table=").$(baseTableToken)
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

    private boolean findCommitTimestampRanges(
            @NotNull MatViewRefreshExecutionContext executionContext,
            @NotNull TableReader baseTableReader,
            @NotNull MatViewDefinition viewDefinition,
            long lastRefreshTxn
    ) throws SqlException {
        final long lastTxn = baseTableReader.getSeqTxn();

        if (lastRefreshTxn > 0) {
            txnRangeLoader.load(engine, Path.PATH.get(), baseTableReader.getTableToken(), lastRefreshTxn, lastTxn);
            long minTs = txnRangeLoader.getMinTimestamp();
            long maxTs = txnRangeLoader.getMaxTimestamp();

            if (minTs <= maxTs) {
                // there are no concurrent accesses to the sampler at this point
                final TimestampSampler sampler = viewDefinition.getTimestampSampler();
                sampler.setStart(viewDefinition.getFixedOffset());
                TimeZoneRules rules = viewDefinition.getTzRules();
                // convert UTC timestamp into Time Zone timestamp
                // round to the nearest sampling interval
                // then convert back to UTC
                long tzMinOffset = rules != null ? rules.getOffset(minTs) : 0;
                long tzMaxOffset = rules != null ? rules.getOffset(maxTs) : 0;

                final long tzMinTs = sampler.round(minTs + tzMinOffset);
                minTs = rules != null ? Timestamps.toUTC(tzMinTs, rules) : tzMinTs - tzMinOffset;

                final long tzMaxTs = sampler.nextTimestamp(sampler.round(maxTs + tzMaxOffset));
                maxTs = rules != null ? Timestamps.toUTC(tzMaxTs, rules) : tzMaxTs - tzMaxOffset;

                executionContext.setRange(minTs, maxTs);
                LOG.info().$("refreshing materialized view [view=").$(viewDefinition.getMatViewToken())
                        .$(", base=").$(baseTableReader.getTableToken())
                        .$(", fromTxn=").$(lastRefreshTxn)
                        .$(", toTxn=").$(lastTxn)
                        .$(", ts>=").$ts(minTs)
                        .$(", ts<").$ts(maxTs)
                        .I$();

                return true;
            }
        } else {
            executionContext.setRange(Long.MIN_VALUE + 1, Long.MAX_VALUE);

            LOG.info().$("refreshing materialized view, full refresh [view=").$(viewDefinition.getMatViewToken())
                    .$(", base=").$(baseTableReader.getTableToken())
                    .$(", toTxn=").$(lastTxn)
                    .I$();
            return true;
        }

        return false;
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
            LOG.info().$("delaying full refresh of materialized view, locked by another refresh run [view=").$(viewToken).I$();
            state.markAsPendingInvalidation();
            stateStore.enqueueFullRefresh(viewToken);
            return false;
        }

        try (WalWriter walWriter = engine.getWalWriter(viewToken)) {
            final TableToken baseTableToken;
            try {
                baseTableToken = engine.verifyTableName(state.getViewDefinition().getBaseTableName());
            } catch (CairoException th) {
                LOG.error().$("full refresh error, cannot resolve base table [view=").$(viewToken)
                        .$(", error=").$(th.getFlyweightMessage())
                        .I$();
                refreshFailState(state, walWriter, microsecondClock.getTicks(), th.getMessage());
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
                    final long toBaseTxn = baseTableReader.getSeqTxn();
                    // Make time interval filter no-op as we're querying all partitions.
                    refreshExecutionContext.setRange(Long.MIN_VALUE + 1, Long.MAX_VALUE);

                    walWriter.truncateSoft();
                    final MatViewDefinition viewDef = state.getViewDefinition();
                    insertAsSelect(state, viewDef, walWriter, toBaseTxn, refreshTriggeredTimestamp);
                    resetInvalidState(state, walWriter);
                } finally {
                    refreshExecutionContext.clearReader();
                    engine.attachReader(baseTableReader);
                }
            }
        } catch (Throwable th) {
            LOG.error().$("full refresh error [view=").$(viewToken).$(", error=").$(th).I$();
            stateStore.removeViewState(viewToken);
            return false;
        } finally {
            state.unlock();
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
            long baseTableTxn,
            long refreshTriggeredTimestamp
    ) throws SqlException {
        assert state.isLocked();

        final int maxRecompileAttempts = configuration.getMatViewMaxRecompileAttempts();
        final long batchSize = configuration.getMatViewInsertAsSelectBatchSize();

        RecordCursorFactory factory = null;
        RecordToRowCopier copier;
        long rowCount = 0;
        long refreshTimestamp = microsecondClock.getTicks();
        try {
            factory = state.acquireRecordFactory();
            copier = state.getRecordToRowCopier();

            for (int i = 0; i < maxRecompileAttempts; i++) {
                try {
                    if (factory == null) {
                        try (SqlCompiler compiler = engine.getSqlCompiler()) {
                            LOG.info().$("compiling materialized view [view=").$(viewDef.getMatViewToken()).$(", attempt=").$(i).I$();
                            final CompiledQuery compiledQuery = compiler.compile(viewDef.getMatViewSql(), refreshExecutionContext);
                            assert compiledQuery.getType() == CompiledQuery.SELECT;
                            factory = compiledQuery.getRecordCursorFactory();

                            if (copier == null || walWriter.getMetadata().getMetadataVersion() != state.getRecordRowCopierMetadataVersion()) {
                                copier = getRecordToRowCopier(walWriter, factory, compiler);
                            }
                        } catch (SqlException e) {
                            factory = Misc.free(factory);
                            LOG.error().$("error refreshing materialized view, compilation error [view=").$(viewDef.getMatViewToken())
                                    .$(", error=").$(e.getFlyweightMessage())
                                    .I$();
                            refreshFailState(state, walWriter, refreshTimestamp, e.getMessage());
                            return false;
                        }
                    }

                    assert factory != null;
                    assert copier != null;

                    final CharSequence timestampName = walWriter.getMetadata().getColumnName(walWriter.getMetadata().getTimestampIndex());
                    final int cursorTimestampIndex = factory.getMetadata().getColumnIndex(timestampName);
                    assert cursorTimestampIndex > -1;

                    try (RecordCursor cursor = factory.getCursor(refreshExecutionContext)) {
                        final Record record = cursor.getRecord();
                        long deadline = batchSize;
                        rowCount = 0;
                        while (cursor.hasNext()) {
                            TableWriter.Row row = walWriter.newRow(record.getTimestamp(cursorTimestampIndex));
                            copier.copy(record, row);
                            row.append();
                            if (++rowCount >= deadline) {
                                walWriter.ic();
                                deadline = rowCount + batchSize;
                            }
                        }
                    }
                    break;
                } catch (TableReferenceOutOfDateException e) {
                    factory = Misc.free(factory);
                    if (i == maxRecompileAttempts - 1) {
                        LOG.info().$("base table is under heavy DDL changes, will reattempt refresh later [view=").$(viewDef.getMatViewToken())
                                .$(", recompileAttempts=").$(maxRecompileAttempts)
                                .I$();
                        stateStore.enqueueIncrementalRefresh(viewDef.getMatViewToken());
                        return false;
                    }
                } catch (Throwable th) {
                    factory = Misc.free(factory);
                    refreshFailState(state, walWriter, refreshTimestamp, th.getMessage());
                    throw th;
                }
            }

            walWriter.commitMatView(baseTableTxn, refreshTimestamp);
            state.refreshSuccess(factory, copier, walWriter.getMetadata().getMetadataVersion(), refreshTimestamp, refreshTriggeredTimestamp, baseTableTxn);
            if (rowCount > 0) {
                state.setLastRefreshBaseTableTxn(baseTableTxn);
            }
        } catch (Throwable th) {
            LOG.error().$("error refreshing materialized view [view=").$(viewDef.getMatViewToken()).$(", error=").$(th).I$();
            Misc.free(factory);
            refreshFailState(state, walWriter, refreshTimestamp, th.getMessage());
            throw th;
        }

        return rowCount > 0;
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
                // Mark the view invalid only if the operation is forced or the view was ever refreshed.
                if (force || state.getLastRefreshBaseTxn() != -1) {
                    setInvalidState(state, walWriter, invalidationReason);
                }
            } finally {
                state.unlock();
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
                try {
                    try (WalWriter walWriter = engine.getWalWriter(viewToken)) {
                        refreshed = refreshIncremental0(state, baseTableToken, walWriter, refreshTriggeredTimestamp);
                    }
                } catch (Throwable th) {
                    LOG.error().$("error refreshing materialized view [view=").$(viewToken).$(", error=").$(th).I$();
                    stateStore.removeViewState(viewToken);
                } finally {
                    state.unlock();
                }
            }
        }
        refreshTask.clear();
        refreshTask.baseTableToken = baseTableToken;
        refreshTask.operation = MatViewRefreshTask.INCREMENTAL_REFRESH;
        stateStore.notifyBaseRefreshed(refreshTask, minRefreshToTxn);

        if (refreshed) {
            LOG.info().$("refreshed materialized views dependent on [table=").$(baseTableToken).I$();
        }
        return refreshed;
    }

    private void refreshFailState(MatViewState state, WalWriter walWriter, long refreshTimestamp, String errorMessage) {
        state.refreshFail(refreshTimestamp, errorMessage);
        walWriter.invalidateMatView(state.getLastRefreshBaseTxn(), state.getLastRefreshTimestamp(), true, errorMessage);
        // Invalidate dependent views recursively.
        enqueueInvalidateDependentViews(state.getViewDefinition().getMatViewToken(), "base materialized view refresh failed");
    }

    private boolean refreshIncremental(@NotNull TableToken viewToken, MatViewStateStore stateStore, long refreshTriggeredTimestamp) {
        final MatViewState state = stateStore.getViewState(viewToken);
        if (state == null || state.isPendingInvalidation() || state.isInvalid() || state.isDropped()) {
            return false;
        }

        if (!state.tryLock()) {
            LOG.debug().$("skipping materialized view refresh, locked by another refresh run [view=").$(viewToken).I$();
            stateStore.enqueueIncrementalRefresh(viewToken);
            return false;
        }

        try (WalWriter walWriter = engine.getWalWriter(viewToken)) {
            final TableToken baseTableToken;
            try {
                baseTableToken = engine.verifyTableName(state.getViewDefinition().getBaseTableName());
            } catch (CairoException th) {
                LOG.error().$("error refreshing materialized view, cannot resolve base table [view=").$(viewToken)
                        .$(", error=").$(th.getFlyweightMessage())
                        .I$();
                refreshFailState(state, walWriter, microsecondClock.getTicks(), th.getMessage());
                return false;
            }

            if (!baseTableToken.isWal()) {
                refreshFailState(state, walWriter, microsecondClock.getTicks(), "base table is not a WAL table");
                return false;
            }

            return refreshIncremental0(state, baseTableToken, walWriter, refreshTriggeredTimestamp);
        } catch (Throwable th) {
            LOG.error().$("error refreshing materialized view [view=").$(viewToken).$(", error=").$(th).I$();
            stateStore.removeViewState(viewToken);
            return false;
        } finally {
            state.unlock();
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
                if (findCommitTimestampRanges(refreshExecutionContext, baseTableReader, viewDef, fromBaseTxn)) {
                    return insertAsSelect(state, viewDef, walWriter, baseTableReader.getSeqTxn(), refreshTriggeredTimestamp);
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
        walWriter.invalidateMatView(state.getLastRefreshBaseTxn(), state.getLastRefreshTimestamp(), false, null);
    }

    private void setInvalidState(MatViewState state, WalWriter walWriter, String invalidationReason) {
        state.markAsInvalid(invalidationReason);
        walWriter.invalidateMatView(state.getLastRefreshBaseTxn(), state.getLastRefreshTimestamp(), true, invalidationReason);
    }
}
