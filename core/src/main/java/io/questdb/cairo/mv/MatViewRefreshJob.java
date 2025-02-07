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

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnFilter;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.EntityColumnFilter;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TableWriterAPI;
import io.questdb.cairo.meta.MetaFileWriter;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
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
    private final int batchSize;
    private final ObjList<TableToken> childViewSink = new ObjList<>();
    private final EntityColumnFilter columnFilter = new EntityColumnFilter();
    private final Path dbRoot;
    private final int dbRootLen;
    private final CairoEngine engine;
    private final int maxRecompileAttempts;
    private final MetaFileWriter metaFileWriter;
    private final MicrosecondClock microsecondClock;
    private final MatViewRefreshExecutionContext mvRefreshExecutionContext;
    private final MatViewRefreshTask mvRefreshTask = new MatViewRefreshTask();
    private final WalTxnRangeLoader txnRangeLoader;
    private final int workerId;

    public MatViewRefreshJob(int workerId, CairoEngine engine, int workerCount, int sharedWorkerCount) {
        try {
            this.workerId = workerId;
            this.engine = engine;
            this.mvRefreshExecutionContext = new MatViewRefreshExecutionContext(engine, workerCount, sharedWorkerCount);
            this.txnRangeLoader = new WalTxnRangeLoader(engine.getConfiguration().getFilesFacade());
            this.microsecondClock = engine.getConfiguration().getMicrosecondClock();
            this.maxRecompileAttempts = engine.getConfiguration().getMatViewMaxRecompileAttempts();
            this.batchSize = engine.getConfiguration().getMatViewInsertAsSelectBatchSize();
            this.metaFileWriter = new MetaFileWriter(engine.getConfiguration().getFilesFacade());
            this.dbRoot = new Path();
            dbRoot.of(engine.getConfiguration().getRoot());
            this.dbRootLen = dbRoot.size();
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
        Misc.free(mvRefreshExecutionContext);
        Misc.free(metaFileWriter);
        Misc.free(dbRoot);
    }

    @Override
    public boolean run(int workerId, @NotNull RunStatus runStatus) {
        // there is job instance per thread, the worker id must never change for this job
        assert this.workerId == workerId;
        return refreshNotifiedViews();
    }

    private boolean findCommitTimestampRanges(
            MatViewRefreshExecutionContext executionContext,
            TableReader baseTableReader,
            long lastRefreshTxn,
            MatViewDefinition viewDefinition
    ) throws SqlException {
        long lastTxn = baseTableReader.getSeqTxn();

        if (lastRefreshTxn > 0) {
            txnRangeLoader.load(engine, Path.PATH.get(), baseTableReader.getTableToken(), lastRefreshTxn, lastTxn);
            long minTs = txnRangeLoader.getMinTimestamp();
            long maxTs = txnRangeLoader.getMaxTimestamp();

            if (minTs <= maxTs) {
                // TODO(glasstiger): reuse the sampler instance

                final TimestampSampler sampler = TimestampSamplerFactory.getInstance(
                        viewDefinition.getSamplingInterval(),
                        viewDefinition.getSamplingIntervalUnit(),
                        0
                );

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

    private ColumnFilter generatedColumnFilter(RecordMetadata cursorMetadata, TableRecordMetadata writerMetadata) throws SqlException {
        for (int i = 0, n = cursorMetadata.getColumnCount(); i < n; i++) {
            int columnType = cursorMetadata.getColumnType(i);
            assert columnType > 0;
            if (columnType != writerMetadata.getColumnType(i)) {
                throw SqlException.$(0, "materialized view column type mismatch. Query column: ")
                        .put(cursorMetadata.getColumnName(i))
                        .put(" type: ")
                        .put(ColumnType.nameOf(columnType))
                        .put(", view column: ")
                        .put(writerMetadata.getColumnName(i))
                        .put(" type: ")
                        .put(ColumnType.nameOf(writerMetadata.getColumnType(i)));
            }
        }
        columnFilter.of(cursorMetadata.getColumnCount());
        return columnFilter;
    }

    private RecordToRowCopier getRecordToRowCopier(TableWriterAPI tableWriter, RecordCursorFactory factory, SqlCompiler compiler) throws SqlException {
        final ColumnFilter entityColumnFilter = generatedColumnFilter(
                factory.getMetadata(),
                tableWriter.getMetadata()
        );
        return RecordToRowCopierUtils.generateCopier(
                compiler.getAsm(),
                factory.getMetadata(),
                tableWriter.getMetadata(),
                entityColumnFilter
        );
    }

    private boolean insertAsSelect(
            MatViewRefreshState state,
            MatViewDefinition viewDef,
            TableWriterAPI tableWriter,
            long baseTableTxn,
            long refreshTriggeredTimestamp
    ) throws SqlException {
        assert state.isLocked();

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
                            CompiledQuery compiledQuery = compiler.compile(viewDef.getMatViewSql(), mvRefreshExecutionContext);
                            if (compiledQuery.getType() != CompiledQuery.SELECT) {
                                throw SqlException.$(0, "materialized view query must be a SELECT statement");
                            }
                            factory = compiledQuery.getRecordCursorFactory();

                            if (copier == null || tableWriter.getMetadata().getMetadataVersion() != state.getRecordRowCopierMetadataVersion()) {
                                copier = getRecordToRowCopier(tableWriter, factory, compiler);
                            }
                        } catch (SqlException e) {
                            factory = Misc.free(factory);
                            LOG.error().$("error refreshing materialized view, compilation error [view=").$(viewDef.getMatViewToken())
                                    .$(", error=").$(e.getFlyweightMessage())
                                    .I$();
                            refreshFailState(state, refreshTimestamp, e.getFlyweightMessage());
                            return false;
                        }
                    }

                    final CharSequence timestampName = tableWriter.getMetadata().getColumnName(tableWriter.getMetadata().getTimestampIndex());
                    final int cursorTimestampIndex = factory.getMetadata().getColumnIndex(timestampName);

                    if (cursorTimestampIndex < 0) {
                        throw SqlException.invalidColumn(0, "timestamp column '")
                                .put(tableWriter.getMetadata().getColumnName(tableWriter.getMetadata().getTimestampIndex()))
                                .put("' not found in view select query");
                    }

                    try (RecordCursor cursor = factory.getCursor(mvRefreshExecutionContext)) {
                        final Record record = cursor.getRecord();
                        long deadline = batchSize;
                        rowCount = 0;
                        while (cursor.hasNext()) {
                            TableWriter.Row row = tableWriter.newRow(record.getTimestamp(cursorTimestampIndex));
                            copier.copy(record, row);
                            row.append();
                            if (++rowCount >= deadline) {
                                tableWriter.ic();
                                deadline = rowCount + batchSize;
                            }
                        }
                    }
                    break;
                } catch (TableReferenceOutOfDateException e) {
                    factory = Misc.free(factory);
                    if (i == maxRecompileAttempts - 1) {
                        LOG.error().$("error refreshing materialized view, base table is under heavy DDL changes [view=").$(viewDef.getMatViewToken())
                                .$(", recompileAttempts=").$(maxRecompileAttempts)
                                .I$();
                        refreshFailState(state, refreshTimestamp, "base table is under heavy DDL changes");
                        return false;
                    }
                } catch (Throwable th) {
                    factory = Misc.free(factory);
                    refreshFailState(state, refreshTimestamp, th.getMessage());
                    throw th;
                }
            }

            tableWriter.commit();
            state.refreshSuccess(factory, copier, tableWriter.getMetadata().getMetadataVersion(), refreshTimestamp, refreshTriggeredTimestamp, baseTableTxn);
        } catch (Throwable th) {
            LOG.error().$("error refreshing materialized view [view=").$(viewDef.getMatViewToken()).$(", error=").$(th).I$();
            Misc.free(factory);
            refreshFailState(state, refreshTimestamp, th.getMessage());
            throw th;
        }

        return rowCount > 0;
    }

    private void invalidateDependentViews(TableToken baseTableToken, MatViewGraph viewGraph) {
        childViewSink.clear();
        viewGraph.getDependentMatViews(baseTableToken, childViewSink);
        for (int v = 0, n = childViewSink.size(); v < n; v++) {
            final TableToken viewToken = childViewSink.get(v);
            invalidateView(viewToken, viewGraph);
        }
    }

    private void invalidateView(TableToken viewToken, MatViewGraph viewGraph) {
        final MatViewRefreshState state = viewGraph.getViewRefreshState(viewToken);
        if (state != null && !state.isDropped()) {
            if (!state.tryLock()) {
                LOG.debug().$("skipping materialized view invalidation, locked by another refresh run [view=").$(viewToken).I$();
                state.markAsPendingInvalidation();
                viewGraph.refresh(viewToken, MatViewRefreshTask.INVALIDATE);
                return;
            }
            try {
                setInvalidState(state);
            } finally {
                state.unlock();
            }
        }
    }

    private boolean rebuildView(@NotNull TableToken viewToken, MatViewGraph viewGraph, long refreshTriggeredTimestamp) {
        final MatViewRefreshState state = viewGraph.getViewRefreshState(viewToken);
        if (state == null || state.isDropped()) {
            return false;
        }

        if (!state.tryLock()) {
            // Someone is refreshing the view, so we're going for another attempt.
            // Just mark the view invalid to prevent intermediate incremental refreshes and republish the task.
            LOG.info().$("delaying materialized view rebuild, locked by another refresh run [view=").$(viewToken).I$();
            state.markAsPendingInvalidation();
            viewGraph.refresh(viewToken, MatViewRefreshTask.REBUILD);
            return false;
        }

        final boolean rebuilt;
        try {
            final TableToken baseTableToken;
            try {
                baseTableToken = engine.verifyTableName(state.getViewDefinition().getBaseTableName());
            } catch (CairoException th) {
                LOG.error().$("error rebuilding materialized view, cannot resolve base table [view=").$(viewToken)
                        .$(", error=").$(th.getFlyweightMessage())
                        .I$();
                refreshFailState(state, microsecondClock.getTicks(), th.getFlyweightMessage());
                return false;
            }

            if (!baseTableToken.isWal()) {
                refreshFailState(state, microsecondClock.getTicks(), "base table is not a WAL table");
                return false;
            }

            final SeqTxnTracker viewSeqTracker = engine.getTableSequencerAPI().getTxnTracker(viewToken);
            rebuilt = rebuildView(state, baseTableToken, viewToken, viewSeqTracker, refreshTriggeredTimestamp);
        } catch (Throwable th) {
            LOG.error().$("error rebuilding materialized view [view=").$(viewToken).$(", error=").$(th).I$();
            refreshFailState(state, microsecondClock.getTicks(), th.getMessage());
            return false;
        } finally {
            state.unlock();
            // Try closing the factory if there was a concurrent drop mat view.
            state.tryCloseIfDropped();
        }

        // Kickstart incremental refresh.
        if (rebuilt) {
            viewGraph.refresh(viewToken, MatViewRefreshTask.INCREMENTAL_REFRESH);
        }
        return rebuilt;
    }

    private boolean rebuildView(
            @NotNull MatViewRefreshState state,
            @NotNull TableToken baseTableToken,
            @NotNull TableToken viewToken,
            @NotNull SeqTxnTracker viewTxnTracker,
            long refreshTriggeredTimestamp
    ) {
        assert state.isLocked();

        final MatViewDefinition viewDef = state.getViewDefinition();
        if (viewDef == null) {
            // The view must have been deleted.
            LOG.info().$("not rebuilding materialized view, new definition does not exist [view=").$(viewToken)
                    .$(", base=").$(baseTableToken)
                    .I$();
            return false;
        }

        // Steps:
        // - truncate view
        // - compile view and insert as select on all base table partitions
        // - write the result set to WAL (or directly to table writer O3 area)
        // - apply resulting commit
        // - update applied to txn in MatViewGraph
        try (TableReader baseTableReader = engine.getReader(baseTableToken)) {
            mvRefreshExecutionContext.of(baseTableReader);
            // Operate SQL on a fixed reader that has known max transaction visible.
            engine.detachReader(baseTableReader);
            try {
                final long toBaseTxn = baseTableReader.getSeqTxn();
                // Make time interval filter no-op as we're querying all partitions.
                mvRefreshExecutionContext.setRange(Long.MIN_VALUE + 1, Long.MAX_VALUE);

                try (TableWriterAPI commitWriter = engine.getTableWriterAPI(viewToken, "Mat View rebuild")) {
                    commitWriter.truncateSoft();
                    insertAsSelect(state, viewDef, commitWriter, toBaseTxn, refreshTriggeredTimestamp);
                    resetInvalidState(state);
                    engine.getTableSequencerAPI().setLastRefreshBaseTxn(viewToken, toBaseTxn);
                    viewTxnTracker.setLastRefreshBaseTxn(toBaseTxn);
                    return true;
                } catch (CairoException ex) {
                    if (ex.isTableDropped() || ex.tableDoesNotExist()) {
                        // There is an ongoing drop mat view. It will clean up the state.
                        LOG.info().$("materialized view is dropped, it will be removed from the graph [view=").$(viewToken).I$();
                    } else {
                        throw ex;
                    }
                }
            } finally {
                engine.attachReader(baseTableReader);
            }
        } catch (SqlException e) {
            LOG.error().$("error refreshing materialized view [view=").$(viewToken)
                    .$(", error=").$(e.getFlyweightMessage())
                    .I$();
            refreshFailState(state, microsecondClock.getTicks(), e.getFlyweightMessage());
        }
        return false;
    }

    private boolean refreshDependentViews(TableToken baseTableToken, MatViewGraph viewGraph, long refreshTriggeredTimestamp) {
        if (!baseTableToken.isWal()) {
            LOG.error().$("found materialized views dependent on non-WAL table that will not be refreshed [parent=").utf8(baseTableToken.getTableName()).I$();
            return false;
        }

        boolean refreshed = false;
        childViewSink.clear();
        viewGraph.getDependentMatViews(baseTableToken, childViewSink);
        final SeqTxnTracker baseSeqTracker = engine.getTableSequencerAPI().getTxnTracker(baseTableToken);
        final long minRefreshToTxn = baseSeqTracker.getWriterTxn();

        for (int v = 0, n = childViewSink.size(); v < n; v++) {
            final TableToken viewToken = childViewSink.get(v);
            final SeqTxnTracker viewSeqTracker = engine.getTableSequencerAPI().getTxnTracker(viewToken);
            final long appliedToViewTxn = viewSeqTracker.getLastRefreshBaseTxn();
            final long lastBaseQueryableTxn = baseSeqTracker.getWriterTxn();

            if (appliedToViewTxn < 0 || appliedToViewTxn < lastBaseQueryableTxn) {
                final MatViewRefreshState state = viewGraph.getViewRefreshState(viewToken);
                if (state != null && !state.isPendindInvalidation() && !state.isInvalid() && !state.isDropped()) {
                    if (!state.tryLock()) {
                        LOG.debug().$("skipping materialized view refresh, locked by another refresh run [view=").$(viewToken).I$();
                        continue;
                    }

                    try {
                        refreshed = refreshView(state, baseTableToken, viewToken, viewSeqTracker, appliedToViewTxn, lastBaseQueryableTxn, refreshTriggeredTimestamp);
                    } catch (Throwable th) {
                        refreshFailState(state, microsecondClock.getTicks(), th.getMessage());
                    } finally {
                        state.unlock();
                        // Try closing the factory if there was a concurrent drop mat view.
                        state.tryCloseIfDropped();
                    }
                }
            }
        }
        mvRefreshTask.baseTableToken = baseTableToken;
        mvRefreshTask.operation = MatViewRefreshTask.INCREMENTAL_REFRESH;
        viewGraph.notifyBaseRefreshed(mvRefreshTask, minRefreshToTxn);

        if (refreshed) {
            LOG.info().$("refreshed materialized views dependent on [table=").$(baseTableToken).I$();
        }
        return refreshed;
    }

    private void refreshFailState(MatViewRefreshState state, long refreshTimestamp, CharSequence errorMessage) {
        state.refreshFail(metaFileWriter, dbRoot.trimTo(dbRootLen), refreshTimestamp, errorMessage);
    }

    private boolean refreshNotifiedViews() {
        final MatViewGraph matViewGraph = engine.getMatViewGraph();
        boolean refreshed = false;
        while (matViewGraph.tryDequeueRefreshTask(mvRefreshTask)) {
            final int operation = mvRefreshTask.operation;
            final TableToken baseTableToken = mvRefreshTask.baseTableToken;
            final TableToken matViewToken = mvRefreshTask.matViewToken;
            final long refreshTriggeredTimestamp = mvRefreshTask.refreshTriggeredTimestamp;

            if (matViewToken == null) {
                try {
                    engine.verifyTableToken(baseTableToken);
                } catch (CairoException ce) {
                    LOG.info().$("base table is dropped or renamed [table=").$(baseTableToken)
                            .$(", error=").$(ce.getFlyweightMessage())
                            .I$();
                    invalidateDependentViews(baseTableToken, matViewGraph);
                    continue;
                }
            }

            switch (operation) {
                case MatViewRefreshTask.INCREMENTAL_REFRESH:
                    if (matViewToken == null) {
                        refreshed |= refreshDependentViews(baseTableToken, matViewGraph, refreshTriggeredTimestamp);
                    } else {
                        refreshed |= refreshView(matViewToken, matViewGraph, refreshTriggeredTimestamp);
                    }
                    break;
                case MatViewRefreshTask.REBUILD:
                    assert matViewToken != null;
                    refreshed |= rebuildView(matViewToken, matViewGraph, refreshTriggeredTimestamp);
                    break;
                case MatViewRefreshTask.INVALIDATE:
                    // TODO(puzpuzpuz): include invalidation reason
                    // TODO(puzpuzpuz): persist invalid flag while holding state's mutex
                    if (matViewToken == null) {
                        invalidateDependentViews(baseTableToken, matViewGraph);
                    } else {
                        invalidateView(matViewToken, matViewGraph);
                    }
                    break;
                default:
                    throw new RuntimeException();
            }
        }
        return refreshed;
    }

    private boolean refreshView(@NotNull TableToken viewToken, MatViewGraph viewGraph, long refreshTriggeredTimestamp) {
        final MatViewRefreshState state = viewGraph.getViewRefreshState(viewToken);
        if (state == null || state.isPendindInvalidation() || state.isInvalid() || state.isDropped()) {
            return false;
        }

        if (!state.tryLock()) {
            LOG.debug().$("skipping materialized view refresh, locked by another refresh run [view=").$(viewToken).I$();
            return false;
        }

        try {
            final TableToken baseTableToken;
            try {
                baseTableToken = engine.verifyTableName(state.getViewDefinition().getBaseTableName());
            } catch (CairoException th) {
                LOG.error().$("error refreshing materialized view, cannot resolve base table [view=").$(viewToken)
                        .$(", error=").$(th.getFlyweightMessage())
                        .I$();
                refreshFailState(state, microsecondClock.getTicks(), th.getFlyweightMessage());
                return false;
            }

            if (!baseTableToken.isWal()) {
                refreshFailState(state, microsecondClock.getTicks(), "base table is not a WAL table");
                return false;
            }

            final SeqTxnTracker baseSeqTracker = engine.getTableSequencerAPI().getTxnTracker(baseTableToken);
            final long lastBaseQueryableTxn = baseSeqTracker.getWriterTxn();
            final SeqTxnTracker viewSeqTracker = engine.getTableSequencerAPI().getTxnTracker(viewToken);
            final long appliedToParentTxn = viewSeqTracker.getLastRefreshBaseTxn();
            if (appliedToParentTxn < 0 || appliedToParentTxn < lastBaseQueryableTxn) {
                return refreshView(state, baseTableToken, viewToken, viewSeqTracker, appliedToParentTxn, lastBaseQueryableTxn, refreshTriggeredTimestamp);
            }
            return false;
        } catch (Throwable th) {
            LOG.error().$("error refreshing materialized view [view=").$(viewToken).$(", error=").$(th).I$();
            refreshFailState(state, microsecondClock.getTicks(), th.getMessage());
            return false;
        } finally {
            state.unlock();
            // Try closing the factory if there was a concurrent drop mat view.
            state.tryCloseIfDropped();
        }
    }

    private boolean refreshView(
            @NotNull MatViewRefreshState state,
            @NotNull TableToken baseTableToken,
            @NotNull TableToken viewToken,
            @NotNull SeqTxnTracker viewTxnTracker,
            long fromBaseTxn,
            long toBaseTxn,
            long refreshTriggeredTimestamp
    ) {
        assert state.isLocked();

        final MatViewDefinition viewDef = state.getViewDefinition();
        if (viewDef == null) {
            // The view must have been deleted.
            LOG.info().$("not refreshing materialized view, new definition does not exist [view=").$(viewToken)
                    .$(", base=").$(baseTableToken)
                    .I$();
            return false;
        }

        // Steps:
        // - compile view and execute with timestamp ranges from the unprocessed commits
        // - write the result set to WAL (or directly to table writer O3 area)
        // - apply resulting commit
        // - update applied to txn in MatViewGraph
        if (fromBaseTxn < 0) {
            fromBaseTxn = engine.getTableSequencerAPI().getLastRefreshBaseTxn(viewToken);
            if (fromBaseTxn >= toBaseTxn) {
                // Already refreshed
                viewTxnTracker.setLastRefreshBaseTxn(fromBaseTxn);
                return false;
            }
        }

        if (viewTxnTracker.shouldBackOffDueToMemoryPressure(microsecondClock.getTicks())) {
            // rely on another pass of refresh job to re-try
            return false;
        }

        try (TableReader baseTableReader = engine.getReader(baseTableToken)) {
            mvRefreshExecutionContext.of(baseTableReader);
            // Operate SQL on a fixed reader that has known max transaction visible.
            engine.detachReader(baseTableReader);
            try {
                if (findCommitTimestampRanges(mvRefreshExecutionContext, baseTableReader, fromBaseTxn, viewDef)) {
                    toBaseTxn = baseTableReader.getSeqTxn();

                    try (TableWriterAPI commitWriter = engine.getTableWriterAPI(viewToken, "Mat View refresh")) {
                        boolean changed = insertAsSelect(state, viewDef, commitWriter, toBaseTxn, refreshTriggeredTimestamp);
                        if (changed) {
                            engine.getTableSequencerAPI().setLastRefreshBaseTxn(viewToken, toBaseTxn);
                            viewTxnTracker.setLastRefreshBaseTxn(toBaseTxn);
                        }
                        return changed;
                    } catch (CairoException ex) {
                        if (ex.isTableDropped() || ex.tableDoesNotExist()) {
                            // There is an ongoing drop mat view. It will clean up the state.
                            LOG.info().$("materialized view is dropped, it will be removed from the graph [view=").$(viewToken).I$();
                        } else {
                            throw ex;
                        }
                    }
                }
            } finally {
                engine.attachReader(baseTableReader);
            }
        } catch (SqlException e) {
            LOG.error().$("error refreshing materialized view [view=").$(viewToken)
                    .$(", error=").$(e.getFlyweightMessage())
                    .I$();
            refreshFailState(state, microsecondClock.getTicks(), e.getFlyweightMessage());
        }
        return false;
    }

    private void resetInvalidState(MatViewRefreshState state) {
        state.markAsValid(metaFileWriter, dbRoot.trimTo(dbRootLen));
    }

    private void setInvalidState(MatViewRefreshState state) {
        state.markAsInvalid(metaFileWriter, dbRoot.trimTo(dbRootLen), "TODO: invalidation reason");
    }
}
