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
import io.questdb.mp.SynchronizedJob;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;

public class MatViewRefreshJob extends SynchronizedJob implements QuietCloseable {
    private static final Log LOG = LogFactory.getLog(MatViewRefreshJob.class);
    private final ObjList<CharSequence> baseTables = new ObjList<>();
    private final ObjList<TableToken> childViewSink = new ObjList<>();
    private final EntityColumnFilter columnFilter = new EntityColumnFilter();
    private final CairoEngine engine;
    private final MatViewRefreshExecutionContext matViewRefreshExecutionContext;
    private final MicrosecondClock microsecondClock;
    private final MatViewRefreshTask mvRefreshTask = new MatViewRefreshTask();
    private final WalTxnRangeLoader txnRangeLoader;

    public MatViewRefreshJob(CairoEngine engine) {
        this.engine = engine;
        this.matViewRefreshExecutionContext = new MatViewRefreshExecutionContext(engine);
        this.txnRangeLoader = new WalTxnRangeLoader(engine.getConfiguration().getFilesFacade());
        this.microsecondClock = engine.getConfiguration().getMicrosecondClock();
    }

    @Override
    public void close() {
        Misc.free(matViewRefreshExecutionContext);
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

            if (minTs <= maxTs && minTs >= viewDefinition.getFromMicros()) {
                // TODO: reuse the sampler instance
                // TODO: Handle sample by with timezones
                TimestampSampler sampler = TimestampSamplerFactory.getInstance(
                        viewDefinition.getSamplingInterval(),
                        viewDefinition.getSamplingIntervalUnit(),
                        0
                );

                long sampleByFromEpoch = viewDefinition.getFromMicros();
                sampler.setStart(sampleByFromEpoch);

                minTs = sampler.round(minTs);
                maxTs = sampler.nextTimestamp(sampler.round(maxTs));

                executionContext.setRanges(minTs, maxTs - 1);

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
            executionContext.setRanges(Long.MIN_VALUE + 1, Long.MAX_VALUE);

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
        RecordToRowCopier copier;
        ColumnFilter entityColumnFilter = generatedColumnFilter(
                factory.getMetadata(),
                tableWriter.getMetadata()
        );
        copier = RecordToRowCopierUtils.generateCopier(
                compiler.getAsm(),
                factory.getMetadata(),
                tableWriter.getMetadata(),
                entityColumnFilter
        );
        return copier;
    }

    private boolean insertAsSelect(MatViewRefreshState state, MatViewDefinition viewDef, TableWriterAPI tableWriter) throws SqlException {
        RecordCursorFactory factory;
        RecordToRowCopier copier;
        long rowCount;
        long refreshTimestamp = microsecondClock.getTicks();
        try {
            factory = state.acquireRecordFactory();
            copier = state.getRecordToRowCopier();

            int maxRecompileAttempts = 10;
            for (int i = 0; i < maxRecompileAttempts; i++) {
                try {
                    if (factory == null) {
                        try (SqlCompiler compiler = engine.getSqlCompiler()) {
                            LOG.info().$("compiling view [view=").$(viewDef.getMatViewToken()).$(", attempt=").$(i).I$();
                            CompiledQuery compiledQuery = compiler.compile(viewDef.getMatViewSql(), matViewRefreshExecutionContext);
                            if (compiledQuery.getType() != CompiledQuery.SELECT) {
                                throw SqlException.$(0, "materialized view query must be a SELECT statement");
                            }
                            factory = compiledQuery.getRecordCursorFactory();

                            if (copier == null || tableWriter.getMetadata().getMetadataVersion() != state.getRecordRowCopierMetadataVersion()) {
                                copier = getRecordToRowCopier(tableWriter, factory, compiler);
                            }
                        } catch (SqlException e) {
                            Misc.free(factory);
                            LOG.error().$("error refreshing materialized view, compilation error [view=").$(viewDef.getMatViewToken()).$(", error=").$(e.getFlyweightMessage()).I$();
                            state.compilationFail(e, refreshTimestamp);
                            return false;
                        }
                    }

                    int cursorTimestampIndex = factory.getMetadata().getColumnIndex(
                            tableWriter.getMetadata().getColumnName(tableWriter.getMetadata().getTimestampIndex())
                    );

                    if (cursorTimestampIndex < 0) {
                        throw SqlException.invalidColumn(0, "timestamp column '")
                                .put(tableWriter.getMetadata().getColumnName(tableWriter.getMetadata().getTimestampIndex()))
                                .put("' not found in view select query");
                    }

                    try (RecordCursor cursor = factory.getCursor(matViewRefreshExecutionContext)) {
                        final Record record = cursor.getRecord();
                        while (cursor.hasNext()) {
                            TableWriter.Row row = tableWriter.newRow(record.getTimestamp(cursorTimestampIndex));
                            copier.copy(record, row);
                            row.append();
                        }
                    }
                    break;
                } catch (TableReferenceOutOfDateException e) {
                    factory = Misc.free(factory);
                    if (i == maxRecompileAttempts - 1) {
                        LOG.error().$("error refreshing materialized view, base table is under heavy DDL changes [view=")
                                .$(viewDef.getMatViewToken()).$(", recompileAttempts=").$(maxRecompileAttempts).I$();
                        state.refreshFail(e, refreshTimestamp);
                        return false;
                    }
                } catch (Throwable th) {
                    Misc.free(factory);
                    state.refreshFail(th, refreshTimestamp);
                    throw th;
                }
            }

            rowCount = tableWriter.getUncommittedRowCount();
            tableWriter.commit();
            state.refreshSuccess(factory, copier, tableWriter.getMetadata().getMetadataVersion(), rowCount, refreshTimestamp);
        } catch (Throwable th) {
            LOG.error().$("error refreshing materialized view [view=").$(viewDef.getMatViewToken()).$(", error=").$(th).I$();
            state.refreshFail(th, refreshTimestamp);
            throw th;
        }

        return rowCount > 0;
    }

    private boolean refreshAll() {
        LOG.info().$("refreshing ALL materialized views").$();
        baseTables.clear();
        MatViewGraph viewGraph = engine.getMaterializedViewGraph();
        viewGraph.getAllBaseTables(baseTables);
        boolean refreshed = false;
        for (int i = 0, size = baseTables.size(); i < size; i++) {
            CharSequence baseName = baseTables.get(i);

            TableToken baseToken = engine.getTableTokenIfExists(baseName);
            if (baseToken != null) {
                refreshed |= refreshDependentViews(baseToken, viewGraph);
            } else {
                // TODO: include more details of the views not to be refreshed
                LOG.error().$("found materialized views dependent on deleted table that will not be refreshed [parent=")
                        .$(baseName).I$();
            }
        }
        return refreshed;
    }

    private boolean refreshDependentViews(TableToken baseToken, MatViewGraph viewGraph) {
        childViewSink.clear();
        boolean refreshed = false;

        if (!baseToken.isWal()) {
            LOG.error().$("Found materialized views dependent on non-WAL table that will not be refreshed [parent=")
                    .$(baseToken.getTableName()).I$();
        }

        viewGraph.getAffectedViews(baseToken, childViewSink);
        SeqTxnTracker baseSeqTracker = engine.getTableSequencerAPI().getTxnTracker(baseToken);
        long minRefreshToTxn = baseSeqTracker.getWriterTxn();

        for (int v = 0, n = childViewSink.size(); v < n; v++) {
            TableToken viewToken = childViewSink.get(v);
            SeqTxnTracker viewSeqTracker = engine.getTableSequencerAPI().getTxnTracker(viewToken);
            long appliedToParentTxn = viewSeqTracker.getLastRefreshBaseTxn();
            long lastBaseQueryableTxn = baseSeqTracker.getWriterTxn();

            if (appliedToParentTxn < 0 || appliedToParentTxn < lastBaseQueryableTxn) {
                MatViewRefreshState state = viewGraph.getViewRefreshState(viewToken);
                assert state != null;
                if (!state.tryLock()) {
                    LOG.info().$("skipping mat view refresh, locked by another refresh run [viewToken=")
                            .$(viewToken)
                            .$();
                    continue;
                }

                try {
                    refreshed = refreshView(viewGraph, state, baseToken, viewSeqTracker, viewToken, appliedToParentTxn, lastBaseQueryableTxn);
                } catch (Throwable th) {
                    state.refreshFail(th, microsecondClock.getTicks());
                } finally {
                    state.unlock();
                }
            }
        }
        mvRefreshTask.baseTable = baseToken;
        viewGraph.notifyBaseRefreshed(mvRefreshTask, minRefreshToTxn);
        return refreshed;
    }

    private boolean refreshNotifiedViews() {
        MatViewGraph materializedViewGraph = engine.getMaterializedViewGraph();
        boolean refreshed = false;
        while (materializedViewGraph.tryDequeueRefreshTask(mvRefreshTask)) {
            TableToken baseTable = mvRefreshTask.baseTable;
            TableToken viewToken = mvRefreshTask.viewToken;
            if (viewToken == null) {
                try {
                    engine.verifyTableToken(baseTable);
                } catch (CairoException th) {
                    LOG.info().$("materialized view base table has name changed or dropped [table=").$(baseTable)
                            .$(", error=").$(th.getFlyweightMessage()).I$();
                    continue;
                }
                LOG.info().$("refreshing materialized views dependent on [table=").$(baseTable).I$();
                refreshed = refreshDependentViews(baseTable, materializedViewGraph);
            } else {
                refreshed = refreshView(viewToken, materializedViewGraph);
            }

        }
        return refreshed;
    }

    private boolean refreshView(@NotNull TableToken viewToken, MatViewGraph viewGraph) {
        MatViewRefreshState state = viewGraph.getViewRefreshState(viewToken);
        if (state == null || state.isDropped()) {
            return false;
        }

        if (!state.tryLock()) {
            LOG.info().$("skipping mat view refresh, locked by another refresh run").$();
            return false;
        }

        try {
            final TableToken baseTableToken;
            try {
                baseTableToken = engine.verifyTableName(state.getViewDefinition().getBaseTableName());
            } catch (CairoException th) {
                LOG.error().$("error refreshing materialized view, cannot resolve base table [view=").$(viewToken)
                        .$(", error=").$(th.getFlyweightMessage()).I$();
                state.refreshFail(th, microsecondClock.getTicks());
                return false;
            }

            if (!baseTableToken.isWal()) {
                state.refreshFail("Base table is not WAL table", microsecondClock.getTicks());
                return false;
            }

            SeqTxnTracker baseSeqTracker = engine.getTableSequencerAPI().getTxnTracker(baseTableToken);
            long lastBaseQueryableTxn = baseSeqTracker.getWriterTxn();
            SeqTxnTracker viewSeqTracker = engine.getTableSequencerAPI().getTxnTracker(viewToken);
            long appliedToParentTxn = viewSeqTracker.getLastRefreshBaseTxn();
            if (appliedToParentTxn < 0 || appliedToParentTxn < lastBaseQueryableTxn) {
                return refreshView(viewGraph, state, baseTableToken, viewSeqTracker, viewToken, appliedToParentTxn, lastBaseQueryableTxn);
            }
            return false;
        } catch (Throwable th) {
            LOG.error().$("error refreshing materialized view [view=").$(viewToken).$(", error=").$(th).I$();
            state.refreshFail(th, microsecondClock.getTicks());
            return false;
        } finally {
            state.unlock();
        }
    }

    private boolean refreshView(
            MatViewGraph viewGraph,
            MatViewRefreshState state,
            TableToken baseToken,
            SeqTxnTracker viewTxnTracker,
            TableToken viewToken,
            long fromBaseTxn,
            long toBaseTxn
    ) {
        MatViewDefinition viewDef = state.getViewDefinition();
        if (viewDef == null) {
            // View must be deleted
            LOG.info().$("not refreshing mat view, new definition does not exist [view=").$(viewToken).$(", base=").$(baseToken).I$();
            return false;
        }

        // Steps:
        // - compile view and execute with timestamp ranges from the unprocessed commits
        // - write the result set to WAL (or directly to table writer O3 area)
        // - apply resulting commit
        // - update applied to Txn in MatViewGraph
        if (fromBaseTxn < 0) {
            fromBaseTxn = engine.getTableSequencerAPI().getLastRefreshBaseTxn(viewToken);
            if (fromBaseTxn >= toBaseTxn) {
                // Already refreshed
                viewTxnTracker.setLastRefreshBaseTxn(fromBaseTxn);
                return false;
            }
        }

        if (viewTxnTracker.shouldBackOffDueToMemoryPressure(MicrosecondClockImpl.INSTANCE.getTicks())) {
            // rely on another pass of refresh job to re-try
            return false;
        }

        try (TableReader baseTableReader = engine.getReader(baseToken)) {
            matViewRefreshExecutionContext.of(baseTableReader);
            try {
                if (findCommitTimestampRanges(matViewRefreshExecutionContext, baseTableReader, fromBaseTxn, viewDef)) {
                    toBaseTxn = baseTableReader.getSeqTxn();

                    try (TableWriterAPI commitWriter = engine.getTableWriterAPI(viewToken, "Mat View refresh")) {
                        boolean changed = insertAsSelect(state, viewDef, commitWriter);
                        if (changed) {
                            engine.getTableSequencerAPI().setLastRefreshBaseTxn(viewToken, toBaseTxn);
                            viewTxnTracker.setLastRefreshBaseTxn(toBaseTxn);
                        }
                        return changed;
                    } catch (CairoException ex) {
                        if (ex.isTableDropped() || ex.tableDoesNotExist()) {
                            LOG.info().$("materialized view is dropped, removing it from materialized view graph" +
                                    " [view=").$(viewToken).I$();
                            viewGraph.dropViewIfExists(viewToken);
                        } else {
                            throw ex;
                        }
                    }
                }
            } finally {
                matViewRefreshExecutionContext.clean();
            }
        } catch (SqlException sqlException) {
            LOG.error().$("error refreshing materialized view [view=").$(viewToken).$(", error=")
                    .$(sqlException.getFlyweightMessage()).I$();
        }
        return false;
    }

    @Override
    protected boolean runSerially() {
        return refreshNotifiedViews();
    }
}
