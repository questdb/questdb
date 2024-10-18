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

import io.questdb.cairo.*;
import io.questdb.cairo.sql.*;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.griffin.*;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.SynchronizedJob;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;
import io.questdb.std.str.Path;

public class MaterializedViewRefreshJob extends SynchronizedJob {
    public static final String MAT_VIEW_REFRESH_TABLE_WRITE_REASON = "Mat View refresh";
    private static final Log LOG = LogFactory.getLog(MaterializedViewRefreshJob.class);
    private final ObjList<CharSequence> baseTables = new ObjList<>();
    private final MatViewGraph.AffectedMatViewsSink childViewSink = new MatViewGraph.AffectedMatViewsSink();
    private final EntityColumnFilter columnFilter = new EntityColumnFilter();
    private final CairoEngine engine;
    private final MatViewRefreshExecutionContext matViewRefreshExecutionContext;
    private final WalTxnRangeLoader txnRangeLoader;
    private final MvRefreshTask mvRefreshTask = new MvRefreshTask();
    private int queueFullCount;

    public MaterializedViewRefreshJob(CairoEngine engine) {
        this.engine = engine;
        this.matViewRefreshExecutionContext = new MatViewRefreshExecutionContext(engine);
        this.txnRangeLoader = new WalTxnRangeLoader(engine.getConfiguration().getFilesFacade());
    }

    private boolean findCommitTimestampRanges(
            MatViewRefreshExecutionContext executionContext,
            TableReader baseTableReader,
            long lastRefreshTxn,
            MaterializedViewDefinition viewDefinition
    ) throws SqlException {
        long lastTxn = baseTableReader.getSeqTxn();

        if (lastRefreshTxn > 0) {
            txnRangeLoader.load(engine, Path.PATH.get(), baseTableReader.getTableToken(), lastRefreshTxn, lastTxn);
            long minTs = txnRangeLoader.getMinTimestamp();
            long maxTs = txnRangeLoader.getMaxTimestamp();

            if (minTs <= maxTs && minTs >= viewDefinition.getSampleByFromEpochMicros()) {
                // Handle sample by with timezones
                long sampleByPeriod = viewDefinition.getSampleByPeriodMicros();
                long sampleByFromEpoch = viewDefinition.getSampleByFromEpochMicros();
                minTs = sampleByFromEpoch + (minTs - sampleByFromEpoch) / sampleByPeriod * sampleByPeriod;
                maxTs = sampleByFromEpoch + ((maxTs - sampleByFromEpoch + sampleByPeriod) / sampleByPeriod) * sampleByPeriod;

                executionContext.setRanges(minTs, maxTs - 1);

                LOG.info().$("refreshing materialized view [view=").$(viewDefinition.getTableToken())
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

            LOG.info().$("refreshing materialized view, full refresh [view=").$(viewDefinition.getTableToken())
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

    private boolean insertAsSelect(MatViewGraph viewGraph, MaterializedViewDefinition viewDef, TableWriterAPI tableWriter) throws SqlException {
        MaterializedViewRefreshState refreshState = viewGraph.getViewRefreshState(viewDef.getTableToken());
        RecordCursorFactory factory;
        RecordToRowCopier copier;
        long rowCount = 0;
        if (refreshState.tryLock()) {
            try {
                factory = refreshState.acquireRecordFactory();
                copier = refreshState.getRecordToRowCopier();

                int maxRecompileAttempts = 10;
                for (int i = 0; i < maxRecompileAttempts; i++) {
                    try {
                        if (factory == null) {
                            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                                LOG.info().$("compiling view [view=").$(viewDef.getTableToken()).$(", attempt=").$(i).I$();
                                CompiledQuery compiledQuery = compiler.compile(viewDef.getViewSql(), matViewRefreshExecutionContext);
                                if (compiledQuery.getType() != CompiledQuery.SELECT) {
                                    throw SqlException.$(0, "materialized view query must be a SELECT statement");
                                }
                                factory = compiledQuery.getRecordCursorFactory();

                                if (copier == null || tableWriter.getMetadata().getMetadataVersion() != refreshState.getRecordRowCopierMetadataVersion()) {
                                    copier = getRecordToRowCopier(tableWriter, factory, compiler);
                                }
                            } catch (SqlException e) {
                                Misc.free(factory);
                                LOG.error().$("error refreshing materialized view, compilation error [view=").$(viewDef.getTableToken()).$(", error=").$(e.getFlyweightMessage()).I$();
                                refreshState.compilationFail(e);
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
                                    .$(viewDef.getTableToken()).$(", recompileAttempts=").$(maxRecompileAttempts).I$();
                            refreshState.refreshFail(e);
                            return false;
                        }
                    } catch (Throwable th) {
                        Misc.free(factory);
                        refreshState.refreshFail(th);
                        throw th;
                    }
                }

                rowCount = tableWriter.getUncommittedRowCount();
                tableWriter.commit();
                refreshState.refreshSuccess(factory, copier, tableWriter.getMetadata().getMetadataVersion(), rowCount);
            } catch (Throwable th) {
                LOG.error().$("error refreshing materialized view [view=").$(viewDef.getTableToken()).$(", error=").$(th).I$();
                refreshState.refreshFail(th);
                throw th;
            } finally {
                refreshState.unlock();
            }
        } else {
            LOG.info().$("skipping mat view refresh, locked by another refresh run").$();
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
        childViewSink.viewsList.clear();
        CharSequence baseName = baseToken.getTableName();
        boolean refreshed = false;

        if (!baseToken.isWal()) {
            // TODO: include more details of the views not to be refreshed
            LOG.error().$("Found materialized views dependent on non-WAL table that will not be refreshed [parent=")
                    .$(baseName).I$();
        }

        viewGraph.getAffectedViews(baseToken, childViewSink);
        SeqTxnTracker baseSeqTracker = engine.getTableSequencerAPI().getTxnTracker(baseToken);
        long minRefreshToTxn = baseSeqTracker.getWriterTxn();

        for (int v = 0, vsize = childViewSink.viewsList.size(); v < vsize; v++) {
            MaterializedViewDefinition viewDef = childViewSink.viewsList.get(v);
            TableToken viewToken = viewDef.getTableToken();
            SeqTxnTracker viewSeqTracker = engine.getTableSequencerAPI().getTxnTracker(viewToken);
            long appliedToParentTxn = viewSeqTracker.getLastRefreshBaseTxn();
            long lastBaseQueryableTxn = baseSeqTracker.getWriterTxn();

            if (appliedToParentTxn < 0 || appliedToParentTxn < lastBaseQueryableTxn) {
                refreshed = refreshView(viewGraph, baseToken, viewSeqTracker, viewToken, viewDef, appliedToParentTxn, lastBaseQueryableTxn);
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
            LOG.info().$("refreshing materialized views dependent on [table=").$(baseTable).I$();
            refreshed |= refreshDependentViews(baseTable, materializedViewGraph);
        }
        return refreshed;
    }

    private boolean refreshView(
            MatViewGraph viewGraph,
            TableToken baseToken,
            SeqTxnTracker viewTxnTracker,
            TableToken viewToken,
            MaterializedViewDefinition viewDef,
            long fromBaseTxn,
            long toBaseTxn
    ) {
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
                        boolean changed = insertAsSelect(viewGraph, viewDef, commitWriter);
                        if (changed) {
                            engine.getTableSequencerAPI().setLastRefreshBaseTxn(viewToken, toBaseTxn);
                            viewTxnTracker.setLastRefreshBaseTxn(toBaseTxn);
                        }
                        return changed;
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
