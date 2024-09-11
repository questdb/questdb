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
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.SynchronizedJob;
import io.questdb.std.ObjList;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;

import static io.questdb.cairo.pool.AbstractMultiTenantPool.NO_LOCK_REASON;

public class MaterializedViewRefreshJob extends SynchronizedJob {
    public static final String MAT_VIEW_REFRESH_TABLE_WRITE_REASON = "Mat View refresh";
    private static final Log LOG = LogFactory.getLog(MaterializedViewRefreshJob.class);
    private final MatViewGraph.AffectedMatViewsSink childViewSink = new MatViewGraph.AffectedMatViewsSink();
    private final CairoEngine engine;
    private final MatViewRefreshExecutionContext matViewRefreshExecutionContext;
    private final ObjList<CharSequence> parents = new ObjList<>();
    private long baseRefreshRangeFrom;
    private long baseRefreshRangeTo;

    public MaterializedViewRefreshJob(CairoEngine engine) {
        this.engine = engine;
        this.matViewRefreshExecutionContext = new MatViewRefreshExecutionContext(engine);
    }

    private void findCommitTimestampRanges(MatViewRefreshExecutionContext executionContext, TableReader baseTableReader, long toParentTxn) throws SqlException {
        long baseRefreshRangeFrom = 0;
        long baseRefreshRangeTo = Long.MAX_VALUE;
        executionContext.getBindVariableService().setTimestamp("from", baseRefreshRangeFrom);
        executionContext.getBindVariableService().setTimestamp("to", baseRefreshRangeTo);
    }

    private void refreshView(TableToken baseToken, SeqTxnTracker viewTxnTracker, TableToken viewToken, MaterializedViewDefinition viewDef, long fromBaseTxn, long toParentTxn) {
        // Steps:
        // - find the commit ranges fromBaseTxn to latest parent transaction
        // - compile view and execute with timestamp ranges from the unprocessed commits
        // - write the result set to WAL (or directly to table writer O3 area)
        // - apply resulting commit
        // - update applied to Txn in MatViewGraph
        try (TableWriter viewTableWriter = engine.getWriterUnsafe(viewToken, MAT_VIEW_REFRESH_TABLE_WRITE_REASON)) {
            assert viewTableWriter.getMetadata().getTableId() == viewToken.getTableId();

            if (fromBaseTxn < 0) {
                fromBaseTxn = viewTableWriter.getMatViewBaseTxn();
                if (fromBaseTxn >= toParentTxn) {
                    // Already refreshed
                    viewTxnTracker.setAppliedToParentTxn(fromBaseTxn);
                    return;
                }
            }

            if (viewTxnTracker.shouldBackOffDueToMemoryPressure(MicrosecondClockImpl.INSTANCE.getTicks())) {
                // rely on another pass of refresh job to re-try
                return;
            }

            try (TableReader baseTableReader = engine.getReader(baseToken)) {
                matViewRefreshExecutionContext.of(baseTableReader, viewTableWriter);
                findCommitTimestampRanges(matViewRefreshExecutionContext, baseTableReader, fromBaseTxn);
                try (var compiler = engine.getSqlCompiler()) {
                    CompiledQuery compiledQuery = compiler.compile(viewDef.getViewSql(), matViewRefreshExecutionContext);
                }
            } catch (SqlException sqlException) {
                LOG.error().$("error refreshing materialized view [view=").$(viewToken).$(", error=")
                        .$((Throwable) sqlException).I$();
            }
        } catch (EntryUnavailableException tableBusy) {
            //noinspection StringEquality
            if (tableBusy.getReason() != NO_LOCK_REASON) {
                LOG.critical().$("unsolicited table lock [table=").utf8(viewToken.getDirName()).$(", lockReason=").$(tableBusy.getReason()).I$();
            }
            // Do not suspend table. Perhaps writer will be unlocked with no transaction applied.
            // We do not suspend table because of having initial value on lastWriterTxn. It will either be
            // "ignore" or last txn we applied.
        }
    }

    @Override
    protected boolean runSerially() {
        parents.clear();
        MatViewGraph viewGraph = engine.getMaterializedViewGraph();
        viewGraph.getAllParents(parents);
        boolean refreshed = false;
        for (int i = 0, size = parents.size(); i < size; i++) {
            CharSequence parentName = parents.get(i);

            TableToken parentToken = engine.getTableTokenIfExists(parentName);
            if (parentToken != null) {
                childViewSink.viewsList.clear();
                viewGraph.getAffectedViews(parentToken, childViewSink);

                if (!parentToken.isWal() && childViewSink.viewsList.size() > 0) {
                    // TODO: include more details of the views not to be refreshed
                    LOG.error().$("Found materialized views dependent on non-WAL table that will not be refreshed [parent=")
                            .$(parentName).I$();
                }

                SeqTxnTracker parentSeqTracker = engine.getTableSequencerAPI().getTxnTracker(parentToken);
                long lastParentApplied = parentSeqTracker.getWriterTxn();

                for (int v = 0, vsize = childViewSink.viewsList.size(); v < vsize; v++) {
                    MaterializedViewDefinition viewDef = childViewSink.viewsList.get(v);
                    TableToken viewToken = viewDef.getTableToken();
                    SeqTxnTracker viewSeqTracker = engine.getTableSequencerAPI().getTxnTracker(viewToken);
                    long appliedToParentTxn = viewSeqTracker.getAppliedToParentTxn();
                    if (appliedToParentTxn < 0 || appliedToParentTxn < lastParentApplied) {
                        refreshView(parentToken, viewSeqTracker, viewToken, viewDef, lastParentApplied, appliedToParentTxn);
                        refreshed = true;
                    }
                }
            } else {
                // TODO: include more details of the views not to be refreshed
                LOG.error().$("Found materialized views dependent on deleted table that will not be refreshed [parent=")
                        .$(parentName).I$();
            }
        }
        return refreshed;
    }
}
