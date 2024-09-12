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
import io.questdb.std.str.Path;

import static io.questdb.cairo.pool.AbstractMultiTenantPool.NO_LOCK_REASON;

public class MaterializedViewRefreshJob extends SynchronizedJob {
    public static final String MAT_VIEW_REFRESH_TABLE_WRITE_REASON = "Mat View refresh";
    private static final Log LOG = LogFactory.getLog(MaterializedViewRefreshJob.class);
    private final MatViewGraph.AffectedMatViewsSink childViewSink = new MatViewGraph.AffectedMatViewsSink();
    private final CairoEngine engine;
    private final MatViewRefreshExecutionContext matViewRefreshExecutionContext;
    private final ObjList<CharSequence> parents = new ObjList<>();
    private final WalTxnRangeLoader txnRangeLoader;

    public MaterializedViewRefreshJob(CairoEngine engine) {
        this.engine = engine;
        this.matViewRefreshExecutionContext = new MatViewRefreshExecutionContext(engine);
        this.txnRangeLoader = new WalTxnRangeLoader(engine.getConfiguration().getFilesFacade());
    }

    private boolean findCommitTimestampRanges(MatViewRefreshExecutionContext executionContext, TableReader baseTableReader, long fromParentTxn, MaterializedViewDefinition viewDefinition) throws SqlException {
        long readToTxn = baseTableReader.getSeqTxn();

        txnRangeLoader.load(engine, Path.PATH.get(), baseTableReader.getTableToken(), fromParentTxn, readToTxn);
        long minTs = txnRangeLoader.getMinTimestamp();
        long maxTs = txnRangeLoader.getMaxTimestamp();

        if (minTs <= maxTs && minTs >= viewDefinition.getSampleByFromEpochMicros()) {
            // Handle sample by with timezones
            long sampleByPeriod = viewDefinition.getSampleByPeriodMicros();
            long sampleByFromEpoch = viewDefinition.getSampleByFromEpochMicros();
            minTs = sampleByFromEpoch + (minTs - sampleByFromEpoch) / sampleByPeriod * sampleByPeriod;
            maxTs = sampleByFromEpoch + ((maxTs - sampleByFromEpoch + sampleByPeriod - 1) / sampleByPeriod) * sampleByPeriod;

            executionContext.getBindVariableService().setTimestamp("from", minTs);
            executionContext.getBindVariableService().setTimestamp("to", maxTs);
            return txnRangeLoader.getMinTimestamp() <= txnRangeLoader.getMaxTimestamp();
        }

        return false;
    }

    private void refreshView(TableToken baseToken, SeqTxnTracker viewTxnTracker, TableToken viewToken, MaterializedViewDefinition viewDef, long fromBaseTxn, long toBaseTxn) {
        // Steps:
        // - compile view and execute with timestamp ranges from the unprocessed commits
        // - write the result set to WAL (or directly to table writer O3 area)
        // - apply resulting commit
        // - update applied to Txn in MatViewGraph
        try (TableWriter viewTableWriter = engine.getWriterUnsafe(viewToken, MAT_VIEW_REFRESH_TABLE_WRITE_REASON)) {
            assert viewTableWriter.getMetadata().getTableId() == viewToken.getTableId();

            if (fromBaseTxn < 0) {
                fromBaseTxn = engine.getTableSequencerAPI().getLastRefreshBaseTxn(viewToken);
                if (fromBaseTxn >= toBaseTxn) {
                    // Already refreshed
                    viewTxnTracker.setLastRefreshBaseTxn(fromBaseTxn);
                    return;
                }
            }

            if (viewTxnTracker.shouldBackOffDueToMemoryPressure(MicrosecondClockImpl.INSTANCE.getTicks())) {
                // rely on another pass of refresh job to re-try
                return;
            }

            try (TableReader baseTableReader = engine.getReader(baseToken)) {
                matViewRefreshExecutionContext.of(baseTableReader, viewTableWriter);
                try {
                    if (findCommitTimestampRanges(matViewRefreshExecutionContext, baseTableReader, fromBaseTxn, viewDef)) {
                        toBaseTxn = baseTableReader.getSeqTxn();
                        LOG.info().$("refreshing materialized view [view=").$(viewToken)
                                .$(", base=").$(baseToken)
                                .$(", fromSeqTxn=").$(fromBaseTxn)
                                .$(", toSeqTxn=").$(toBaseTxn).I$();

                        try (var compiler = engine.getSqlCompiler()) {
                            CompiledQuery compiledQuery = compiler.compile(viewDef.getViewSql(), matViewRefreshExecutionContext);
                        }
                        engine.getTableSequencerAPI().setLastRefreshBaseTxn(viewToken, toBaseTxn);
                        viewTxnTracker.setLastRefreshBaseTxn(toBaseTxn);
                    }
                } finally {
                    matViewRefreshExecutionContext.clean();
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
                long lastBaseQueryableTxn = parentSeqTracker.getWriterTxn();

                for (int v = 0, vsize = childViewSink.viewsList.size(); v < vsize; v++) {
                    MaterializedViewDefinition viewDef = childViewSink.viewsList.get(v);
                    TableToken viewToken = viewDef.getTableToken();
                    SeqTxnTracker viewSeqTracker = engine.getTableSequencerAPI().getTxnTracker(viewToken);
                    long appliedToParentTxn = viewSeqTracker.getLastRefreshBaseTxn();
                    if (appliedToParentTxn < 0 || appliedToParentTxn < lastBaseQueryableTxn) {
                        refreshView(parentToken, viewSeqTracker, viewToken, viewDef, appliedToParentTxn, lastBaseQueryableTxn);
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
