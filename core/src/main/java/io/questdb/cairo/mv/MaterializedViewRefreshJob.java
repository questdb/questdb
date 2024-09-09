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
import io.questdb.cairo.TableToken;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.SynchronizedJob;
import io.questdb.std.ObjList;

public class MaterializedViewRefreshJob extends SynchronizedJob {
    private static final Log LOG = LogFactory.getLog(MaterializedViewRefreshJob.class);
    private final MatViewGraph.AffectedMatViewsSink childViewSink = new MatViewGraph.AffectedMatViewsSink();
    private final CairoEngine engine;
    private final ObjList<CharSequence> parents = new ObjList<>();

    public MaterializedViewRefreshJob(CairoEngine engine) {
        this.engine = engine;
    }

    private void refreshView(MaterializedViewDefinition viewDef, long fromParentTxn, long toParentTxn) {
        // Steps:
        // - if fromParentTxn is not set, find the last parent txn the view was refreshed if any
        // - find the commit ranges fromParentTxn to latest parent transaction
        // - compile view and execute with timestamp ranges from the unprocessed commits
        // - write the result set to WAL (or directly to table writer O3 area)
        // - apply resulting commit
        // - update applied to Txn in MatViewGraph
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
                        refreshView(viewDef, appliedToParentTxn, lastParentApplied);
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
