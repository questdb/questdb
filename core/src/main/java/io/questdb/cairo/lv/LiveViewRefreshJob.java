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

package io.questdb.cairo.lv;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.Job;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Job that processes WAL commit notifications and refreshes live view instances.
 * On each refresh, compiles the live view's SELECT query, runs the cursor,
 * and copies all results into the InMemoryTable.
 */
public class LiveViewRefreshJob implements Job, QuietCloseable {
    private static final Log LOG = LogFactory.getLog(LiveViewRefreshJob.class);
    private final CairoEngine engine;
    private final SqlExecutionContextImpl executionContext;
    private final ObjList<LiveViewInstance> viewInstanceSink = new ObjList<>();

    public LiveViewRefreshJob(CairoEngine engine) {
        this.engine = engine;
        this.executionContext = new SqlExecutionContextImpl(engine, 1).with(AllowAllSecurityContext.INSTANCE);
    }

    @Override
    public void close() {
        executionContext.close();
    }

    @Override
    public boolean run(int workerId, @NotNull Job.RunStatus runStatus) {
        return processNotifications();
    }

    private boolean processNotifications() {
        boolean didWork = false;
        ConcurrentLinkedQueue<LiveViewRefreshTask> queue = engine.getLiveViewTaskQueue();
        LiveViewRefreshTask polled;
        while ((polled = queue.poll()) != null) {
            refreshViewsForBaseTable(polled.baseTableToken, polled.seqTxn);
            didWork = true;
        }
        return didWork;
    }

    // TODO(live-view): zero-GC + algorithmic — recompiles the SELECT, rebuilds the factory, and runs a fresh cursor
    //  on every WAL commit. SqlCompiler.compile() and factory construction allocate heavily. Incremental refresh
    //  (WalTxnRangeLoader + computeNext()) removes the recompile entirely; until then, at minimum cache the
    //  compiled factory on LiveViewInstance and reuse the cursor across refreshes.
    private void refreshInstance(LiveViewInstance instance, long seqTxn) {
        if (!instance.tryLockForRefresh()) {
            // A concurrent drop or refresh holds the latch; skip this turn and let the other
            // party complete. The next WAL notification will re-enqueue the task.
            return;
        }
        try {
            if (instance.isDropped() || instance.isInvalid()) {
                return;
            }
            String selectSql = instance.getDefinition().getViewSql();
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                CompiledQuery cq = compiler.compile(selectSql, executionContext);
                try (RecordCursorFactory factory = cq.getRecordCursorFactory()) {
                    try (RecordCursor cursor = factory.getCursor(executionContext)) {
                        instance.refresh(cursor);
                    }
                }
            } catch (Throwable t) {
                LOG.error().$("live view refresh failed [view=").$(instance.getDefinition().getViewName())
                        .$(", error=").$(t.getMessage())
                        .I$();
                return;
            }
            instance.setLastProcessedSeqTxn(seqTxn);
        } finally {
            instance.unlockAfterRefresh();
            instance.tryCloseIfDropped();
        }
    }

    private void refreshViewsForBaseTable(TableToken baseTableToken, long seqTxn) {
        LiveViewRegistry registry = engine.getLiveViewRegistry();
        registry.getViewsForBaseTable(baseTableToken.getTableName(), viewInstanceSink);

        for (int i = 0, n = viewInstanceSink.size(); i < n; i++) {
            LiveViewInstance instance = viewInstanceSink.getQuick(i);
            if (instance.isDropped() || instance.isInvalid()) {
                continue;
            }
            if (seqTxn > instance.getLastProcessedSeqTxn()) {
                refreshInstance(instance, seqTxn);
            }
        }
    }
}
