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

package io.questdb.cairo.view;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.Job;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;

public class ViewCompilerJob implements Job, QuietCloseable {
    private static final Log LOG = LogFactory.getLog(ViewCompilerJob.class);
    private final ObjList<TableToken> compileViewsSink = new ObjList<>();
    private final ViewCompilerExecutionContext compilerExecutionContext;
    private final ViewCompilerTask compilerTask = new ViewCompilerTask();
    private final CairoEngine engine;
    private final ObjList<TableToken> invalidateViewsSink = new ObjList<>();
    private final ViewStateStore stateStore;
    private final ViewGraph viewGraph;
    private final int workerId;

    public ViewCompilerJob(int workerId, CairoEngine engine, int sharedQueryWorkerCount) {
        try {
            this.workerId = workerId;
            this.engine = engine;
            this.compilerExecutionContext = new ViewCompilerExecutionContext(engine, sharedQueryWorkerCount);
            this.viewGraph = engine.getViewGraph();
            this.stateStore = engine.getViewStateStore();
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    @TestOnly
    public ViewCompilerJob(int workerId, CairoEngine engine) {
        this(workerId, engine, 1);
    }

    @Override
    public void close() {
        LOG.info().$("view compiler job closing [workerId=").$(workerId).I$();
        Misc.free(compilerExecutionContext);
    }

    @Override
    public boolean run(int workerId, @NotNull RunStatus runStatus) {
        // there is job instance per thread, the worker id must never change for this job
        assert this.workerId == workerId;
        return processNotifications();
    }

    private void compile(TableToken tableToken, long updateTimestamp) {
        compileDependentViews(tableToken, updateTimestamp);
        if (tableToken.isView()) {
            compileView(tableToken, updateTimestamp);
        }
    }

    private void compileDependentViews(TableToken tableToken, long updateTimestamp) {
        compileViewsSink.clear();
        viewGraph.getDependentViews(tableToken, compileViewsSink);
        for (int i = 0, n = compileViewsSink.size(); i < n; i++) {
            final TableToken viewToken = compileViewsSink.get(i);
            compileView(viewToken, updateTimestamp);
        }
    }

    private void compileView(TableToken viewToken, long updateTimestamp) {
        final ViewDefinition viewDefinition = viewGraph.getViewDefinition(viewToken);
        if (viewDefinition == null) {
            LOG.error().$("cannot compile view, probably dropped concurrently [token=").$(viewToken).I$();
            return;
        }

        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            compiler.testCompileModel(viewDefinition.getViewSql(), compilerExecutionContext);
            reset(viewToken, updateTimestamp);
        } catch (SqlException | CairoException e) {
            invalidate(viewToken, e.getFlyweightMessage(), updateTimestamp);
        } catch (Throwable e) {
            invalidate(viewToken, e.getMessage(), updateTimestamp);
        }
    }

    private void invalidate(TableToken tableToken, CharSequence invalidationReason, long updateTimestamp) {
        invalidateDependentViews(tableToken, invalidationReason, updateTimestamp);
        if (tableToken.isView()) {
            updateViewState(tableToken, true, invalidationReason, updateTimestamp);
        }
    }

    private void invalidateDependentViews(TableToken tableToken, CharSequence invalidationReason, long updateTimestamp) {
        invalidateViewsSink.clear();
        viewGraph.getDependentViews(tableToken, invalidateViewsSink);
        for (int i = 0, n = invalidateViewsSink.size(); i < n; i++) {
            final TableToken viewToken = invalidateViewsSink.get(i);
            updateViewState(viewToken, true, invalidationReason, updateTimestamp);
        }
    }

    private boolean processNotifications() {
        while (stateStore.tryDequeueCompilerTask(compilerTask)) {
            compile(compilerTask.tableToken, compilerTask.updateTimestamp);
        }
        return false;
    }

    private void reset(TableToken tableToken, long updateTimestamp) {
        if (tableToken == null || !tableToken.isView()) {
            LOG.error().$("cannot reset view state, not a view token [token=").$(tableToken).I$();
            return;
        }

        updateViewState(tableToken, false, null, updateTimestamp);
    }

    private void updateViewState(TableToken viewToken, boolean invalid, CharSequence invalidationReason, long updateTimestamp) {
        final ViewDefinition viewDefinition = viewGraph.getViewDefinition(viewToken);
        if (viewDefinition == null) {
            LOG.error().$("view definition is missing, probably dropped concurrently [token=").$(viewToken).I$();
            return;
        }

        final ViewState state = stateStore.getViewState(viewToken);
        if (state == null) {
            LOG.error().$("view state is missing [token=").$(viewToken).I$();
            return;
        }
        if (state.isInvalid() == invalid) {
            // there is no state change, just return
            return;
        }
        state.setInvalidFlag(invalid);

        LOG.info().$("updating view state [viewToken=").$(viewToken).$(", invalid=").$(invalid).$(", updateTimestamp=").$(updateTimestamp).I$();
        try (WalWriter walWriter = engine.getWalWriter(viewToken)) {
            walWriter.resetViewState(updateTimestamp, invalid, invalidationReason);
        }
    }
}
