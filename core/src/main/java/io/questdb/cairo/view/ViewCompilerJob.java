/*******************************************************************************
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

package io.questdb.cairo.view;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.MetadataCacheWriter;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlUtil;
import io.questdb.griffin.model.ExecutionModel;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.log.LogRecord;
import io.questdb.mp.Job;
import io.questdb.std.Misc;
import io.questdb.std.ObjHashSet;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import io.questdb.std.datetime.MicrosecondClock;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
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

    /**
     * Used on a background thread at startup to compile all views.
     * Compiling views initializes view state and hydrates metadata cache.
     */
    public void compileAllViews() {
        try {
            final ObjHashSet<TableToken> tableTokens = new ObjHashSet<>();
            engine.getTableTokens(tableTokens, false);
            final ObjList<TableToken> tokens = tableTokens.getList();

            LOG.info().$("compiling all views [workerId=").$(workerId).I$();
            final MicrosecondClock microsClock = engine.getConfiguration().getMicrosecondClock();
            for (int i = 0, n = tokens.size(); i < n; i++) {
                final TableToken token = tokens.getQuick(i);
                if (token.isView()) {
                    compileView(token, microsClock.getTicks());
                }
            }
        } catch (CairoException e) {
            LogRecord l = e.isCritical() ? LOG.critical() : LOG.error();
            l.$safe(e.getFlyweightMessage()).$();
        } finally {
            Path.clearThreadLocals();
        }
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
            // the view could have been dropped concurrently
            if (!engine.isTableDropped(viewToken)) {
                LOG.error().$("cannot compile view, missing view definition [token=").$(viewToken).I$();
            }
            return;
        }

        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            final ExecutionModel executionModel = compiler.generateExecutionModel(viewDefinition.getViewSql(), compilerExecutionContext);
            // view went from invalid to valid state
            // we should also update view metadata, if there was a change
            final ViewMetadata viewMetadata = getUpdatedViewMetadata(viewToken, compiler, executionModel);
            reset(viewToken, viewMetadata, updateTimestamp);
        } catch (SqlException | CairoException e) {
            invalidate(viewToken, e.getFlyweightMessage(), updateTimestamp);
        } catch (Throwable e) {
            invalidate(viewToken, e.getMessage(), updateTimestamp);
        }
    }

    // checks for view metadata changes
    // returns new view metadata if there is a change, otherwise returns null
    private @Nullable ViewMetadata getUpdatedViewMetadata(TableToken viewToken, SqlCompiler compiler, ExecutionModel executionModel) throws SqlException {
        try (
                RecordCursorFactory factory = SqlUtil.generateFactory(compiler, executionModel, compilerExecutionContext);
                TableMetadata currentMetadata = engine.getTableMetadata(viewToken)
        ) {
            final RecordMetadata newMetadata = factory.getMetadata();
            final int columnCount = newMetadata.getColumnCount();
            if (currentMetadata == null || currentMetadata.getColumnCount() != columnCount) {
                return ViewMetadata.newInstance(viewToken, newMetadata);
            }

            boolean metadataChanged = false;
            for (int i = 0; i < columnCount; i++) {
                final String colName = newMetadata.getColumnName(i);
                final int newColType = newMetadata.getColumnType(i);
                final int oldColIndex = currentMetadata.getColumnIndexQuiet(colName);
                if (oldColIndex != i) {
                    metadataChanged = true;
                    break;
                }
                final int oldColType = currentMetadata.getColumnType(i);
                if (oldColType != newColType) {
                    metadataChanged = true;
                    break;
                }
            }
            if (newMetadata.getTimestampIndex() != currentMetadata.getTimestampIndex()) {
                metadataChanged = true;
            }
            return metadataChanged ? ViewMetadata.newInstance(viewToken, newMetadata) : null;
        }
    }

    private void invalidate(TableToken tableToken, CharSequence invalidationReason, long updateTimestamp) {
        invalidateDependentViews(tableToken, invalidationReason, updateTimestamp);
        if (tableToken.isView()) {
            updateViewState(tableToken, true, invalidationReason, null, updateTimestamp);
        }
    }

    private void invalidateDependentViews(TableToken tableToken, CharSequence invalidationReason, long updateTimestamp) {
        invalidateViewsSink.clear();
        viewGraph.getDependentViews(tableToken, invalidateViewsSink);
        for (int i = 0, n = invalidateViewsSink.size(); i < n; i++) {
            final TableToken viewToken = invalidateViewsSink.get(i);
            updateViewState(viewToken, true, invalidationReason, null, updateTimestamp);
        }
    }

    private boolean processNotifications() {
        while (stateStore.tryDequeueCompilerTask(compilerTask)) {
            compile(compilerTask.tableToken, compilerTask.updateTimestamp);
        }
        return false;
    }

    private void reset(TableToken tableToken, @Nullable ViewMetadata viewMetadata, long updateTimestamp) {
        if (tableToken == null || !tableToken.isView()) {
            LOG.error().$("cannot reset view state, not a view token [token=").$(tableToken).I$();
            return;
        }
        updateViewState(tableToken, false, null, viewMetadata, updateTimestamp);
    }

    // if viewMetadata is null, no metadata update needed
    private void updateViewState(TableToken viewToken, boolean invalid, CharSequence invalidationReason, @Nullable ViewMetadata viewMetadata, long updateTimestamp) {
        final ViewDefinition viewDefinition = viewGraph.getViewDefinition(viewToken);
        if (viewDefinition == null) {
            LOG.info().$("view definition is missing, probably dropped concurrently [token=").$(viewToken).I$();
            return;
        }

        final ViewState viewState = stateStore.getViewState(viewToken);
        if (viewState == null) {
            LOG.info().$("view state is missing, probably dropped concurrently [token=").$(viewToken).I$();
            return;
        }

        try {
            viewState.lockForWrite();
            if (viewState.isInvalid() == invalid && viewMetadata == null) {
                // there is no change, just return
                return;
            }
            LOG.info().$("updating view state [view=").$safe(viewToken.getTableName())
                    .$(", invalid=").$(invalid)
                    .$(", reason=").$safe(invalidationReason)
                    .$(", updateTimestamp=").$(updateTimestamp)
                    .I$();
            viewState.updateState(invalid, invalidationReason, viewMetadata, updateTimestamp);

            if (viewMetadata != null) {
                try (MetadataCacheWriter metadataRW = engine.getMetadataCache().writeLock()) {
                    metadataRW.hydrateTable(viewMetadata);
                }
            }
        } finally {
            viewState.unlockAfterWrite();
        }
    }
}
