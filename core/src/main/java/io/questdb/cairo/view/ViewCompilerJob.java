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
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlUtil;
import io.questdb.griffin.engine.ops.AlterOperationBuilder;
import io.questdb.griffin.model.ExecutionModel;
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
            // the view could have been dropped concurrently
            if (!engine.isTableDropped(viewToken)) {
                LOG.error().$("cannot compile view, missing view definition [token=").$(viewToken).I$();
            }
            return;
        }

        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            final ExecutionModel executionModel = compiler.testCompileModel(viewDefinition.getViewSql(), compilerExecutionContext);
            if (reset(viewToken, updateTimestamp)) {
                // view went from invalid to valid state, we should sync view metadata
                syncViewMetadata(viewToken, compiler, executionModel);
            }
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

    private boolean reset(TableToken tableToken, long updateTimestamp) {
        if (tableToken == null || !tableToken.isView()) {
            LOG.error().$("cannot reset view state, not a view token [token=").$(tableToken).I$();
            return false;
        }
        return updateViewState(tableToken, false, null, updateTimestamp);
    }

    // checks for column type changes in underlying tables, and updates view metadata
    // column count and column names expected to be the same because the view query can be changed with ALTER VIEW only
    private void syncViewMetadata(TableToken viewToken, SqlCompiler compiler, ExecutionModel executionModel) throws SqlException {
        try (
                RecordCursorFactory factory = SqlUtil.generateFactory(compiler, executionModel, compilerExecutionContext);
                TableMetadata currentMetadata = engine.getTableMetadata(viewToken)
        ) {
            final RecordMetadata newMetadata = factory.getMetadata();
            final int columnCount = newMetadata.getColumnCount();
            assert columnCount == currentMetadata.getColumnCount();
            final AlterOperationBuilder alterOperationBuilder = new AlterOperationBuilder();
            boolean metadataChanged = false;
            for (int i = 0; i < columnCount; i++) {
                final String colName = newMetadata.getColumnName(i);
                final int newColType = newMetadata.getColumnType(i);
                final int oldColType = currentMetadata.getColumnType(colName);
                if (oldColType != newColType) {
                    final TableColumnMetadata columnMetadata = newMetadata.getColumnMetadata(i);
                    alterOperationBuilder.addColumnToList(
                            colName,
                            0,
                            newColType,
                            columnMetadata.getSymbolCapacity(),
                            columnMetadata.isSymbolCacheFlag(),
                            columnMetadata.isSymbolIndexFlag(),
                            newMetadata.getIndexValueBlockCapacity(i),
                            columnMetadata.isDedupKeyFlag()
                    );
                    metadataChanged = true;
                }
            }
            if (metadataChanged) {
                try (WalWriter walWriter = engine.getWalWriter(viewToken)) {
                    alterOperationBuilder.ofColumnChangeType(0, viewToken, viewToken.getTableId());
                    walWriter.apply(alterOperationBuilder.build(), true);
                }
            }
        }
    }

    private boolean updateViewState(TableToken viewToken, boolean invalid, CharSequence invalidationReason, long updateTimestamp) {
        final ViewDefinition viewDefinition = viewGraph.getViewDefinition(viewToken);
        if (viewDefinition == null) {
            LOG.info().$("view definition is missing, probably dropped concurrently [token=").$(viewToken).I$();
            return false;
        }

        final ViewState viewState = stateStore.getViewState(viewToken);
        if (viewState == null) {
            LOG.info().$("view state is missing, probably dropped concurrently [token=").$(viewToken).I$();
            return false;
        }

        try {
            viewState.lockForWrite();
            if (viewState.isInvalid() == invalid) {
                // there is no state change, just return
                return false;
            }
            LOG.info().$("updating view state [view=").$safe(viewToken.getTableName())
                    .$(", invalid=").$(invalid)
                    .$(", reason=").$safe(invalidationReason)
                    .$(", updateTimestamp=").$(updateTimestamp)
                    .I$();
            viewState.updateState(invalid, invalidationReason, updateTimestamp);
            return true;
        } finally {
            viewState.unlockAfterWrite();
        }
    }
}
