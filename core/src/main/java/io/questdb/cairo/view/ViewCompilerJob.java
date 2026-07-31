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
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlUtil;
import io.questdb.griffin.model.ExecutionModel;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.log.LogRecord;
import io.questdb.mp.Job;
import io.questdb.mp.continuation.Fiber;
import io.questdb.mp.continuation.FiberRuntime;
import io.questdb.mp.continuation.FiberTask;
import io.questdb.mp.continuation.LaunchResult;
import io.questdb.mp.continuation.SuspensionScope;
import io.questdb.std.Misc;
import io.questdb.std.ObjHashSet;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import io.questdb.std.datetime.MicrosecondClock;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.util.concurrent.atomic.AtomicBoolean;

public class ViewCompilerJob implements Job, QuietCloseable {
    private static final Log LOG = LogFactory.getLog(ViewCompilerJob.class);
    private final ObjList<TableToken> compileViewsSink = new ObjList<>();
    private final ViewCompilerExecutionContext compilerExecutionContext;
    private final ViewCompilerTask compilerTask = new ViewCompilerTask();
    private final CairoEngine engine;
    private final @Nullable FiberRuntime fiberRuntime;
    private final @Nullable FiberCompilerTask fiberTask;
    private final ObjList<TableToken> invalidateViewsSink = new ObjList<>();
    private final int sharedQueryWorkerCount;
    private final ViewStateStore stateStore;
    private final ViewGraph viewGraph;

    public ViewCompilerJob(int workerId, CairoEngine engine, int sharedQueryWorkerCount) {
        this(engine, sharedQueryWorkerCount);
    }

    public ViewCompilerJob(CairoEngine engine, int sharedQueryWorkerCount) {
        this(engine, sharedQueryWorkerCount, null);
    }

    public ViewCompilerJob(
            CairoEngine engine,
            int sharedQueryWorkerCount,
            @Nullable FiberRuntime fiberRuntime
    ) {
        try {
            this.engine = engine;
            this.fiberRuntime = fiberRuntime;
            this.sharedQueryWorkerCount = sharedQueryWorkerCount;
            this.compilerExecutionContext = engine.createViewCompilerContext(sharedQueryWorkerCount);
            this.fiberTask = fiberRuntime != null ? new FiberCompilerTask() : null;
            this.viewGraph = engine.getViewGraph();
            this.stateStore = engine.getViewStateStore();
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    @TestOnly
    public ViewCompilerJob(int workerId, CairoEngine engine) {
        this(engine, 1);
    }

    /**
     * Used on a background thread at startup to compile all views.
     * Compiling views initializes view state and hydrates metadata cache.
     */
    public static void compileAllViews(
            CairoEngine engine,
            SqlExecutionContext executionContext,
            ObjList<TableToken> tempSink
    ) {
        final SuspensionScope.Mode previousMode = SuspensionScope.enter(
                SuspensionScope.Mode.BLOCKING
        );
        try {
            final ObjHashSet<TableToken> tableTokens = new ObjHashSet<>();
            engine.getTableTokens(tableTokens, false);
            final ObjList<TableToken> tokens = tableTokens.getList();

            LOG.info().$("compiling views").$();
            final MicrosecondClock microsClock = engine.getConfiguration().getMicrosecondClock();
            for (int i = 0, n = tokens.size(); i < n; i++) {
                final TableToken token = tokens.getQuick(i);
                if (token.isView()) {
                    compileView(engine, executionContext, token, microsClock.getTicks(), tempSink);
                }
            }
        } catch (CairoException e) {
            LogRecord l = e.isCritical() ? LOG.critical() : LOG.error();
            l.$safe(e.getFlyweightMessage()).$();
        } finally {
            try {
                Path.clearThreadLocals();
            } finally {
                SuspensionScope.restore(previousMode);
            }
        }
    }

    @Override
    public Job cloneInstance() {
        final FiberRuntime runtime = fiberRuntime;
        return runtime != null
                ? new ViewCompilerJob(engine, sharedQueryWorkerCount, runtime)
                : new ViewCompilerJob(engine, sharedQueryWorkerCount);
    }

    @Override
    public void close() {
        LOG.debug().$("view compiler job closing").$();
        Misc.free(compilerExecutionContext);
    }

    @Override
    public void closeInstance() {
        close();
    }

    @Override
    public boolean run(@NotNull WorkerContext workerContext) {
        if (fiberRuntime != null) {
            return processNotificationsOnFiber();
        }
        final SuspensionScope.Mode previousMode = SuspensionScope.enter(
                SuspensionScope.Mode.BLOCKING
        );
        try {
            return processNotifications();
        } finally {
            SuspensionScope.restore(previousMode);
        }
    }

    private static void compileView(
            CairoEngine engine,
            SqlExecutionContext executionContext,
            TableToken viewToken,
            long updateTimestamp,
            ObjList<TableToken> invalidateViewsSink
    ) {
        final ViewDefinition viewDefinition = engine.getViewGraph().getViewDefinition(viewToken);
        if (viewDefinition == null) {
            // the view could have been dropped concurrently
            if (!engine.isTableDropped(viewToken)) {
                LOG.error().$("cannot compile view, missing view definition [token=").$(viewToken).I$();
            }
            return;
        }

        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            final ExecutionModel executionModel = compiler.generateExecutionModel(viewDefinition.getViewSql(), executionContext);
            // view went from invalid to valid state
            // we should also update view metadata, if there was a change
            final ViewMetadata viewMetadata = getUpdatedViewMetadata(executionContext, viewToken, compiler, executionModel);
            reset(engine, viewToken, viewMetadata, updateTimestamp);
        } catch (SqlException | CairoException e) {
            invalidate(engine, viewToken, e.getFlyweightMessage(), updateTimestamp, invalidateViewsSink);
        } catch (Throwable e) {
            invalidate(engine, viewToken, e.getMessage(), updateTimestamp, invalidateViewsSink);
        }
    }

    // checks for view metadata changes
    // returns new view metadata if there is a change, otherwise returns null
    private static @Nullable ViewMetadata getUpdatedViewMetadata(
            SqlExecutionContext executionContext,
            TableToken viewToken,
            SqlCompiler compiler,
            ExecutionModel executionModel
    ) throws SqlException {
        try (
                RecordCursorFactory factory = SqlUtil.generateFactory(compiler, executionModel, executionContext);
                TableMetadata currentMetadata = compiler.getEngine().getTableMetadata(viewToken)
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

    private static void invalidate(
            CairoEngine engine,
            TableToken tableToken,
            CharSequence invalidationReason,
            long updateTimestamp,
            ObjList<TableToken> invalidateViewsSink
    ) {
        invalidateDependentViews(engine, tableToken, invalidationReason, updateTimestamp, invalidateViewsSink);
        if (tableToken.isView()) {
            updateViewState(engine, tableToken, true, invalidationReason, null, updateTimestamp);
        }
    }

    private static void invalidateDependentViews(
            CairoEngine engine,
            TableToken tableToken,
            CharSequence invalidationReason,
            long updateTimestamp,
            ObjList<TableToken> invalidateViewsSink
    ) {
        invalidateViewsSink.clear();
        engine.getViewGraph().getDependentViews(tableToken, invalidateViewsSink);
        for (int i = 0, n = invalidateViewsSink.size(); i < n; i++) {
            final TableToken viewToken = invalidateViewsSink.get(i);
            updateViewState(engine, viewToken, true, invalidationReason, null, updateTimestamp);
        }
    }

    private static void reset(CairoEngine engine, TableToken tableToken, @Nullable ViewMetadata viewMetadata, long updateTimestamp) {
        if (tableToken == null || !tableToken.isView()) {
            LOG.error().$("cannot reset view state, not a view token [token=").$(tableToken).I$();
            return;
        }
        updateViewState(engine, tableToken, false, null, viewMetadata, updateTimestamp);
    }

    // if viewMetadata is null, no metadata update needed
    private static void updateViewState(
            CairoEngine engine,
            TableToken viewToken,
            boolean invalid,
            CharSequence invalidationReason,
            @Nullable ViewMetadata viewMetadata,
            long updateTimestamp
    ) {
        final ViewDefinition viewDefinition = engine.getViewGraph().getViewDefinition(viewToken);
        if (viewDefinition == null) {
            LOG.info().$("view definition is missing, probably dropped concurrently [token=").$(viewToken).I$();
            return;
        }

        final ViewState viewState = engine.getViewStateStore().getViewState(viewToken);
        if (viewState == null) {
            LOG.info().$("view state is missing, probably dropped concurrently [token=").$(viewToken).I$();
            return;
        }

        try {
            viewState.lockForWrite();
            // Skip stale updates - if a more recent update has already been applied
            if (updateTimestamp < viewState.getUpdateTimestamp()) {
                LOG.debug().$("skipping stale view state update [view=").$safe(viewToken.getTableName())
                        .$(", staleTimestamp=").$(updateTimestamp)
                        .$(", currentTimestamp=").$(viewState.getUpdateTimestamp())
                        .I$();
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

    private void compile(TableToken tableToken, long updateTimestamp) {
        compileDependentViews(tableToken, updateTimestamp);
        if (tableToken.isView()) {
            compileView(engine, compilerExecutionContext, tableToken, updateTimestamp, invalidateViewsSink);
        }
    }

    private void compileDependentViews(TableToken tableToken, long updateTimestamp) {
        compileViewsSink.clear();
        viewGraph.getDependentViews(tableToken, compileViewsSink);
        for (int i = 0, n = compileViewsSink.size(); i < n; i++) {
            final TableToken viewToken = compileViewsSink.get(i);
            compileView(engine, compilerExecutionContext, viewToken, updateTimestamp, invalidateViewsSink);
        }
    }

    private boolean processNotifications() {
        while (stateStore.tryDequeueCompilerTask(compilerTask)) {
            compile(compilerTask.tableToken, compilerTask.updateTimestamp);
        }
        return false;
    }

    private boolean processNotificationsOnFiber() {
        final FiberRuntime runtime = fiberRuntime;
        final FiberCompilerTask task = fiberTask;
        if (runtime == null || task == null || !task.isAvailable()) {
            return false;
        }
        final Fiber fiber = runtime.tryReserveFiber();
        if (fiber == null) {
            return false;
        }

        final long reservationEpoch = fiber.getReservationEpoch();
        TableToken dequeuedToken = null;
        try {
            if (!stateStore.tryDequeueCompilerTask(compilerTask)) {
                return false;
            }
            dequeuedToken = compilerTask.tableToken;
            if (!task.prepare(compilerTask)) {
                return false;
            }
            final LaunchResult result = runtime.launchReserved(
                    fiber,
                    reservationEpoch,
                    task,
                    task.getIncarnation()
            );
            if (result == LaunchResult.LAUNCHED) {
                dequeuedToken = null;
                return true;
            }
            task.releaseAfterLaunchFailure();
            return false;
        } finally {
            runtime.releaseReservedFiber(fiber, reservationEpoch);
            if (dequeuedToken != null) {
                stateStore.enqueueCompile(dequeuedToken);
            }
        }
    }

    private class FiberCompilerTask extends FiberTask {
        private final AtomicBoolean isAvailable = new AtomicBoolean(true);
        private final ViewCompilerTask notification = new ViewCompilerTask();

        private boolean isAvailable() {
            return isAvailable.get();
        }

        @Override
        protected void onAbandoned() {
            if (notification.tableToken != null) {
                stateStore.enqueueCompile(notification.tableToken);
            }
        }

        @Override
        protected void onDone() {
            notification.clear();
            isAvailable.set(true);
        }

        @Override
        protected void onError(Throwable th) {
            LOG.critical().$("view compilation failed on fiber [view=").$(notification.tableToken)
                    .$(", ex=").$(th)
                    .I$();
        }

        @Override
        protected boolean runStep() {
            compile(notification.tableToken, notification.updateTimestamp);
            return true;
        }

        private boolean prepare(ViewCompilerTask source) {
            if (!isAvailable.compareAndSet(true, false)) {
                return false;
            }
            boolean isPrepared = false;
            try {
                if (isDone() && !tryReopen()) {
                    return false;
                }
                source.copyTo(notification);
                isPrepared = true;
                return true;
            } finally {
                if (!isPrepared) {
                    isAvailable.set(true);
                }
            }
        }

        private void releaseAfterLaunchFailure() {
            if (isIdle(getIncarnation())) {
                notification.clear();
                isAvailable.set(true);
            }
        }
    }
}
