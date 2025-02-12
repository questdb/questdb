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

import io.questdb.Telemetry;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.ConcurrentQueue;
import io.questdb.std.ConcurrentHashMap;
import io.questdb.std.Misc;
import io.questdb.std.ObjHashSet;
import io.questdb.std.ObjList;
import io.questdb.std.ThreadLocal;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.tasks.TelemetryMatViewTask;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;

import java.util.ArrayDeque;
import java.util.function.Function;

public class MatViewGraphImpl implements MatViewGraph {
    private static final Log LOG = LogFactory.getLog(MatViewGraphImpl.class);
    private final Function<CharSequence, MatViewRefreshList> createRefreshList;
    // TODO(puzpuzpuz): this map is grow-only, i.e. keys are never removed
    private final ConcurrentHashMap<MatViewRefreshList> dependentViewsByTableName = new ConcurrentHashMap<>();
    private final Telemetry<TelemetryMatViewTask> matViewTelemetry;
    private final MatViewTelemetryFacade matViewTelemetryFacade;
    private final MicrosecondClock microsecondClock;
    private final ConcurrentHashMap<MatViewRefreshState> refreshStateByTableDirName = new ConcurrentHashMap<>();
    private final ConcurrentQueue<MatViewRefreshTask> refreshTaskQueue = new ConcurrentQueue<>(MatViewRefreshTask::new);
    private final ThreadLocal<MatViewRefreshTask> taskHolder = new ThreadLocal<>(MatViewRefreshTask::new);

    public MatViewGraphImpl(CairoEngine engine) {
        this.createRefreshList = name -> new MatViewRefreshList();
        this.matViewTelemetry = engine.getTelemetryMatView();
        this.matViewTelemetryFacade = matViewTelemetry.isEnabled() ? this::storeMatViewTelemetry : this::storeMatViewTelemetryNoOp;
        this.microsecondClock = engine.getConfiguration().getMicrosecondClock();
    }

    @Override
    public MatViewRefreshState addView(MatViewDefinition viewDefinition, boolean isInvalid) {
        final TableToken matViewToken = viewDefinition.getMatViewToken();
        final MatViewRefreshState state = new MatViewRefreshState(
                viewDefinition,
                isInvalid,
                matViewTelemetryFacade
        );

        final MatViewRefreshState prevState = refreshStateByTableDirName.putIfAbsent(matViewToken.getDirName(), state);
        // WAL table directories are unique, so we don't expect previous value
        if (prevState != null) {
            Misc.free(state);
            throw CairoException.critical(0).put("materialized view state already exists [dir=")
                    .put(matViewToken.getDirName());
        }

        final MatViewRefreshList list = getOrCreateDependentViews(viewDefinition.getBaseTableName());
        final ObjList<TableToken> matViews = list.lockForWrite();
        try {
            matViews.add(matViewToken);
        } finally {
            list.unlockAfterWrite();
        }
        return state;
    }

    @TestOnly
    public void clear() {
        close();
    }

    @Override
    public void close() {
        for (MatViewRefreshState state : refreshStateByTableDirName.values()) {
            Misc.free(state);
        }
        dependentViewsByTableName.clear();
        refreshStateByTableDirName.clear();
    }

    @Override
    public void createView(MatViewDefinition viewDefinition) {
        addView(viewDefinition).init();
        enqueueRefreshTask(viewDefinition.getMatViewToken(), MatViewRefreshTask.REFRESH, null);
    }

    @Override
    public void dropViewIfExists(TableToken matViewToken) {
        final MatViewRefreshState state = refreshStateByTableDirName.remove(matViewToken.getDirName());
        if (state != null) {
            state.markAsDropped();
            state.tryCloseIfDropped();

            final CharSequence baseTableName = state.getViewDefinition().getBaseTableName();
            final MatViewRefreshList dependentViews = dependentViewsByTableName.get(baseTableName);
            if (dependentViews != null) {
                final ObjList<TableToken> matViews = dependentViews.lockForWrite();
                try {
                    for (int i = 0, n = matViews.size(); i < n; i++) {
                        final TableToken matView = matViews.get(i);
                        if (matView.equals(matViewToken)) {
                            matViews.remove(i);
                            return;
                        }
                    }
                } finally {
                    dependentViews.unlockAfterWrite();
                }
            }
        }
    }

    @Override
    public void enqueueInvalidate(TableToken matViewToken, String invalidationReason) {
        enqueueRefreshTaskIfStateExists(matViewToken, MatViewRefreshTask.INVALIDATE, invalidationReason);
    }

    @Override
    public void enqueueRebuild(TableToken matViewToken) {
        enqueueRefreshTaskIfStateExists(matViewToken, MatViewRefreshTask.REBUILD, null);
    }

    @Override
    public void enqueueRefresh(TableToken matViewToken) {
        enqueueRefreshTaskIfStateExists(matViewToken, MatViewRefreshTask.REFRESH, null);
    }

    public void enqueueRefreshTaskIfStateExists(TableToken matViewToken, int operation, String invalidationReason) {
        final MatViewRefreshState state = refreshStateByTableDirName.get(matViewToken.getDirName());
        if (state != null && !state.isDropped()) {
            enqueueRefreshTask(matViewToken, operation, invalidationReason);
        }
    }

    @Override
    public void getDependentMatViews(TableToken baseTableToken, ObjList<TableToken> sink) {
        final MatViewRefreshList list = getOrCreateDependentViews(baseTableToken.getTableName());
        final ObjList<TableToken> matViews = list.lockForRead();
        try {
            sink.addAll(matViews);
        } finally {
            list.unlockAfterRead();
        }
    }

    @Override
    public void getDependentViewsInOrder(ObjHashSet<TableToken> tables, ObjList<TableToken> ordered) {
        ordered.clear();
        ObjHashSet<TableToken> seen = new ObjHashSet<>();
        ArrayDeque<TableToken> stack = new ArrayDeque<>();
        for (int i = 0, n = tables.size(); i < n; i++) {
            TableToken token = tables.get(i);
            if (!seen.contains(token)) {
                getDependentViewsInOrder(token, seen, stack, ordered);
            }
        }
    }

    @Override
    public MatViewDefinition getMatViewDefinition(TableToken matViewToken) {
        final MatViewRefreshState state = refreshStateByTableDirName.get(matViewToken.getDirName());
        if (state != null) {
            if (state.isDropped()) {
                // Housekeeping
                refreshStateByTableDirName.remove(matViewToken.getDirName(), state);
                state.tryCloseIfDropped();
                return null;
            }
            return state.getViewDefinition();
        }
        return null;
    }

    @Override
    public MatViewRefreshState getViewRefreshState(TableToken matViewToken) {
        return refreshStateByTableDirName.get(matViewToken.getDirName());
    }

    @Override
    public void getViews(ObjList<TableToken> bucket) {
        for (MatViewRefreshState state : refreshStateByTableDirName.values()) {
            bucket.add(state.getViewDefinition().getMatViewToken());
        }
    }

    @Override
    public void notifyBaseInvalidated(TableToken baseTableToken) {
        final MatViewRefreshList list = dependentViewsByTableName.get(baseTableToken.getTableName());
        if (list != null) {
            list.notifyOnBaseInvalidated();
        }
    }

    @Override
    public void notifyBaseRefreshed(MatViewRefreshTask task, long seqTxn) {
        final MatViewRefreshList list = dependentViewsByTableName.get(task.baseTableToken.getTableName());
        if (list != null) {
            if (list.notifyOnBaseTableRefreshedNoLock(seqTxn)) {
                // While refreshing more txn were committed. Refresh will need to re-run.
                task.refreshTriggeredTimestamp = microsecondClock.getTicks();
                refreshTaskQueue.enqueue(task);
            }
        }
    }

    @Override
    public void notifyTxnApplied(MatViewRefreshTask task, long seqTxn) {
        final MatViewRefreshList list = dependentViewsByTableName.get(task.baseTableToken.getTableName());
        if (list != null) {
            // Always notify refresh job in case of mat view invalidation or rebuild.
            // For incremental refresh we check if we haven't already notified on the given txn.
            if (task.operation != MatViewRefreshTask.REFRESH || list.notifyOnBaseTableCommitNoLock(seqTxn)) {
                task.refreshTriggeredTimestamp = microsecondClock.getTicks();
                refreshTaskQueue.enqueue(task);
                LOG.debug().$("refresh job notified [table=").$(task.baseTableToken.getTableName()).I$();
            } else {
                LOG.debug().$("no need to notify to refresh job [table=").$(task.baseTableToken.getTableName()).I$();
            }
        }
    }

    @Override
    public boolean tryDequeueRefreshTask(MatViewRefreshTask task) {
        return refreshTaskQueue.tryDequeue(task);
    }

    private void enqueueRefreshTask(TableToken matViewToken, int operation, String invalidationReason) {
        final MatViewRefreshTask task = taskHolder.get();
        task.baseTableToken = null;
        task.matViewToken = matViewToken;
        task.operation = operation;
        task.invalidationReason = invalidationReason;
        task.refreshTriggeredTimestamp = microsecondClock.getTicks();
        refreshTaskQueue.enqueue(task);
    }

    private void getDependentViewsInOrder(
            TableToken current,
            ObjHashSet<TableToken> seen,
            ArrayDeque<TableToken> stack,
            ObjList<TableToken> sink) {
        stack.push(current);
        while (!stack.isEmpty()) {
            TableToken top = stack.peek();
            if (!seen.contains(top)) {
                MatViewRefreshList list = dependentViewsByTableName.get(top.getTableName());
                if (list == null) {
                    sink.add(top);
                    seen.add(top);
                    stack.pop();
                } else {
                    boolean allDependentSeen = true;
                    ObjList<TableToken> views = list.lockForRead();
                    for (int i = 0, n = views.size(); i < n; i++) {
                        TableToken view = views.get(i);
                        if (!seen.contains(view)) {
                            stack.push(view);
                            allDependentSeen = false;
                        }
                    }
                    list.unlockAfterRead();
                    if (allDependentSeen) {
                        sink.add(top);
                        seen.add(top);
                        stack.pop();
                    }
                }
            } else {
                stack.pop();
            }
        }
    }

    @NotNull
    private MatViewRefreshList getOrCreateDependentViews(CharSequence baseTableName) {
        return dependentViewsByTableName.computeIfAbsent(baseTableName, createRefreshList);
    }

    private void storeMatViewTelemetry(short event, TableToken tableToken, long baseTableTxn, CharSequence errorMessage, long latencyUs) {
        TelemetryMatViewTask.store(matViewTelemetry, event, tableToken.getTableId(), baseTableTxn, errorMessage, latencyUs);
    }

    private void storeMatViewTelemetryNoOp(short event, TableToken tableToken, long baseTableTxn, CharSequence errorMessage, long latencyUs) {
    }
}
