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
import io.questdb.std.ObjList;
import io.questdb.std.ThreadLocal;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.tasks.TelemetryMatViewTask;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;

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
        this.matViewTelemetryFacade = matViewTelemetry.isEnabled() ? this::storeMatViewTelemetry : this::storeMatViewTelemetryNoop;
        this.microsecondClock = engine.getConfiguration().getMicrosecondClock();
    }

    @Override
    public MatViewRefreshState addView(MatViewDefinition viewDefinition, boolean isInvalid) {
        final TableToken matViewToken = viewDefinition.getMatViewToken();
        final MatViewRefreshState state = new MatViewRefreshState(viewDefinition, isInvalid, matViewTelemetryFacade);
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
        enqueueRefreshTask(viewDefinition.getMatViewToken(), MatViewRefreshTask.INCREMENTAL_REFRESH);
    }

    @Override
    public void dropViewIfExists(TableToken viewToken) {
        final MatViewRefreshState state = refreshStateByTableDirName.remove(viewToken.getDirName());
        if (state != null) {
            state.markAsDropped();
            state.tryCloseIfDropped();

            final CharSequence baseTableName = state.getViewDefinition().getBaseTableName();
            final MatViewRefreshList dependentViews = dependentViewsByTableName.get(baseTableName);
            if (dependentViews != null) {
                final ObjList<TableToken> matViews = dependentViews.lockForWrite();
                try {
                    for (int i = 0, n = matViews.size(); i < n; i++) {
                        TableToken view = matViews.get(i);
                        if (view.equals(viewToken)) {
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
    public void getDependentMatViews(TableToken table, ObjList<TableToken> sink) {
        final MatViewRefreshList list = getOrCreateDependentViews(table.getTableName());
        final ObjList<TableToken> matViews = list.lockForRead();
        try {
            sink.addAll(matViews);
        } finally {
            list.unlockAfterRead();
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
    public MatViewRefreshState getViewRefreshState(TableToken tableToken) {
        return refreshStateByTableDirName.get(tableToken.getDirName());
    }

    @Override
    public void getViews(ObjList<TableToken> bucket) {
        for (MatViewRefreshState state : refreshStateByTableDirName.values()) {
            bucket.add(state.getViewDefinition().getMatViewToken());
        }
    }

    @Override
    public void notifyBaseRefreshed(MatViewRefreshTask task, long seqTxn) {
        final TableToken tableToken = task.baseTableToken;
        final MatViewRefreshList list = dependentViewsByTableName.get(tableToken.getTableName());
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
            if (list.notifyOnBaseTableCommitNoLock(seqTxn)) {
                task.refreshTriggeredTimestamp = microsecondClock.getTicks();
                refreshTaskQueue.enqueue(task);
                LOG.debug().$("refresh notified [table=").$(task.baseTableToken.getTableName()).I$();
            } else {
                LOG.debug().$("no need to notify to refresh [table=").$(task.baseTableToken.getTableName()).I$();
            }
        }
    }

    @Override
    public void refresh(TableToken viewToken, int operation) {
        final MatViewRefreshState state = refreshStateByTableDirName.get(viewToken.getDirName());
        if (state != null && !state.isDropped()) {
            enqueueRefreshTask(viewToken, operation);
        }
    }

    @Override
    public boolean tryDequeueRefreshTask(MatViewRefreshTask task) {
        return refreshTaskQueue.tryDequeue(task);
    }

    private void enqueueRefreshTask(TableToken viewToken, int operation) {
        final MatViewRefreshTask task = taskHolder.get();
        task.baseTableToken = null;
        task.viewToken = viewToken;
        task.operation = operation;
        task.refreshTriggeredTimestamp = microsecondClock.getTicks();
        refreshTaskQueue.enqueue(task);
    }

    @NotNull
    private MatViewRefreshList getOrCreateDependentViews(CharSequence tableName) {
        return dependentViewsByTableName.computeIfAbsent(tableName, createRefreshList);
    }

    private void storeMatViewTelemetry(short event, TableToken tableToken, long baseTableTxn, CharSequence errorMessage, long latencyUs) {
        TelemetryMatViewTask.store(matViewTelemetry, event, tableToken.getTableId(), baseTableTxn, errorMessage, latencyUs);
    }

    private void storeMatViewTelemetryNoop(short event, TableToken tableToken, long baseTableTxn, CharSequence errorMessage, long latencyUs) {
    }
}
