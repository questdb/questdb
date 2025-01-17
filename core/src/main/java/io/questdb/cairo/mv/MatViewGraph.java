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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.ConcurrentQueue;
import io.questdb.std.ConcurrentHashMap;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import io.questdb.std.ThreadLocal;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.util.function.Function;

public class MatViewGraph implements QuietCloseable {
    private static final Log LOG = LogFactory.getLog(MatViewGraph.class);
    private final Function<CharSequence, MatViewRefreshList> createRefreshList;
    // TODO(puzpuzpuz): this map is grow-only, i.e. keys are never removed
    private final ConcurrentHashMap<MatViewRefreshList> dependentViewsByTableName = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<MatViewRefreshState> refreshStateByTableDirName = new ConcurrentHashMap<>();
    private final ConcurrentQueue<MatViewRefreshTask> refreshTaskQueue = new ConcurrentQueue<>(MatViewRefreshTask::new);
    private final ThreadLocal<MatViewRefreshTask> taskHolder = new ThreadLocal<>(MatViewRefreshTask::new);

    public MatViewGraph() {
        this.createRefreshList = name -> new MatViewRefreshList();
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

    // must be called after creating the underlying table
    public void createView(MatViewDefinition viewDefinition) {
        final TableToken matViewToken = viewDefinition.getMatViewToken();
        final MatViewRefreshState viewRefreshState = new MatViewRefreshState(viewDefinition);
        final MatViewRefreshState prevState = refreshStateByTableDirName.putIfAbsent(matViewToken.getDirName(), viewRefreshState);
        // WAL table directories are unique, so we don't expect previous value
        if (prevState != null) {
            Misc.free(viewRefreshState);
            throw CairoException.critical(0).put("mat view state already exists [dir=")
                    .put(matViewToken.getDirName());
        }

        final MatViewRefreshList list = getOrCreateDependentViews(viewDefinition.getBaseTableName());
        final ObjList<TableToken> matViews = list.lockForWrite();
        try {
            matViews.add(matViewToken);
        } finally {
            list.unlockAfterWrite();
        }
    }

    public void createView(TableToken baseTableToken, MatViewDefinition viewDefinition) {
        assert baseTableToken.getTableName().equals(viewDefinition.getBaseTableName());
        createView(viewDefinition);
        addToRefreshQueue(baseTableToken, viewDefinition.getMatViewToken());
    }

    public void dropViewIfExists(TableToken viewToken) {
        final MatViewRefreshState refreshState = refreshStateByTableDirName.remove(viewToken.getDirName());
        if (refreshState != null) {
            if (refreshState.tryLock()) {
                try {
                    Misc.free(refreshState);
                } finally {
                    refreshState.unlock();
                }
            } else {
                refreshState.markAsDropped();
            }

            final CharSequence baseTableName = refreshState.getViewDefinition().getBaseTableName();
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

    public void getDependentMatViews(TableToken table, ObjList<TableToken> sink) {
        final MatViewRefreshList list = getOrCreateDependentViews(table.getTableName());
        final ObjList<TableToken> matViews = list.lockForRead();
        try {
            sink.addAll(matViews);
        } finally {
            list.unlockAfterRead();
        }
    }

    public MatViewDefinition getMatView(TableToken matViewToken) {
        final MatViewRefreshState state = refreshStateByTableDirName.get(matViewToken.getDirName());
        if (state != null) {
            if (state.isDropped()) {
                // Housekeeping
                refreshStateByTableDirName.remove(matViewToken.getDirName(), state);
                return null;
            }
            return state.getViewDefinition();
        }
        return null;
    }

    public MatViewRefreshState getViewRefreshState(TableToken tableToken) {
        return refreshStateByTableDirName.get(tableToken.getDirName());
    }

    public void getViews(ObjList<TableToken> bucket) {
        for (MatViewRefreshState state : refreshStateByTableDirName.values()) {
            bucket.add(state.getViewDefinition().getMatViewToken());
        }
    }

    public void notifyBaseRefreshed(MatViewRefreshTask task, long seqTxn) {
        final TableToken tableToken = task.baseTable;
        final MatViewRefreshList state = dependentViewsByTableName.get(tableToken.getTableName());
        if (state != null) {
            if (state.notifyOnBaseTableRefreshedNoLock(seqTxn)) {
                // While refreshing more txn were committed. Refresh will need to re-run.
                refreshTaskQueue.enqueue(task);
            }
        }
    }

    public void notifyTxnApplied(MatViewRefreshTask task, long seqTxn) {
        final MatViewRefreshList state = dependentViewsByTableName.get(task.baseTable.getTableName());
        if (state != null) {
            if (state.notifyOnBaseTableCommitNoLock(seqTxn)) {
                refreshTaskQueue.enqueue(task);
                LOG.info().$("refresh notified table=").$(task.baseTable.getTableName()).$();
            } else {
                LOG.info().$("no need to notify to refresh table=").$(task.baseTable.getTableName()).$();
            }
        }
    }

    public void refresh(TableToken viewTableToken) {
        final MatViewRefreshState state = refreshStateByTableDirName.get(viewTableToken.getDirName());
        if (state != null && !state.isDropped()) {
            final MatViewRefreshTask task = taskHolder.get();
            task.baseTable = state.getViewDefinition().getMatViewToken();
            task.viewToken = viewTableToken;
            refreshTaskQueue.enqueue(task);
        }
    }

    public boolean tryDequeueRefreshTask(MatViewRefreshTask task) {
        return refreshTaskQueue.tryDequeue(task);
    }

    private void addToRefreshQueue(TableToken baseToken, @Nullable TableToken viewToken) {
        final MatViewRefreshTask task = taskHolder.get();
        task.baseTable = baseToken;
        task.viewToken = viewToken;
        refreshTaskQueue.enqueue(task);
    }

    @NotNull
    private MatViewRefreshList getOrCreateDependentViews(CharSequence tableName) {
        return dependentViewsByTableName.computeIfAbsent(tableName, createRefreshList);
    }
}
