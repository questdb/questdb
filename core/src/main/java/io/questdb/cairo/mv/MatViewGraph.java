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

// TODO(puzpuzpuz): extract interface and introduce a no-op implementation to be used when mat views are disabled
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
        final MatViewRefreshState state = new MatViewRefreshState(viewDefinition);
        final MatViewRefreshState prevState = refreshStateByTableDirName.putIfAbsent(matViewToken.getDirName(), state);
        // WAL table directories are unique, so we don't expect previous value
        if (prevState != null) {
            Misc.free(state);
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
        // when the view is new and empty, incremental refresh is same as full
        addToRefreshQueue(baseTableToken, viewDefinition.getMatViewToken(), MatViewRefreshTask.INCREMENTAL_REFRESH);
    }

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
                state.tryCloseIfDropped();
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
        final TableToken tableToken = task.baseTableToken;
        final MatViewRefreshList state = dependentViewsByTableName.get(tableToken.getTableName());
        if (state != null) {
            if (state.notifyOnBaseTableRefreshedNoLock(seqTxn)) {
                // While refreshing more txn were committed. Refresh will need to re-run.
                refreshTaskQueue.enqueue(task);
            }
        }
    }

    public void notifyTxnApplied(MatViewRefreshTask task, long seqTxn) {
        final MatViewRefreshList list = dependentViewsByTableName.get(task.baseTableToken.getTableName());
        if (list != null) {
            if (list.notifyOnBaseTableCommitNoLock(seqTxn)) {
                refreshTaskQueue.enqueue(task);
                LOG.info().$("refresh notified table=").$(task.baseTableToken.getTableName()).$();
            } else {
                LOG.info().$("no need to notify to refresh table=").$(task.baseTableToken.getTableName()).$();
            }
        }
    }

    public void refresh(TableToken viewTableToken) {
        final MatViewRefreshState state = refreshStateByTableDirName.get(viewTableToken.getDirName());
        if (state != null && !state.isDropped()) {
            final MatViewRefreshTask task = taskHolder.get();
            task.baseTableToken = state.getViewDefinition().getMatViewToken();
            task.viewToken = viewTableToken;
            refreshTaskQueue.enqueue(task);
        }
    }

    public boolean tryDequeueRefreshTask(MatViewRefreshTask task) {
        return refreshTaskQueue.tryDequeue(task);
    }

    private void addToRefreshQueue(TableToken baseToken, @Nullable TableToken viewToken, int operation) {
        final MatViewRefreshTask task = taskHolder.get();
        task.baseTableToken = baseToken;
        task.viewToken = viewToken;
        task.operation = operation;
        refreshTaskQueue.enqueue(task);
    }

    @NotNull
    private MatViewRefreshList getOrCreateDependentViews(CharSequence tableName) {
        return dependentViewsByTableName.computeIfAbsent(tableName, createRefreshList);
    }
}
