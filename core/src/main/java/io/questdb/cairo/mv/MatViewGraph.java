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

public class MatViewGraph implements QuietCloseable {
    private static final Log LOG = LogFactory.getLog(MatViewGraph.class);
    private final ConcurrentHashMap<MatViewRefreshList> dependantViewsByTableName = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<MatViewRefreshState> refreshStateByTableDirName = new ConcurrentHashMap<>();
    private final ConcurrentQueue<MvRefreshTask> refreshTaskQueue = new ConcurrentQueue<>(MvRefreshTask::new);
    private final ThreadLocal<MvRefreshTask> taskHolder = new ThreadLocal<>(MvRefreshTask::new);

    public void clear() {
        close();
    }

    @Override
    public void close() {
        for (MatViewRefreshState state : refreshStateByTableDirName.values()) {
            Misc.free(state);
        }
        dependantViewsByTableName.clear();
        refreshStateByTableDirName.clear();
    }

    public void createView(TableToken base, MaterializedViewDefinition viewDefinition) {
        var viewRefreshState = refreshStateByTableDirName.get(viewDefinition.getTableToken().getDirName());
        if (viewRefreshState != null && !viewRefreshState.isDropped()) {
            if (viewRefreshState.getViewDefinition() != viewDefinition) {
                throw CairoException.nonCritical().put("view already exists [view=")
                        .put(viewDefinition.getTableToken().getTableName());
            }
        } else {
            viewRefreshState = new MatViewRefreshState(viewDefinition);
            refreshStateByTableDirName.putIfAbsent(viewDefinition.getTableToken().getDirName(), viewRefreshState);

            MatViewRefreshList list = getDependencyList(base);
            try {
                list.writeLock();
                for (int i = 0, size = list.matViews.size(); i < size; i++) {
                    TableToken existingView = list.matViews.get(0);
                    if (existingView.equals(viewDefinition.getTableToken())) {
                        break;
                    }
                }
                list.matViews.add(viewDefinition.getTableToken());
            } finally {
                list.unlockWrite();
            }
        }

        addToRefreshQueue(base, viewDefinition.getTableToken());
    }

    public void dropViewIfExists(TableToken viewToken) {
        MatViewRefreshState refreshState = refreshStateByTableDirName.remove(viewToken.getDirName());
        if (refreshState != null) {
            CharSequence baseTableName = refreshState.getViewDefinition().getBaseTableName();
            if (refreshState.tryLock()) {
                refreshStateByTableDirName.remove(viewToken.getDirName());
                Misc.free(refreshState);
            } else {
                refreshState.markAsDropped();
            }

            MatViewRefreshList state = dependantViewsByTableName.get(baseTableName);
            if (state != null) {
                try {
                    state.writeLock();
                    for (int i = 0, size = state.matViews.size(); i < size; i++) {
                        TableToken view = state.matViews.get(i);
                        if (view.equals(viewToken)) {
                            state.matViews.remove(i);
                            return;
                        }
                    }
                } finally {
                    state.unlockWrite();
                }
            }
        }
    }

    public void getAffectedViews(TableToken table, ObjList<TableToken> sink) {
        MatViewRefreshList list = getDependencyList(table);
        try {
            list.readLock();
            sink.addAll(list.matViews);
        } finally {
            list.unlockRead();
        }
    }

    public void getAllBaseTables(ObjList<CharSequence> sink) {
        for (CharSequence tableName : dependantViewsByTableName.keySet()) {
            sink.add(tableName);
        }
    }

    public MaterializedViewDefinition getMatView(TableToken matViewToken) {
        final MatViewRefreshState state = refreshStateByTableDirName.get(matViewToken.getDirName());
        if (state != null && !state.isDropped()) {
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
        return getRefreshState(tableToken.getDirName());
    }

    public void getViews(ObjList<TableToken> bucket) {
        for (MatViewRefreshState state : refreshStateByTableDirName.values()) {
            bucket.add(state.getViewDefinition().getTableToken());
        }
    }

    public void notifyBaseRefreshed(MvRefreshTask task, long seqTxn) {
        TableToken tableToken = task.baseTable;
        MatViewRefreshList state = dependantViewsByTableName.get(tableToken.getTableName());
        if (state != null) {
            if (state.notifyOnBaseTableRefreshedNoLock(seqTxn)) {
                // While refreshing more txn were committed. Refresh will need to re-run.
                refreshTaskQueue.enqueue(task);
            }
        }
    }

    public void notifyTxnApplied(MvRefreshTask task, long seqTxn) {
        MatViewRefreshList state = dependantViewsByTableName.get(task.baseTable.getTableName());
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
        var state = refreshStateByTableDirName.get(viewTableToken.getDirName());
        var task = taskHolder.get();
        task.baseTable = state.getViewDefinition().getTableToken();
        task.viewToken = viewTableToken;
        refreshTaskQueue.enqueue(task);
    }

    public boolean tryDequeueRefreshTask(MvRefreshTask task) {
        return refreshTaskQueue.tryDequeue(task);
    }

    private void addToRefreshQueue(TableToken baseToken, @Nullable TableToken viewToken) {
        MvRefreshTask task = taskHolder.get();
        task.baseTable = baseToken;
        task.viewToken = viewToken;
        refreshTaskQueue.enqueue(task);
    }

    @NotNull
    private MatViewRefreshList getDependencyList(TableToken tableToken) {
        return getDependencyList(tableToken.getTableName());
    }

    @NotNull
    private MatViewRefreshList getDependencyList(CharSequence tableName) {
        MatViewRefreshList state = dependantViewsByTableName.get(tableName);
        if (state == null) {
            state = new MatViewRefreshList();
            MatViewRefreshList existingState = dependantViewsByTableName.putIfAbsent(tableName, state);
            return existingState != null ? existingState : state;
        }
        return state;
    }

    @Nullable
    private MatViewRefreshState getRefreshState(CharSequence tableDirName) {
        return refreshStateByTableDirName.get(tableDirName);
    }
}
