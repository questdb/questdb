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

import io.questdb.cairo.TableToken;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.ConcurrentQueue;
import io.questdb.std.ConcurrentHashMap;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.NotNull;

public class MatViewGraph implements QuietCloseable {
    private static final Log LOG = LogFactory.getLog(MatViewGraph.class);
    private final ConcurrentHashMap<BaseTableMatViewRefreshState> dependantViewsByTableName = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<MaterializedViewRefreshState> refreshStateByTableDirName = new ConcurrentHashMap<>();
    private final ConcurrentQueue<MvRefreshTask> refreshTaskQueue = new ConcurrentQueue<>(MvRefreshTask::new);

    public void clear() {
        close();
    }

    @Override
    public void close() {
        for (MaterializedViewRefreshState state : refreshStateByTableDirName.values()) {
            Misc.free(state);
        }
        dependantViewsByTableName.clear();
        refreshStateByTableDirName.clear();
    }

    public void dropView(CharSequence baseTableName, TableToken viewToken) {
        BaseTableMatViewRefreshState state = dependantViewsByTableName.get(baseTableName);
        MaterializedViewRefreshState refreshState = refreshStateByTableDirName.remove(viewToken.getDirName());
        if (refreshState != null) {
            if (refreshState.tryLock()) {
                refreshStateByTableDirName.remove(viewToken.getDirName());
                Misc.free(refreshState);
            } else {
                refreshState.markAsDropped();
            }
        }

        if (state != null) {
            try {
                state.writeLock();
                for (int i = 0, size = state.matViews.size(); i < size; i++) {
                    MaterializedViewDefinition view = state.matViews.get(i);
                    if (view.getTableToken().equals(viewToken)) {
                        state.matViews.remove(i);
                        return;
                    }
                }
            } finally {
                state.unlockWrite();
            }
        }
    }

    public void getAffectedViews(TableToken table, AffectedMatViewsSink sink) {
        BaseTableMatViewRefreshState list = getDependencyList(table);
        try {
            list.readLock();
            sink.viewsList.addAll(list.matViews);
        } finally {
            list.unlockRead();
        }
    }

    public void getAllBaseTables(ObjList<CharSequence> sink) {
        for (BaseTableMatViewRefreshState state : dependantViewsByTableName.values()) {
            try {
                state.readLock();
                if (state.matViews.size() > 0) {
                    sink.add(state.matViews.get(0).getBaseTableName());
                }
            } finally {
                state.unlockRead();
            }
        }
    }

    public MaterializedViewDefinition getMatView(CharSequence baseTableName, TableToken matViewToken) {
        final BaseTableMatViewRefreshState state = dependantViewsByTableName.get(baseTableName);
        if (state != null) {
            try {
                state.readLock();
                for (int i = 0, n = state.matViews.size(); i < n; i++) {
                    final MaterializedViewDefinition matView = state.matViews.get(i);
                    if (matView.getTableToken().equals(matViewToken)) {
                        return matView;
                    }
                }
            } finally {
                state.unlockRead();
            }
        }
        return null;
    }

    public MaterializedViewRefreshState getViewRefreshState(TableToken tableToken) {
        return getRefreshState(tableToken.getDirName());
    }

    public void notifyBaseRefreshed(MvRefreshTask task, long seqTxn) {
        TableToken tableToken = task.baseTable;
        BaseTableMatViewRefreshState state = dependantViewsByTableName.get(tableToken.getTableName());
        if (state != null) {
            if (state.notifyOnBaseTableRefreshedNoLock(seqTxn)) {
                // While refreshing more txn were committed. Refresh will need to re-run.
                addToRefreshQueue(task);
            }
        }
    }

    public void notifyTxnApplied(MvRefreshTask task, long seqTxn) {
        BaseTableMatViewRefreshState state = dependantViewsByTableName.get(task.baseTable.getTableName());
        if (state != null) {
            if (state.notifyOnBaseTableCommitNoLock(seqTxn)) {
                addToRefreshQueue(task);
                LOG.info().$("refresh notified table=").$(task.baseTable.getTableName()).$();
            } else {
                LOG.info().$("no need to notify to refresh table=").$(task.baseTable.getTableName()).$();
            }
        }
    }

    public boolean tryDequeueRefreshTask(MvRefreshTask task) {
        return refreshTaskQueue.tryDequeue(task);
    }

    public void upsertView(TableToken base, MaterializedViewDefinition viewDefinition) {
        BaseTableMatViewRefreshState state = getDependencyList(base);
        try {
            state.writeLock();
            for (int i = 0, size = state.matViews.size(); i < size; i++) {
                MaterializedViewDefinition existingView = state.matViews.get(0);
                if (existingView.getTableToken().equals(viewDefinition.getTableToken())) {
                    state.matViews.set(i, viewDefinition);
                    return;
                }
            }
            state.matViews.add(viewDefinition);
        } finally {
            state.unlockWrite();
        }
    }

    private void addToRefreshQueue(MvRefreshTask task) {
        refreshTaskQueue.enqueue(task);
    }

    @NotNull
    private BaseTableMatViewRefreshState getDependencyList(TableToken tableToken) {
        return getDependencyList(tableToken.getTableName());
    }

    @NotNull
    private BaseTableMatViewRefreshState getDependencyList(CharSequence tableName) {
        BaseTableMatViewRefreshState state = dependantViewsByTableName.get(tableName);
        if (state == null) {
            state = new BaseTableMatViewRefreshState();
            BaseTableMatViewRefreshState existingState = dependantViewsByTableName.putIfAbsent(tableName, state);
            return existingState != null ? existingState : state;
        }
        return state;
    }

    @NotNull
    private MaterializedViewRefreshState getRefreshState(CharSequence tableDirName) {
        MaterializedViewRefreshState state = refreshStateByTableDirName.get(tableDirName);
        if (state == null) {
            state = new MaterializedViewRefreshState();
            MaterializedViewRefreshState existingState = refreshStateByTableDirName.putIfAbsent(tableDirName, state);
            return existingState != null ? existingState : state;
        }
        return state;
    }

    public static class AffectedMatViewsSink {
        public ObjList<MaterializedViewDefinition> viewsList = new ObjList<>();
    }
}
