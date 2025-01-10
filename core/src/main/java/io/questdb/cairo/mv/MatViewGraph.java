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

public class MatViewGraph implements QuietCloseable {
    private static final Log LOG = LogFactory.getLog(MatViewGraph.class);
    private final ConcurrentHashMap<MatViewRefreshList> dependantViewsByTableName = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<MatViewRefreshState> refreshStateByTableDirName = new ConcurrentHashMap<>();
    private final ConcurrentQueue<MatViewRefreshTask> refreshTaskQueue = new ConcurrentQueue<>(MatViewRefreshTask::new);
    private final ThreadLocal<MatViewRefreshTask> taskHolder = new ThreadLocal<>(MatViewRefreshTask::new);

    @TestOnly
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

    public void createView(MatViewDefinition viewDefinition) {
        final TableToken matViewToken = viewDefinition.getMatViewToken();
        MatViewRefreshState viewRefreshState = refreshStateByTableDirName.get(matViewToken.getDirName());
        if (viewRefreshState != null && !viewRefreshState.isDropped()) {
            if (viewRefreshState.getViewDefinition() != viewDefinition) {
                throw CairoException.nonCritical().put("view already exists [view=")
                        .put(matViewToken.getTableName());
            }
        } else {
            // TODO(eugene): `MatViewRefreshState` is Closable.
            //  review data race/memory leak case.
            //  use computeIfAbsent?
            //  close `viewRefreshState` explicitly ?
            //  `viewRefreshState` not null and not dropped, same definition ?
            viewRefreshState = new MatViewRefreshState(viewDefinition);
            refreshStateByTableDirName.putIfAbsent(matViewToken.getDirName(), viewRefreshState);

            MatViewRefreshList list = getDependencyList(viewDefinition.getBaseTableName());
            try {
                ObjList<TableToken> matViews = list.writeLock();
                // TODO(eugene): what the purpose of this loop?
                //  `matViews` is updated unconditionaly with `matViewToken`
                //  can `matViews` contain token for the dropped view?
                for (int i = 0, n = matViews.size(); i < n; i++) {
                    // TODO(eugene): index is always 0 !!!
                    final TableToken existingViewToken = matViews.getQuick(0);
                    if (existingViewToken.equals(matViewToken)) {
                        break;
                    }
                }
                matViews.add(matViewToken);
            } finally {
                list.unlockWrite();
            }
        }
    }

    public void createView(TableToken baseTableToken, MatViewDefinition viewDefinition) {
        assert baseTableToken.getTableName().equals(viewDefinition.getBaseTableName());
        createView(viewDefinition);
        addToRefreshQueue(baseTableToken, viewDefinition.getMatViewToken());
    }

    public void dropViewIfExists(TableToken viewToken) {
        MatViewRefreshState refreshState = refreshStateByTableDirName.remove(viewToken.getDirName());
        if (refreshState != null) {
            if (refreshState.tryLock()) {
                // TODO(eugene): `refreshState` already removed from `refreshStateByTableDirName`
                refreshStateByTableDirName.remove(viewToken.getDirName());
                Misc.free(refreshState);
            } else {
                refreshState.markAsDropped();
            }

            final CharSequence baseTableName = refreshState.getViewDefinition().getBaseTableName();
            final MatViewRefreshList state = dependantViewsByTableName.get(baseTableName);
            if (state != null) {
                try {
                    ObjList<TableToken> matViews = state.writeLock();
                    for (int i = 0, n = matViews.size(); i < n; i++) {
                        TableToken view = matViews.get(i);
                        if (view.equals(viewToken)) {
                            matViews.remove(i);
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
        MatViewRefreshList list = getDependencyList(table.getTableName());
        try {
            ObjList<TableToken> matViews = list.readLock();
            sink.addAll(matViews);
        } finally {
            list.unlockRead();
        }
    }

    public void getAllBaseTables(ObjList<CharSequence> sink) {
        for (CharSequence tableName : dependantViewsByTableName.keySet()) {
            sink.add(tableName);
        }
    }

    public MatViewDefinition getMatView(TableToken matViewToken) {
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
        return refreshStateByTableDirName.get(tableToken.getDirName());
    }

    public void getViews(ObjList<TableToken> bucket) {
        for (MatViewRefreshState state : refreshStateByTableDirName.values()) {
            bucket.add(state.getViewDefinition().getMatViewToken());
        }
    }

    public void notifyBaseRefreshed(MatViewRefreshTask task, long seqTxn) {
        TableToken tableToken = task.baseTable;
        MatViewRefreshList state = dependantViewsByTableName.get(tableToken.getTableName());
        if (state != null) {
            if (state.notifyOnBaseTableRefreshedNoLock(seqTxn)) {
                // While refreshing more txn were committed. Refresh will need to re-run.
                refreshTaskQueue.enqueue(task);
            }
        }
    }

    public void notifyTxnApplied(MatViewRefreshTask task, long seqTxn) {
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
        final MatViewRefreshState state = refreshStateByTableDirName.get(viewTableToken.getDirName());
        // TODO(puzpuzpuz): state can be null???
        final MatViewRefreshTask task = taskHolder.get();
        task.baseTable = state.getViewDefinition().getMatViewToken();
        task.viewToken = viewTableToken;
        refreshTaskQueue.enqueue(task);
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
    private MatViewRefreshList getDependencyList(CharSequence tableName) {
        MatViewRefreshList state = dependantViewsByTableName.get(tableName);
        if (state == null) {
            // TODO(eugene): what if `MatViewRefreshList` will become Closable later?
            state = new MatViewRefreshList();
            // TODO(eugene): use computeIfAbsent ?
            MatViewRefreshList existingState = dependantViewsByTableName.putIfAbsent(tableName, state);
            return existingState != null ? existingState : state;
        }
        return state;
    }
}
