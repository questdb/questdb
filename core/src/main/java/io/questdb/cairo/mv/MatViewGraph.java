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
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.std.*;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.locks.ReadWriteLock;

public class MatViewGraph implements QuietCloseable {
    private final ConcurrentHashMap<DependentMatViewState> dependantViewsByTableName = new ConcurrentHashMap<>();

    @Override
    public void close() {
        for (DependentMatViewState state : dependantViewsByTableName.values()) {
            try {
                state.writeLock();
                Misc.free(state);
            } finally {
                state.unlockWrite();
            }
        }
        dependantViewsByTableName.clear();
    }

    public void getAffectedViews(TableToken table, AffectedMatViewsSink sink) {
        DependentMatViewState list = getState(table);
        try {
            list.readLock();
            sink.viewsList.addAll(list.matViews);
        } finally {
            list.unlockRead();
        }
    }

    public void getAllBaseTables(ObjList<CharSequence> sink) {
        for (DependentMatViewState state : dependantViewsByTableName.values()) {
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

    public MaterializedViewRefreshState getViewRefreshState(TableToken tableToken) {
        DependentMatViewState state = getState(tableToken);
        return state.getRefreshState();
    }

    public void upsertView(TableToken base, MaterializedViewDefinition viewDefinition) {
        DependentMatViewState state = getState(base);
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

    public void upsertViewRefreshState(TableToken viewTableToken, RecordCursorFactory cursorFactory) {
        DependentMatViewState state = getState(viewTableToken);
        try {
            state.writeLock();
            state.updateStateSuccess(cursorFactory);
        } finally {
            state.unlockWrite();
        }
    }

    public void upsertViewRefreshStateError(TableToken viewTableToken, CharSequence updateError) {
        DependentMatViewState state = getState(viewTableToken);
        try {
            state.writeLock();
            state.updateStateError(updateError);
        } finally {
            state.unlockWrite();
        }
    }

    @NotNull
    private DependentMatViewState getState(TableToken tableToken) {
        return getState(tableToken.getDirName());
    }

    @NotNull
    private DependentMatViewState getState(CharSequence tableDirName) {
        DependentMatViewState state = dependantViewsByTableName.get(tableDirName);
        if (state == null) {
            state = new DependentMatViewState();
            DependentMatViewState existingState = dependantViewsByTableName.putIfAbsent(tableDirName, state);
            return existingState != null ? existingState : state;
        }
        return state;
    }

    public static class AffectedMatViewsSink {
        public ObjList<MaterializedViewDefinition> viewsList = new ObjList<>();

    }

    private static class DependentMatViewState implements QuietCloseable {
        private final ReadWriteLock lock = new SimpleReadWriteLock();
        ObjList<MaterializedViewDefinition> matViews = new ObjList<>();
        private final MaterializedViewRefreshState refreshState = new MaterializedViewRefreshState();
        private RecordCursorFactory cursorFactory;
        private long lastCommitWallClockMicro;
        private long lastBaseTxnSeq;
        private StringSink lastUpdateError;

        @Override
        public void close() {
            cursorFactory = Misc.free(cursorFactory);
        }

        public MaterializedViewRefreshState getRefreshState() {
            return refreshState;
        }

        public void updateStateError(CharSequence updateError) {
            if (lastUpdateError == null) {
                lastUpdateError = new StringSink();
            }
            lastUpdateError.clear();
            lastUpdateError.put(updateError);
            this.cursorFactory = Misc.free(cursorFactory);
        }

        public void updateStateSuccess(RecordCursorFactory cursorFactory) {
            this.cursorFactory = cursorFactory;
            if (lastUpdateError != null) {
                lastUpdateError.clear();
            }
        }

        void readLock() {
            lock.readLock().lock();
        }

        void unlockRead() {
            lock.readLock().unlock();
        }

        void unlockWrite() {
            lock.writeLock().unlock();
        }

        void writeLock() {
            lock.writeLock().lock();
        }
    }
}
