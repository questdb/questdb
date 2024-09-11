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
import io.questdb.std.ConcurrentHashMap;
import io.questdb.std.ObjList;
import io.questdb.std.SimpleReadWriteLock;

import java.util.concurrent.locks.ReadWriteLock;

public class MatViewGraph {
    private final ConcurrentHashMap<DependentMatViewList> dependantViewsByTableName = new ConcurrentHashMap<>();

    public synchronized void getAffectedViews(TableToken table, AffectedMatViewsSink sink) {
        DependentMatViewList list = dependantViewsByTableName.get(table.getTableName());
        if (list != null) {
            // TODO: lock/unlock instead of sycnronized
            sink.viewsList.addAll(list.matViews);
        }
    }

    public synchronized void getAllParents(ObjList<CharSequence> sink) {
        for (CharSequence key : dependantViewsByTableName.keySet()) {
            DependentMatViewList list = dependantViewsByTableName.get(key);
            if (list.matViews.size() > 0) {
                sink.add(key);
            }
        }
    }

    public synchronized void upsertView(TableToken parent, MaterializedViewDefinition viewDefinition) {
        DependentMatViewList list = dependantViewsByTableName.get(parent.getTableName());
        if (list != null) {
            // TODO: lock/unlock instead of sycnronized
            for (int i = 0, size = list.matViews.size(); i < size; i++) {
                MaterializedViewDefinition existingView = list.matViews.get(0);
                if (existingView.getTableToken().equals(viewDefinition.getTableToken())) {
                    list.matViews.set(i, viewDefinition);
                    return;
                }
            }
            list.matViews.add(viewDefinition);
        } else {
            list = new DependentMatViewList();
            list.matViews.add(viewDefinition);
            dependantViewsByTableName.put(parent.getTableName(), list);
        }
    }

    public static class AffectedMatViewsSink {
        public ObjList<MaterializedViewDefinition> viewsList = new ObjList<>();

    }

    private static class DependentMatViewList {
        private final ReadWriteLock lock = new SimpleReadWriteLock();
        ObjList<MaterializedViewDefinition> matViews = new ObjList<>();
        private long lastCommitWallClockMicro;
        private long lastParentTxnSeq;

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
