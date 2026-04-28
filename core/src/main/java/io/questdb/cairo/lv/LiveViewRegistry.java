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

package io.questdb.cairo.lv;

import io.questdb.std.ConcurrentHashMap;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import io.questdb.std.SimpleReadWriteLock;

import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.function.Function;

/**
 * Thread-safe registry of live view instances.
 * <p>
 * Two maps kept in sync: {@code viewsByName} for O(1) name lookup, and
 * {@code viewsByBaseTable} (grow-only) for O(1) base-table fan-out on the WAL
 * notification path and DDL invalidation paths. Both updates happen under the
 * per-base-table write lock so that refresh/invalidate readers never observe a
 * torn state.
 */
public class LiveViewRegistry implements QuietCloseable {
    private final Function<CharSequence, DepList> createDepList = name -> new DepList();
    private final ConcurrentHashMap<LiveViewInstance> viewsByName = new ConcurrentHashMap<>();
    // Key is the base table name. Entries are never removed (grow-only, bounded by
    // distinct base tables that ever had a live view registered).
    private final ConcurrentHashMap<DepList> viewsByBaseTable = new ConcurrentHashMap<>(false);

    @Override
    public void close() {
        clear();
    }

    public void clear() {
        for (Map.Entry<CharSequence, LiveViewInstance> entry : viewsByName.entrySet()) {
            Misc.free(entry.getValue());
        }
        viewsByName.clear();
        for (DepList list : viewsByBaseTable.values()) {
            ObjList<LiveViewInstance> views = list.lockForWrite();
            try {
                views.clear();
            } finally {
                list.unlockAfterWrite();
            }
        }
    }

    public LiveViewInstance getViewInstance(CharSequence name) {
        return viewsByName.get(name);
    }

    /**
     * Collects all live view instances into the given sink.
     */
    public void getViews(ObjList<LiveViewInstance> sink) {
        sink.clear();
        for (LiveViewInstance instance : viewsByName.values()) {
            sink.add(instance);
        }
    }

    /**
     * Collects all live view instances that depend on the given base table.
     * O(k) where k is the number of dependents — no full-registry scan.
     */
    public void getViewsForBaseTable(CharSequence baseTableName, ObjList<LiveViewInstance> sink) {
        sink.clear();
        DepList list = viewsByBaseTable.get(baseTableName);
        if (list == null) {
            return;
        }
        ObjList<LiveViewInstance> views = list.lockForRead();
        try {
            sink.addAll(views);
        } finally {
            list.unlockAfterRead();
        }
    }

    /**
     * Invalidates all live view instances that depend on the given base table.
     */
    public void invalidateViewsForBaseTable(CharSequence baseTableName, String reason) {
        DepList list = viewsByBaseTable.get(baseTableName);
        if (list == null) {
            return;
        }
        ObjList<LiveViewInstance> views = list.lockForRead();
        try {
            for (int i = 0, n = views.size(); i < n; i++) {
                views.getQuick(i).invalidate(reason);
            }
        } finally {
            list.unlockAfterRead();
        }
    }

    public boolean hasView(CharSequence name) {
        return viewsByName.get(name) != null;
    }

    public void registerView(LiveViewInstance instance) {
        viewsByName.put(instance.getDefinition().getViewName(), instance);
        DepList list = viewsByBaseTable.computeIfAbsent(instance.getDefinition().getBaseTableName(), createDepList);
        ObjList<LiveViewInstance> views = list.lockForWrite();
        try {
            views.add(instance);
        } finally {
            list.unlockAfterWrite();
        }
    }

    public LiveViewInstance removeView(CharSequence name) {
        LiveViewInstance instance = viewsByName.remove(name);
        if (instance != null) {
            DepList list = viewsByBaseTable.get(instance.getDefinition().getBaseTableName());
            if (list != null) {
                ObjList<LiveViewInstance> views = list.lockForWrite();
                try {
                    for (int i = 0, n = views.size(); i < n; i++) {
                        if (views.getQuick(i) == instance) {
                            views.remove(i);
                            break;
                        }
                    }
                } finally {
                    list.unlockAfterWrite();
                }
            }
        }
        return instance;
    }

    private static class DepList {
        private final ReadWriteLock lock = new SimpleReadWriteLock();
        private final ObjList<LiveViewInstance> views = new ObjList<>();

        ObjList<LiveViewInstance> lockForRead() {
            lock.readLock().lock();
            return views;
        }

        ObjList<LiveViewInstance> lockForWrite() {
            lock.writeLock().lock();
            return views;
        }

        void unlockAfterRead() {
            lock.readLock().unlock();
        }

        void unlockAfterWrite() {
            lock.writeLock().unlock();
        }
    }
}
