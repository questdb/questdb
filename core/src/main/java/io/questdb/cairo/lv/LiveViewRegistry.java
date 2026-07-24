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

import io.questdb.cairo.CairoConfiguration;
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
 * <p>
 * The registry also owns the engine-wide checkpoint page cache budget the
 * per-view caches allocate against. Its lifetime is exactly the registry's: the
 * instances holding those caches are the ones {@link #clear()} frees, so the
 * budget cannot outlive the memory charged to it.
 */
public class LiveViewRegistry implements QuietCloseable {
    private final LiveViewCheckpointPageCacheBudget checkpointPageCacheBudget;
    private final Function<CharSequence, DepList> createDepList = name -> new DepList();
    private final ConcurrentHashMap<LiveViewInstance> viewsByName = new ConcurrentHashMap<>();
    // Key is the base table name. Entries are never removed (grow-only, bounded by
    // distinct base tables that ever had a live view registered).
    private final ConcurrentHashMap<DepList> viewsByBaseTable = new ConcurrentHashMap<>(false);

    public LiveViewRegistry(CairoConfiguration configuration) {
        this.checkpointPageCacheBudget =
                new LiveViewCheckpointPageCacheBudget(configuration.getLiveViewCheckpointPageCacheMaxBytes());
    }

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

    /**
     * Abandons any localized out-of-order repair parked between refresh turns
     * across every registered view, releasing its pinned base reader, its
     * live-view writer and its staged data segment. Called during engine teardown
     * before those pools are freed, so a repair that yielded mid-run leaves
     * nothing borrowed behind. Must run after the refresh workers have stopped
     * (no concurrent turn can resume it).
     */
    public void discardSuspendedRepairs() {
        for (LiveViewInstance instance : viewsByName.values()) {
            instance.discardSuspendedRepair();
        }
    }

    /**
     * Releases any base-table reader pinned by an in-flight seed sweep across
     * every registered view. Called during engine teardown before the reader pool
     * is freed, so a sweep that yielded mid-run does not leave its borrowed base
     * reader behind when the pool closes. Must run after the refresh workers have
     * stopped (no concurrent sweep turn).
     */
    public void freeSeedBaseReaders() {
        for (LiveViewInstance instance : viewsByName.values()) {
            instance.freeSeedBaseReader();
        }
    }

    /**
     * @return the engine-wide ceiling every view's checkpoint page cache
     * allocates against. Shared, so N views cannot each take the configured cap
     */
    public LiveViewCheckpointPageCacheBudget getCheckpointPageCacheBudget() {
        return checkpointPageCacheBudget;
    }

    /**
     * Collects only the live view instances this worker owns in the idle-scan shard into
     * {@code sink}. Each worker still walks the registry, but copies (and hence has
     * {@code scanForLaggingViews} process) only its own shard, so per sweep the pool copies and
     * scans each view once in total (O(views)) instead of every worker copying every view and
     * discarding the non-owned ones afterwards (O(workers * views) copies). The shard predicate
     * mirrors {@link LiveViewRefreshJob#ownsViewShard(int)}: a pool of one ({@code workerCount <= 1})
     * owns every view. Table ids are stable per view, so ownership never drifts between sweeps.
     */
    public void getShardedViews(ObjList<LiveViewInstance> sink, int workerId, int workerCount) {
        sink.clear();
        for (LiveViewInstance instance : viewsByName.values()) {
            if (workerCount <= 1 || Math.floorMod(instance.getLiveViewToken().getTableId(), workerCount) == workerId) {
                sink.add(instance);
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

    public boolean hasView(CharSequence name) {
        return viewsByName.get(name) != null;
    }

    public void registerView(LiveViewInstance instance) {
        DepList list = viewsByBaseTable.computeIfAbsent(instance.getDefinition().getBaseTableName(), createDepList);
        ObjList<LiveViewInstance> views = list.lockForWrite();
        try {
            // Publish the name entry under the fan-out list's write lock so a
            // concurrent getViewsForBaseTable reader never sees one map but not the
            // other (matches the class contract).
            viewsByName.put(instance.getDefinition().getViewName(), instance);
            views.add(instance);
        } finally {
            list.unlockAfterWrite();
        }
    }

    /**
     * Registers a definition-less stub for a view the load path could not fully
     * load (a too-new format version, or a torn / corrupt state file). Such an
     * instance has no resolvable base table (its {@code _lv} / {@code _lv.s} could
     * not be read), so it lives only in {@code viewsByName} for catalogue visibility
     * and droppability, and is not added to the base-table fan-out index.
     */
    public void registerStubView(LiveViewInstance instance) {
        viewsByName.put(instance.getLiveViewToken().getTableName(), instance);
    }

    public LiveViewInstance removeView(CharSequence name) {
        LiveViewInstance instance = viewsByName.remove(name);
        // A version-unsupported stub has a null definition and was never added to
        // the base-table fan-out index, so skip that cleanup for it.
        if (instance != null && instance.getDefinition() != null) {
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
