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

import io.questdb.cairo.TableToken;
import io.questdb.std.ConcurrentHashMap;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import io.questdb.std.SimpleReadWriteLock;

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
 * Whole-registry readers walk {@link #allViews} instead of a map view. A
 * {@code ConcurrentHashMap} iterator allocates one wrapper per step - and an
 * {@code entrySet()} one allocates a {@code Map.Entry} per view on top - which
 * the refresh pool's idle scan would charge to every sweep. Registration
 * rebuilds the snapshot instead, so the cost lands on DDL, which is rare, and
 * the recurring scan reads a plain {@link ObjList}.
 */
public class LiveViewRegistry implements QuietCloseable {
    private final Function<CharSequence, DepList> createDepList = name -> new DepList();
    private final ConcurrentHashMap<LiveViewInstance> viewsByName = new ConcurrentHashMap<>();
    // Key is the base table name. Entries are never removed (grow-only, bounded by
    // distinct base tables that ever had a live view registered).
    private final ConcurrentHashMap<DepList> viewsByBaseTable = new ConcurrentHashMap<>(false);
    /**
     * Every registered instance, republished as a fresh list on each registration
     * change. A reader takes the reference once and walks it without allocating;
     * a rebuild never mutates a list a reader may already hold, so the snapshot
     * a scan reads stays internally consistent even when a concurrent DDL
     * replaces it. Its staleness window matches the weakly consistent map
     * iterator it replaces.
     */
    private volatile ObjList<LiveViewInstance> allViews = new ObjList<>();

    @Override
    public void close() {
        clear();
    }

    public void clear() {
        final ObjList<LiveViewInstance> views = allViews;
        for (int i = 0, n = views.size(); i < n; i++) {
            Misc.free(views.getQuick(i));
        }
        viewsByName.clear();
        republishViews();
        for (DepList list : viewsByBaseTable.values()) {
            ObjList<LiveViewInstance> baseViews = list.lockForWrite();
            try {
                baseViews.clear();
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
        final ObjList<LiveViewInstance> views = allViews;
        for (int i = 0, n = views.size(); i < n; i++) {
            views.getQuick(i).discardSuspendedRepair();
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
        final ObjList<LiveViewInstance> views = allViews;
        for (int i = 0, n = views.size(); i < n; i++) {
            views.getQuick(i).freeSeedBaseReader();
        }
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
        final ObjList<LiveViewInstance> views = allViews;
        for (int i = 0, n = views.size(); i < n; i++) {
            final LiveViewInstance instance = views.getQuick(i);
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
        sink.addAll(allViews);
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
        republishViews();
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
        republishViews();
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
        if (instance != null) {
            republishViews();
        }
        return instance;
    }

    /**
     * Re-keys a registered view from {@code oldName} to {@code updatedToken}'s name, and
     * re-points the instance and its definition at the new token. Only the replication apply
     * path renames a live view: a downloaded view whose real name is still taken registers
     * under a pending temp name, and {@code CairoEngine.applyTableRename} moves it once the
     * name frees up. Without the re-key the instance stays reachable only under the dead name -
     * every later {@code getViewInstance(realName)} misses it, so a drop never tears it down
     * and {@code WalPurgeJob} keeps clamping the base WAL floor to its frozen watermark.
     * <p>
     * The name map is re-keyed under the base-table fan-out write lock, like
     * {@link #registerView} and {@link #removeView}, so a concurrent
     * {@link #getViewsForBaseTable} reader never observes the two maps torn apart. The fan-out
     * list holds instances, not names, so it needs no update.
     *
     * @return the renamed instance, or {@code null} when no view is registered under
     * {@code oldName}
     */
    public LiveViewInstance renameView(CharSequence oldName, TableToken updatedToken) {
        final LiveViewInstance instance = viewsByName.get(oldName);
        if (instance == null) {
            return null;
        }
        final LiveViewDefinition definition = instance.getDefinition();
        if (definition == null) {
            // A definition-less load-failure stub only ever lived in the name map
            // (registerStubView), so there is no fan-out list to lock.
            viewsByName.remove(oldName);
            instance.updateToken(updatedToken);
            viewsByName.put(updatedToken.getTableName(), instance);
            return instance;
        }
        // The snapshot holds instances rather than names, and a rename moves the
        // same instance from one key to another, so it needs no republication.
        final DepList list = viewsByBaseTable.computeIfAbsent(definition.getBaseTableName(), createDepList);
        list.lockForWrite();
        try {
            viewsByName.remove(oldName);
            instance.updateToken(updatedToken);
            definition.updateViewName(updatedToken.getTableName());
            viewsByName.put(updatedToken.getTableName(), instance);
        } finally {
            list.unlockAfterWrite();
        }
        return instance;
    }

    /**
     * Rebuilds the whole-registry snapshot from the name map. Registration,
     * removal and teardown call it; each publishes a new list rather than
     * mutating the one readers hold, so a scan already walking the previous
     * snapshot finishes over a stable view.
     */
    private synchronized void republishViews() {
        final ObjList<LiveViewInstance> rebuilt = new ObjList<>(viewsByName.size());
        for (LiveViewInstance instance : viewsByName.values()) {
            rebuilt.add(instance);
        }
        allViews = rebuilt;
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
