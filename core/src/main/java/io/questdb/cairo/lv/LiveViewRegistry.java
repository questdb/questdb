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
     * Publishes {@code instance} under its view name only when the name is currently
     * unowned - the CAS publication shape {@code TableNameRegistryRW.registerName} gives
     * table tokens. A late publisher (a recovery that stalled across a drop-and-recreate
     * of the same name) is refused instead of silently displacing the replacement
     * generation's entry, which a bare {@link #registerView} put would overwrite. Both
     * maps are updated under the fan-out write lock, like {@link #registerView}, so
     * concurrent readers never observe them torn apart.
     *
     * @return {@code null} when {@code instance} was published, or the current owner
     * when the name is taken and nothing was changed
     */
    public LiveViewInstance registerViewIfAbsent(LiveViewInstance instance) {
        DepList list = viewsByBaseTable.computeIfAbsent(instance.getDefinition().getBaseTableName(), createDepList);
        ObjList<LiveViewInstance> views = list.lockForWrite();
        try {
            final LiveViewInstance owner = viewsByName.putIfAbsent(instance.getDefinition().getViewName(), instance);
            if (owner == null) {
                views.add(instance);
            }
            return owner;
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

    /**
     * Removes the name entry only when {@code expected} still owns it, mirroring
     * {@code TableNameRegistryRW.dropTable}'s expected-value removal
     * ({@code map.replace(name, token, LOCKED_DROP_TOKEN)}): a rollback or replicated
     * drop that runs arbitrarily late cannot take out a same-name entry another
     * generation published in the meantime. When {@code expected} does own the name,
     * its fan-out entry is removed alongside under the same write lock, exactly as
     * {@link #removeView(CharSequence)} would.
     *
     * @return {@code true} when {@code expected} owned the name and was removed
     */
    public boolean removeView(CharSequence name, LiveViewInstance expected) {
        final LiveViewDefinition definition = expected.getDefinition();
        if (definition == null) {
            // A definition-less stub only ever lived in the name map (registerStubView),
            // so there is no fan-out list to lock or clean.
            return viewsByName.remove(name, expected);
        }
        DepList list = viewsByBaseTable.computeIfAbsent(definition.getBaseTableName(), createDepList);
        ObjList<LiveViewInstance> views = list.lockForWrite();
        try {
            if (!viewsByName.remove(name, expected)) {
                return false;
            }
            for (int i = 0, n = views.size(); i < n; i++) {
                if (views.getQuick(i) == expected) {
                    views.remove(i);
                    break;
                }
            }
            return true;
        } finally {
            list.unlockAfterWrite();
        }
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
