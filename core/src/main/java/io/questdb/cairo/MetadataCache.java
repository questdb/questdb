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

package io.questdb.cairo;

import io.questdb.TelemetryConfigLogger;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMR;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.log.LogRecord;
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.CharSequenceObjMap;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.FlyweightMessageContainer;
import io.questdb.std.IntList;
import io.questdb.std.IntLongHashMap;
import io.questdb.std.LongHashSet;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjHashSet;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import io.questdb.std.SimpleReadWriteLock;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.tasks.TelemetryTask;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.util.Comparator;

/**
 * A metadata cache for serving SQL requests for basic table info.
 */
public class MetadataCache implements QuietCloseable {
    private static final Log LOG = LogFactory.getLog(MetadataCache.class);
    // Upper bound on the number of consecutive zero-progress catalogue reconcile passes
    // (a pass that still finds a table missing yet hydrates none of the missing tables)
    // before {@link #hydrateAllTables()} stops retrying and latches {@code cacheComplete}
    // anyway. Without this cap a table whose {@code _meta} cannot be read (corrupt, or
    // transiently unavailable at startup) would be re-read - and logged as CRITICAL - on
    // every single catalogue query, since a failed hydration never lands in the cache and
    // so always looks "missing". The cap trades the (already master-equivalent) hiding of
    // a genuinely unhydratable table for bounded work and log output, while still giving
    // transient failures several passes to self-heal: any pass that hydrates at least one
    // missing table resets the budget, so a post-restart storm making incremental
    // progress (e.g. a PG-introspection burst racing the startup hydrator) cannot exhaust
    // it in a single concurrent burst. A writer touching the table still re-hydrates it,
    // and clearCache() resets the counter so a fresh epoch reconciles again.
    private static final int MAX_INCOMPLETE_RECONCILE_PASSES = 8;
    private static final Comparator<CairoColumn> comparator = Comparator.comparingInt(CairoColumn::getPosition);
    private final MetadataCacheReaderImpl cacheReader = new MetadataCacheReaderImpl();
    private final MetadataCacheWriterImpl cacheWriter = new MetadataCacheWriterImpl();
    private final CairoEngine engine;
    private final SimpleReadWriteLock rwLock = new SimpleReadWriteLock();
    private final CharSequenceObjHashMap<CairoTable> tableMap = new CharSequenceObjHashMap<>();
    private ColumnVersionReader columnVersionReader;
    // Counts consecutive catalogue reconcile passes that made no progress - i.e. that
    // still found a table missing AND hydrated none of the previously-missing tables. A
    // pass that hydrates at least one missing table resets it (the cache is self-healing),
    // so it counts only genuinely-stuck rounds rather than every concurrent invocation.
    // Bounded by MAX_INCOMPLETE_RECONCILE_PASSES; reset by clearCache(). Only mutated
    // under the write lock (by the give-up logic in hydrateAllTables() and by
    // clearCache()), so it stays in step with cacheComplete and a concurrent clear cannot
    // race the budgeted latch.
    private int incompleteReconcilePasses;
    private MemoryCMR metaMem = Vm.getCMRInstance();
    // True once the cache is known to hold every registered (non-view) table. Set by
    // the startup hydrator only when it actually hydrated every table, and by any
    // catalogue reconcile that observes the cache as already complete (or that gives
    // up after MAX_INCOMPLETE_RECONCILE_PASSES); from then on writers keep the cache
    // in sync incrementally, so hydrateAllTables() short-circuits. Reset by
    // clearCache() so a wiped cache is reconciled afresh.
    private volatile boolean cacheComplete;
    // Bumped by clearCache() on every wipe. hydrateAllTables() snapshots it before
    // scanning, and assessReconcileGiveUp() refuses to spend the give-up budget when it
    // moved - a concurrent clear, not an unreadable _meta, made that pass look stuck.
    private volatile long clearEpoch;
    private TxReader txReader;
    private long version;
    // Row-expiry trust gate. Before complete hydration, lookups stay conservative. Afterwards one immutable
    // snapshot authoritatively contains both active policy IDs (for lock-free read gating) and active table
    // entries (for cleanup deadline discovery). Metadata updates rebuild it once; startup publishes one batch.
    private volatile ExpiryPolicySnapshot expiryPolicySnapshot = ExpiryPolicySnapshot.EMPTY;
    private volatile boolean fullyHydrated = false;
    private final Object expiryPolicySnapshotLock = new Object();
    private final LongHashSet pendingExpiryPolicyIds = new LongHashSet();
    private final IntLongHashMap pendingExpiryPolicyVersions = new IntLongHashMap();
    private volatile long expiryPolicyVersion;

    public MetadataCache(CairoEngine engine) {
        this.engine = engine;
    }

    @Override
    public void close() {
        metaMem = Misc.free(metaMem);
        txReader = Misc.free(txReader);
        tableMap.clear();
    }

    /**
     * Used on a background thread at startup to populate the cache.
     * Cache is also populated on-demand by SQL metadata functions.
     * Takes a lock per table to not prevent the ongoing progress of the database.
     * Generally completes quickly.
     */
    public void onStartupAsyncHydrator() {
        try {
            final ObjHashSet<TableToken> tableTokensSet = new ObjHashSet<>();
            engine.getTableTokens(tableTokensSet, false);
            final ObjList<TableToken> tableTokens = tableTokensSet.getList();

            LOG.info().$("metadata hydration started [tables=").$(tableTokens.size()).I$();
            for (int i = 0, n = tableTokens.size(); i < n; i++) {
                // acquire a write lock for each table
                try (MetadataCacheWriter ignore = writeLock()) {
                    hydrateTableStartup(tableTokens.getQuick(i), false, true);
                }
            }

            // Verify completeness and publish the flag under a single read lock. The
            // check-and-set is then mutually exclusive with clearCache() (write lock),
            // so a concurrent clear cannot slip between observing completeness and
            // latching the flag and leave an emptied cache marked complete.
            //
            // We latch only if every registered table is actually present: a per-table
            // hydration can fail silently (hydrateTableStartup swallows it with
            // throwError=false and evicts the table), and a swallowed failure must leave
            // the flag unset so the catalogue reconcile (hydrateAllTables) keeps retrying
            // - self-healing transient failures - until it succeeds or exhausts its
            // retry budget. This mirrors the contract honoured by hydrateAllTables()'s
            // second pass, which likewise never latches a flag it cannot back up. The
            // flag is also left unset on an abnormal abort below (e.g. getTableTokens()/
            // lock failure).
            try (MetadataCacheReader metadataRO = readLock()) {
                LOG.info().$("metadata hydration completed [tables=").$(metadataRO.getTableCount()).I$();
                latchCacheCompleteIfWarmed(tableTokens);
            }
        } catch (CairoException e) {
            LogRecord l = e.isCritical() ? LOG.critical() : LOG.error();
            l.$safe(e.getFlyweightMessage()).$();
        } finally {
            Path.clearThreadLocals();
        }
    }

    /**
     * Cancels a pending EXPIRE ROWS metadata transition after the table writer failed to publish it. The cache
     * write lock protects the active-table scan while the rebuilt snapshot removes only this transition's
     * conservative pending mark.
     */
    public void cancelExpiryPolicyUpdate(int tableId) {
        try (MetadataCacheWriter ignore = writeLock()) {
            if (clearPendingExpiryPolicy(tableId)) {
                publishActiveExpiryPolicySnapshot();
            }
        }
    }

    /**
     * Cancels one unpublished transition without clearing a newer or previously retained pending transition.
     */
    public void cancelExpiryPolicyUpdate(int tableId, long updateVersion, long previousPendingVersion) {
        try (MetadataCacheWriter ignore = writeLock()) {
            synchronized (expiryPolicySnapshotLock) {
                if (pendingExpiryPolicyVersions.get(tableId) != updateVersion) {
                    return;
                }
                if (previousPendingVersion > -1 && !isExpiryPolicyVersionCached(tableId, previousPendingVersion)) {
                    pendingExpiryPolicyVersions.put(tableId, previousPendingVersion);
                } else {
                    pendingExpiryPolicyIds.remove(tableId);
                    pendingExpiryPolicyVersions.remove(tableId);
                }
            }
            publishActiveExpiryPolicySnapshot();
        }
    }

    /**
     * Identity of the published expiry-policy snapshot. A republish constructs a new snapshot
     * object, so tests can assert whether an operation republished without exposing the contents.
     */
    @TestOnly
    public Object getExpiryPolicySnapshotIdentity() {
        return expiryPolicySnapshot;
    }

    public long getExpiryPolicyVersion() {
        return expiryPolicyVersion;
    }

    /**
     * Returns whether this table has an EXPIRE ROWS metadata transition in progress. The immutable snapshot
     * makes this a lock-free check on the SQL parse path.
     */
    public boolean isExpiryPolicyUpdatePending(TableToken tableToken) {
        return expiryPolicySnapshot.pendingTableIds.contains(tableToken.getTableId());
    }

    /**
     * Marks any SET or DROP EXPIRE transition before the writer mutates metadata. The mark opens the read-filter
     * gate and makes SQL use authoritative metadata until hydration publishes the new cache entry. Replacement
     * SET and DROP must also create a pending mark even though the active-policy ID already exists.
     */
    public void markExpiryPolicyPossible(int tableId) {
        markExpiryPolicyPossible(tableId, 0);
    }

    /**
     * Marks a transition to the given metadata version and returns the target of any superseded pending mark.
     */
    public long markExpiryPolicyPossible(int tableId, long updateVersion) {
        synchronized (expiryPolicySnapshotLock) {
            final long previousPendingVersion = pendingExpiryPolicyVersions.get(tableId);
            pendingExpiryPolicyIds.add(tableId);
            pendingExpiryPolicyVersions.put(tableId, updateVersion);
            final ExpiryPolicySnapshot current = expiryPolicySnapshot;
            final LongHashSet ids;
            if (current.tableIds.contains(tableId)) {
                ids = current.tableIds;
            } else {
                ids = copyTableIds(current.tableIds, 1);
                ids.add(tableId);
            }
            // Publish the pending IDs independently from active IDs: the parser must distinguish an active,
            // cache-resident policy from a transition whose policy must come from authoritative metadata.
            expiryPolicySnapshot = new ExpiryPolicySnapshot(
                    ids,
                    copyTableIds(pendingExpiryPolicyIds, 0),
                    copyActiveExpiryTablesExcluding(current.tables, tableId)
            );
            expiryPolicyVersion++;
            return previousPendingVersion;
        }
    }

    /**
     * Whether any table may carry an EXPIRE ROWS policy. Returns true while the cache is still
     * hydrating at startup (so the read filter is never skipped before a policy becomes visible), and
     * thereafter true only once a policied table has been cached. Lets the read filter and the cleanup
     * discovery skip their per-table work on databases that never use EXPIRE ROWS.
     */
    public boolean mayHaveExpiryPolicy() {
        return !fullyHydrated || expiryPolicySnapshot.tableIds.size() > 0;
    }

    /**
     * Per-table refinement of {@link #mayHaveExpiryPolicy()}: whether THIS table may carry an EXPIRE ROWS
     * policy. Conservatively true while hydrating (the id set is not yet complete); afterwards true only
     * for tables whose id is in the policied set. Lock-free - a volatile read of an immutable snapshot -
     * so the read filter can skip the cache read lock + predicate lookup for the (typically many)
     * non-policied tables once some unrelated table has a policy.
     */
    public boolean mayTableHaveExpiryPolicy(TableToken tableToken) {
        return !fullyHydrated || expiryPolicySnapshot.tableIds.contains(tableToken.getTableId());
    }

    /**
     * Advances the policy epoch immediately after _meta/_txn publish a pending transition. Compilers use the
     * epoch to reject a parse decision made against the previous authoritative metadata version.
     */
    public void publishExpiryPolicyUpdate() {
        synchronized (expiryPolicySnapshotLock) {
            expiryPolicyVersion++;
        }
    }

    private boolean clearPendingExpiryPolicy(int tableId) {
        synchronized (expiryPolicySnapshotLock) {
            pendingExpiryPolicyVersions.remove(tableId);
            return pendingExpiryPolicyIds.remove(tableId) > -1;
        }
    }

    private boolean clearPendingExpiryPolicy(int tableId, long hydratedMetadataVersion) {
        synchronized (expiryPolicySnapshotLock) {
            final long updateVersion = pendingExpiryPolicyVersions.get(tableId);
            if (updateVersion < 0 || hydratedMetadataVersion < updateVersion) {
                return false;
            }
            pendingExpiryPolicyVersions.remove(tableId);
            return pendingExpiryPolicyIds.remove(tableId) > -1;
        }
    }

    private static ObjList<CairoTable> copyActiveExpiryTablesExcluding(ObjList<CairoTable> current, int tableId) {
        final ObjList<CairoTable> copy = new ObjList<>(current.size());
        for (int i = 0, n = current.size(); i < n; i++) {
            final CairoTable table = current.getQuick(i);
            if (table != null && table.getTableToken().getTableId() != tableId) {
                copy.add(table);
            }
        }
        return copy;
    }

    private static LongHashSet copyTableIds(LongHashSet current, int extraCapacity) {
        final LongHashSet copy = new LongHashSet(current.size() + extraCapacity);
        for (int i = 0, n = current.size(); i < n; i++) {
            copy.add(current.get(i));
        }
        return copy;
    }

    private boolean isExpiryPolicyVersionCached(int tableId, long metadataVersion) {
        for (int i = 0, n = tableMap.size(); i < n; i++) {
            final CairoTable table = tableMap.getAt(i);
            if (table != null
                    && table.getTableToken().getTableId() == tableId
                    && table.getMetadataVersion() >= metadataVersion) {
                return true;
            }
        }
        return false;
    }

    /**
     * Ensures that every table currently registered with the engine is present
     * in the cache, hydrating on demand any tables that the background startup
     * hydrator ({@link #onStartupAsyncHydrator()}) has not processed yet.
     * <p>
     * Catalogue queries such as {@code tables()} and {@code all_tables()} read a
     * {@link MetadataCacheReader#snapshot snapshot} of the cache, which only
     * contains tables that have already been hydrated. Without this
     * reconciliation those queries would race the async startup hydrator and
     * could return an incomplete catalogue immediately after a restart. Calling
     * this first observes the complete set of tables whenever every table can be
     * hydrated. A table whose {@code _meta} cannot be read (corrupt, or transiently
     * unavailable) cannot be hydrated and is omitted - the same outcome as on a
     * release without this reconcile - until a writer next touches it or the process
     * restarts; see {@link #MAX_INCOMPLETE_RECONCILE_PASSES}.
     * <p>
     * The reconcile is only needed until the cache is known to be complete. Once
     * {@link #onStartupAsyncHydrator()} hydrates every table, a reconcile pass observes
     * the cache already holding every registered table, or the reconcile gives up after
     * {@link #MAX_INCOMPLETE_RECONCILE_PASSES} passes still find a table missing, writers
     * keep the cache in sync incrementally, so this method short-circuits on a single
     * volatile read and adds no per-query overhead (no allocation, no lock). This
     * matters for embedded engines, which never run the startup hydrator: without it
     * every catalogue query would reconcile forever. {@link MetadataCacheWriter#clearCache()}
     * resets the state so a wiped cache is reconciled afresh.
     */
    public void hydrateAllTables() {
        // Fast path: once the cache is known complete it is kept current by writers,
        // so no reconcile (or allocation/lock) is needed.
        if (cacheComplete) {
            return;
        }
        // Snapshot the clear epoch before scanning: if clearCache() bumps it before we
        // reach the give-up path below, a concurrent wipe (not an unhydratable table) is
        // what made this pass look stuck, so it must not spend the give-up budget.
        final long startEpoch = clearEpoch;
        final ObjHashSet<TableToken> tableTokensSet = new ObjHashSet<>();
        engine.getTableTokens(tableTokensSet, false);
        final ObjList<TableToken> tableTokens = tableTokensSet.getList();

        // First pass: under a single read lock, collect tables that exist in the
        // registry but are missing from the cache.
        ObjList<TableToken> missing = null;
        try (MetadataCacheReader ignore = readLock()) {
            for (int i = 0, n = tableTokens.size(); i < n; i++) {
                final TableToken token = tableTokens.getQuick(i);
                // views are never stored in the cache (they have no _meta file),
                // skip them so we don't take a write lock on every invocation
                if (token.isView()) {
                    continue;
                }
                if (tableMap.get(token.getTableName()) == null) {
                    if (missing == null) {
                        missing = new ObjList<>();
                    }
                    missing.add(token);
                }
            }
            if (missing == null) {
                // Nothing missing: publish both completeness and the expiry trust gate while still under the
                // read lock, mutually exclusive with clearCache() (write lock).
                latchCacheCompleteIfWarmed(tableTokens);
            }
        }

        if (missing == null) {
            // The cache already holds every registered table; the flag was latched
            // inside the read lock above (mutually exclusive with clearCache()). From
            // here writers keep it in sync incrementally, so future catalogue queries
            // short-circuit on the fast path above. This is what spares embedded engines
            // (which never run the startup hydrator) a per-query reconcile once warmed
            // up; the flag is cleared by clearCache() so a wiped cache is reconciled
            // again.
            return;
        }

        // Second pass: hydrate the missing tables. Take the write lock per table
        // (matching onStartupAsyncHydrator) so we do not stall ongoing work.
        try {
            for (int i = 0, n = missing.size(); i < n; i++) {
                try (MetadataCacheWriter ignore = writeLock()) {
                    hydrateTableStartup(missing.getQuick(i), false, true);
                }
            }
        } finally {
            // hydrateTableStartup() acquires a thread-local Path; release it so a caller
            // running on a short-lived (non-pooled) thread does not leak the
            // NATIVE_PATH_THREAD_LOCAL buffer when the thread terminates. Mirrors
            // onStartupAsyncHydrator(). Only reached on the hydration slow path; once the
            // cache is warm hydrateAllTables() short-circuits before allocating.
            Path.clearThreadLocals();
        }

        // Confirm completeness immediately, reusing the token snapshot we already
        // collected (no second getTableTokens() / ObjHashSet allocation): if every
        // missing table was hydrated above, the cache now holds every registered table.
        // Latch here - under the read lock, mutually exclusive with clearCache() (write
        // lock), exactly like onStartupAsyncHydrator() - so the next catalogue query
        // short-circuits on the fast path instead of repeating a full reconcile only to
        // observe "nothing missing" and set the flag. A silently-failed hydration
        // (throwError=false evicts the table) leaves a gap, so latchCacheCompleteIfWarmed()
        // latches only when genuinely warm; otherwise we fall through to the bounded retry
        // budget below.
        try (MetadataCacheReader ignore = readLock()) {
            latchCacheCompleteIfWarmed(tableTokens);
        }
        if (cacheComplete) {
            return;
        }

        // We could not confirm completeness this pass (some tables were missing):
        // fall through to the bounded give-up assessment.
        assessReconcileGiveUp(missing, startEpoch);
    }

    /**
     * Assesses the give-up budget after a {@link #hydrateAllTables()} pass that could
     * not confirm completeness. A per-table hydration can fail silently
     * (throwError=false), so normally the pass leaves {@link #cacheComplete} unset and
     * the next reconcile re-scans - that is how a transient failure self-heals. But a
     * table whose {@code _meta} is genuinely unreadable would otherwise be re-read (and
     * logged CRITICAL) on every catalogue query forever. Bound that: after
     * {@link #MAX_INCOMPLETE_RECONCILE_PASSES} consecutive genuinely-stuck passes, give
     * up and latch the flag, leaving the unhydratable table(s) to be picked up by a
     * writer (hydrateTable/registerName) or the next clearCache() epoch.
     * <p>
     * Only genuinely-stuck rounds spend the budget:
     * <ul>
     * <li>a pass that hydrated at least one previously-missing table resets the counter
     * - the cache is self-healing, and the budget must be a function of sequential
     * stuck rounds, not of the number of concurrent invocations (e.g. a post-restart
     * introspection storm racing the startup hydrator);</li>
     * <li>a pass whose reconcile overlapped a {@link MetadataCacheWriter#clearCache()}
     * ({@code clearEpoch} moved past {@code startEpoch}) is ignored: the wipe - not an
     * unreadable {@code _meta} - made the pass look stuck, and spending the budget
     * could latch {@link #cacheComplete} over a merely-emptied, still-hydratable cache.
     * The clear already reset the counter; a later clear-free reconcile latches
     * correctly (all tables present) or gives up (a table is truly unreadable).</li>
     * </ul>
     * The progress check, budget bump and latch run under the write lock, mutually
     * exclusive with clearCache() (which resets both the counter and the flag under
     * the same lock); otherwise a concurrent clear could slip between the budget check
     * and the latch and leave an emptied cache marked complete.
     * <p>
     * Called while holding no lock. Protected so tests can subclass the cache and
     * deterministically interleave concurrent operations (e.g. a clearCache()) right
     * before the assessment.
     */
    protected void assessReconcileGiveUp(ObjList<TableToken> missing, long startEpoch) {
        final boolean gaveUp;
        try (MetadataCacheWriter ignore = writeLock()) {
            boolean progressed = false;
            for (int i = 0, n = missing.size(); i < n; i++) {
                if (tableMap.get(missing.getQuick(i).getTableName()) != null) {
                    progressed = true;
                    break;
                }
            }
            if (progressed) {
                // Self-healing round: do not spend the give-up budget; the next
                // reconcile re-scans for whatever is still missing.
                incompleteReconcilePasses = 0;
                gaveUp = false;
            } else if (clearEpoch != startEpoch) {
                // A clearCache() overlapped this reconcile - not a genuinely stuck
                // round, so do not spend the budget (see javadoc).
                gaveUp = false;
            } else {
                gaveUp = ++incompleteReconcilePasses >= MAX_INCOMPLETE_RECONCILE_PASSES;
                if (gaveUp) {
                    publishCacheComplete();
                    // Policied views that DID hydrate must still reach the cleanup job: its discovery
                    // reads only the published snapshot, and once cacheComplete short-circuits
                    // hydrateAllTables() no later pass would ever publish one — physical cleanup
                    // would silently stall until a restart or clearCache(). fullyHydrated stays
                    // false (tables are genuinely missing), so reads keep the conservative
                    // per-table policy gate.
                    publishActiveExpiryPolicySnapshot();
                }
            }
        }
        if (gaveUp) {
            LOG.error().$("catalogue reconcile giving up after repeated incomplete passes [passes=")
                    .$(MAX_INCOMPLETE_RECONCILE_PASSES).$(", missing=").$(missing.size()).I$();
        }
    }

    /**
     * Ensures a single registered table is present in the cache, hydrating it on demand
     * if it is missing. This is the point-lookup analogue of {@link #hydrateAllTables()}
     * for callers that resolve a {@link TableToken} from the synchronously loaded table
     * registry but then read the lazily hydrated cache - e.g. {@code SHOW COLUMNS},
     * {@code SHOW CREATE TABLE}, {@code SHOW CREATE MATERIALIZED VIEW} and parquet
     * partition-format probes. Without it those paths would report a registered table
     * as non-existent (or skip parquet pruning) during the startup hydration window, or
     * indefinitely for an embedded engine that never runs the startup hydrator and has
     * not yet had its cache warmed by an enumerating catalogue query.
     * <p>
     * Best-effort: a per-table hydration failure is swallowed (mirrors
     * {@link #hydrateAllTables()}), leaving the table absent so the caller reports it as
     * missing rather than failing hard. Plain views are never cached (no {@code _meta}
     * file) and are skipped. Once {@link #cacheComplete} is latched the cache is kept
     * current by writers, so a still-absent table is genuinely gone (dropped) or was
     * given up on as unhydratable; re-reading it on every point lookup would only add
     * load and CRITICAL-log noise, so this short-circuits.
     */
    public void hydrateTableOnDemand(@Nullable TableToken token) {
        if (token == null || token.isView()) {
            return;
        }
        if (cacheComplete) {
            return;
        }
        // Cheap check under the read lock first: skip the version-bumping write lock
        // when the table is already cached (the common case once the cache is warm).
        try (MetadataCacheReader ignore = readLock()) {
            if (tableMap.get(token.getTableName()) != null) {
                return;
            }
        }
        try (MetadataCacheWriter ignore = writeLock()) {
            // skipIfCached=true: another thread may have hydrated it between the two
            // locks. throwError=false: swallow a per-table failure so the caller reports
            // the table as missing rather than failing hard (mirrors hydrateAllTables()).
            hydrateTableStartup(token, false, true);
        } finally {
            // hydrateTableStartup() acquires a thread-local Path; release it so a caller
            // running on a short-lived (non-pooled) thread does not leak the
            // NATIVE_PATH_THREAD_LOCAL buffer when the thread terminates. Mirrors
            // onStartupAsyncHydrator(); only reached on the hydration slow path, since the
            // cacheComplete / already-cached checks above short-circuit once warm.
            Path.clearThreadLocals();
        }
    }

    /**
     * Latches {@link #cacheComplete} iff every registered (non-view) table is already
     * present in the cache. The caller must hold the read or write lock: performing
     * the scan and the publish under that lock keeps the check-and-set mutually
     * exclusive with {@link MetadataCacheWriter#clearCache()} (write lock), so a
     * concurrent clear cannot interleave between observing completeness and setting
     * the flag and leave an emptied cache marked complete.
     */
    private void latchCacheCompleteIfWarmed(ObjList<TableToken> tableTokens) {
        for (int i = 0, n = tableTokens.size(); i < n; i++) {
            final TableToken token = tableTokens.getQuick(i);
            // views are never stored in the cache (they have no _meta file)
            if (token.isView()) {
                continue;
            }
            if (tableMap.get(token.getTableName()) == null) {
                return;
            }
        }
        publishCacheComplete();
        // Publish all active policies in one O(T + K) batch while the completeness-proving lock remains held.
        // This replaces startup's former K copy-on-write publications (O(K^2) aggregate copying).
        publishActiveExpiryPolicySnapshot();
        fullyHydrated = true;
    }

    /**
     * Whether the cache is currently latched as holding every registered (non-view)
     * table. Exposed for tests that assert the latch is only ever published while the
     * cache is actually complete; production code uses the volatile fast path inside
     * {@link #hydrateAllTables()} instead.
     */
    @TestOnly
    public boolean isCacheComplete() {
        return cacheComplete;
    }

    /**
     * Publishes the completeness latch - the single choke point for every
     * {@link #cacheComplete} publish. Callers invoke it inside a lock scope that
     * excludes {@link MetadataCacheWriter#clearCache()} (read lock for the reconcile
     * latches, write lock for the give-up latch), so a concurrent clear cannot slip
     * between observing completeness and setting the flag. Protected so tests can
     * subclass the cache and interleave a clearCache() with the publish; an override
     * runs inside the caller's lock scope, so a regression that moved a publish
     * outside its lock is still exposed.
     */
    protected void publishCacheComplete() {
        cacheComplete = true;
    }

    /**
     * Begins the read-path by taking a read lock and acquiring a thread-local
     * {@link MetadataCacheReaderImpl}, an implementation of {@link MetadataCacheReader}.
     *
     * @return {@link MetadataCacheReader}
     */
    public MetadataCacheReader readLock() {
        rwLock.readLock().lock();
        return cacheReader;
    }

    /**
     * Refreshes {@code localCache} with a complete, current view of the cache and
     * returns the new snapshot version. This is the catalogue read-path used by
     * {@code tables()}, {@code all_tables()}, {@code information_schema.columns()}
     * and {@code pg_catalog.pg_attribute}: it first reconciles the cache against
     * the table registry via {@link #hydrateAllTables()} (so the snapshot is
     * complete even mid startup hydration), then snapshots under the read lock.
     * <p>
     * Prefer this over taking {@link #readLock()} and calling
     * {@link MetadataCacheReader#snapshot} directly; the reader-level variant
     * skips the reconcile and exists for callers that already hold the read lock
     * or deliberately want the raw, possibly-incomplete cache contents.
     *
     * @param localCache   the snapshot to be refreshed
     * @param priorVersion the version of the snapshot
     * @return the current version of the snapshot
     */
    public long snapshot(CharSequenceObjMap<CairoTable> localCache, long priorVersion) {
        // Reconcile first (cheap no-op once startup hydration has completed), then
        // snapshot under a single read lock.
        hydrateAllTables();
        try (MetadataCacheReader metadataRO = readLock()) {
            return metadataRO.snapshot(localCache, priorVersion);
        }
    }

    /**
     * Begins the read-path by taking a write lock and acquiring a thread-local
     * {@link MetadataCacheWriterImpl}, an implementation of {@link MetadataCacheWriter}.
     * Also increments a version counter, used to invalidate the cache.
     * The version counter does not guarantee a version change for any particular table,
     * so each {@link CairoTable} has its own metadata version.
     * Returns a singleton writer, not a thread-local, since there should be one
     * writer at a time.
     *
     * @return {@link MetadataCacheWriter}
     */
    public MetadataCacheWriter writeLock() {
        rwLock.writeLock().lock();
        version++;
        return cacheWriter;
    }

    private @NotNull ColumnVersionReader getColumnVersionReader() {
        //noinspection ReplaceNullCheck
        if (columnVersionReader != null) {
            return columnVersionReader;
        }
        return columnVersionReader = new ColumnVersionReader();
    }

    private void hydrateTableStartup(@NotNull TableToken token, boolean throwError, boolean skipIfCached) {
        if (engine.isTableDropped(token)) {
            // Table writer can still process some transactions when DROP table has already
            // been executed, essentially updating dropped table. We should ignore such updates.
            return;
        }

        if (token.isView()) {
            // views do not have _meta file
            // view metadata will be added to the cache when the view is compiled
            return;
        }

        // Exactly-once hydration per clearCache() epoch. The startup hydrator and
        // the catalogue reconcile (hydrateAllTables) only need to ensure a table is
        // present; both run under the write lock. If another pass already hydrated
        // this table since the last clear, skip the redundant meta-file read. The
        // write lock makes this check-and-hydrate atomic, so each table is read from
        // disk exactly once between clears. The writer-driven refresh path
        // (hydrateTable(TableToken)) passes skipIfCached=false and still re-reads to
        // pick up newer on-disk metadata.
        if (skipIfCached && tableMap.get(token.getTableName()) != null) {
            return;
        }

        Path path = Path.getThreadLocal(engine.getConfiguration().getDbRoot());

        // set up the dir path
        path.concat(token.getDirName());

        boolean isSoftLink = Files.isSoftLink(path.$());

        // set up the table path
        path.concat(TableUtils.META_FILE_NAME).trimTo(path.size());

        // create table to work with
        CairoTable table = new CairoTable(token);

        try {
            // open metadata
            metaMem.smallFile(engine.getConfiguration().getFilesFacade(), path.$(), MemoryTag.NATIVE_METADATA_READER);
            TableUtils.validateMeta(path, metaMem, null, ColumnType.VERSION);

            table.setMetadataVersion(Long.MIN_VALUE);

            long metadataVersion = metaMem.getLong(TableUtils.META_OFFSET_METADATA_VERSION);

            // make sure we aren't duplicating work
            CairoTable potentiallyExistingTable = tableMap.get(token.getTableName());
            if (potentiallyExistingTable != null && potentiallyExistingTable.getMetadataVersion() > metadataVersion) {
                LOG.debug().$("table in cache with newer version [table=").$(token)
                        .$(", version=").$(potentiallyExistingTable.getMetadataVersion())
                        .I$();
                return;
            }

            // get basic metadata
            int columnCount = metaMem.getInt(TableUtils.META_OFFSET_COUNT);

            LOG.debug().$("reading columns [table=").$(token)
                    .$(", count=").$(columnCount)
                    .I$();

            table.setMetadataVersion(metadataVersion);

            LOG.debug().$("set metadata version [table=").$(token)
                    .$(", version=").$(metadataVersion)
                    .I$();

            int partitionBy = metaMem.getInt(TableUtils.META_OFFSET_PARTITION_BY);
            table.setPartitionBy(partitionBy);
            table.setMaxUncommittedRows(metaMem.getInt(TableUtils.META_OFFSET_MAX_UNCOMMITTED_ROWS));
            table.setO3MaxLag(metaMem.getLong(TableUtils.META_OFFSET_O3_MAX_LAG));
            int timestampWriterIndex = metaMem.getInt(TableUtils.META_OFFSET_TIMESTAMP_INDEX);
            table.setTimestampIndex(-1);
            table.setTtlHoursOrMonths(TableUtils.getTtlHoursOrMonths(metaMem));
            // Read the trailing row-expiry policy (predicate + cleanup interval) in a SINGLE offset walk.
            // Byte-identical to TableReaderMetadata.readExpiryPolicy / TableWriterMetadata: the shared
            // getMetaExpiryPolicyOffset + getMetaExpiryPredicateStorageLength helpers keep the two
            // drift-prone, corruption-sensitive steps in one place.
            String expiryPredicate = null;
            long expiryCleanupIntervalMicros = 0;
            if (TableUtils.isMetaFormatAtLeast(metaMem, TableUtils.META_FORMAT_MINOR_VERSION_EXPIRE_ROWS)) {
                long policyOffset = TableUtils.getMetaExpiryPolicyOffset(metaMem, columnCount);
                long memSize = metaMem.size();
                if (policyOffset >= 0 && policyOffset + Integer.BYTES <= memSize) {
                    long predicateStorageLength = TableUtils.getMetaExpiryPredicateStorageLength(metaMem, policyOffset, memSize);
                    if (predicateStorageLength >= 0) {
                        CharSequence pred = metaMem.getStrA(policyOffset);
                        if (pred != null && pred.length() > 0) {
                            expiryPredicate = Chars.toString(pred);
                        }
                        policyOffset += predicateStorageLength;
                        if (policyOffset + Long.BYTES <= memSize) {
                            expiryCleanupIntervalMicros = metaMem.getLong(policyOffset);
                        }
                    }
                }
            }
            table.setExpiry(expiryPredicate, expiryCleanupIntervalMicros);
            table.setTableFormat(TableUtils.getTableFormat(metaMem));
            table.setSoftLinkFlag(isSoftLink);

            TableUtils.buildColumnListFromMetadataFile(metaMem, columnCount, table.columnOrderList);
            // Inline symbol capacity is trustworthy from META_FORMAT_MINOR_VERSION_SYMBOL_CAPACITY onward.
            // Gate on THAT version, not LATEST: a later LATEST bump (TTL, then EXPIRE_ROWS) must not force
            // every pre-existing table onto the slow per-column loadCapacities() path on each hydration.
            boolean symbolCapacityInMeta = TableUtils.isMetaFormatAtLeast(metaMem, TableUtils.META_FORMAT_MINOR_VERSION_SYMBOL_CAPACITY);
            boolean isMetaFormatUpToDate = TableUtils.isMetaFormatAtLeast(metaMem, TableUtils.META_FORMAT_MINOR_VERSION_PARQUET_ENCODING_CONFIG);
            boolean hasParquetEncodingConfig = TableUtils.hasParquetEncodingConfig(metaMem);
            // populate columns
            for (int i = 0, n = table.columnOrderList.size(); i < n; i += 3) {
                int writerIndex = table.columnOrderList.get(i);
                // negative writer index columns were "replaced", as in renamed, or had their type changed
                if (writerIndex < 0) {
                    continue;
                }

                CharSequence name = metaMem.getStrA(table.columnOrderList.get(i + 1));

                assert name != null;
                int columnType = TableUtils.getColumnType(metaMem, writerIndex);

                // negative type means column was deleted
                if (columnType < 0) {
                    continue;
                }
                String columnName = Chars.toString(name);
                CairoColumn column = new CairoColumn();

                LOG.debug().$("hydrating column [table=").$(token).$(", column=").$safe(columnName).I$();

                column.setName(columnName);

                // Column positions already determined
                column.setPosition(table.getColumnCount());
                column.setType(columnType);
                column.setIndexType(TableUtils.getColumnIndexType(metaMem, writerIndex));
                column.setIndexBlockCapacity(TableUtils.getIndexBlockCapacity(metaMem, writerIndex));
                column.setSymbolTableStaticFlag(true);
                column.setDedupKeyFlag(TableUtils.isColumnDedupKey(metaMem, writerIndex));
                // Pre-9.3.4 _meta files can carry non-zero bytes at column-entry offset 20 from
                // older layouts (e.g. Mig608 wrote a random column hash at 20-27); read 0 there.
                column.setParquetEncodingConfig(hasParquetEncodingConfig ? TableUtils.getParquetEncodingConfig(metaMem, writerIndex) : 0);
                column.setWriterIndex(writerIndex);

                boolean isDesignated = writerIndex == timestampWriterIndex;
                column.setDesignatedFlag(isDesignated);
                if (isDesignated) {
                    // Timestamp index is the logical index of the column in the column list. Rather than
                    // physical index of the column in the metadata file (writer index).
                    table.setTimestampType(columnType);
                    table.setTimestampIndex(table.getColumnCount());
                }

                if (column.isDedupKey()) {
                    table.setDedupFlag(true);
                }

                if (columnType == ColumnType.SYMBOL) {
                    if (symbolCapacityInMeta) {
                        column.setSymbolCapacity(TableUtils.getSymbolCapacity(metaMem, writerIndex));
                        column.setSymbolCached(TableUtils.isSymbolCached(metaMem, writerIndex));
                    } else {
                        LOG.debug().$("updating symbol capacity [table=").$(token).$(", column=").$safe(columnName).I$();
                        loadCapacities(column, token, path, engine.getConfiguration(), getColumnVersionReader());
                    }
                }

                table.upsertColumn(column);
            }

            readCoveringColumnIndicesIntoTable(metaMem, columnCount, table);

            if (PartitionBy.isPartitioned(partitionBy)) {
                try {
                    if (txReader == null) {
                        txReader = new TxReader(engine.getConfiguration().getFilesFacade());
                    }
                    Path txnPath = Path.getThreadLocal2(engine.getConfiguration().getDbRoot());
                    txReader.ofRO(txnPath.concat(token.getDirName()).concat(TableUtils.TXN_FILE_NAME).$(), table.getTimestampType(), partitionBy);
                    if (txReader.unsafeLoadAll()) {
                        table.setHasParquetPartitions(txReader.hasParquetPartitions());
                    } else {
                        table.setHasParquetPartitions(true);
                    }
                } catch (CairoException e) {
                    LOG.error().$("could not read partition format, assuming parquet [table=").$(token)
                            .$(", error=").$((Throwable) e).I$();
                    table.setHasParquetPartitions(true);
                }
            }

            tableMap.put(table.getTableName(), table);
            final boolean hasPendingExpiryPublication = clearPendingExpiryPolicy(
                    token.getTableId(),
                    table.getMetadataVersion()
            );
            // Same narrowing as hydrateTable(TableMetadata): only a table that can affect the policy
            // snapshot pays the O(all tables) rebuild.
            final String hydratedPredicate = table.getExpiryPredicate();
            final boolean isPolicySnapshotAffected = (hydratedPredicate != null && !hydratedPredicate.isEmpty())
                    || expiryPolicySnapshot.tableIds.contains(token.getTableId());
            // Republish once the cache is trustworthy as the discovery source: fully hydrated, or given up
            // (cacheComplete without full hydration). The give-up case is essential -- a policied object first
            // hydrated after give-up would otherwise never enter the published snapshot, and the cleanup
            // fallback (hydrateAllTables) short-circuits on cacheComplete, so discovery would never see it and
            // reclamation would stall. During active startup both flags are false, so per-table republishing
            // stays suppressed and the single batch publish in latchCacheCompleteIfWarmed() covers every policy.
            if (hasPendingExpiryPublication || ((fullyHydrated || cacheComplete) && isPolicySnapshotAffected)) {
                publishActiveExpiryPolicySnapshot();
            }
            LOG.debug().$("hydrated metadata [table=").$(token).I$();
        } catch (Throwable e) {
            // get rid of stale metadata
            tableMap.remove(token.getTableName());
            // if we can't hydrate and the table is not dropped, it's a critical error
            LogRecord log = engine.isTableDropped(token) ? LOG.info() : LOG.critical();
            try {
                log
                        .$("could not hydrate metadata [table=").$(token)
                        .$(", msg=");
                if (e instanceof FlyweightMessageContainer) {
                    log.$safe(((FlyweightMessageContainer) e).getFlyweightMessage());
                } else {
                    log.$safe(e.getMessage());
                }
                log.$(", errno=").$(e instanceof CairoException ? ((CairoException) e).errno : 0);
            } finally {
                log.I$();
            }
            if (throwError) {
                throw e;
            }
        } finally {
            Misc.free(metaMem);
            Misc.free(txReader);
        }
    }

    private void loadCapacities(
            CairoColumn column,
            TableToken token,
            Path path,
            CairoConfiguration configuration,
            ColumnVersionReader columnVersionReader
    ) {
        final CharSequence columnName = column.getName();
        final int writerIndex = column.getWriterIndex();

        try (columnVersionReader) {
            LOG.debug().$("hydrating symbol metadata [table=").$(token).$(", column=").$safe(columnName).I$();

            // get column version
            path.trimTo(configuration.getDbRoot().length()).concat(token);
            final int rootLen = path.size();
            path.concat(TableUtils.COLUMN_VERSION_FILE_NAME);

            final long symbolTableNameTxn;
            final FilesFacade ff = configuration.getFilesFacade();
            try (columnVersionReader) {
                columnVersionReader.ofRO(ff, path.$());
                columnVersionReader.readUnsafe();
                symbolTableNameTxn = columnVersionReader.getSymbolTableNameTxn(writerIndex);
            }

            // initialize symbol map memory
            final long capacityOffset = SymbolMapWriter.HEADER_CAPACITY;
            final int capacity;
            final byte isCached;
            final LPSZ offsetFileName = TableUtils.offsetFileName(path.trimTo(rootLen), columnName, symbolTableNameTxn);
            long fd = TableUtils.openRO(ff, offsetFileName, LOG);
            try {
                // use txn to find the correct symbol entry
                capacity = ff.readNonNegativeInt(fd, capacityOffset);
                isCached = ff.readNonNegativeByte(fd, SymbolMapWriter.HEADER_CACHE_ENABLED);
            } finally {
                ff.close(fd);
            }

            // get symbol properties
            if (capacity > 0 && isCached >= 0) {
                column.setSymbolCapacity(capacity);
                column.setSymbolCached(isCached != 0);
            } else {
                if (capacity <= 0) {
                    throw CairoException.nonCritical().put("invalid capacity [table=").put(token.getTableName()).put(", column=").put(columnName)
                            .put(", capacityOffset=").put(capacityOffset).put(", capacity=").put(capacity).put(']');
                }
                throw CairoException.nonCritical().put("invalid cache flag [table=").put(token.getTableName()).put(", column=").put(columnName)
                        .put(", cacheFlagOffset=").put(SymbolMapWriter.HEADER_CACHE_ENABLED).put(", cacheFlag=").put(isCached).put(']');
            }
        } catch (CairoException ex) {
            // Don't stall startup.
            LOG.error().$("could not load symbol metadata [table=").$(token).$(", column=").$safe(columnName)
                    .$(", errno=").$(ex.getErrno())
                    .$(", message=").$safe(ex.getMessage())
                    .I$();
        }
    }

    private void readCoveringColumnIndicesIntoTable(MemoryCMR mem, int columnCount, CairoTable table) {
        // The covering INCLUDE list is appended to _meta after the
        // column-name region. Walk past the names, then for every column
        // whose META_FLAG_BIT_COVERING flag is set, decode the trailing
        // (count, writerIdx, writerIdx, ...) record and attach the *dense*
        // translation to the matching CairoColumn. The on-disk format
        // stores writer indices because they are stable across DROP COLUMN;
        // the metadata cache is dense-keyed throughout (CairoTable.columns
        // is a dense list, CairoColumn.position is dense), so we translate
        // here once at hydration to keep CairoColumn.coveringColumnIndices
        // in the same index space as everything else the renderers see.
        final long memSize = mem.size();
        long offset = TableUtils.getColumnNameOffset(columnCount);
        for (int i = 0; i < columnCount; i++) {
            if (offset + Integer.BYTES > memSize) {
                return;
            }
            int strLen = mem.getInt(offset);
            offset += Vm.getStorageLength(strLen);
        }
        if (offset >= memSize) {
            return;
        }
        for (int writerIndex = 0; writerIndex < columnCount; writerIndex++) {
            if (!TableUtils.isColumnCovering(mem, writerIndex)) {
                continue;
            }
            if (offset + Integer.BYTES > memSize) {
                return;
            }
            int includeCount = mem.getInt(offset);
            offset += Integer.BYTES;
            if (includeCount <= 0) {
                continue;
            }
            if (offset + (long) includeCount * Integer.BYTES > memSize) {
                return;
            }
            IntList denseIndices = new IntList(includeCount);
            for (int j = 0; j < includeCount; j++) {
                int covWriterIdx = mem.getInt(offset);
                offset += Integer.BYTES;
                denseIndices.add(toDenseIndex(table, covWriterIdx));
            }
            CairoColumn target = findColumnByWriterIndex(table, writerIndex);
            if (target != null) {
                target.setCoveringColumnIndices(denseIndices);
            }
        }
    }

    private void publishActiveExpiryPolicySnapshot() {
        synchronized (expiryPolicySnapshotLock) {
            final LongHashSet tableIds = copyTableIds(pendingExpiryPolicyIds, 0);
            final ObjList<CairoTable> tables = new ObjList<>();
            for (int i = 0, n = tableMap.size(); i < n; i++) {
                final CairoTable table = tableMap.getAt(i);
                if (table == null || table.getTableToken() == null || !table.getTableToken().isMatView()) {
                    continue;
                }
                final String predicate = table.getExpiryPredicate();
                if (predicate != null && !predicate.isEmpty()) {
                    final int tableId = table.getTableToken().getTableId();
                    tableIds.add(tableId);
                    if (!pendingExpiryPolicyIds.contains(tableId)) {
                        // Cleanup must not act on P0 after a replacement SET or DROP starts. Hydration or
                        // cancellation republishes the authoritative policy after clearing the pending ID.
                        tables.add(table);
                    }
                }
            }
            final ExpiryPolicySnapshot previous = expiryPolicySnapshot;
            expiryPolicySnapshot = tables.size() == 0 && tableIds.size() == 0
                    ? ExpiryPolicySnapshot.EMPTY
                    : new ExpiryPolicySnapshot(tableIds, copyTableIds(pendingExpiryPolicyIds, 0), tables);
            // Advance the policy epoch only when the policy topology actually changed. A routine
            // hydration of a policy-less table (e.g. a freshly created plain view) rebuilds an
            // identical id set; a spurious bump makes policy-version-guarded compilation (CREATE VIEW,
            // SELECT, INSERT SELECT, UPDATE, EXPLAIN) retry for no reason, and CREATE VIEW additionally
            // persists a redundant VIEW_DEFINITION that advances the view's seqTxn. Real SET/DROP
            // transitions still bump the epoch directly via markExpiryPolicyPossible() and
            // publishExpiryPolicyUpdate(), so predicate edits remain observable to compilers.
            if (!sameTableIds(previous.tableIds, expiryPolicySnapshot.tableIds)
                    || !sameTableIds(previous.pendingTableIds, expiryPolicySnapshot.pendingTableIds)) {
                expiryPolicyVersion++;
            }
        }
    }

    private static CairoColumn findColumnByWriterIndex(CairoTable table, int writerIndex) {
        if (writerIndex < 0) {
            return null;
        }
        for (int k = 0, n = table.getColumnCount(); k < n; k++) {
            CairoColumn c = table.getColumnQuiet(k);
            if (c != null && c.getWriterIndex() == writerIndex) {
                return c;
            }
        }
        return null;
    }

    private static boolean sameTableIds(LongHashSet a, LongHashSet b) {
        if (a.size() != b.size()) {
            return false;
        }
        for (int i = 0, n = a.size(); i < n; i++) {
            if (!b.contains(a.get(i))) {
                return false;
            }
        }
        return true;
    }

    private static int toDenseIndex(CairoTable table, int writerIndex) {
        if (writerIndex < 0) {
            return -1;
        }
        for (int k = 0, n = table.getColumnCount(); k < n; k++) {
            CairoColumn c = table.getColumnQuiet(k);
            if (c != null && c.getWriterIndex() == writerIndex) {
                return k;
            }
        }
        return -1;
    }

    private static void translateCoveringIndicesToDense(CairoTable table) {
        // CairoColumn.coveringColumnIndices is currently sharing the writer-
        // index list owned by TableColumnMetadata. Replace each non-empty
        // list with a freshly-allocated dense translation so the cache
        // stays internally dense-keyed and renderers can use the regular
        // CairoTable.getColumnQuiet(int) accessor.
        for (int i = 0, n = table.getColumnCount(); i < n; i++) {
            CairoColumn c = table.getColumnQuiet(i);
            if (c == null) {
                continue;
            }
            IntList writerCovering = c.getCoveringColumnIndices();
            if (writerCovering == null || writerCovering.size() == 0) {
                continue;
            }
            IntList denseCovering = new IntList(writerCovering.size());
            for (int j = 0, m = writerCovering.size(); j < m; j++) {
                denseCovering.add(toDenseIndex(table, writerCovering.getQuick(j)));
            }
            c.setCoveringColumnIndices(denseCovering);
        }
    }

    private static final class ExpiryPolicySnapshot {
        private static final ExpiryPolicySnapshot EMPTY = new ExpiryPolicySnapshot(
                new LongHashSet(),
                new LongHashSet(),
                new ObjList<>()
        );
        private final LongHashSet pendingTableIds;
        private final LongHashSet tableIds;
        private final ObjList<CairoTable> tables;

        private ExpiryPolicySnapshot(LongHashSet tableIds, LongHashSet pendingTableIds, ObjList<CairoTable> tables) {
            this.pendingTableIds = pendingTableIds;
            this.tableIds = tableIds;
            this.tables = tables;
        }
    }

    /**
     * An implementation of {@link MetadataCacheReader }. Provides a read-path into the metadata cache.
     */
    private class MetadataCacheReaderImpl implements MetadataCacheReader, QuietCloseable {

        @Override
        public void close() {
            rwLock.readLock().unlock();
        }

        @Override
        public void collectPoliciedTables(ObjList<TableToken> tokensOut, ObjList<String> predicatesOut, LongList cleanupIntervalsOut) {
            final ObjList<CairoTable> activePolicies = expiryPolicySnapshot.tables;
            for (int i = 0, n = activePolicies.size(); i < n; i++) {
                final CairoTable table = activePolicies.getQuick(i);
                if (table == null) {
                    continue;
                }
                final String predicate = table.getExpiryPredicate();
                if (predicate == null || predicate.isEmpty()) {
                    continue;
                }
                final TableToken token = table.getTableToken();
                if (token == null || !token.isMatView()) {
                    // EXPIRE ROWS is materialized-view-only; require isMatView() (not merely !isView()) so a
                    // policy that ever leaks onto a plain table is inert here rather than physically deleting
                    // its rows. The compiler is the gatekeeper; this is defense-in-depth for an irreversible op.
                    continue;
                }
                tokensOut.add(token);
                predicatesOut.add(predicate);
                cleanupIntervalsOut.add(table.getExpiryCleanupIntervalMicros());
            }
        }

        /**
         * Returns a table ONLY if it is already present in the cache.
         *
         * @param tableToken the token for the table
         * @return CairoTable the table, if present in the cache.
         */
        @Override
        public @Nullable CairoTable getTable(@NotNull TableToken tableToken) {
            return tableMap.get(tableToken.getTableName());
        }

        /**
         * Returns a count of the tables in the cache.
         */
        @Override
        public int getTableCount() {
            return tableMap.size();
        }

        /**
         * Returns the current cache version.
         */
        @Override
        public long getVersion() {
            return version;
        }

        @Override
        public boolean isVisibleTable(@NotNull String tableName) {
            final CairoConfiguration configuration = engine.getConfiguration();
            if (
                    engine.getTableFlagResolver().isSystem(tableName)
                            && !Chars.startsWith(tableName, configuration.getParquetExportTableNamePrefix())
                            && !Chars.equalsIgnoreCase(tableName, TelemetryConfigLogger.TELEMETRY_CONFIG_TABLE_NAME)
                            && !Chars.equalsIgnoreCase(tableName, TelemetryTask.TABLE_NAME)
            ) {
                return false;
            }

            // special handling for telemetry tables
            if (configuration.getTelemetryConfiguration().hideTables()
                    && (Chars.equals(tableName, TelemetryTask.TABLE_NAME)
                    || Chars.equals(tableName, TelemetryConfigLogger.TELEMETRY_CONFIG_TABLE_NAME))
            ) {
                return false;
            }

            return TableUtils.isFinalTableName(tableName, configuration.getTempRenamePendingTablePrefix());
        }

        /**
         * Refreshes a snapshot of the cache by checking versions of the cache,
         * and inner tables, and copying references to any tables that have changed.
         *
         * @param localCache   the snapshot to be refreshed
         * @param priorVersion the version of the snapshot
         * @return the current version of the snapshot
         */
        @Override
        public long snapshot(CharSequenceObjMap<CairoTable> localCache, long priorVersion) {
            if (priorVersion >= getVersion()) {
                return priorVersion;
            }

            // pull from cairoTables into localCache
            for (int i = tableMap.size() - 1; i >= 0; i--) {
                CairoTable latestTable = tableMap.getAt(i);
                CairoTable cachedTable = localCache.get(latestTable.getTableName());

                if (
                        (cachedTable == null
                                || cachedTable.getMetadataVersion() < latestTable.getMetadataVersion()
                                || cachedTable.getTableToken().getTableId() != latestTable.getTableToken().getTableId())
                                && isVisibleTable(latestTable.getTableName())
                ) {
                    localCache.put(latestTable.getTableName(), latestTable);
                }
            }

            for (int i = localCache.size() - 1; i >= 0; i--) {
                CairoTable cachedTable = localCache.getAt(i);
                CairoTable latestTable = tableMap.get(cachedTable.getTableName());

                if (latestTable == null) {
                    localCache.remove(cachedTable.getTableName());
                }
            }

            return version;
        }

        @Override
        public void toSink(@NotNull CharSink<?> sink) {
            sink.put("MetadataCache [");
            sink.put("tableCount=").put(tableMap.size()).put(']');
            sink.put('\n');

            for (int i = 0, n = tableMap.size(); i < n; i++) {
                sink.put('\t');
                tableMap.getAt(i).toSink(sink);
                sink.put('\n');
            }
        }
    }

    /**
     * An implementation of {@link MetadataCacheWriter }. Provides a read-path into the metadata cache.
     */
    private class MetadataCacheWriterImpl extends MetadataCacheReaderImpl implements MetadataCacheWriter {

        /**
         * Clears the table cache.
         */
        @Override
        public void clearCache() {
            tableMap.clear();
            // The cache is no longer complete; force the next catalogue reconcile to
            // re-hydrate from the registry (see hydrateAllTables()) and give it a fresh
            // retry budget for any table that fails to hydrate.
            incompleteReconcilePasses = 0;
            cacheComplete = false;
            fullyHydrated = false;
            synchronized (expiryPolicySnapshotLock) {
                expiryPolicySnapshot = ExpiryPolicySnapshot.EMPTY;
                expiryPolicyVersion++;
                pendingExpiryPolicyIds.clear();
                pendingExpiryPolicyVersions.clear();
            }
            // Advance the epoch so a reconcile that overlapped this clear does not count
            // its (clear-induced) "no progress" toward the give-up budget.
            clearEpoch++;
        }

        /**
         * Closes the writer, releasing the global metadata cache write-lock.
         */
        @Override
        public void close() {
            rwLock.writeLock().unlock();
        }

        /**
         * Removes a table from the cache
         *
         * @param tableToken the table token.
         */
        @Override
        public void dropTable(@NotNull TableToken tableToken) {
            final String tableName = tableToken.getTableName();
            final CairoTable entry = tableMap.get(tableName);
            if (entry != null && !tableToken.equals(entry.getTableToken())) {
                return; // stale token must not remove a newer table's cache or pending-policy state
            }
            if (entry != null) {
                tableMap.remove(tableName);
            }
            final boolean hasPendingExpiryPublication = clearPendingExpiryPolicy(tableToken.getTableId());
            final String droppedPredicate = entry != null ? entry.getExpiryPredicate() : null;
            if (hasPendingExpiryPublication
                    || expiryPolicySnapshot.tableIds.contains(tableToken.getTableId())
                    || (droppedPredicate != null && !droppedPredicate.isEmpty())) {
                // DROP is authoritative even when failed hydration already evicted the CairoTable. Rebuild the
                // immutable snapshot so this exact table ID cannot keep the conservative gate open forever.
                // A policy-less table contributed nothing to the snapshot, so its drop skips the rebuild.
                publishActiveExpiryPolicySnapshot();
            }
            if (entry != null) {
                LOG.info().$("dropped [table=").$(tableToken).I$();
            }
        }

        /**
         * @see MetadataCacheWriter#hydrateTable(TableToken)
         */
        @Override
        public void hydrateTable(@NotNull TableMetadata tableMetadata) {
            if (engine.isTableDropped(tableMetadata.getTableToken())) {
                // Table writer can still process some transactions when DROP table has already been
                // executed, essentially updating a dropped table. We ignore such updates for caching, but a
                // SET/DROP EXPIRE apply on the dropped table already added a pending mark via
                // markExpiryPolicyPossible(); release it here and rebuild the snapshot so the id leaves the
                // policy set. Table ids never reuse, and dropTable ran before this apply added the mark, so
                // nothing else would ever clear it - leaving mayHaveExpiryPolicy() stuck true forever.
                if (clearPendingExpiryPolicy(tableMetadata.getTableToken().getTableId())) {
                    publishActiveExpiryPolicySnapshot();
                }
                return;
            }

            final TableToken tableToken = tableMetadata.getTableToken();
            final CairoTable table = new CairoTable(tableToken);
            final long metadataVersion = tableMetadata.getMetadataVersion();
            table.setMetadataVersion(metadataVersion);

            LOG.debug().$("set metadata version [table=").$(tableToken)
                    .$(", version=").$(metadataVersion)
                    .I$();

            CairoTable potentiallyExistingTable = tableMap.get(tableToken.getTableName());
            if (potentiallyExistingTable != null && potentiallyExistingTable.getMetadataVersion() > metadataVersion) {
                LOG.info()
                        .$("table in cache with newer version [table=").$(tableToken)
                        .$(", version=").$(potentiallyExistingTable.getMetadataVersion())
                        .I$();
                return;
            }

            int columnCount = tableMetadata.getColumnCount();

            LOG.debug().$("reading columns [table=").$(tableToken)
                    .$(", count=").$(columnCount)
                    .I$();

            table.setPartitionBy(tableMetadata.getPartitionBy());
            table.setMaxUncommittedRows(tableMetadata.getMaxUncommittedRows());
            table.setO3MaxLag(tableMetadata.getO3MaxLag());
            table.setHasParquetPartitions(tableMetadata.hasParquetPartitions());
            int timestampWriterIndex = tableMetadata.getTimestampIndex();
            table.setTimestampIndex(-1);
            table.setTtlHoursOrMonths(tableMetadata.getTtlHoursOrMonths());
            final String expiryPredicate = tableMetadata.getExpiryPredicate();
            table.setExpiry(expiryPredicate, tableMetadata.getExpiryCleanupIntervalMicros());
            table.setTableFormat(tableMetadata.getTableFormat());
            Path tempPath = Path.getThreadLocal(engine.getConfiguration().getDbRoot());
            table.setSoftLinkFlag(Files.isSoftLink(tempPath.concat(tableToken.getDirNameUtf8()).$()));

            for (int i = 0; i < columnCount; i++) {
                final TableColumnMetadata columnMetadata = tableMetadata.getColumnMetadata(i);

                int columnType = columnMetadata.getColumnType();
                if (columnType < 0) {
                    continue; // marked for deletion
                }

                String columnName = columnMetadata.getColumnName();
                LOG.debug().$("hydrating column [table=").$(tableToken).$(", column=").$safe(columnName).I$();

                CairoColumn column = new CairoColumn();

                column.setName(columnName);
                column.setType(columnType);
                // A type-converted column keeps its original catalogue position. Use the
                // chain root (originalWriterIndex), not the immediate predecessor: after two
                // or more conversions getReplacingIndex() points at the intermediate column,
                // which would push the column to the end of the catalogue. getOriginalWriterIndex()
                // is the precomputed chain head and equals the column's own writer index when
                // it was never converted.
                int originalWriterIndex = columnMetadata.getOriginalWriterIndex();
                column.setPosition(originalWriterIndex > -1 ? originalWriterIndex : i);
                column.setIndexType(columnMetadata.getIndexType());
                column.setIndexBlockCapacity(columnMetadata.getIndexValueBlockCapacity());
                // Translate from writer to dense after the column list is
                // finalized (post-sort) below.
                column.setCoveringColumnIndices(columnMetadata.getCoveringColumnIndices());
                column.setSymbolTableStaticFlag(columnMetadata.isSymbolTableStatic());
                column.setDedupKeyFlag(columnMetadata.isDedupKeyFlag());
                column.setParquetEncodingConfig(columnMetadata.getParquetEncodingConfig());

                int writerIndex = columnMetadata.getWriterIndex();
                column.setWriterIndex(writerIndex);

                boolean isDesignated = writerIndex == timestampWriterIndex;
                column.setDesignatedFlag(isDesignated);
                if (isDesignated) {
                    table.setTimestampIndex(table.getColumnCount());
                }

                if (column.isDedupKey()) {
                    table.setDedupFlag(true);
                }

                if (ColumnType.isSymbol(column.getType())) {
                    LOG.debug().$("hydrating symbol metadata [table=").$(tableToken).$(", column=").$safe(columnName).I$();
                    column.setSymbolCapacity(tableMetadata.getSymbolCapacity(i));
                    column.setSymbolCached(tableMetadata.getSymbolCacheFlag(i));
                }

                table.upsertColumn(column);
            }

            table.columns.sort(comparator);

            for (int i = 0, n = table.columns.size(); i < n; i++) {
                // Update column positions after sort
                final CairoColumn column = table.columns.getQuick(i);
                column.setPosition(i);
                // Update designated timestamp index
                if (column.isDesignated()) {
                    table.setTimestampIndex(i);
                }
                // Update column name index map
                table.columnNameIndexMap.put(table.columns.getQuick(i).getName(), i);
            }

            translateCoveringIndicesToDense(table);

            tableMap.put(table.getTableName(), table);
            final boolean hasPendingExpiryPublication = clearPendingExpiryPolicy(tableToken.getTableId(), metadataVersion);
            // Republish only when this table can affect the policy snapshot: it carries a predicate
            // (the snapshot must reference the fresh CairoTable), its id is in the published policied
            // set (covers a dropped policy), or its pending mark was just cleared. Hydration of any
            // other table would rebuild an identical snapshot, making every DDL of any table - the
            // ILP auto-ADD-COLUMN path included - O(all tables) for no observable change.
            final boolean isPolicySnapshotAffected = (expiryPredicate != null && !expiryPredicate.isEmpty())
                    || expiryPolicySnapshot.tableIds.contains(tableToken.getTableId());
            // Trust the cache as the discovery source once fully hydrated or given up (cacheComplete): a
            // policied object first hydrated after give-up must still reach the snapshot, since the cleanup
            // fallback short-circuits on cacheComplete. Active startup leaves both flags false, so the single
            // batch publish still covers every policy without per-table rebuilds.
            if (hasPendingExpiryPublication || ((fullyHydrated || cacheComplete) && isPolicySnapshotAffected)) {
                publishActiveExpiryPolicySnapshot();
            }
            LOG.info().$("hydrated [table=").$(table.getTableToken()).I$();
        }

        @Override
        public void hydrateTable(@NotNull TableToken token) {
            hydrateTableStartup(token, true, false);
        }

        @Override
        public void renameTable(@NotNull TableToken fromTableToken, @NotNull TableToken toTableToken) {
            String tableName = fromTableToken.getTableName();
            final int index = tableMap.keyIndex(tableName);

            // Metadata may not be hydrated at this point, handle this case
            if (index < 0) {
                CairoTable fromTab = tableMap.valueAt(index);
                tableMap.removeAt(index);
                final CairoTable toTab = new CairoTable(toTableToken, fromTab);
                tableMap.put(toTableToken.getTableName(), toTab);
                // Rename keeps the table id, so the snapshot needs a rebuild only when it references
                // this table - cleanup discovery must see the renamed token. Trust the cache as the discovery
                // source once fully hydrated or given up (cacheComplete), so a policied view renamed after
                // give-up does not leave a stale token that discovery skips forever.
                final String renamedPredicate = toTab.getExpiryPredicate();
                if ((fullyHydrated || cacheComplete)
                        && ((renamedPredicate != null && !renamedPredicate.isEmpty())
                        || expiryPolicySnapshot.tableIds.contains(toTableToken.getTableId()))) {
                    publishActiveExpiryPolicySnapshot();
                }
            }
        }

        @Override
        public void setHasParquetPartitions(@NotNull TableToken tableToken, boolean hasParquetPartitions) {
            CairoTable table = tableMap.get(tableToken.getTableName());
            if (table != null && tableToken.equals(table.getTableToken())) {
                table.setHasParquetPartitions(hasParquetPartitions);
            }
        }
    }
}
