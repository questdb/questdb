/*******************************************************************************
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

package io.questdb.cairo.wal;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.std.BiLongFunction;
import io.questdb.std.ConcurrentHashMap;
import io.questdb.std.ConcurrentLongHashMap;
import io.questdb.std.Numbers;
import io.questdb.std.WeakClosableObjectPool;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * Manages in-memory locks for WAL directories and segments.
 * <p>
 * WAL directories are created per wal writer and contain multiple segments with each segment
 * representing a batch of writes.
 * Regularly, the Wal purge job will try to delete older segments to free disk space. To avoid
 * conflicts between writers and the purge job, this lock manager provides a way to coordinate
 * access to WAL directories and segments.
 * It is assumed that no 2 writers will write to the same WAL directory and no 2 purge jobs
 * will purge the same WAL directory concurrently.
 * <p>
 * Implementation details:
 * We're relying on {@link ConcurrentLongHashMap} to access and mutates WAL entries.
 * Each WAL entry contains a {@link Semaphore} to provide mutual exclusion, note that this is only
 * used on writer as they cannot proceed if the purge job is already active on the same WAL.
 * <p>
 * Thread safety: All methods are thread-safe and can be called concurrently.
 * It is the caller's responsibility to ensure that the same WAL directory is not
 * locked by multiple writers or purge jobs concurrently.
 */
public class WalLockManager {
    private final ConcurrentLongHashMap<WalEntry> entries = new ConcurrentLongHashMap<>();
    // ThreadLocal context for passing parameters to non-capturing lambdas
    private final ThreadLocal<OpContext> opContext = ThreadLocal.withInitial(OpContext::new);
    private final WeakClosableObjectPool<WalEntry> pool = new WeakClosableObjectPool<>(WalEntry::new, 8);
    private final BiLongFunction<WalEntry, WalEntry> lockPurgeFn = this::doLockPurge;
    private final BiLongFunction<WalEntry, WalEntry> lockWriterFn = this::doLockWriter;
    private final BiLongFunction<WalEntry, WalEntry> setMinSegmentFn = this::doSetMinSegment;
    /**
     * Table ID from {@link TableToken} are not unique.
     * To circumvent this, we maintain our own sequence of table IDs because table directory names are unique.
     */
    private final AtomicInteger tableIdSeq = new AtomicInteger(0);
    private final Function<CharSequence, Integer> getNextTableIdFn = k -> tableIdSeq.getAndIncrement();
    private final ConcurrentHashMap<Integer> tableIds = new ConcurrentHashMap<>();
    private final BiLongFunction<WalEntry, WalEntry> unlockPurgeFn = this::doUnlockPurge;
    private final BiLongFunction<WalEntry, WalEntry> unlockWriterFn = this::doUnlockWriter;

    /**
     * Drop a table ID mapping. Should be called when a table is dropped and no more writers/purge jobs
     * will access its WAL directories.
     *
     * @param tableDirName the table directory name identifying the table
     */
    public void dropTable(@NotNull CharSequence tableDirName) {
        tableIds.remove(tableDirName);
    }

    /**
     * Checks if a specific WAL segment is currently locked.
     *
     * @param tableDirName the table directory name identifying the table
     * @param walId        the WAL identifier (e.g., 1 for wal1)
     * @param segmentId    the segment identifier within the WAL
     * @return {@code true} if the segment is locked, {@code false} otherwise
     */
    @TestOnly
    public boolean isSegmentLocked(@NotNull CharSequence tableDirName, int walId, int segmentId) {
        final int tableId = getTableId(tableDirName);
        final long walKey = Numbers.encodeLowHighInts(tableId, walId);
        final WalEntry entry = this.entries.get(walKey);
        return entry != null && entry.hasSegment(segmentId);
    }

    /**
     * Checks if a WAL directory is currently locked.
     *
     * @param tableDirName the table directory name identifying the table
     * @param walId        the WAL identifier (e.g., 1 for wal1)
     * @return {@code true} if the WAL is locked, {@code false} otherwise
     */
    @TestOnly
    public boolean isWalLocked(@NotNull CharSequence tableDirName, int walId) {
        final int tableId = getTableId(tableDirName);
        final long walKey = Numbers.encodeLowHighInts(tableId, walId);
        return this.entries.containsKey(walKey);
    }

    /**
     * Locks the purge lock on the specified WAL directory.
     * This will try to take an exclusive lock on the WAL, blocking any writers and returning {@link WalUtils#SEG_NONE_ID}.
     * If an active writer is present, this method will take a shared lock with the writer and return the maximum segment ID
     * that can be safely purged.
     *
     * @param tableDirName the table directory name identifying the table
     * @param walId        the WAL identifier (e.g., 1 for wal1)
     * @return the maximum segment ID that can be safely purged, or {@link WalUtils#SEG_NONE_ID} if exclusive lock is held
     * and the whole WAL can be purged.
     */
    public int lockPurge(@NotNull CharSequence tableDirName, int walId) {
        final int tableId = getTableId(tableDirName);
        final long walKey = Numbers.encodeLowHighInts(tableId, walId);
        final WalEntry entry = this.entries.compute(walKey, lockPurgeFn);

        // Race safety: minSegmentId is only mutated in 2 cases:
        //  - when the purge lock is released: as we assume that no 2 purge jobs will run concurrently
        //  on the same WAL, this read is safe.
        //  - when {@link #setWalSegmentMinId(CharSequence, int, int)} is called: this is safe as the caller
        //  must ensure that the new minSegmentId is greater than or equal to the current minSegmentId.
        //  A stale read returns a conservative (lower) max-purgeable ID, which is safe - we may under-purge
        //  but never over-purge.
        if (entry.minSegmentId == WalUtils.SEG_NONE_ID) {
            return WalUtils.SEG_NONE_ID;
        } else {
            return entry.minSegmentId - 1;
        }
    }

    /**
     * Locks the writer lock on the specified WAL directory.
     * This will block if a purge job is currently holding an exclusive lock on the WAL.
     * The caller must ensure to call {@link #unlockWriter(CharSequence, int)} to release the lock.
     *
     * @param tableDirName the table directory name identifying the table
     * @param walId        the WAL identifier (e.g., 1 for wal1)
     * @param minSegmentId the minimum segment ID that the writer will be working with
     */
    public void lockWriter(@NotNull CharSequence tableDirName, int walId, int minSegmentId) {
        final int tableId = getTableId(tableDirName);
        final long walKey = Numbers.encodeLowHighInts(tableId, walId);

        final OpContext ctx = opContext.get();
        ctx.minSegmentId = minSegmentId;

        final WalEntry entry = this.entries.compute(walKey, lockWriterFn);

        try {
            entry.lock.acquire();
        } catch (InterruptedException e) {
            unlockWriter(walKey);
            throw CairoException.critical(0)
                    .put("Interrupted while acquiring WAL lock [table=").put(tableDirName)
                    .put(", wal=").put(walId).put(']');
        }
    }

    /**
     * Clears all locks from the manager.
     * <p>
     * This method should only be used during testing or shutdown scenarios.
     * Calling this while locks are held may lead to inconsistent state.
     */
    public void reset() {
        entries.clear();
        pool.close();
        tableIds.clear();
        tableIdSeq.set(0);
    }

    /**
     * Sets the minimum segment ID for the specified WAL directory.
     * As the purge job may also hold the lock, the new minimum segment ID must be
     * greater than or equal to the current minimum segment ID.
     *
     * @param tableDirName    the table directory name identifying the table
     * @param walId           the WAL identifier (e.g., 1 for wal1)
     * @param newMinSegmentId the new minimum segment ID to set
     */
    public void setWalSegmentMinId(@NotNull CharSequence tableDirName, int walId, int newMinSegmentId) {
        final int tableId = getTableId(tableDirName);
        final long walKey = Numbers.encodeLowHighInts(tableId, walId);

        final OpContext ctx = opContext.get();
        ctx.minSegmentId = newMinSegmentId;

        this.entries.computeIfPresent(walKey, setMinSegmentFn);
    }

    /**
     * Unlock the purge lock on the specified WAL directory.
     * This will either release the lock if no writers are active,
     * or downgrade the lock to active writer if a writer is active/waiting.
     *
     * @param tableDirName the table directory name identifying the table
     * @param walId        the WAL identifier (e.g., 1 for wal1)
     */
    public void unlockPurge(@NotNull CharSequence tableDirName, int walId) {
        final int tableId = getTableId(tableDirName);
        final long walKey = Numbers.encodeLowHighInts(tableId, walId);
        this.entries.compute(walKey, unlockPurgeFn);
    }

    /**
     * Unlock the writer lock on the specified WAL directory.
     * This will either release the lock if no purge is active,
     * or downgrade the lock to purge exclusive if a purge is also active.
     *
     * @param tableDirName the table directory name identifying the table
     * @param walId        the WAL identifier (e.g., 1 for wal1)
     */
    public void unlockWriter(@NotNull CharSequence tableDirName, int walId) {
        final int tableId = getTableId(tableDirName);
        final long walKey = Numbers.encodeLowHighInts(tableId, walId);
        unlockWriter(walKey);
    }

    private WalEntry doLockPurge(long walKey, WalEntry existingEntry) {
        if (existingEntry == null) {
            final WalEntry newEntry;
            synchronized (pool) {
                newEntry = pool.pop();
            }
            newEntry.of(walKey, WalEntry.STATUS_PURGE_EXCLUSIVE, WalUtils.SEG_NONE_ID);
            // We can hold the lock safely here as there are no other holders.
            newEntry.lock.acquireUninterruptibly();
            return newEntry;
        }
        if (existingEntry.status != WalEntry.STATUS_ACTIVE_WRITER) {
            throw CairoException.critical(0).put("cannot attach purge, WAL is not writer locked [status=").put(existingEntry.status).put(']');
        }
        // The semaphore is already held by the writer, we don't need to wait for it.
        existingEntry.status = WalEntry.STATUS_WRITER_PURGE;
        return existingEntry;
    }

    private WalEntry doLockWriter(long walKey, WalEntry existingEntry) {
        final int minSegmentId = opContext.get().minSegmentId;
        if (existingEntry == null) {
            final WalEntry newEntry;
            synchronized (pool) {
                newEntry = pool.pop();
            }
            newEntry.of(walKey, WalEntry.STATUS_ACTIVE_WRITER, minSegmentId);
            return newEntry;
        }
        if (existingEntry.status != WalEntry.STATUS_PURGE_EXCLUSIVE) {
            throw CairoException.critical(0).put("cannot attach writer, WAL is already writer locked [status=").put(existingEntry.status).put(']');
        }
        existingEntry.status = WalEntry.STATUS_WRITER_ACQUIRING;
        existingEntry.nextMinSegmentId = minSegmentId;
        return existingEntry;
    }

    private WalEntry doSetMinSegment(long walKey, WalEntry existingEntry) {
        final int newMinSegmentId = opContext.get().minSegmentId;
        if (existingEntry.minSegmentId > newMinSegmentId) {
            throw CairoException.critical(0).put("new minSegmentId must be >= current [current=").put(existingEntry.minSegmentId).put(", new=").put(newMinSegmentId).put(']');
        }
        existingEntry.minSegmentId = newMinSegmentId;
        return existingEntry;
    }

    private WalEntry doUnlockPurge(long walKey, WalEntry existingEntry) {
        if (existingEntry.status == WalEntry.STATUS_PURGE_EXCLUSIVE) {
            // We can safely release the lock and give back the entry to the pool.
            existingEntry.lock.release();
            existingEntry.walKey = -1;
            synchronized (pool) {
                pool.push(existingEntry);
            }
            return null;
        }

        if (existingEntry.status == WalEntry.STATUS_WRITER_PURGE) {
            // Downgrade to active writer, let the writer release the lock.
            existingEntry.status = WalEntry.STATUS_ACTIVE_WRITER;
            return existingEntry;
        }

        if (existingEntry.status != WalEntry.STATUS_WRITER_ACQUIRING) {
            throw CairoException.critical(0).put("unexpected WAL status on purge unlock [status=").put(existingEntry.status).put(']');
        }
        existingEntry.status = WalEntry.STATUS_ACTIVE_WRITER;
        existingEntry.minSegmentId = existingEntry.nextMinSegmentId;
        existingEntry.lock.release();
        return existingEntry;
    }

    private WalEntry doUnlockWriter(long walKey, WalEntry existingEntry) {
        if (existingEntry.status == WalEntry.STATUS_ACTIVE_WRITER) {
            // We can safely release the lock and give back the entry to the pool.
            existingEntry.lock.release();
            existingEntry.walKey = -1;
            synchronized (pool) {
                pool.push(existingEntry);
            }
            return null;
        }

        if (existingEntry.status == WalEntry.STATUS_WRITER_ACQUIRING) {
            // Downgrade to purge exclusive, let the purge release the lock.
            existingEntry.status = WalEntry.STATUS_PURGE_EXCLUSIVE;
            return existingEntry;
        }

        if (existingEntry.status != WalEntry.STATUS_WRITER_PURGE) {
            throw CairoException.critical(0).put("unexpected WAL status on writer unlock [status=").put(existingEntry.status).put(']');
        }
        existingEntry.status = WalEntry.STATUS_PURGE_EXCLUSIVE;
        return existingEntry;
    }

    private int getTableId(@NotNull CharSequence tableDirName) {
        return tableIds.computeIfAbsent(tableDirName, getNextTableIdFn);
    }

    private void unlockWriter(long walKey) {
        this.entries.compute(walKey, unlockWriterFn);
    }

    // Context for passing parameters to non-capturing lambdas via ThreadLocal
    private static class OpContext {
        int minSegmentId;
    }

    // A WalEntry stores the lock and state for a specific WAL directory.
    private static class WalEntry {
        // There is an active writer, purge cannot take exclusive lock.
        private static final int STATUS_ACTIVE_WRITER = 2;
        // The purge job has an exclusive lock on the WAL, no writers are allowed.
        private static final int STATUS_PURGE_EXCLUSIVE = 1;
        // A writer is acquiring the lock but purge already holds exclusive access over it.
        private static final int STATUS_WRITER_ACQUIRING = 0;
        // Both purge and writer are active.
        private static final int STATUS_WRITER_PURGE = 3;
        private final Semaphore lock = new Semaphore(1);
        int minSegmentId = Integer.MAX_VALUE;
        // Solely used when a writer is acquiring the lock while purge is active.
        int nextMinSegmentId = Integer.MAX_VALUE;
        long walKey = -1;
        private int status = STATUS_WRITER_ACQUIRING;

        public boolean hasSegment(int segmentId) {
            return segmentId >= minSegmentId;
        }

        public void of(long walKey, int status, int minSegmentId) {
            this.walKey = walKey;
            this.status = status;
            this.minSegmentId = minSegmentId;
            this.nextMinSegmentId = -1;
        }
    }
}
