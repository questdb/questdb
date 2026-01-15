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

import io.questdb.std.ConcurrentLongHashMap;
import io.questdb.std.QuietCloseable;
import io.questdb.std.str.DirectUtf8Sequence;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;

import java.util.concurrent.Semaphore;

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
public class WalLockManager implements QuietCloseable {
    private final WalLocker locker;

    public WalLockManager(WalLocker locker) {
        this.locker = locker;
    }

    @Override
    public void close() {
        locker.close();
    }

    /**
     * Drop a table ID mapping. Should be called when a table is dropped and no more writers/purge jobs
     * will access its WAL directories.
     *
     * @param tableDirName the table directory name identifying the table
     */
    public void dropTable(@NotNull DirectUtf8Sequence tableDirName) {
        locker.purgeTable(tableDirName);
    }

    @SuppressWarnings("unused")
    public WalLocker getWalLocker() {
        return locker;
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
    public boolean isSegmentLocked(@NotNull DirectUtf8Sequence tableDirName, int walId, int segmentId) {
        return locker.isSegmentLocked(tableDirName, walId, segmentId);
    }

    /**
     * Checks if a WAL directory is currently locked.
     *
     * @param tableDirName the table directory name identifying the table
     * @param walId        the WAL identifier (e.g., 1 for wal1)
     * @return {@code true} if the WAL is locked, {@code false} otherwise
     */
    @TestOnly
    public boolean isWalLocked(@NotNull DirectUtf8Sequence tableDirName, int walId) {
        return locker.isWalLocked(tableDirName, walId);
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
    public int lockPurge(@NotNull DirectUtf8Sequence tableDirName, int walId) {
        // Race safety: minSegmentId is only mutated in 2 cases:
        //  - when the purge lock is released: as we assume that no 2 purge jobs will run concurrently
        //  on the same WAL, this read is safe.
        //  - when {@link #setWalSegmentMinId(CharSequence, int, int)} is called: this is safe as the caller
        //  must ensure that the new minSegmentId is greater than or equal to the current minSegmentId.
        //  A stale read returns a conservative (lower) max-purgeable ID, which is safe - we may under-purge
        //  but never over-purge.
        final int lockedMinSegmentId = locker.lockPurge(tableDirName, walId);
        if (lockedMinSegmentId == Integer.MAX_VALUE) {
            // We have exclusive lock, whole WAL can be purged
            return WalUtils.SEG_NONE_ID;
        } else {
            return lockedMinSegmentId - 1;
        }
    }

    /**
     * Locks the writer lock on the specified WAL directory.
     * This will block if a purge job is currently holding an exclusive lock on the WAL.
     * The caller must ensure to call {@link #unlockWriter(DirectUtf8Sequence, int)} to release the lock.
     *
     * @param tableDirName the table directory name identifying the table
     * @param walId        the WAL identifier (e.g., 1 for wal1)
     * @param minSegmentId the minimum segment ID that the writer will be working with
     */
    public void lockWriter(@NotNull DirectUtf8Sequence tableDirName, int walId, int minSegmentId) {
        locker.lockWriter(tableDirName, walId, minSegmentId);
    }

    /**
     * Clears all locks from the manager.
     * <p>
     * This method should only be used during testing or shutdown scenarios.
     * Calling this while locks are held may lead to inconsistent state.
     */
    public void reset() {
        locker.clear();
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
    public void setWalSegmentMinId(@NotNull DirectUtf8Sequence tableDirName, int walId, int newMinSegmentId) {
        locker.setWalSegmentMinId(tableDirName, walId, newMinSegmentId);
    }

    /**
     * Unlock the purge lock on the specified WAL directory.
     * This will either release the lock if no writers are active,
     * or downgrade the lock to active writer if a writer is active/waiting.
     *
     * @param tableDirName the table directory name identifying the table
     * @param walId        the WAL identifier (e.g., 1 for wal1)
     */
    public void unlockPurge(@NotNull DirectUtf8Sequence tableDirName, int walId) {
        locker.unlockPurge(tableDirName, walId);
    }

    /**
     * Unlock the writer lock on the specified WAL directory.
     * This will either release the lock if no purge is active,
     * or downgrade the lock to purge exclusive if a purge is also active.
     *
     * @param tableDirName the table directory name identifying the table
     * @param walId        the WAL identifier (e.g., 1 for wal1)
     */
    public void unlockWriter(@NotNull DirectUtf8Sequence tableDirName, int walId) {
        locker.unlockWriter(tableDirName, walId);
    }

    public interface WalLocker extends QuietCloseable {
        void clear();

        boolean isSegmentLocked(DirectUtf8Sequence tableDirName, int walId, int segmentId);

        boolean isWalLocked(DirectUtf8Sequence tableDirName, int walId);

        int lockPurge(DirectUtf8Sequence tableDirName, int walId);

        void lockWriter(DirectUtf8Sequence tableDirName, int walId, int minSegmentId);

        void purgeTable(DirectUtf8Sequence tableDirName);

        void setWalSegmentMinId(DirectUtf8Sequence tableDirName, int walId, int newMinSegmentId);

        void unlockPurge(DirectUtf8Sequence tableDirName, int walId);

        void unlockWriter(DirectUtf8Sequence tableDirName, int walId);
    }
}
