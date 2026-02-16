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

import io.questdb.cairo.TableToken;
import io.questdb.std.QuietCloseable;


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
 * Thread safety: All methods are thread-safe and can be called concurrently.
 * It is the caller's responsibility to ensure that the same WAL directory is not
 * locked by multiple writers or purge jobs concurrently.
 */
public interface WalLocker extends QuietCloseable {
    /**
     * Clears all the existing locks and gives up all held resources.
     * <p>
     * This method should only be used during testing or shutdown scenarios.
     * Calling this while locks are held may lead to inconsistent state.
     */
    void clear();

    /**
     * Drop a table ID mapping. Should be called when a table is dropped and no more writers/purge jobs
     * will access its WAL directories.
     *
     * @param token the table token
     */
    void clearTable(TableToken token);

    /**
     * Checks if a specific WAL segment is currently locked.
     *
     * @param token     the table token
     * @param walId     the WAL identifier (e.g., 1 for wal1)
     * @param segmentId the segment identifier within the WAL
     * @return {@code true} if the segment is locked, {@code false} otherwise
     */
    boolean isSegmentLocked(TableToken token, int walId, int segmentId);

    /**
     * Checks if a WAL directory is currently locked.
     *
     * @param token the table token
     * @param walId the WAL identifier (e.g., 1 for wal1)
     * @return {@code true} if the WAL is locked, {@code false} otherwise
     */
    boolean isWalLocked(TableToken token, int walId);

    /**
     * Locks the purge lock on the specified WAL directory.
     * This will try to take an exclusive lock on the WAL, blocking any writers and returning {@link WalUtils#SEG_NONE_ID}.
     * If an active writer is present, this method will take a shared lock with the writer and return the maximum segment ID
     * that can be safely purged.
     *
     * @param token the table token
     * @param walId the WAL identifier (e.g., 1 for wal1)
     * @return the maximum segment ID that can be safely purged, or {@link WalUtils#SEG_NONE_ID} if exclusive lock is held
     * and the whole WAL can be purged.
     */
    int lockPurge(TableToken token, int walId);

    /**
     * Locks the writer lock on the specified WAL directory.
     * This will block if a purge job is currently holding an exclusive lock on the WAL.
     * The caller must ensure to call {@link #unlockWriter(TableToken, int)} to release the lock.
     *
     * @param token        the table token
     * @param walId        the WAL identifier (e.g., 1 for wal1)
     * @param minSegmentId the minimum segment ID that the writer will be working with
     */
    void lockWriter(TableToken token, int walId, int minSegmentId);

    /**
     * Sets the minimum segment ID for the specified WAL directory.
     * As the purge job may also hold the lock, the new minimum segment ID must be
     * greater than or equal to the current minimum segment ID.
     *
     * @param token           the table token
     * @param walId           the WAL identifier (e.g., 1 for wal1)
     * @param newMinSegmentId the new minimum segment ID to set
     */
    void setWalSegmentMinId(TableToken token, int walId, int newMinSegmentId);

    /**
     * Unlock the purge lock on the specified WAL directory.
     * This will either release the lock if no writers are active,
     * or downgrade the lock to active writer if a writer is active/waiting.
     *
     * @param token the table token
     * @param walId the WAL identifier (e.g., 1 for wal1)
     */
    void unlockPurge(TableToken token, int walId);

    /**
     * Unlock the writer lock on the specified WAL directory.
     * This will either release the lock if no purge is active,
     * or downgrade the lock to purge exclusive if a purge is also active.
     *
     * @param token the table token
     * @param walId the WAL identifier (e.g., 1 for wal1)
     */
    void unlockWriter(TableToken token, int walId);
}
