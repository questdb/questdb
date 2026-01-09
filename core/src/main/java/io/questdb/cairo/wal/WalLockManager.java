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
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.ConcurrentHashMap;
import io.questdb.std.ThreadLocal;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;

import java.util.concurrent.Semaphore;

/**
 * Manages in-memory locks for WAL directories and segments.
 * <p>
 * This provides proper synchronization between Java and Rust code
 * for WAL operations, replacing file-based locking which has
 * platform-specific issues on Windows.
 * <p>
 * The manager maintains two types of locks:
 * <ul>
 *   <li><b>WAL locks</b> - Protect an entire WAL directory (e.g., wal1, wal2)</li>
 *   <li><b>Segment locks</b> - Protect individual segments within a WAL (e.g., wal1/0, wal1/1)</li>
 * </ul>
 * <p>
 * Locks are implemented using {@link Semaphore} with a single permit, providing
 * mutual exclusion. The blocking methods ({@link #lockWal}, {@link #lockSegment})
 * will wait indefinitely until the lock becomes available, while the try methods
 * ({@link #tryLockWal}, {@link #tryLockSegment}) return immediately.
 * <p>
 * Thread safety: All methods are thread-safe and can be called concurrently.
 */
public class WalLockManager {
    private static final Log LOG = LogFactory.getLog(WalLockManager.class);
    private static final ThreadLocal<StringSink> sinks = new ThreadLocal<>(StringSink::new);
    private final ConcurrentHashMap<Semaphore> locks = new ConcurrentHashMap<>();

    /**
     * Checks if a specific WAL segment is currently locked.
     *
     * @param tableDirName the table directory name identifying the table
     * @param walId      the WAL identifier (e.g., 1 for wal1)
     * @param segmentId  the segment identifier within the WAL
     * @return {@code true} if the segment is locked, {@code false} otherwise
     */
    @TestOnly
    public boolean isSegmentLocked(@NotNull CharSequence tableDirName, int walId, int segmentId) {
        final CharSequence key = makeKey(tableDirName, walId, segmentId);
        Semaphore lock = locks.get(key);
        if (lock == null) {
            return false;
        }
        return lock.availablePermits() == 0;
    }

    /**
     * Checks if a WAL directory is currently locked.
     *
     * @param tableDirName the table directory name identifying the table
     * @param walId      the WAL identifier (e.g., 1 for wal1)
     * @return {@code true} if the WAL is locked, {@code false} otherwise
     */
    @TestOnly
    public boolean isWalLocked(@NotNull CharSequence tableDirName, int walId) {
        final CharSequence key = makeKey(tableDirName, walId);
        Semaphore lock = locks.get(key);
        if (lock == null) {
            return false;
        }
        return lock.availablePermits() == 0;
    }

    /**
     * Acquires a lock on a specific WAL segment, blocking until available.
     * <p>
     * This method blocks indefinitely until the lock can be acquired.
     * Use {@link #tryLockSegment} for non-blocking acquisition.
     *
     * @param tableDirName the table directory name identifying the table
     * @param walId      the WAL identifier (e.g., 1 for wal1)
     * @param segmentId  the segment identifier within the WAL
     * @throws CairoException if the thread is interrupted while waiting
     */
    public void lockSegment(@NotNull CharSequence tableDirName, int walId, int segmentId) {
        final CharSequence key = makeKey(tableDirName, walId, segmentId);
        Semaphore lock = locks.computeIfAbsent(key, k -> new Semaphore(1));
        LOG.debug().$("locking WAL segment [table=").$(tableDirName)
                .$(", wal=").$(walId)
                .$(", segment=").$(segmentId)
                .$(", semaphore=").$(lock)
                .I$();
        try {
            lock.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw CairoException.critical(0)
                    .put("Interrupted while acquiring WAL segment lock [table=").put(tableDirName)
                    .put(", wal=").put(walId)
                    .put(", segment=").put(segmentId).put(']');
        }
        LOG.debug().$("locked WAL segment [table=").$(tableDirName)
                .$(", wal=").$(walId)
                .$(", segment=").$(segmentId)
                .$(", semaphore=").$(lock)
                .I$();
    }

    /**
     * Acquires a lock on an entire WAL directory, blocking until available.
     * <p>
     * This method blocks indefinitely until the lock can be acquired.
     * Use {@link #tryLockWal} for non-blocking acquisition.
     *
     * @param tableDirName the table directory name identifying the table
     * @param walId      the WAL identifier (e.g., 1 for wal1)
     * @throws CairoException if the thread is interrupted while waiting
     */
    public void lockWal(@NotNull CharSequence tableDirName, int walId) {
        final CharSequence key = makeKey(tableDirName, walId);
        Semaphore lock = locks.computeIfAbsent(key, k -> new Semaphore(1));
        LOG.debug().$("locking WAL [table=").$(tableDirName).$(", wal=").$(walId)
                .$(", semaphore=").$(lock).I$();
        try {
            lock.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw CairoException.critical(0)
                    .put("Interrupted while acquiring WAL lock [table=").put(tableDirName)
                    .put(", wal=").put(walId).put(']');
        }
        LOG.debug().$("locked WAL [table=").$(tableDirName).$(", wal=").$(walId)
                .$(", semaphore=").$(lock).I$();
    }

    /**
     * Clears all locks from the manager.
     * <p>
     * This method should only be used during testing or shutdown scenarios.
     * Calling this while locks are held may lead to inconsistent state.
     */
    public void reset() {
        locks.clear();
    }

    /**
     * Attempts to acquire a lock on a specific WAL segment without blocking.
     *
     * @param tableDirName the table directory name identifying the table
     * @param walId      the WAL identifier (e.g., 1 for wal1)
     * @param segmentId  the segment identifier within the WAL
     * @return {@code true} if the lock was acquired, {@code false} if already held
     */
    public boolean tryLockSegment(@NotNull CharSequence tableDirName, int walId, int segmentId) {
        final CharSequence key = makeKey(tableDirName, walId, segmentId);
        Semaphore lock = locks.computeIfAbsent(key, k -> new Semaphore(1));
        boolean locked = lock.tryAcquire();
        if (locked) {
            LOG.debug().$("lock WAL segment [table=").$(tableDirName)
                    .$(", wal=").$(walId)
                    .$(", segment=").$(segmentId)
                    .$(", semaphore=").$(lock)
                    .I$();
        } else {
            LOG.debug().$("fail to lock WAL segment [table=").$(tableDirName)
                    .$(", wal=").$(walId)
                    .$(", segment=").$(segmentId)
                    .$(", semaphore=").$(lock)
                    .I$();
        }
        return locked;
    }

    /**
     * Attempts to acquire a lock on a WAL directory without blocking.
     *
     * @param tableDirName the table directory name identifying the table
     * @param walId      the WAL identifier (e.g., 1 for wal1)
     * @return {@code true} if the lock was acquired, {@code false} if already held
     */
    public boolean tryLockWal(@NotNull CharSequence tableDirName, int walId) {
        final CharSequence key = makeKey(tableDirName, walId);
        Semaphore lock = locks.computeIfAbsent(key, k -> new Semaphore(1));
        boolean locked = lock.tryAcquire();
        if (locked) {
            LOG.debug().$("lock WAL [table=").$(tableDirName).$(", wal=").$(walId)
                    .$(", semaphore=").$(lock).I$();
        } else {
            LOG.debug().$("fail to lock WAL [table=").$(tableDirName).$(", wal=").$(walId)
                    .$(", semaphore=").$(lock).I$();
        }
        return locked;
    }

    /**
     * Releases a lock on a specific WAL segment.
     * <p>
     * If the segment was not previously locked, this method logs a debug message
     * but does not throw an exception.
     *
     * @param tableDirName the table directory name identifying the table
     * @param walId      the WAL identifier (e.g., 1 for wal1)
     * @param segmentId  the segment identifier within the WAL
     */
    public void unlockSegment(@NotNull CharSequence tableDirName, int walId, int segmentId) {
        final CharSequence key = makeKey(tableDirName, walId, segmentId);
        Semaphore lock = locks.get(key);
        if (lock != null) {
            LOG.debug().$("unlock WAL segment [table=").$(tableDirName)
                    .$(", wal=").$(walId)
                    .$(", segment=").$(segmentId)
                    .$(", semaphore=").$(lock)
                    .I$();
            lock.release();
        } else {
            LOG.debug()
                    .$("fail to unlock WAL segment: no lock [table=").$(tableDirName)
                    .$(", wal=").$(walId)
                    .$(", segment=").$(segmentId)
                    .I$();
        }
    }

    /**
     * Releases a lock on a WAL directory.
     * <p>
     * If the WAL was not previously locked, this method logs a debug message
     * but does not throw an exception.
     *
     * @param tableDirName the table directory name identifying the table
     * @param walId      the WAL identifier (e.g., 1 for wal1)
     */
    public void unlockWal(@NotNull CharSequence tableDirName, int walId) {
        final CharSequence key = makeKey(tableDirName, walId);
        Semaphore lock = locks.get(key);
        if (lock != null) {
            LOG.debug().$("unlock WAL [table=").$(tableDirName).$(", wal=").$(walId)
                    .$(", semaphore=").$(lock).I$();
            lock.release();
        } else {
            LOG.debug().$("fail to unlock WAL: no lock [table=").$(tableDirName).$(", wal=").$(walId).I$();
        }
    }

    private static CharSequence makeKey(@NotNull CharSequence tableDirName, int walId, int segmentId) {
        final StringSink sink = sinks.get();
        sink.clear();
        sink.put(tableDirName).put('/').put(walId).put('/').put(segmentId);
        return sink;
    }

    private static CharSequence makeKey(@NotNull CharSequence tableDirName, int walId) {
        final StringSink sink = sinks.get();
        sink.clear();
        sink.put(tableDirName).put('/').put(walId);
        return sink;
    }
}
