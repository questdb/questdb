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

package io.questdb.cairo.wal;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.ConcurrentHashMap;
import io.questdb.std.ThreadLocal;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.TestOnly;

import java.util.concurrent.Semaphore;

/**
 * Manages in-memory locks for WAL directories and segments.
 * This provides proper synchronization between Java and Rust code
 * for WAL operations, replacing file-based locking which has
 * platform-specific issues on Windows.
 */
public class WALSegmentLockManager {
    private static final Log LOG = LogFactory.getLog(WALSegmentLockManager.class);
    // Use same sentinel as Rust for WAL directory locks (not actual segments)
    private static final int WAL_LOCK_SENTINEL = WalUtils.SEG_NONE_ID;
    private static final ThreadLocal<StringSink> sinks = new ThreadLocal<>(StringSink::new);
    private final ConcurrentHashMap<Semaphore> locks = new ConcurrentHashMap<>();

    @TestOnly
    public boolean isSegmentLocked(TableToken tableToken, int walId, int segmentId) {
        final CharSequence key = makeKey(tableToken, walId, segmentId);
        Semaphore lock = locks.get(key);
        if (lock == null) {
            return false;
        }
        return lock.availablePermits() == 0;
    }

    @TestOnly
    public boolean isWalLocked(TableToken tableToken, int walId) {
        final CharSequence key = makeKey(tableToken, walId, WAL_LOCK_SENTINEL);
        Semaphore lock = locks.get(key);
        if (lock == null) {
            return false;
        }
        return lock.availablePermits() == 0;
    }

    // Lock specific segment
    public void lockSegment(TableToken tableToken, int walId, int segmentId) {
        final CharSequence key = makeKey(tableToken, walId, segmentId);
        Semaphore lock = locks.computeIfAbsent(key, k -> new Semaphore(1));
        LOG.debug().$("locking WAL segment [key=").$(key).I$();
        try {
            lock.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw CairoException.critical(0)
                    .put("Interrupted while acquiring WAL segment lock [key=").put(key).put(']');
        }
        LOG.debug().$("locked WAL segment [key=").$(key).$(", semaphore=").$(lock).I$();
    }

    // Lock entire WAL directory
    public void lockWal(TableToken tableToken, int walId) {
        final CharSequence key = makeKey(tableToken, walId, WAL_LOCK_SENTINEL);
        Semaphore lock = locks.computeIfAbsent(key, k -> new Semaphore(1));
        LOG.debug().$("locking WAL [key=").$(key).I$();
        try {
            lock.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw CairoException.critical(0)
                    .put("Interrupted while acquiring WAL lock [key=").put(key).put(']');
        }
        LOG.debug().$("locked WAL [key=").$(key).$(", semaphore=").$(lock).I$();
    }

    public boolean tryLockSegment(TableToken tableToken, int walId, int segmentId) {
        final CharSequence key = makeKey(tableToken, walId, segmentId);
        Semaphore lock = locks.computeIfAbsent(key, k -> new Semaphore(1));
        boolean locked = lock.tryAcquire();
        if (locked) {
            LOG.debug().$("lock WAL segment [key=").$(key).$(", semaphore=").$(lock).I$();
        } else {
            LOG.debug().$("fail to lock WAL segment [key=").$(key).I$();
        }
        return locked;
    }

    public boolean tryLockWal(TableToken tableToken, int walId) {
        final CharSequence key = makeKey(tableToken, walId, WAL_LOCK_SENTINEL);
        Semaphore lock = locks.computeIfAbsent(key, k -> new Semaphore(1));
        boolean locked = lock.tryAcquire();
        if (locked) {
            LOG.debug().$("lock WAL [key=").$(key).$(", semaphore=").$(lock).I$();
        } else {
            LOG.debug().$("fail to lock WAL [key=").$(key).I$();
        }
        return locked;
    }

    public void unlockSegment(TableToken tableToken, int walId, int segmentId) {
        final CharSequence key = makeKey(tableToken, walId, segmentId);
        Semaphore lock = locks.get(key);
        if (lock != null) {
            LOG.debug().$("unlock WAL segment [key=").$(key).I$();
            lock.release();
        } else {
            LOG.debug().$("fail to unlock WAL segment: no lock [key=").$(key).I$();
        }
    }

    public void unlockWal(TableToken tableToken, int walId) {
        final CharSequence key = makeKey(tableToken, walId, WAL_LOCK_SENTINEL);
        Semaphore lock = locks.get(key);
        if (lock != null) {
            LOG.debug().$("unlock WAL [key=").$(key).I$();
            lock.release();
        } else {
            LOG.debug().$("fail to unlock WAL: no lock [key=").$(key).I$();
        }
    }

    private static CharSequence makeKey(TableToken tableToken, int walId, int segmentId) {
        final StringSink sink = sinks.get();
        sink.clear();
        sink.put(tableToken.getTableId()).put('/').put(walId).put('/').put(segmentId);
        return sink;
    }
}
