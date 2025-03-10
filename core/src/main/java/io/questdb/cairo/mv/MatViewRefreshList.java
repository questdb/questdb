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

package io.questdb.cairo.mv;

import io.questdb.cairo.TableToken;
import io.questdb.std.ObjList;
import io.questdb.std.ReadOnlyObjList;
import io.questdb.std.SimpleReadWriteLock;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * Refresh list serves two purposes. First, it holds the list of mat view tokens for the given
 * base table (or base mat view). The list is protected with a R/W mutex, so it can be read
 * concurrently. Second, it tracks the last base table txn for which a notification message
 * was sent to one of {@link MatViewRefreshJob}s. The goal here is to avoid sending excessive
 * incremental refresh messages to the underlying queue.
 */
public class MatViewRefreshList {
    // Flips to negative value once a refresh message is processed.
    // Long.MIN_VALUE stands for "just invalidated" state.
    private final AtomicLong lastNotifiedBaseTableTxn = new AtomicLong(0);
    private final ReadWriteLock lock = new SimpleReadWriteLock();
    private final ObjList<TableToken> matViews = new ObjList<>();

    // Called by refresh job once it invalidated dependent mat views.
    public void notifyOnBaseInvalidated() {
        lastNotifiedBaseTableTxn.set(Long.MIN_VALUE);
    }

    // Called by WAL apply job once it applied transaction(s) to the base table
    // and wants to send refresh job an incremental refresh message.
    public boolean notifyOnBaseTableCommitNoLock(long seqTxn) {
        long lastNotified;
        boolean retry;
        do {
            lastNotified = lastNotifiedBaseTableTxn.get();
            retry = Math.abs(lastNotified) < seqTxn && !lastNotifiedBaseTableTxn.compareAndSet(lastNotified, seqTxn);
        } while (retry);
        // The job is allowed to send a notification once incremental refresh finishes.
        return lastNotified <= 0;
    }

    // Called by refresh job once it incrementally refreshed dependent mat views.
    public boolean notifyOnBaseTableRefreshedNoLock(long seqTxn) {
        // Flip the sign bit in the last notified base table txn number.
        lastNotifiedBaseTableTxn.compareAndSet(seqTxn, -seqTxn);
        long lastNotified = lastNotifiedBaseTableTxn.get();
        // No need to notify if the view was just invalidated or there is no newer txn.
        return lastNotified != Long.MIN_VALUE && lastNotified != -seqTxn;
    }

    ReadOnlyObjList<TableToken> lockForRead() {
        lock.readLock().lock();
        return matViews;
    }

    ObjList<TableToken> lockForWrite() {
        lock.writeLock().lock();
        return matViews;
    }

    void unlockAfterRead() {
        lock.readLock().unlock();
    }

    void unlockAfterWrite() {
        lock.writeLock().unlock();
    }
}
