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
import io.questdb.std.SimpleReadWriteLock;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;

public class MatViewRefreshList {
    private final AtomicLong lastCommittedBaseTableTxn = new AtomicLong(0);
    private final ReadWriteLock lock = new SimpleReadWriteLock();
    ObjList<TableToken> matViews = new ObjList<>();

    public boolean notifyOnBaseTableCommitNoLock(long seqTxn) {
        boolean retry;
        long lastNotified;

        do {
            lastNotified = lastCommittedBaseTableTxn.get();
            retry = (Math.abs(lastNotified) < seqTxn) && !lastCommittedBaseTableTxn.compareAndSet(lastNotified, seqTxn);
        } while (retry);

        return lastNotified <= 0;
    }

    public boolean notifyOnBaseTableRefreshedNoLock(long seqTxn) {
        lastCommittedBaseTableTxn.compareAndSet(seqTxn, -seqTxn);
        return lastCommittedBaseTableTxn.get() != -seqTxn;
    }

    void readLock() {
        lock.readLock().lock();
    }

    void unlockRead() {
        lock.readLock().unlock();
    }

    void unlockWrite() {
        lock.writeLock().unlock();
    }

    void writeLock() {
        lock.writeLock().lock();
    }
}
