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
import io.questdb.mp.ConcurrentQueue;
import io.questdb.mp.Queue;
import io.questdb.std.ConcurrentHashMap;
import io.questdb.std.ThreadLocal;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

public class LiveViewStateStoreImpl implements LiveViewStateStore {
    private final Function<CharSequence, AtomicLong> createLastNotifiedTxn;
    // Table name to last notified base-table seqTxn. A positive value means a task is
    // already queued (or in flight) for that txn; the refresh job flips the sign via
    // notifyBaseRefreshed() once the view catches up, which reopens the gate for the
    // next notification. Entries are never removed (grow-only, bounded by distinct
    // base tables that ever had a live view).
    private final ConcurrentHashMap<AtomicLong> lastNotifiedTxnByTableName = new ConcurrentHashMap<>(false);
    private final ThreadLocal<LiveViewRefreshTask> taskHolder = new ThreadLocal<>(LiveViewRefreshTask::new);
    private final Queue<LiveViewRefreshTask> taskQueue = ConcurrentQueue.createConcurrentQueue(LiveViewRefreshTask::new);

    public LiveViewStateStoreImpl() {
        this.createLastNotifiedTxn = name -> new AtomicLong();
    }

    // kept public for tests
    public static boolean notifyBaseTableCommit(AtomicLong lastNotifiedBaseTableTxn, long seqTxn) {
        long lastNotified;
        boolean retry;
        do {
            lastNotified = lastNotifiedBaseTableTxn.get();
            retry = Math.abs(lastNotified) < seqTxn && !lastNotifiedBaseTableTxn.compareAndSet(lastNotified, seqTxn);
        } while (retry);
        // The job is allowed to send a notification once incremental refresh finishes.
        return lastNotified <= 0;
    }

    // kept public for tests
    // Returns the current lastNotified txn if a newer txn landed while we were refreshing
    // (so the caller should re-enqueue); returns 0 if there is nothing newer to process.
    public static long notifyOnBaseTableRefreshed(AtomicLong lastNotifiedTxn, long seqTxn) {
        // Flip the sign bit in the last notified base table txn number.
        lastNotifiedTxn.compareAndSet(seqTxn, -seqTxn);
        long lastNotified = lastNotifiedTxn.get();
        // Re-enqueue only if a newer txn landed while we were refreshing.
        return lastNotified > 0 ? lastNotified : 0;
    }

    @Override
    @org.jetbrains.annotations.TestOnly
    public void clear() {
        taskQueue.clear();
        lastNotifiedTxnByTableName.clear();
    }

    @Override
    public void close() {
        // Queues and counters are GC-managed; nothing to free.
    }

    @Override
    public void enqueueForceDrain(TableToken baseTableToken) {
        final AtomicLong lastNotifiedBaseTableTxn = lastNotifiedTxnByTableName.get(baseTableToken.getTableName());
        if (lastNotifiedBaseTableTxn == null) {
            // No live view depends on this base table.
            return;
        }
        final LiveViewRefreshTask task = taskHolder.get();
        task.clear();
        task.baseTableToken = baseTableToken;
        // Use the absolute value so the refresh job sees the most recent seqTxn it already
        // processed; the refresh path will take the idle-flush branch.
        task.seqTxn = Math.abs(lastNotifiedBaseTableTxn.get());
        task.forceDrain = true;
        taskQueue.enqueue(task);
    }

    @Override
    public void notifyBaseRefreshed(LiveViewRefreshTask task, long seqTxn) {
        final AtomicLong lastNotifiedTxn = lastNotifiedTxnByTableName.get(task.baseTableToken.getTableName());
        if (lastNotifiedTxn == null) {
            return;
        }
        long latestTxn = notifyOnBaseTableRefreshed(lastNotifiedTxn, seqTxn);
        if (latestTxn > 0) {
            // Another txn landed while we were refreshing. Re-enqueue with the latest seqTxn
            // so a worker picks the view up and processes the newer commits.
            task.seqTxn = latestTxn;
            taskQueue.enqueue(task);
        }
    }

    @Override
    public void notifyBaseTableCommit(TableToken baseTableToken, long seqTxn) {
        final AtomicLong lastNotifiedBaseTableTxn = lastNotifiedTxnByTableName.get(baseTableToken.getTableName());
        if (lastNotifiedBaseTableTxn == null) {
            // No live view depends on this base table.
            return;
        }
        if (notifyBaseTableCommit(lastNotifiedBaseTableTxn, seqTxn)) {
            final LiveViewRefreshTask task = taskHolder.get();
            task.clear();
            task.baseTableToken = baseTableToken;
            task.seqTxn = seqTxn;
            taskQueue.enqueue(task);
        }
    }

    @Override
    public void registerBaseTable(CharSequence baseTableName) {
        lastNotifiedTxnByTableName.computeIfAbsent(baseTableName, createLastNotifiedTxn);
    }

    @Override
    public boolean tryDequeueRefreshTask(LiveViewRefreshTask target) {
        return taskQueue.tryDequeue(target);
    }
}
