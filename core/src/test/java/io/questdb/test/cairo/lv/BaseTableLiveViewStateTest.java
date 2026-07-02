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

package io.questdb.test.cairo.lv;

import io.questdb.cairo.lv.LiveViewStateStoreImpl;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Concurrency test for the live-view base-table notification gate - the twin of
 * {@code BaseTableMatViewStateTest}. Live views run one refresh worker per pool
 * worker (2-4 by default, {@code ServerMain.setupLiveViewJobs}), so the gate
 * primitives {@link LiveViewStateStoreImpl#notifyBaseTableCommit} and
 * {@link LiveViewStateStoreImpl#notifyOnBaseTableRefreshed} are hit concurrently by
 * many committer threads (on each base WAL commit) and many refresh threads (once a
 * view catches up). The gate must never lose a notification: after every commit is
 * drained and every refresh processed, the gate has to be reopened so a fresh commit
 * is again allowed to enqueue.
 * <p>
 * The sign convention: a positive stored value means a task is queued / in flight; a
 * refresh flips the sign negative to reopen the gate. The one API difference from the
 * mat-view twin is the return of {@code notifyOnBaseTableRefreshed}: the live-view
 * version returns the latest txn ({@code > 0}) when a newer commit landed mid-refresh
 * (so the caller must re-enqueue) or {@code 0} when the gate reopened cleanly, where
 * the mat-view version returns a boolean.
 */
public class BaseTableLiveViewStateTest extends AbstractCairoTest {

    @Test
    public void testNoMissingNotifications() {
        Rnd rnd = TestUtils.generateRandom(LOG);
        int baseCommitThreads = 1 + rnd.nextInt(4);
        int baseRefreshThreads = 1 + rnd.nextInt(4);
        AtomicLong seqTxn = new AtomicLong();
        AtomicLong refreshNotification = new AtomicLong();
        AtomicLong refreshNotificationProcessed = new AtomicLong();
        AtomicBoolean stop = new AtomicBoolean();

        AtomicLong lastNotifiedTxn = new AtomicLong();
        int commits = 1 + rnd.nextInt(1_000_000);

        CyclicBarrier barrier = new CyclicBarrier(baseCommitThreads + baseRefreshThreads);
        ObjList<Thread> threads = new ObjList<>();
        ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();

        // Committer threads: each grabs the next base seqTxn and, when the gate says
        // it may (an idle, non-positive sign), raises a refresh notification.
        for (int i = 0; i < baseCommitThreads; i++) {
            Thread t = new Thread(() -> {
                try {
                    barrier.await();
                    long nextTxn;
                    while ((nextTxn = seqTxn.incrementAndGet()) < commits) {
                        if (LiveViewStateStoreImpl.notifyBaseTableCommit(lastNotifiedTxn, nextTxn)) {
                            refreshNotification.incrementAndGet();
                        }
                    }
                } catch (Throwable e) {
                    errors.add(e);
                }
            }, "lv-committer-" + i);
            threads.add(t);
            t.start();
        }

        // Refresh threads: whenever an unprocessed notification exists, refresh up to
        // the latest committed seqTxn and reopen the gate; if a newer commit landed
        // meanwhile the gate returns it, so re-notify. Stops once ingestion is done
        // and there is nothing left to process.
        for (int i = 0; i < baseRefreshThreads; i++) {
            Thread t = new Thread(() -> {
                try {
                    barrier.await();
                    while (true) {
                        long notification = refreshNotification.get();
                        if (notification > refreshNotificationProcessed.get()) {
                            long refreshToSeqTxn = Math.min(seqTxn.get(), commits - 1);
                            boolean notifyAgain = LiveViewStateStoreImpl.notifyOnBaseTableRefreshed(lastNotifiedTxn, refreshToSeqTxn) > 0;

                            long processed;
                            do {
                                processed = refreshNotificationProcessed.get();
                            } while (processed < notification && !refreshNotificationProcessed.compareAndSet(processed, notification));

                            if (notifyAgain) {
                                refreshNotification.incrementAndGet();
                            }
                        } else if (stop.get() && refreshNotification.get() == refreshNotificationProcessed.get()) {
                            break;
                        }
                    }
                } catch (Throwable e) {
                    errors.add(e);
                }
            }, "lv-refresher-" + i);
            threads.add(t);
            t.start();
        }

        for (int i = 0; i < baseCommitThreads; i++) {
            try {
                threads.getQuick(i).join();
            } catch (InterruptedException e) {
                errors.add(e);
            }
        }

        // Let the refreshers drain every outstanding notification before stopping.
        while (refreshNotification.get() != refreshNotificationProcessed.get()) {
            Os.sleep(1);
        }
        stop.set(true);

        for (int i = baseCommitThreads; i < threads.size(); i++) {
            try {
                threads.getQuick(i).join();
            } catch (InterruptedException e) {
                errors.add(e);
            }
        }

        if (!errors.isEmpty()) {
            throw new RuntimeException("worker thread failed", errors.peek());
        }

        // Every notification was processed, so the gate must be reopened: a fresh
        // commit is allowed to enqueue again. A lost notification would leave the
        // sign positive here and fail this assertion.
        Assert.assertTrue(LiveViewStateStoreImpl.notifyBaseTableCommit(lastNotifiedTxn, commits));
    }
}
