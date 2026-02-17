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

package io.questdb.test.cairo.mv;

import io.questdb.cairo.mv.MatViewStateStoreImpl;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class BaseTableMatViewStateTest extends AbstractCairoTest {

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
        int commits = rnd.nextInt(1_000_000);
        System.out.println("commits: " + commits);

        CyclicBarrier barrier = new CyclicBarrier(baseCommitThreads + baseRefreshThreads);
        ObjList<Thread> threads = new ObjList<>();

        for (int i = 0; i < baseCommitThreads; i++) {
            Thread t = new Thread(() -> {
                try {
                    barrier.await();
                    long nextTxn;
                    while ((nextTxn = seqTxn.incrementAndGet()) < commits) {
                        if (MatViewStateStoreImpl.notifyBaseTableCommit(lastNotifiedTxn, nextTxn)) {
                            long refreshNot = refreshNotification.incrementAndGet();
                            System.out.println("refresh notification:" + refreshNot + " added on commit " + nextTxn);
                        }
                    }
                } catch (Throwable e) {
                    e.printStackTrace(System.out);
                }
            });
            threads.add(t);
            t.start();
        }

        for (int i = 0; i < baseRefreshThreads; i++) {
            Thread t = new Thread(() -> {
                try {
                    barrier.await();
                    while (true) {
                        long notification = refreshNotification.get();
                        if (notification > refreshNotificationProcessed.get()) {
                            long refreshToSeqTxn = Math.min(seqTxn.get(), commits - 1);
                            boolean notifyAgain = MatViewStateStoreImpl.notifyOnBaseTableRefreshed(lastNotifiedTxn, refreshToSeqTxn);
                            System.out.println("notification " + notification + " processed, refreshed to: " + refreshToSeqTxn + " out of " + (commits - 1) + ", will notify again: " + notifyAgain);

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
                    e.printStackTrace(System.out);
                }
            });
            threads.add(t);
            t.start();
        }

        for (int i = 0; i < baseCommitThreads; i++) {
            try {
                threads.getQuick(i).join();
            } catch (InterruptedException e) {
                e.printStackTrace(System.out);
            }
        }
        System.out.println("commit threads complete");

        while (refreshNotification.get() != refreshNotificationProcessed.get()) {
            Os.sleep(1);
        }
        System.out.println("stopping refresh threads");
        stop.set(true);

        for (int i = baseCommitThreads; i < threads.size(); i++) {
            try {
                threads.getQuick(i).join();
            } catch (InterruptedException e) {
                e.printStackTrace(System.out);
            }
        }

        Assert.assertTrue(MatViewStateStoreImpl.notifyBaseTableCommit(lastNotifiedTxn, commits));
    }
}
