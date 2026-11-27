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

package io.questdb.test.cairo.wal;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.O3PartitionJob;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.mv.MatViewRefreshTask;
import io.questdb.cairo.wal.ApplyWal2TableJob;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.mp.Job;
import io.questdb.std.Misc;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A save point request, such as the one a Delta base capture makes, posts one notification. With two apply workers,
 * the second can take it while the first still holds the writer past its check for the request, and drops it: the
 * holder serves the request on its way out instead.
 */
public class WalApplyIdleSavePointRequestTest extends AbstractCairoTest {
    private static volatile Thread heldApplyThread;
    private static volatile CountDownLatch writerHeld;
    private static volatile CountDownLatch writerReleaseAllowed;

    @BeforeClass
    public static void setUpStatic() throws Exception {
        // The apply worker in heldApplyThread stops after it has applied its batch, checked for a save point request
        // and drained the writer's command queue, while it still holds the writer.
        AbstractCairoTest.engineFactory = conf -> new CairoEngine(conf) {
            @Override
            public void notifyMatViewBaseTableCommit(MatViewRefreshTask task, long seqTxn) {
                super.notifyMatViewBaseTableCommit(task, seqTxn);
                if (Thread.currentThread() == heldApplyThread) {
                    writerHeld.countDown();
                    try {
                        Assert.assertTrue(writerReleaseAllowed.await(1, TimeUnit.MINUTES));
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        };
        AbstractCairoTest.setUpStatic();
    }

    @Test
    public void testRequestDroppedOnBusyWriterIsServedByHolder() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00.000000Z', 1)");
            drainWalQueue();
            final TableToken tt = engine.verifyTableName("t");
            final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(tt);
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:01.000000Z', 2)");

            writerHeld = new CountDownLatch(1);
            writerReleaseAllowed = new CountDownLatch(1);
            final AtomicReference<Throwable> holderFailure = new AtomicReference<>();
            try (
                    ApplyWal2TableJob holderJob = new ApplyWal2TableJob(engine, 0);
                    ApplyWal2TableJob otherJob = new ApplyWal2TableJob(engine, 0)
            ) {
                final Thread holder = new Thread(() -> {
                    try {
                        holderJob.run(Job.RUNNING_STATUS);
                    } catch (Throwable th) {
                        holderFailure.set(th);
                    } finally {
                        writerHeld.countDown();
                        Path.clearThreadLocals();
                        Misc.free(O3PartitionJob.THREAD_LOCAL_CLEANER);
                    }
                });
                heldApplyThread = holder;
                holder.start();
                try {
                    Assert.assertTrue(writerHeld.await(1, TimeUnit.MINUTES));
                    // What a Delta base capture does once the holder is past its check for a request.
                    tracker.setDurableEpochCompletionRequired(true);
                    tracker.requestIdleEpoch(engine.getConfiguration().getMicrosecondClock().getTicks() / 1000L);
                    engine.notifyWalTxnCommitted(tt);
                    // The second worker takes the notification and finds the writer busy.
                    Assert.assertTrue("the other job must take the notification", otherJob.run(Job.RUNNING_STATUS));
                    Assert.assertTrue("the request is still pending", tracker.isIdleEpochRequested());
                } finally {
                    writerReleaseAllowed.countDown();
                    holder.join();
                    heldApplyThread = null;
                }
                Assert.assertNull(holderFailure.get());
                otherJob.drain(0);
            }
            Assert.assertFalse("the request must be served", tracker.isIdleEpochRequested());
            Assert.assertFalse("the save points must be completed", tracker.isDurableEpochCompletionRequired());
            assertQuery("SELECT count() FROM t")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n2\n");
        });
    }
}
