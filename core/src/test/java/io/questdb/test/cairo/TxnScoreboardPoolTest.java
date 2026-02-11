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

package io.questdb.test.cairo;

import io.questdb.cairo.TableToken;
import io.questdb.cairo.TxnScoreboard;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.test.tools.TestUtils.generateRandom;

public class TxnScoreboardPoolTest extends AbstractCairoTest {

    @Test
    public void testConcurrentRelease() throws Exception {
        assertMemoryLeak(() -> {
            Rnd rnd = generateRandom(LOG);
            int threadCount = 2 + rnd.nextInt(4);
            int iterations = 500 + rnd.nextInt(2000);
            SOCountDownLatch latch = new SOCountDownLatch(threadCount);
            ObjList<Thread> threads = new ObjList<>();
            AtomicBoolean stopped = new AtomicBoolean();

            engine.execute("create table x (i int)");
            TableToken token = engine.verifyTableName("x");
            AtomicInteger errors = new AtomicInteger();

            for (int i = 0; i < threadCount; i++) {
                int thread = i;
                threads.add(
                        new Thread(() -> {
                            latch.countDown();

                            try {
                                for (int it = 0; it < iterations; it++) {
                                    try (TxnScoreboard sc1 = engine.getTxnScoreboard(token)) {
                                        if (sc1.acquireTxn(thread, it)) {
                                            try (TxnScoreboard sc2 = engine.getTxnScoreboard(token)) {
                                                if (sc2.isTxnAvailable(it)) {
                                                    LOG.error().$("=== error: iteration").$(it)
                                                            .$(" thread=").$(thread)
                                                            .$(", sc1=").$(System.identityHashCode(sc1))
                                                            .$(", sc2=").$(System.identityHashCode(sc2))
                                                            .$();
                                                    errors.incrementAndGet();
                                                }
                                            }
                                            sc1.releaseTxn(thread, it);
                                        }
                                    }
                                    Os.pause();
                                }
                            } finally {
                                Path.clearThreadLocals();
                            }
                        })
                );
                threads.getLast().start();
            }

            threads.add(
                    new Thread(() -> {
                        latch.countDown();
                        while (!stopped.get()) {
                            engine.getTxnScoreboardPool().releaseInactive();
                        }
                    })
            );
            threads.getLast().start();

            for (int i = 0; i < threadCount; i++) {
                threads.getQuick(i).join();
            }
            stopped.set(true);
            threads.getLast().join();

            Assert.assertEquals(0, errors.get());
        });
    }

    @Test
    public void testDelayedCloseOnClear() throws Exception {
        assertMemoryLeak(() -> {
            engine.execute("create table x (i int)");
            TableToken token = engine.verifyTableName("x");
            TxnScoreboard sc1 = engine.getTxnScoreboard(token);

            engine.getTxnScoreboardPool().clear();

            Assert.assertTrue(sc1.acquireTxn(0, 10));

            sc1.close();
        });
    }

    @Test
    public void testWalTableRename() throws Exception {
        assertMemoryLeak(() -> {
            engine.execute("create table x (i int, ts timestamp) timestamp(ts) PARTITION BY DAY WAL");
            TableToken token = engine.verifyTableName("x");

            TxnScoreboard sc1 = engine.getTxnScoreboard(token);
            Assert.assertTrue(sc1.acquireTxn(0, 10));

            engine.execute("rename table x to x1");
            engine.execute("create table x (i int, ts timestamp) timestamp(ts) PARTITION BY DAY WAL");
            TableToken token2 = engine.verifyTableName("x");

            TxnScoreboard sc2 = engine.getTxnScoreboard(token2);
            Assert.assertTrue(sc2.isRangeAvailable(0, 10));
            Assert.assertTrue(sc2.acquireTxn(0, 10));
            Assert.assertTrue(sc2.acquireTxn(1, 11));

            Assert.assertFalse(sc1.isRangeAvailable(0, 11));
            try (TxnScoreboard sc3 = engine.getTxnScoreboard(token)) {
                Assert.assertFalse(sc3.isRangeAvailable(0, 11));
                sc3.releaseTxn(0, 10);
            }
            Assert.assertTrue(sc1.isRangeAvailable(0, 11));


            sc1.close();
            sc2.close();
        });
    }
}
