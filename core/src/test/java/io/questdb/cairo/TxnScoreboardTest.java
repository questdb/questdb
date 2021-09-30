/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.cairo;

import io.questdb.mp.SOCountDownLatch;
import io.questdb.std.FilesFacade;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

public class TxnScoreboardTest extends AbstractCairoTest {
    @Test
    public void testCleanFailsNoResourceLeak() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            FilesFacade ff = new FilesFacadeImpl() {
                @Override
                public long openCleanRW(LPSZ name, long fd) {
                    return -1;
                }
            };

            assertMemoryLeak(() -> {
                try (final Path shmPath = new Path()) {
                    try (TxnScoreboard ignored = new TxnScoreboard(ff, shmPath.of(root), 2048)) {
                        Assert.fail();
                    } catch (CairoException ex) {
                        TestUtils.assertContains(ex.getFlyweightMessage(), "could not open read-write with clean allocation");
                    }
                }
            });
        });
    }

    @Test
    public void testCleanOnExclusiveOpen() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final Path shmPath = new Path()) {
                try (
                        final TxnScoreboard scoreboard = new TxnScoreboard(FilesFacadeImpl.INSTANCE, shmPath.of(root), 1024)
                ) {
                    for (int i = 0; i < 1500; i++) {
                        scoreboard.acquireTxn(i);
                        scoreboard.releaseTxn(i);
                    }
                    Assert.assertEquals(1499, scoreboard.getMin());
                }


                // second open is exclusive, file should be truncated
                try (
                        final TxnScoreboard scoreboard2 = new TxnScoreboard(FilesFacadeImpl.INSTANCE, shmPath.of(root), 2048)
                ) {
                    Assert.assertEquals(0, scoreboard2.getMin());
                }
            }
        });
    }

    @Test
    public void testCleanOnExclusiveOpenLocksFile() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final Path shmPath = new Path()) {
                FilesFacade ff = FilesFacadeImpl.INSTANCE;
                try (
                        final TxnScoreboard scoreboard = new TxnScoreboard(ff, shmPath.of(root), 1024)
                ) {
                    for (int i = 0; i < 1500; i++) {
                        scoreboard.acquireTxn(i);
                        scoreboard.releaseTxn(i);
                    }
                    Assert.assertEquals(1499, scoreboard.getMin());
                }

                // second open is exclusive, file should be truncated
                try (
                        final TxnScoreboard scoreboard2 = new TxnScoreboard(ff, shmPath.of(root), 2048)
                ) {
                    Assert.assertEquals(0, scoreboard2.getMin());
                    for (int i = 0; i < 10; i++) {
                        scoreboard2.acquireTxn(i);
                        scoreboard2.releaseTxn(i);
                    }

                    // This should not obtain exclusive lock even though file was empty when scoreboard2 put shared lock
                    try (
                            final TxnScoreboard scoreboard3 = new TxnScoreboard(ff, shmPath.of(root), 2048)
                    ) {
                        Assert.assertEquals(9, scoreboard2.getMin());
                        Assert.assertEquals(9, scoreboard3.getMin());
                    }

                }
            }
        });
    }

    @Test
    public void testCrawl() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final Path shmPath = new Path()) {
                try (
                        final TxnScoreboard scoreboard = new TxnScoreboard(FilesFacadeImpl.INSTANCE, shmPath.of(root), 1024)
                ) {
                    for (int i = 0; i < 1500; i++) {
                        scoreboard.acquireTxn(i);
                        scoreboard.releaseTxn(i);
                    }
                    Assert.assertEquals(1499, scoreboard.getMin());

                    // increase scoreboard size
                    try (
                            final TxnScoreboard scoreboard2 = new TxnScoreboard(FilesFacadeImpl.INSTANCE, shmPath.of(root), 2048)
                    ) {
                        Assert.assertEquals(1499, scoreboard2.getMin());
                        for (int i = 1500; i < 3000; i++) {
                            scoreboard2.acquireTxn(i);
                            scoreboard2.releaseTxn(i);
                        }
                        Assert.assertEquals(2999, scoreboard2.getMin());
                        Assert.assertEquals(2999, scoreboard.getMin());
                    }
                }
            }
        });
    }

    @Test
    public void testDoesNotCleanOnNonExclusiveOpen() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final Path shmPath = new Path()) {
                try (
                        final TxnScoreboard rootBoard = new TxnScoreboard(FilesFacadeImpl.INSTANCE, shmPath.of(root), 2048)
                ) {
                    try (
                            final TxnScoreboard scoreboard = new TxnScoreboard(FilesFacadeImpl.INSTANCE, shmPath.of(root), 1024)
                    ) {
                        for (int i = 0; i < 1500; i++) {
                            scoreboard.acquireTxn(i);
                            scoreboard.releaseTxn(i);
                        }
                        Assert.assertEquals(1499, scoreboard.getMin());
                    }

                    try (
                            final TxnScoreboard scoreboard2 = new TxnScoreboard(FilesFacadeImpl.INSTANCE, shmPath.of(root), 2048)
                    ) {
                        Assert.assertEquals(1499, scoreboard2.getMin());
                        for (int i = 1500; i < 3000; i++) {
                            scoreboard2.acquireTxn(i);
                            scoreboard2.releaseTxn(i);
                        }
                        Assert.assertEquals(2999, scoreboard2.getMin());
                    }

                    Assert.assertEquals(2999, rootBoard.getMin());
                }
            }
        });
    }

    @Test
    public void testLimits() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            int expect = 2048;
            try (final Path shmPath = new Path()) {
                try (TxnScoreboard scoreboard2 = new TxnScoreboard(FilesFacadeImpl.INSTANCE, shmPath.of(root), expect)) {
                    try (TxnScoreboard scoreboard1 = new TxnScoreboard(FilesFacadeImpl.INSTANCE, shmPath.of(root), expect)) {
                        // we should successfully acquire expected number of entries
                        for (int i = 0; i < expect; i++) {
                            scoreboard1.acquireTxn(i + 134);
                        }
                        // scoreboard capacity should be exhausted
                        // and we should be refused to acquire any more slots
                        try {
                            scoreboard1.acquireTxn(expect + 134);
                            Assert.fail();
                        } catch (CairoException ignored) {
                        }

                        // now we release middle slot, this does not free any more slots
                        scoreboard1.releaseTxn(11 + 134);
                        Assert.assertEquals(134, scoreboard1.getMin());
                        // we should NOT be able to allocate more slots
                        try {
                            scoreboard1.acquireTxn(expect + 134);
                            Assert.fail();
                        } catch (CairoException ignored) {
                        }

                        // now that we release "head" slot we should be able to acquire more
                        scoreboard1.releaseTxn(134);
                        Assert.assertEquals(135, scoreboard1.getMin());
                        // and we should be able to allocate another one
                        scoreboard1.acquireTxn(expect + 134);

                        // now check that all counts are intact
                        for (int i = 1; i <= expect; i++) {
                            if (i != 11) {
                                Assert.assertEquals(1, scoreboard1.getActiveReaderCount(i + 134));
                            } else {
                                Assert.assertEquals(0, scoreboard1.getActiveReaderCount(i + 134));
                            }
                        }
                    }

                    for (int i = 1; i <= expect; i++) {
                        if (i != 11) {
                            Assert.assertEquals(1, scoreboard2.getActiveReaderCount(i + 134));
                        } else {
                            Assert.assertEquals(0, scoreboard2.getActiveReaderCount(i + 134));
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testLimitsLoop() throws Exception {
        LOG.debug().$("starting testLimitsLoop").$();
        for (int i = 0; i < 10000; i++) {
            testLimits();
        }
    }

    @Test
    public void testStressOpenParallel() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            int parallel = 16;
            int iterations = (int) 1E3;
            SOCountDownLatch latch = new SOCountDownLatch(parallel);
            AtomicInteger errors = new AtomicInteger();
            for (int i = 0; i < parallel; i++) {
                new Thread(() -> {
                    try (final Path shmPath = new Path()) {
                        for (int j = 0; j < iterations; j++) {
                            //noinspection EmptyTryBlock
                            try (TxnScoreboard ignored = new TxnScoreboard(FilesFacadeImpl.INSTANCE, shmPath.of(root), 1024)) {
                                // empty body because we need to close this
                            } catch (Exception ex) {
                                ex.printStackTrace();
                                errors.incrementAndGet();
                                break;
                            }
                        }
                    }
                    latch.countDown();
                }).start();
            }

            latch.await();
            Assert.assertEquals(0, errors.get());
        });
    }

    @Test
    public void testVanilla() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (
                    final Path shmPath = new Path();
                    final TxnScoreboard scoreboard2 = new TxnScoreboard(FilesFacadeImpl.INSTANCE, shmPath.of(root), 1024)
            ) {
                try (TxnScoreboard scoreboard1 = new TxnScoreboard(FilesFacadeImpl.INSTANCE, shmPath.of(root), 1024)) {
                    scoreboard1.acquireTxn(67);
                    scoreboard1.acquireTxn(68);
                    scoreboard1.acquireTxn(68);
                    scoreboard1.acquireTxn(69);
                    scoreboard1.acquireTxn(70);
                    scoreboard1.acquireTxn(71);

                    scoreboard1.releaseTxn(68);
                    Assert.assertEquals(67, scoreboard1.getMin());
                    Assert.assertFalse(scoreboard2.isTxnAvailable(68));
                    scoreboard1.releaseTxn(68);
                    Assert.assertEquals(67, scoreboard1.getMin());
                    scoreboard1.releaseTxn(67);
                    Assert.assertEquals(69, scoreboard1.getMin());

                    scoreboard1.releaseTxn(69);
                    Assert.assertEquals(70, scoreboard1.getMin());
                    scoreboard1.releaseTxn(71);
                    Assert.assertTrue(scoreboard1.isTxnAvailable(71));
                    Assert.assertEquals(70, scoreboard1.getMin());
                    scoreboard1.releaseTxn(70);
                    Assert.assertEquals(71, scoreboard1.getMin());

                    scoreboard1.acquireTxn(72);
                }
                scoreboard2.acquireTxn(72);
                Assert.assertEquals(2, scoreboard2.getActiveReaderCount(72));

                scoreboard2.releaseTxn(72);
                scoreboard2.releaseTxn(72);
                Assert.assertEquals(0, scoreboard2.getActiveReaderCount(72));

                Assert.assertTrue(scoreboard2.isTxnAvailable(77));
            }
        });
    }

    @Test
    public void testWideRange() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (
                    final Path shmPath = new Path();
                    final TxnScoreboard scoreboard = new TxnScoreboard(FilesFacadeImpl.INSTANCE, shmPath.of(root), 1024)
            ) {
                scoreboard.acquireTxn(15);
                scoreboard.releaseTxn(15);
                scoreboard.acquireTxn(900992);
            }
        });
    }
}