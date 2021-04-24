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

import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.str.Path;
import org.junit.Assert;
import org.junit.Test;

public class TxnScoreboardTest extends AbstractCairoTest {
    @Test
    public void testLimits() {
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
    }

    @Test
    public void testWideRange() {
        try (
                final Path shmPath = new Path();
                final TxnScoreboard scoreboard = new TxnScoreboard(FilesFacadeImpl.INSTANCE, shmPath.of(root), 1024)
        ) {
            scoreboard.acquireTxn(15);
            scoreboard.releaseTxn(15);
            scoreboard.acquireTxn(900992);
        }
    }

    @Test
    public void testCrawl() {
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

            // increase scoreboard size
            try (
                    final TxnScoreboard scoreboard = new TxnScoreboard(FilesFacadeImpl.INSTANCE, shmPath.of(root), 2048)
            ) {
                Assert.assertEquals(1499, scoreboard.getMin());
                for (int i = 1500; i < 3000; i++) {
                    scoreboard.acquireTxn(i);
                    scoreboard.releaseTxn(i);
                }
                Assert.assertEquals(2999, scoreboard.getMin());
            }
        }
    }

    @Test
    public void testVanilla() {
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
                scoreboard1.releaseTxn(68);
                Assert.assertEquals(67, scoreboard1.getMin());
                scoreboard1.releaseTxn(67);
                Assert.assertEquals(69, scoreboard1.getMin());

                scoreboard1.releaseTxn(69);
                Assert.assertEquals(70, scoreboard1.getMin());
                scoreboard1.releaseTxn(71);
                Assert.assertEquals(70, scoreboard1.getMin());
                scoreboard1.releaseTxn(70);
                Assert.assertEquals(71, scoreboard1.getMin());

                scoreboard1.acquireTxn(72);
            }
            scoreboard2.acquireTxn(72);
            Assert.assertEquals(2, scoreboard2.getActiveReaderCount(72));
        }
    }
}