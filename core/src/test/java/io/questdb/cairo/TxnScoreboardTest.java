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

import io.questdb.std.Os;
import io.questdb.std.str.Path;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static io.questdb.cairo.TxnScoreboard.acquire;
import static io.questdb.cairo.TxnScoreboard.release;

public class TxnScoreboardTest {

    @BeforeClass
    public static void setUp() throws Exception {
        Os.init();
    }

    @Test
    public void testLimits() {

        try (final Path shmPath = new Path()) {
            int expect = 4096;
            long p2 = TxnScoreboard.create(shmPath, "hello");
            try {
                long p1 = TxnScoreboard.create(shmPath, "hello");
                try {
                    // we should successfully acquire expected number of entries
                    for (int i = 0; i < expect; i++) {
                        Assert.assertTrue(TxnScoreboard.acquire(p1, i + 134));
                    }
                    // scoreboard capacity should be exhausted
                    // and we should be refused to acquire any more slots
                    Assert.assertFalse(TxnScoreboard.acquire(p1, expect + 134));

                    // now we release middle slot, this does not free any more slots
                    Assert.assertEquals(133, TxnScoreboard.release(p1, 11 + 134));
                    // we should NOT be able to allocate more slots
                    Assert.assertFalse(TxnScoreboard.acquire(p1, expect + 134));

                    // now that we release "head" slot we should be able to acquire more
                    Assert.assertEquals(134, TxnScoreboard.release(p1, 134));
                    // and we should be able to allocate another one
                    Assert.assertTrue(TxnScoreboard.acquire(p1, expect + 134));

                    // now check that all counts are intact
                    for (int i = 1; i <= expect; i++) {
                        if (i != 11) {
                            Assert.assertEquals(1, TxnScoreboard.getCount(p1, i + 134));
                        } else {
                            Assert.assertEquals(0, TxnScoreboard.getCount(p1, i + 134));
                        }
                    }
                } finally {
                    TxnScoreboard.close(shmPath, "hello", p1);
                }
            } finally {
                // now check that all counts are available via another memory space
                for (int i = 1; i <= expect; i++) {
                    if (i != 11) {
                        Assert.assertEquals(1, TxnScoreboard.getCount(p2, i + 134));
                    } else {
                        Assert.assertEquals(0, TxnScoreboard.getCount(p2, i + 134));
                    }
                }
            }
        }
    }

    @Test
    public void testVanilla() {
        try (final Path shmPath = new Path()) {
            long p2 = TxnScoreboard.create(shmPath, "tab1");
            try {
                long p1 = TxnScoreboard.create(shmPath, "tab1");
                try {
                    Assert.assertTrue(acquire(p1, 67));
                    Assert.assertTrue(acquire(p1, 68));
                    Assert.assertTrue(acquire(p1, 68));
                    Assert.assertTrue(acquire(p1, 69));
                    Assert.assertTrue(acquire(p1, 70));
                    Assert.assertTrue(acquire(p1, 71));

                    Assert.assertEquals(66, release(p1, 68));
                    Assert.assertEquals(66, release(p1, 68));
                    Assert.assertEquals(68, release(p1, 67));

                    Assert.assertEquals(69, release(p1, 69));
                    Assert.assertEquals(69, release(p1, 71));
                    Assert.assertEquals(71, release(p1, 70));

                    Assert.assertTrue(acquire(p1, 72));
                } finally {
                    TxnScoreboard.close(shmPath, "tab1", p1);
                }
                Assert.assertTrue(acquire(p2, 72));
                Assert.assertEquals(2, TxnScoreboard.getCount(p2, 72));
            } finally {
                TxnScoreboard.close(shmPath, "tab1", p2);
            }
        }
    }
}