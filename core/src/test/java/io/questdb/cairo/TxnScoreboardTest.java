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
import io.questdb.std.str.CharSequenceZ;
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
        long p = TxnScoreboard.create(new CharSequenceZ("hello"));
        try {
            int expect = 4096;
            // we should successfully acquire expected number of entries
            for (int i = 0; i < expect; i++) {
                Assert.assertTrue(TxnScoreboard.acquire(p, i + 134));
            }
            // scoreboard capacity should be exhausted
            // and we should be refused to acquire any more slots
            Assert.assertFalse(TxnScoreboard.acquire(p, expect + 134));

            // now we release middle slot, this does not free any more slots
            Assert.assertEquals(133, TxnScoreboard.release(p, 11 + 134));
            // we should NOT be able to allocate more slots
            Assert.assertFalse(TxnScoreboard.acquire(p, expect + 134));

            // now that we release "head" slot we should be able to acquire more
            Assert.assertEquals(134, TxnScoreboard.release(p, 134));
            // and we should be able to allocate another one
            Assert.assertTrue(TxnScoreboard.acquire(p, expect + 134));

            // now check that all counts are intact
            for (int i = 1; i <= expect; i++) {
                if (i != 11) {
                    Assert.assertEquals(1, TxnScoreboard.getCount(p, i + 134));
                } else {
                    Assert.assertEquals(0, TxnScoreboard.getCount(p, i + 134));
                }
            }
        } finally {
            TxnScoreboard.close(p);
        }
    }

    @Test
    public void testVanilla() {
        long p = TxnScoreboard.create(new CharSequenceZ("ok"));
        try {
            Assert.assertTrue(acquire(p, 67));
            Assert.assertTrue(acquire(p, 68));
            Assert.assertTrue(acquire(p, 68));
            Assert.assertTrue(acquire(p, 69));
            Assert.assertTrue(acquire(p, 70));
            Assert.assertTrue(acquire(p, 71));

            Assert.assertEquals(66, release(p, 68));
            Assert.assertEquals(66, release(p, 68));
            Assert.assertEquals(68, release(p, 67));

            Assert.assertEquals(69, release(p, 69));
            Assert.assertEquals(69, release(p, 71));
            Assert.assertEquals(71, release(p, 70));
        } finally {
            TxnScoreboard.close(p);
        }
    }
}