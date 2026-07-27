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

package io.questdb.test.griffin.engine.join;

import io.questdb.griffin.engine.LimitOverflowException;
import io.questdb.griffin.engine.join.LongChain;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.test.tools.TestUtils.assertMemoryLeak;

public class LongChainTest {
    @SuppressWarnings("unused")
    private static final Log LOG = LogFactory.getLog(LongChainTest.class);

    @Test
    public void testAll() throws Exception {
        assertMemoryLeak(() -> {
            try (LongChain chain = new LongChain(1024, Integer.MAX_VALUE)) {
                final int N = 1000;
                final int nChains = 10;
                final Rnd rnd = new Rnd();
                final IntList tails = new IntList(nChains);
                final ObjList<LongList> expectedValues = new ObjList<>();

                for (int i = 0; i < nChains; i++) {
                    LongList expected = new LongList(N);
                    tails.add(populateChain(chain, rnd, expected));
                    expectedValues.add(expected);
                    Assert.assertEquals(N, expected.size());
                }
                Assert.assertEquals(nChains, expectedValues.size());

                // values are expected in reverse order
                for (int i = 0; i < nChains; i++) {
                    LongChain.Cursor cursor = chain.getCursor(tails.getQuick(i));
                    LongList expected = expectedValues.get(i);
                    int count = 0;
                    while (cursor.hasNext()) {
                        Assert.assertEquals(expected.getQuick(count), cursor.next());
                        count++;
                    }
                    Assert.assertEquals(N, count);
                }
            }
        });
    }

    @Test
    public void testHeapClampsToMaxHeapSize() throws Exception {
        assertMemoryLeak(() -> {
            // 64B page x 3 pages = a 192B budget, which is not a power of two. Doubling goes
            // 64 -> 128 -> 256, and 256 overshoots, so rejecting there stranded a third of the
            // configured budget. Clamping to 192 fits 16 12-byte values instead of 10.
            try (LongChain chain = new LongChain(64, 3)) {
                final LongList expected = new LongList();
                int tail = -1;
                try {
                    for (int i = 0; i < 100; i++) {
                        tail = chain.put(i, tail);
                        expected.add(i);
                    }
                    Assert.fail("expected LimitOverflowException");
                } catch (LimitOverflowException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "limit of 192 memory exceeded in LongChain");
                }
                Assert.assertEquals(16, expected.size());

                // Everything written into the clamped heap must still read back, in reverse order.
                expected.reverse();
                LongChain.Cursor cursor = chain.getCursor(tail);
                int count = 0;
                while (cursor.hasNext()) {
                    Assert.assertEquals(expected.getQuick(count), cursor.next());
                    count++;
                }
                Assert.assertEquals(expected.size(), count);
            }
        });
    }

    private int populateChain(LongChain chain, Rnd rnd, LongList expectedValues) {
        int tail = -1;
        for (int i = 0; i < 1000; i++) {
            long expected = rnd.nextLong();
            tail = chain.put(expected, tail);
            expectedValues.add(expected);
        }
        expectedValues.reverse();
        return tail;
    }
}
