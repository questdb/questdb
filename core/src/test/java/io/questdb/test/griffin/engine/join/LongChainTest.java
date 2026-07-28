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
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
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
    public void testBudgetFlooredAtOnePage() throws Exception {
        // cairo.sql.hash.join.light.value.max.pages = 0 is accepted by config and used to give a
        // zero budget, so every hash join failed with "limit of 0" even though the chain had
        // already allocated a full page. Flooring the budget at one page makes the reported limit
        // agree with what the chain actually holds, and is user-visible behaviour: without the
        // floor the first put below throws instead of succeeding.
        assertMemoryLeak(() -> {
            try (LongChain chain = new LongChain(64, 0)) {
                // 64-byte page, 12-byte values: five fit, the sixth needs 72.
                int prev = -1;
                for (int i = 0; i < 5; i++) {
                    prev = chain.put(i, prev);
                }
                try {
                    chain.put(5, prev);
                    Assert.fail("expected LimitOverflowException");
                } catch (LimitOverflowException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "limit of 64 memory exceeded in LongChain");
                }
            }
        });
    }

    @Test
    public void testHeapAcceptsRequiredEqualToMaxHeapSize() throws Exception {
        assertMemoryLeak(() -> {
            // 12B page x 3 pages = a 36B budget, which is an exact multiple of the 12-byte value.
            // The third value makes required exactly 36, so the throw predicate sees its boundary
            // case: a value that fits exactly must be accepted, not rejected.
            try (LongChain chain = new LongChain(12, 3)) {
                int tail = -1;
                tail = chain.put(10, tail);
                tail = chain.put(20, tail);
                tail = chain.put(30, tail); // required == 36 == the budget
                try {
                    chain.put(40, tail);
                    Assert.fail("expected LimitOverflowException");
                } catch (LimitOverflowException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "limit of 36 memory exceeded in LongChain");
                }

                LongChain.Cursor cursor = chain.getCursor(tail);
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(30, cursor.next());
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(20, cursor.next());
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(10, cursor.next());
                Assert.assertFalse(cursor.hasNext());
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

    @Test
    public void testKeepClosedChainAllocatesOnFirstPut() throws Exception {
        // All three production owners construct the chain with keepClosed == true, yet no test
        // built one that way. Such a chain allocates nothing until reopen(), so a put() that skips
        // reopen() has to allocate the configured page rather than grow from heapSize 0.
        assertMemoryLeak(() -> {
            try (LongChain chain = new LongChain(64, 3, true)) {
                final long usedBefore = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DEFAULT);
                final int first = chain.put(42, -1);

                // The configured 64-byte page, not the 12 bytes this one value needs. Growing from
                // a closed heap instead of opening it leaves the chain one value wide, re-doubling
                // from there for the rest of its life, and reallocs off a null pointer.
                // NATIVE_DEFAULT is engine-wide and the most widely shared tag in the codebase, so
                // assert a lower bound rather than an exact delta: any unrelated allocation between
                // the two reads would turn an equality check into a spurious hard failure. The
                // bound still separates the two outcomes, since the broken path books only 12.
                Assert.assertTrue(
                        "a keepClosed chain must allocate its configured page on first put",
                        Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DEFAULT) - usedBefore >= 64
                );

                final int second = chain.put(43, first);

                LongChain.Cursor cursor = chain.getCursor(second);
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(43, cursor.next());
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(42, cursor.next());
                Assert.assertFalse(cursor.hasNext());
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
