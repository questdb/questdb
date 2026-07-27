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

import java.lang.reflect.Method;

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
    public void testOffsetCompressionRoundTripsAboveSignedIntRange() throws Exception {
        // Compressed offsets are unsigned 32-bit and 4-byte scaled, with -1 reserved as the
        // end-of-chain sentinel. Offsets at or above 2^31 * 4 set the top bit of the raw int;
        // reading them back as a signed int yielded a negative offset, so the cursor walked
        // 8GB below the heap. Unlike OrderedMap there is no +1 bias, so offset 0 legitimately
        // compresses to 0 and only -1 is out of bounds.
        final Method compressOffset = LongChain.class.getDeclaredMethod("compressOffset", long.class);
        final Method uncompressOffset = LongChain.class.getDeclaredMethod("uncompressOffset", int.class);
        compressOffset.setAccessible(true);
        uncompressOffset.setAccessible(true);

        final long chainValueSize = 12;
        final long maxHeapSize = (Integer.toUnsignedLong(-1) - 1) << 2; // (2^32 - 2) * 4
        final long lastSignedOffset = ((long) Integer.MAX_VALUE) << 2; // compresses to Integer.MAX_VALUE
        final long firstUnsignedOffset = (1L << 31) << 2;              // compresses to Integer.MIN_VALUE
        final long lastValueOffset = maxHeapSize - chainValueSize;     // last offset a value can start at
        final long[] offsets = {
                0,
                4,
                1L << 30,
                lastSignedOffset,
                firstUnsignedOffset,
                3L << 32, // mid-unsigned range: compresses negative, but to neither boundary
                lastValueOffset,
        };
        for (long offset : offsets) {
            int rawOffset = (Integer) compressOffset.invoke(null, offset);
            Assert.assertNotEquals("offset " + offset + " must not compress to the chain-end sentinel", -1, rawOffset);
            Assert.assertEquals("offset " + offset, offset, ((Long) uncompressOffset.invoke(null, rawOffset)).longValue());
        }

        // Offset 0 is a legal value here, so 0 is not a sentinel and must round-trip as itself.
        Assert.assertEquals(0, (int) (Integer) compressOffset.invoke(null, 0L));
        // The upper half of the range is exactly what the signed reading got wrong.
        Assert.assertTrue((Integer) compressOffset.invoke(null, lastSignedOffset) > 0);
        Assert.assertTrue((Integer) compressOffset.invoke(null, firstUnsignedOffset) < 0);
        Assert.assertTrue((Integer) compressOffset.invoke(null, lastValueOffset) < 0);
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
