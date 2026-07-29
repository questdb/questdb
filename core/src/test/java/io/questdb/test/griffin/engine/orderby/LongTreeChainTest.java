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

package io.questdb.test.griffin.engine.orderby;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.LimitOverflowException;
import io.questdb.griffin.engine.RecordComparator;
import io.questdb.griffin.engine.orderby.LongTreeChain;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class LongTreeChainTest extends AbstractCairoTest {

    @Test
    public void testConstructorFailureFreesKeyHeap() throws Exception {
        // An eager constructor mallocs the key heap and then the value heap, so an RSS limit sized
        // between the two makes the second one throw. The constructor has to release the key heap
        // by hand: the half-built chain never escapes, so no caller can close it. assertMemoryLeak
        // is the whole assertion - drop the hand-free and the 64-byte key heap leaks.
        assertMemoryLeak(() -> {
            final long savedLimit = Unsafe.getRssMemLimit();
            try {
                Unsafe.setRssMemLimit(Unsafe.getRssMemUsed() + 768);
                new LongTreeChain(
                        64,             // key page, allocated first
                        Long.MAX_VALUE,
                        1024,           // value page, allocated second - the one that must fail
                        Long.MAX_VALUE,
                        PropertyKey.CAIRO_SQL_SORT_KEY_MAX_BYTES.getPropertyPath(),
                        PropertyKey.CAIRO_SQL_SORT_LIGHT_VALUE_MAX_BYTES.getPropertyPath()
                );
                Assert.fail("expected CairoException");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "global RSS memory limit exceeded");
                TestUtils.assertContains(e.getFlyweightMessage(), "size=1024");
            } finally {
                Unsafe.setRssMemLimit(savedLimit);
            }
        });
    }

    @Test
    public void testKeyHeapAcceptsRequiredEqualToMaxHeapSize() throws Exception {
        // A 144-byte key budget is an exact multiple of the 24-byte block, so the 6th block makes
        // required exactly 144. That is the boundary of the throw predicate: a block that fits
        // exactly must be accepted, so the tree takes 6 blocks rather than stopping at 5.
        assertMemoryLeak(() -> {
            try (
                    LongTreeChain chain = new LongTreeChain(
                            64,             // key page >= BLOCK_SIZE
                            144,            // key heap budget == 6 blocks exactly
                            128 * 1024,
                            Long.MAX_VALUE, // value heap uncapped
                            PropertyKey.CAIRO_SQL_SORT_KEY_MAX_BYTES.getPropertyPath(),
                            PropertyKey.CAIRO_SQL_SORT_LIGHT_VALUE_MAX_BYTES.getPropertyPath()
                    )
            ) {
                Assert.assertEquals(6, fillUntilOverflow(chain, "limit of 144 memory exceeded in RedBlackTree"));
            }
        });
    }

    @Test
    public void testKeyHeapClampsToMaxHeapSize() throws Exception {
        // A 200-byte key budget is not a power of two, while every doubling step is. The tree
        // goes 64 -> 128 and then wants 256; rejecting there stranded a quarter of the budget
        // at 5 blocks. Clamping to 200 fits 8 of the 24-byte blocks instead.
        assertMemoryLeak(() -> {
            try (
                    LongTreeChain chain = new LongTreeChain(
                            64,             // key page >= BLOCK_SIZE
                            200,            // key heap budget, deliberately not a power of two
                            128 * 1024,
                            Long.MAX_VALUE, // value heap uncapped
                            PropertyKey.CAIRO_SQL_SORT_KEY_MAX_BYTES.getPropertyPath(),
                            PropertyKey.CAIRO_SQL_SORT_LIGHT_VALUE_MAX_BYTES.getPropertyPath()
                    )
            ) {
                Assert.assertEquals(8, fillUntilOverflow(chain, "memory exceeded in RedBlackTree"));
            }
        });
    }

    @Test
    public void testLazyChainAllocatesOnFirstPutWithoutReopen() throws Exception {
        // Every production owner constructs the chain with openOnInit == false and calls reopen()
        // before use. The constructor records the configured page size in the heap size fields
        // either way, so a put() that skips reopen() grew from a null heap pointer while booking
        // only the doubling delta: the counters were charged one page less than was allocated, and
        // close() then freed the full amount. assertMemoryLeak observes exactly that imbalance.
        assertMemoryLeak(() -> {
            try (
                    LongTreeChain chain = new LongTreeChain(
                            64,
                            Long.MAX_VALUE,
                            128,
                            Long.MAX_VALUE,
                            PropertyKey.CAIRO_SQL_SORT_KEY_MAX_BYTES.getPropertyPath(),
                            PropertyKey.CAIRO_SQL_SORT_LIGHT_VALUE_MAX_BYTES.getPropertyPath(),
                            false
                    )
            ) {
                final long[] values = new long[8];
                for (int i = 0; i < values.length; i++) {
                    values[i] = i;
                }
                final TestRecordCursor cursor = new TestRecordCursor(values);
                final Record left = cursor.getRecord();
                final Record placeholder = cursor.getRecordB();
                final RecordComparator comparator = new TestRecordComparator();

                int inserted = 0;
                while (cursor.hasNext()) {
                    chain.put(left, cursor, placeholder, comparator);
                    inserted++;
                }

                Assert.assertEquals(values.length, inserted);
                assertReadsBack(chain, inserted);
            }
        });
    }

    @Test
    public void testValueHeapAcceptsRequiredEqualToMaxHeapSize() throws Exception {
        // Same boundary on the value heap: a 36-byte budget is an exact multiple of the 12-byte
        // chain value, so the 3rd value makes required exactly 36 and must be accepted.
        assertMemoryLeak(() -> {
            try (
                    LongTreeChain chain = new LongTreeChain(
                            64,
                            Long.MAX_VALUE, // key heap uncapped
                            12,             // value page == CHAIN_VALUE_SIZE
                            36,             // value heap budget == 3 values exactly
                            PropertyKey.CAIRO_SQL_SORT_KEY_MAX_BYTES.getPropertyPath(),
                            PropertyKey.CAIRO_SQL_SORT_LIGHT_VALUE_MAX_BYTES.getPropertyPath()
                    )
            ) {
                Assert.assertEquals(3, fillUntilOverflow(chain, "limit of 36 memory exceeded in LongTreeChain"));
            }
        });
    }

    @Test
    public void testValueHeapAllocatesAfterPartialReopen() throws Exception {
        // reopen() takes the key heap first and the value heap second, so an RSS limit sized to sit
        // between the two mallocs leaves the tree half-open: key heap allocated, value heap still
        // null - but valueHeapSize already committed to the configured page size, because reopen()
        // assigns it before the malloc that throws.
        //
        // The next put() therefore reaches growValueHeap() with valueHeapStart == 0. Its guard must
        // allocate rather than grow: a realloc off a null pointer books only the doubling delta
        // (newHeapSize - valueHeapSize) while really allocating newHeapSize, so the counters come up
        // one page short of what close() later frees. assertMemoryLeak observes exactly that.
        assertMemoryLeak(() -> {
            try (
                    LongTreeChain chain = new LongTreeChain(
                            64,             // key page, allocated first
                            Long.MAX_VALUE,
                            1024,           // value page, allocated second - the one that must fail
                            Long.MAX_VALUE,
                            PropertyKey.CAIRO_SQL_SORT_KEY_MAX_BYTES.getPropertyPath(),
                            PropertyKey.CAIRO_SQL_SORT_LIGHT_VALUE_MAX_BYTES.getPropertyPath(),
                            false           // lazy: nothing allocated until reopen()
                    )
            ) {
                final long savedLimit = Unsafe.getRssMemLimit();
                try {
                    // Headroom for the 64-byte key heap but not the 1024-byte value heap. The band
                    // tolerates up to 704 bytes of concurrent drift before the key malloc would
                    // start failing instead, and the size= assertion below catches it if it does.
                    Unsafe.setRssMemLimit(Unsafe.getRssMemUsed() + 768);
                    chain.reopen();
                    Assert.fail("expected CairoException");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "global RSS memory limit exceeded");
                    // Pin WHICH malloc failed. Both emit the same prefix, so without this a drifted
                    // key-heap failure would leave the test green while covering growKeyHeap's
                    // guard - which testLazyChainAllocatesOnFirstPutWithoutReopen already covers -
                    // instead of the value-heap guard this test exists for.
                    TestUtils.assertContains(e.getFlyweightMessage(), "size=1024");
                } finally {
                    Unsafe.setRssMemLimit(savedLimit);
                }

                final long[] values = new long[8];
                for (int i = 0; i < values.length; i++) {
                    values[i] = i;
                }
                final TestRecordCursor cursor = new TestRecordCursor(values);
                final Record left = cursor.getRecord();
                final Record placeholder = cursor.getRecordB();
                final RecordComparator comparator = new TestRecordComparator();

                int inserted = 0;
                while (cursor.hasNext()) {
                    chain.put(left, cursor, placeholder, comparator);
                    inserted++;
                }

                Assert.assertEquals(values.length, inserted);
                assertReadsBack(chain, inserted);
            }
        });
    }

    @Test
    public void testValueHeapClampsToMaxHeapSize() throws Exception {
        // Same clamp on the value heap: a 96-byte budget is not a power of two, the chain goes
        // 16 -> 32 -> 64 and then wants 128. Clamping to 96 fits 8 of the 12-byte chain values
        // instead of the 5 that fitted before.
        assertMemoryLeak(() -> {
            try (
                    LongTreeChain chain = new LongTreeChain(
                            64,
                            Long.MAX_VALUE, // key heap uncapped
                            16,             // value page >= CHAIN_VALUE_SIZE
                            96,             // value heap budget, deliberately not a power of two
                            PropertyKey.CAIRO_SQL_SORT_KEY_MAX_BYTES.getPropertyPath(),
                            PropertyKey.CAIRO_SQL_SORT_LIGHT_VALUE_MAX_BYTES.getPropertyPath()
                    )
            ) {
                Assert.assertEquals(8, fillUntilOverflow(chain, "memory exceeded in LongTreeChain"));
            }
        });
    }

    /**
     * Walks the tree back after a clamped growth step. Values go in ascending with rowId == value,
     * so the cursor has to yield 0..inserted-1 in order - which only holds if every heap-relative
     * offset survived the reallocs that moved the heap.
     */
    private void assertReadsBack(LongTreeChain chain, int inserted) {
        LongTreeChain.TreeCursor cursor = chain.getCursor();
        int count = 0;
        while (cursor.hasNext()) {
            Assert.assertEquals(count, cursor.next());
            count++;
        }
        Assert.assertEquals(inserted, count);
    }

    /**
     * Inserts distinct ascending values until one of the heaps runs out, and returns how many
     * of them the chain accepted. Fails when no overflow happens at all.
     */
    private int fillUntilOverflow(LongTreeChain chain, String expectedMessage) {
        final long[] values = new long[256];
        for (int i = 0; i < values.length; i++) {
            values[i] = i;
        }
        final TestRecordCursor cursor = new TestRecordCursor(values);
        final Record left = cursor.getRecord();
        final Record placeholder = cursor.getRecordB();
        final RecordComparator comparator = new TestRecordComparator();

        // LongTreeChain.put() sets the comparator's left side itself, so the loop must not.
        int inserted = 0;
        while (cursor.hasNext()) {
            try {
                chain.put(left, cursor, placeholder, comparator);
            } catch (LimitOverflowException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), expectedMessage);
                assertReadsBack(chain, inserted);
                return inserted;
            }
            inserted++;
        }
        Assert.fail("expected LimitOverflowException");
        return -1;
    }
}
