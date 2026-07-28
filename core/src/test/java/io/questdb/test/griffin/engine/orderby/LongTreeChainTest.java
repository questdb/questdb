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
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.LimitOverflowException;
import io.questdb.griffin.engine.RecordComparator;
import io.questdb.griffin.engine.orderby.LongTreeChain;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class LongTreeChainTest extends AbstractCairoTest {

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
