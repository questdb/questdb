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
import io.questdb.griffin.engine.LimitOverflowException;
import io.questdb.griffin.engine.RecordComparator;
import io.questdb.griffin.engine.orderby.LimitedSizeLongTreeChain;
import io.questdb.std.LongList;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

/**
 * Test RBTree removal cases asserting final tree structure.
 */
public class LimitedSizeLongTreeChainTest extends AbstractCairoTest {
    // used in all tests to hide api complexity
    LimitedSizeLongTreeChain chain;
    RecordComparator comparator;
    TestRecordCursor cursor;
    SingleLongRecord left;
    SingleLongRecord placeholder;

    @After
    public void after() {
        chain.close();
    }

    @Before
    public void before() {
        chain = new LimitedSizeLongTreeChain(
                configuration.getSqlSortKeyPageSize(),
                configuration.getSqlSortKeyMaxBytes(),
                configuration.getSqlSortLightValuePageSize(),
                configuration.getSqlSortLightValueMaxBytes(),
                PropertyKey.CAIRO_SQL_SORT_KEY_MAX_BYTES.getPropertyPath(),
                PropertyKey.CAIRO_SQL_SORT_LIGHT_VALUE_MAX_BYTES.getPropertyPath()
        );
        chain.updateLimits(true, 20);
    }

    @Test
    public void testCreateOrderedTree() {
        assertTree(
                """
                        [Black,2]
                         L-[Black,1]
                         R-[Black,4]
                           L-[Red,3]
                           R-[Red,5]
                        """,
                1, 2, 3, 4, 5
        );
    }

    @Test
    public void testCreateOrderedTreeWithDuplicates() {
        assertTree(
                """
                        [Black,2]
                         L-[Black,1(2)]
                         R-[Black,4(2)]
                           L-[Red,3(2)]
                           R-[Red,5]
                        """,
                1, 2, 3, 4, 5, 1, 4, 3
        );
    }

    @Test
    public void testCreateOrderedTreeWithInputInDescendingOrder() {
        assertTree(
                """
                        [Black,4]
                         L-[Black,2]
                           L-[Red,1]
                           R-[Red,3]
                         R-[Black,5]
                        """,
                5, 4, 3, 2, 1
        );
    }

    @Test
    public void testCreateOrderedTreeWithInputInNoOrder() {
        assertTree(
                """
                        [Black,3]
                         L-[Black,2]
                           L-[Red,1]
                         R-[Black,5]
                           L-[Red,4]
                        """,
                3, 2, 5, 1, 4
        );
    }

    @Test
    public void testCursorClearLeavesCursorExhausted() throws Exception {
        // clear() used to reset the cursor to 0/0, but 0 is a legal block and value offset, so
        // hasNext() reported true and next() read rowId(0) - from address 0 once the chain had
        // been closed. It has to clear to the sentinels instead.
        assertMemoryLeak(() -> {
            createTree(5, 3, 9);
            LimitedSizeLongTreeChain.TreeCursor treeCursor = chain.getCursor();
            Assert.assertTrue(treeCursor.hasNext());

            treeCursor.clear();
            Assert.assertFalse("a cleared cursor must be exhausted", treeCursor.hasNext());
        });
    }

    @Test
    public void testIncrementalMinMaxMatchesFullWalk() {
        // put() and removeAndCache() maintain the cached extreme in place instead of re-walking the
        // spine from root on every accepted row. The cache decides which row a full chain evicts,
        // so a wrong cache silently keeps the wrong rows rather than failing an invariant. Drive
        // random data through both limit directions and compare against a sorted reference: any
        // drift in the cache shows up as a wrong result set.
        final Rnd rnd = TestUtils.generateRandom(LOG);
        for (int trial = 0; trial < 40; trial++) {
            final int n = 1 + rnd.nextInt(200);
            final int limit = 1 + rnd.nextInt(32);
            final boolean isFirstN = rnd.nextBoolean();
            final long[] values = new long[n];
            for (int i = 0; i < n; i++) {
                // A narrow range on purpose, so duplicates land on the chain-append path too.
                values[i] = rnd.nextInt(40);
            }

            chain.clear();
            chain.updateLimits(isFirstN, limit);
            createTree(values);

            final long[] sorted = values.clone();
            Arrays.sort(sorted);
            final int kept = Math.min(limit, n);
            final long[] expected = new long[kept];
            System.arraycopy(sorted, isFirstN ? 0 : n - kept, expected, 0, kept);

            final LongList actual = new LongList();
            LimitedSizeLongTreeChain.TreeCursor treeCursor = chain.getCursor();
            while (treeCursor.hasNext()) {
                cursor.recordAt(placeholder, treeCursor.next());
                actual.add(placeholder.getLong(0));
            }
            // The tree yields ascending order, and so does the reference slice.
            Assert.assertEquals("trial " + trial + " isFirstN=" + isFirstN + " limit=" + limit
                    + " n=" + n + " kept count", expected.length, actual.size());
            for (int i = 0; i < expected.length; i++) {
                Assert.assertEquals("trial " + trial + " isFirstN=" + isFirstN + " limit=" + limit
                        + " position " + i, expected[i], actual.getQuick(i));
            }
        }
    }

    @Test
    public void testKeyHeapClampsToMaxHeapSize() throws Exception {
        // A 200-byte key budget is not a power of two, while every doubling step is. The chain
        // goes 64 -> 128 and then wants 256; rejecting there stranded a quarter of the budget
        // at 5 blocks. Clamping to 200 fits 8 of the 24-byte blocks instead.
        // Close the @Before chain first, so the leak check brackets only the
        // replacement chain's clamped, non-power-of-two realloc/free accounting.
        chain.close();
        assertMemoryLeak(() -> {
            chain = new LimitedSizeLongTreeChain(
                    64,             // key page >= BLOCK_SIZE
                    200,            // key heap budget, deliberately not a power of two
                    128 * 1024,
                    Long.MAX_VALUE, // value heap uncapped
                    PropertyKey.CAIRO_SQL_SORT_KEY_MAX_BYTES.getPropertyPath(),
                    PropertyKey.CAIRO_SQL_SORT_LIGHT_VALUE_MAX_BYTES.getPropertyPath()
            );
            chain.updateLimits(true, 1_000);
            Assert.assertEquals(8, fillUntilOverflow("memory exceeded in RedBlackTree"));
            chain.close();
        });
    }

    @Test
    public void testKeyHeapOverflowNamesConfigKey() throws Exception {
        // Tiny key-heap budget (one page) with an uncapped value heap: the red-black key heap
        // overflows and the message must name the sort.key config key. Pins the LimitedSize key
        // path's (raise ...) hint that no query-level test exercises.
        chain.close();
        assertMemoryLeak(() -> {
            chain = new LimitedSizeLongTreeChain(
                    64,             // key page >= BLOCK_SIZE; doubling past one page overflows
                    64,             // key heap budget = one page
                    128 * 1024,
                    Long.MAX_VALUE, // value heap uncapped
                    PropertyKey.CAIRO_SQL_SORT_KEY_MAX_BYTES.getPropertyPath(),
                    PropertyKey.CAIRO_SQL_SORT_LIGHT_VALUE_MAX_BYTES.getPropertyPath()
            );
            chain.updateLimits(true, 1_000);
            final long[] values = new long[256];
            for (int i = 0; i < values.length; i++) {
                values[i] = i;
            }
            try {
                createTree(values);
                Assert.fail("expected LimitOverflowException");
            } catch (LimitOverflowException e) {
                TestUtils.assertContains(e.getFlyweightMessage(),
                        "memory exceeded in RedBlackTree (raise cairo.sql.sort.key.max.bytes)");
            }
            chain.close();
        });
    }

    @Test
    public void testLazyChainAllocatesOnFirstPutWithoutReopen() throws Exception {
        // LimitedSizeSortedLightRecordCursorFactory and AsyncTopKAtom both construct this chain
        // with openOnInit == false and reopen() before use. The constructor still records the
        // configured page size in both heap-size fields, so a put() that skips reopen() grew from
        // a null heap pointer while booking only the doubling delta: the counters ended up short
        // of what was allocated, and close() then freed the full amount.
        //
        // This pins the key-heap guard in AbstractRedBlackTree. The value-heap guard cannot be
        // reached from here - allocateBlock() runs first and its reopen() opens both heaps - and
        // only fires when a previous reopen() failed part way, leaving the key heap open and the
        // value heap null.
        chain.close();
        assertMemoryLeak(() -> {
            chain = new LimitedSizeLongTreeChain(
                    64,
                    Long.MAX_VALUE,
                    128,
                    Long.MAX_VALUE,
                    PropertyKey.CAIRO_SQL_SORT_KEY_MAX_BYTES.getPropertyPath(),
                    PropertyKey.CAIRO_SQL_SORT_LIGHT_VALUE_MAX_BYTES.getPropertyPath(),
                    false
            );
            chain.updateLimits(true, 1_000);

            final long[] values = new long[8];
            for (int i = 0; i < values.length; i++) {
                values[i] = i;
            }
            createTree(values);

            assertReadsBack(values.length);
            chain.close();
        });
    }

    @Test
    public void testPutAfterNonExtremeRemovalRecomputesMinMax() {
        // put() maintains the cached extreme in place and falls back to the full spine walk only
        // when removeAndCache() could not name a replacement. Its production caller only ever
        // removes the cached extreme, so after that optimisation the fallback - and with it
        // refreshMinMaxNode(), findMinNode() and findMaxNode() - is reachable only through the
        // removeAndCache(node) API a dozen tests here already drive. Keep it covered: the walk is
        // the safety net for every one of those removals.
        chain.updateLimits(true, 4);
        createTree(1, 2, 3, 4, 5);

        // 2 is neither the smallest nor the cached maximum, so removeAndCache() cannot name a
        // replacement and invalidates the cache outright.
        removeRowWithValue(2);

        // Back below the limit, so this insert takes the fallback and has to find the new maximum
        // from the tree rather than trust the cache.
        putValue(5);
        // At the limit again. The accept/reject compare now runs against whatever the fallback
        // decided the maximum is: 2 is below it, so it must be accepted and evict 5.
        putValue(2);

        assertChainHolds(1, 2, 3, 4);
    }

    @Test
    public void testRemoveBlackNodeWithBlackSiblingWithBothChildrenBlack() {
        assertTree(
                """
                        [Black,30]
                         L-[Black,20]
                         R-[Black,40]
                           L-[Red,35]
                        """,
                30, 20, 40, 35
        );
        removeRowWithValue(20L);
        assertTree(
                """
                        [Black,35]
                         L-[Black,30]
                         R-[Black,40]
                        """
        );
    }

    // right left case
    @Test
    public void testRemoveBlackNodeWithBlackSiblingWithRedLeftChild() {
        assertTree(
                """
                        [Black,30]
                         L-[Black,20]
                         R-[Black,40]
                           L-[Red,35]
                        """,
                30, 20, 40, 35
        );
        removeRowWithValue(20L);
        assertTree(
                """
                        [Black,35]
                         L-[Black,30]
                         R-[Black,40]
                        """
        );
    }

    // new test cases
    // current node is double black and not the root; sibling is black and at least one of its children is red
    // right-right case
    @Test
    public void testRemoveBlackNodeWithBlackSiblingWithRedRightChild() {
        assertTree(
                """
                        [Black,30]
                         L-[Black,20]
                         R-[Black,40]
                           L-[Red,35]
                           R-[Red,50]
                        """,
                30, 20, 40, 35, 50
        );
        removeRowWithValue(20L);
        assertTree(
                """
                        [Black,40]
                         L-[Black,30]
                           R-[Red,35]
                         R-[Black,50]
                        """
        );
    }

    @Test
    public void testRemoveBlackNodeWithBothChildrenAndRightIsNotSuccessor() {
        assertTree(
                """
                        [Black,1]
                         L-[Black,0]
                         R-[Red,3]
                           L-[Black,2]
                           R-[Black,5]
                             L-[Red,4]
                        """,
                0, 1, 2, 3, 5, 4
        );

        removeRowWithValue(3);
        assertTree(
                """
                        [Black,1]
                         L-[Black,0]
                         R-[Red,4]
                           L-[Black,2]
                           R-[Black,5]
                        """
        );
    }

    @Test
    public void testRemoveBlackNodeWithBothChildrenAndRightIsNotSuccessorButDoesNotRequireRotation() {
        assertTree(
                """
                        [Black,3]
                         L-[Red,1]
                           L-[Black,0]
                           R-[Black,2]
                         R-[Red,5]
                           L-[Black,4]
                           R-[Black,6]
                             R-[Red,7]
                        """,
                0, 1, 2, 3, 4, 5, 6, 7
        );

        removeRowWithValue(3);

        assertTree(
                """
                        [Black,4]
                         L-[Red,1]
                           L-[Black,0]
                           R-[Black,2]
                         R-[Red,6]
                           L-[Black,5]
                           R-[Black,7]
                        """
        );
    }

    @Test
    public void testRemoveBlackNodeWithBothChildrenAndRightIsNotSuccessorRequiresRotationTodo() {
        assertTree(
                """
                        [Black,1]
                         L-[Black,0]
                         R-[Red,3]
                           L-[Black,2]
                           R-[Black,5]
                             L-[Red,4]
                        """,
                0, 1, 2, 3, 5, 4
        );

        removeRowWithValue(3);

        assertTree(
                """
                        [Black,1]
                         L-[Black,0]
                         R-[Red,4]
                           L-[Black,2]
                           R-[Black,5]
                        """
        );
    }

    @Test
    public void testRemoveBlackNodeWithBothChildrenAndRightIsSuccessor() {
        assertTree(
                """
                        [Black,1]
                         L-[Black,0]
                         R-[Black,3]
                           L-[Red,2]
                           R-[Red,4]
                        """,
                0, 1, 2, 3, 4
        );
        removeRowWithValue(3);
        assertTree(
                """
                        [Black,1]
                         L-[Black,0]
                         R-[Black,4]
                           L-[Red,2]
                        """
        );
    }

    @Test
    public void testRemoveBlackNodeWithLeftChildOnly() {
        assertTree(
                """
                        [Black,1]
                         L-[Black,0]
                           L-[Red,-1]
                         R-[Black,2]
                           R-[Red,3]
                        """,
                0, 1, 2, 3, -1
        );
        removeRowWithValue(0);
        assertTree(
                """
                        [Black,1]
                         L-[Black,-1]
                         R-[Black,2]
                           R-[Red,3]
                        """
        );
    }

    @Test
    public void testRemoveBlackNodeWithNoChildren() {
        assertTree(
                """
                        [Black,1]
                         L-[Red,0]
                         R-[Red,2]
                        """,
                0, 1, 2
        );
        removeRowWithValue(2);
        assertTree(
                """
                        [Black,1]
                         L-[Red,0]
                        """
        );
    }

    @Test
    public void testRemoveBlackNodeWithRightChildOnly() {
        assertTree(
                """
                        [Black,1]
                         L-[Black,0]
                         R-[Black,2]
                           R-[Red,3]
                        """,
                0, 1, 2, 3
        );
        removeRowWithValue(2);
        assertTree(
                """
                        [Black,1]
                         L-[Black,0]
                         R-[Black,3]
                        """
        );
    }

    @Test
    public void testRemoveRedNodeWithBothChildrenAndRightIsBlackSuccessor() {
        assertTree(
                """
                        [Black,3]
                         L-[Red,1]
                           L-[Black,0]
                           R-[Black,2]
                         R-[Red,5]
                           L-[Black,4]
                           R-[Black,6]
                             R-[Red,7]
                        """,
                0, 1, 2, 3, 4, 5, 6, 7
        );
        removeRowWithValue(5);
        assertTree(
                """
                        [Black,3]
                         L-[Red,1]
                           L-[Black,0]
                           R-[Black,2]
                         R-[Red,6]
                           L-[Black,4]
                           R-[Black,7]
                        """
        );
    }

    @Test
    public void testValueHeapAcceptsRequiredEqualToMaxHeapSize() throws Exception {
        // A 36-byte value budget is an exact multiple of the 12-byte chain value, so the 3rd value
        // makes required exactly 36. That is the boundary of the throw predicate: a value that fits
        // exactly must be accepted, not rejected.
        // Close the @Before chain first, so the leak check brackets only the
        // replacement chain's clamped, non-power-of-two realloc/free accounting.
        chain.close();
        assertMemoryLeak(() -> {
            chain = new LimitedSizeLongTreeChain(
                    64,
                    Long.MAX_VALUE, // key heap uncapped
                    12,             // value page == CHAIN_VALUE_SIZE
                    36,             // value heap budget == 3 values exactly
                    PropertyKey.CAIRO_SQL_SORT_KEY_MAX_BYTES.getPropertyPath(),
                    PropertyKey.CAIRO_SQL_SORT_LIGHT_VALUE_MAX_BYTES.getPropertyPath()
            );
            chain.updateLimits(true, 1_000);
            Assert.assertEquals(3, fillUntilOverflow("limit of 36 memory exceeded in LimitedSizeLongTreeChain"));
            chain.close();
        });
    }

    @Test
    public void testValueHeapClampsToMaxHeapSize() throws Exception {
        // Same clamp on the value heap: a 96-byte budget is not a power of two, the chain goes
        // 16 -> 32 -> 64 and then wants 128. Clamping to 96 fits 8 of the 12-byte chain values
        // instead of the 5 that fitted before.
        // Close the @Before chain first, so the leak check brackets only the
        // replacement chain's clamped, non-power-of-two realloc/free accounting.
        chain.close();
        assertMemoryLeak(() -> {
            chain = new LimitedSizeLongTreeChain(
                    64,
                    Long.MAX_VALUE, // key heap uncapped
                    16,             // value page >= CHAIN_VALUE_SIZE
                    96,             // value heap budget, deliberately not a power of two
                    PropertyKey.CAIRO_SQL_SORT_KEY_MAX_BYTES.getPropertyPath(),
                    PropertyKey.CAIRO_SQL_SORT_LIGHT_VALUE_MAX_BYTES.getPropertyPath()
            );
            chain.updateLimits(true, 1_000);
            Assert.assertEquals(8, fillUntilOverflow("memory exceeded in LimitedSizeLongTreeChain"));
            chain.close();
        });
    }

    @Test
    public void testValueHeapOverflowNamesConfigKey() throws Exception {
        // Tiny value-heap budget (one page) with an uncapped key heap: the rowid value chain
        // overflows and the message must name the sort.light.value config key. This branch is
        // otherwise never fired by a test.
        chain.close();
        assertMemoryLeak(() -> {
            chain = new LimitedSizeLongTreeChain(
                    64,
                    Long.MAX_VALUE, // key heap uncapped
                    16,             // value page >= CHAIN_VALUE_SIZE; doubling past one page overflows
                    16,             // value heap budget = one page
                    PropertyKey.CAIRO_SQL_SORT_KEY_MAX_BYTES.getPropertyPath(),
                    PropertyKey.CAIRO_SQL_SORT_LIGHT_VALUE_MAX_BYTES.getPropertyPath()
            );
            chain.updateLimits(true, 1_000);
            final long[] values = new long[256];
            for (int i = 0; i < values.length; i++) {
                values[i] = i;
            }
            try {
                createTree(values);
                Assert.fail("expected LimitOverflowException");
            } catch (LimitOverflowException e) {
                TestUtils.assertContains(e.getFlyweightMessage(),
                        "memory exceeded in LimitedSizeLongTreeChain (raise cairo.sql.sort.light.value.max.bytes)");
            }
            chain.close();
        });
    }

    /**
     * Walks the tree back after a clamped growth step. Values go in ascending with rowId == value,
     * so the cursor has to yield 0..inserted-1 in order - which only holds if every heap-relative
     * offset survived the reallocs that moved the heap.
     */
    private void assertChainHolds(long... expected) {
        final LongList actual = new LongList();
        LimitedSizeLongTreeChain.TreeCursor treeCursor = chain.getCursor();
        while (treeCursor.hasNext()) {
            cursor.recordAt(placeholder, treeCursor.next());
            actual.add(placeholder.getLong(0));
        }
        Assert.assertEquals("kept count", expected.length, actual.size());
        for (int i = 0; i < expected.length; i++) {
            Assert.assertEquals("position " + i, expected[i], actual.getQuick(i));
        }
    }

    private void assertReadsBack(int inserted) {
        LimitedSizeLongTreeChain.TreeCursor treeCursor = chain.getCursor();
        int count = 0;
        while (treeCursor.hasNext()) {
            Assert.assertEquals(count, treeCursor.next());
            count++;
        }
        Assert.assertEquals(inserted, count);
    }

    private void assertTree(String expected, long... values) {
        createTree(values);
        assertTree(expected);
    }

    private void assertTree(String expected) {
        TestUtils.assertEquals(expected, toString(cursor, placeholder));
    }

    private void createTree(long... values) {
        cursor = new TestRecordCursor(values);
        left = (SingleLongRecord) cursor.getRecord();
        placeholder = (SingleLongRecord) cursor.getRecordB();
        comparator = new TestRecordComparator();
        comparator.setLeft(left);
        while (cursor.hasNext()) {
            chain.put(left, cursor, placeholder, comparator);
        }
    }

    /**
     * Inserts distinct ascending values until one of the heaps runs out, and returns how many
     * of them the chain accepted. Fails when no overflow happens at all.
     * <p>
     * Deliberately not shared with the namesake in {@code LongTreeChainTest}: this chain's
     * {@code put()} expects the caller to have set the comparator's left side, while
     * {@link io.questdb.griffin.engine.orderby.LongTreeChain#put} sets it itself, and the two
     * cursor types are unrelated inner classes.
     */
    private int fillUntilOverflow(String expectedMessage) {
        final long[] values = new long[256];
        for (int i = 0; i < values.length; i++) {
            values[i] = i;
        }
        cursor = new TestRecordCursor(values);
        left = (SingleLongRecord) cursor.getRecord();
        placeholder = (SingleLongRecord) cursor.getRecordB();
        comparator = new TestRecordComparator();
        comparator.setLeft(left);

        int inserted = 0;
        while (cursor.hasNext()) {
            try {
                chain.put(left, cursor, placeholder, comparator);
            } catch (LimitOverflowException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), expectedMessage);
                assertReadsBack(inserted);
                return inserted;
            }
            inserted++;
        }
        Assert.fail("expected LimitOverflowException");
        return -1;
    }

    private void putValue(long value) {
        cursor.recordAtValue(left, value);
        chain.put(left, cursor, placeholder, comparator);
    }

    private void removeRowWithValue(long value) {
        cursor.recordAtValue(left, value);
        int node = chain.find(left, cursor, placeholder, comparator);
        chain.removeAndCache(node);
    }

    @NotNull
    private String toString(TestRecordCursor cursor, SingleLongRecord right) {
        StringSink sink = new StringSink();
        chain.print(sink, rowid -> {
            cursor.recordAt(right, rowid);
            return String.valueOf(right.getLong(0));
        });
        return sink.toString();
    }
}
