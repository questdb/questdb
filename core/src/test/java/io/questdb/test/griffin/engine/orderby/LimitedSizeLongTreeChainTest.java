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
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.griffin.engine.LimitOverflowException;
import io.questdb.griffin.engine.RecordComparator;
import io.questdb.griffin.engine.orderby.LimitedSizeLongTreeChain;
import io.questdb.std.LongList;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test RBTree removal cases asserting final tree structure.
 */
public class LimitedSizeLongTreeChainTest extends AbstractCairoTest {
    // used in all tests to hide api complexity
    LimitedSizeLongTreeChain chain;
    RecordComparator comparator;
    TestRecordCursor cursor;
    TestRecord left;
    TestRecord placeholder;

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
    public void testKeyHeapOverflowNamesConfigKey() {
        // Tiny key-heap budget (one page) with an uncapped value heap: the red-black key heap
        // overflows and the message must name the sort.key config key. Pins the LimitedSize key
        // path's (raise ...) hint that no query-level test exercises.
        chain.close();
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
    }

    // sibling is black and its both children are black (?)
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
    public void testValueHeapOverflowNamesConfigKey() {
        // Tiny value-heap budget (one page) with an uncapped key heap: the rowid value chain
        // overflows and the message must name the sort.light.value config key. This branch is
        // otherwise never fired by a test.
        chain.close();
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
        left = (TestRecord) cursor.getRecord();
        placeholder = (TestRecord) cursor.getRecordB();
        comparator = new TestRecordComparator();
        comparator.setLeft(left);
        while (cursor.hasNext()) {
            chain.put(left, cursor, placeholder, comparator);
        }
    }

    private void removeRowWithValue(long value) {
        cursor.recordAtValue(left, value);
        int node = chain.find(left, cursor, placeholder, comparator);
        chain.removeAndCache(node);
    }

    @NotNull
    private String toString(TestRecordCursor cursor, TestRecord right) {
        StringSink sink = new StringSink();
        chain.print(sink, rowid -> {
            cursor.recordAt(right, rowid);
            return String.valueOf(right.getLong(0));
        });
        return sink.toString();
    }

    static class TestRecord implements Record {
        long position;
        long value;

        @Override
        public long getLong(int col) {
            return value;
        }

        @Override
        public long getRowId() {
            return position;
        }
    }

    static class TestRecordComparator implements RecordComparator {
        Record left;

        @Override
        public int compare(Record record) {
            return (int) (left.getLong(0) - record.getLong(0));
        }

        @Override
        public void setLeft(Record record) {
            left = record;
        }
    }

    static class TestRecordCursor implements RecordCursor {
        final Record left = new TestRecord();
        final Record right = new TestRecord();
        final LongList values = new LongList();
        int position = -1;

        TestRecordCursor(long... newValues) {
            for (int i = 0; i < newValues.length; i++) {
                this.values.add(newValues[i]);
            }
        }

        @Override
        public void close() {
            // nothing to do here
        }

        @Override
        public Record getRecord() {
            return left;
        }

        @Override
        public Record getRecordB() {
            return right;
        }

        @Override
        public boolean hasNext() {
            if (position < values.size() - 1) {
                position++;
                recordAt(left, position);
                return true;
            }

            return false;
        }

        @Override
        public long preComputedStateSize() {
            return 0;
        }

        @Override
        public void recordAt(Record record, long atRowId) {
            ((TestRecord) record).value = values.get((int) atRowId);
            ((TestRecord) record).position = atRowId;
        }

        public void recordAtValue(Record record, long value) {
            for (int i = 0; i < values.size(); i++) {
                if (values.get(i) == value) {
                    recordAt(record, i);
                    return;
                }
            }

            throw new RuntimeException("Can't find value " + value + " in " + values);
        }

        @Override
        public long size() {
            return values.size();
        }

        @Override
        public void toTop() {
            position = 0;
        }
    }
}
