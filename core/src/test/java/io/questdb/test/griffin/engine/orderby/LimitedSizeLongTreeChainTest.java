/*******************************************************************************
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

import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.griffin.engine.RecordComparator;
import io.questdb.griffin.engine.orderby.LimitedSizeLongTreeChain;
import io.questdb.std.LongList;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
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
                configuration.getSqlSortKeyMaxPages(),
                configuration.getSqlSortLightValuePageSize(),
                configuration.getSqlSortLightValueMaxPages()
        );
        chain.updateLimits(true, 20);
    }

    @Test
    public void testCreateOrderedTree() {
        assertTree(
                "[Black,2]\n" +
                        " L-[Black,1]\n" +
                        " R-[Black,4]\n" +
                        "   L-[Red,3]\n" +
                        "   R-[Red,5]\n",
                1, 2, 3, 4, 5
        );
    }

    @Test
    public void testCreateOrderedTreeWithDuplicates() {
        assertTree(
                "[Black,2]\n" +
                        " L-[Black,1(2)]\n" +
                        " R-[Black,4(2)]\n" +
                        "   L-[Red,3(2)]\n" +
                        "   R-[Red,5]\n",
                1, 2, 3, 4, 5, 1, 4, 3
        );
    }

    @Test
    public void testCreateOrderedTreeWithInputInDescendingOrder() {
        assertTree(
                "[Black,4]\n" +
                        " L-[Black,2]\n" +
                        "   L-[Red,1]\n" +
                        "   R-[Red,3]\n" +
                        " R-[Black,5]\n",
                5, 4, 3, 2, 1
        );
    }

    @Test
    public void testCreateOrderedTreeWithInputInNoOrder() {
        assertTree(
                "[Black,3]\n" +
                        " L-[Black,2]\n" +
                        "   L-[Red,1]\n" +
                        " R-[Black,5]\n" +
                        "   L-[Red,4]\n",
                3, 2, 5, 1, 4
        );
    }

    // sibling is black and its both children are black (?)
    @Test
    public void testRemoveBlackNodeWithBlackSiblingWithBothChildrenBlack() {
        assertTree(
                "[Black,30]\n" +
                        " L-[Black,20]\n" +
                        " R-[Black,40]\n" +
                        "   L-[Red,35]\n",
                30, 20, 40, 35
        );
        removeRowWithValue(20L);
        assertTree(
                "[Black,35]\n" +
                        " L-[Black,30]\n" +
                        " R-[Black,40]\n"
        );
    }

    // right left case
    @Test
    public void testRemoveBlackNodeWithBlackSiblingWithRedLeftChild() {
        assertTree(
                "[Black,30]\n" +
                        " L-[Black,20]\n" +
                        " R-[Black,40]\n" +
                        "   L-[Red,35]\n",
                30, 20, 40, 35
        );
        removeRowWithValue(20L);
        assertTree(
                "[Black,35]\n" +
                        " L-[Black,30]\n" +
                        " R-[Black,40]\n"
        );
    }

    // new test cases
    // current node is double black and not the root; sibling is black and at least one of its children is red
    // right-right case
    @Test
    public void testRemoveBlackNodeWithBlackSiblingWithRedRightChild() {
        assertTree(
                "[Black,30]\n" +
                        " L-[Black,20]\n" +
                        " R-[Black,40]\n" +
                        "   L-[Red,35]\n" +
                        "   R-[Red,50]\n",
                30, 20, 40, 35, 50
        );
        removeRowWithValue(20L);
        assertTree(
                "[Black,40]\n" +
                        " L-[Black,30]\n" +
                        "   R-[Red,35]\n" +
                        " R-[Black,50]\n"
        );
    }

    @Test
    public void testRemoveBlackNodeWithBothChildrenAndRightIsNotSuccessor() {
        assertTree(
                "[Black,1]\n" +
                        " L-[Black,0]\n" +
                        " R-[Red,3]\n" +
                        "   L-[Black,2]\n" +
                        "   R-[Black,5]\n" +
                        "     L-[Red,4]\n",
                0, 1, 2, 3, 5, 4
        );

        removeRowWithValue(3);
        assertTree(
                "[Black,1]\n" +
                        " L-[Black,0]\n" +
                        " R-[Red,4]\n" +
                        "   L-[Black,2]\n" +
                        "   R-[Black,5]\n"
        );
    }

    @Test
    public void testRemoveBlackNodeWithBothChildrenAndRightIsNotSuccessorButDoesNotRequireRotation() {
        assertTree(
                "[Black,3]\n" +
                        " L-[Red,1]\n" +
                        "   L-[Black,0]\n" +
                        "   R-[Black,2]\n" +
                        " R-[Red,5]\n" +
                        "   L-[Black,4]\n" +
                        "   R-[Black,6]\n" +
                        "     R-[Red,7]\n",
                0, 1, 2, 3, 4, 5, 6, 7
        );

        removeRowWithValue(3);

        assertTree(
                "[Black,4]\n" +
                        " L-[Red,1]\n" +
                        "   L-[Black,0]\n" +
                        "   R-[Black,2]\n" +
                        " R-[Red,6]\n" +
                        "   L-[Black,5]\n" +
                        "   R-[Black,7]\n"
        );
    }

    @Test
    public void testRemoveBlackNodeWithBothChildrenAndRightIsNotSuccessorRequiresRotationTodo() {
        assertTree(
                "[Black,1]\n" +
                        " L-[Black,0]\n" +
                        " R-[Red,3]\n" +
                        "   L-[Black,2]\n" +
                        "   R-[Black,5]\n" +
                        "     L-[Red,4]\n",
                0, 1, 2, 3, 5, 4
        );

        removeRowWithValue(3);

        assertTree(
                "[Black,1]\n" +
                        " L-[Black,0]\n" +
                        " R-[Red,4]\n" +
                        "   L-[Black,2]\n" +
                        "   R-[Black,5]\n"
        );
    }

    @Test
    public void testRemoveBlackNodeWithBothChildrenAndRightIsSuccessor() {
        assertTree(
                "[Black,1]\n" +
                        " L-[Black,0]\n" +
                        " R-[Black,3]\n" +
                        "   L-[Red,2]\n" +
                        "   R-[Red,4]\n",
                0, 1, 2, 3, 4
        );
        removeRowWithValue(3);
        assertTree(
                "[Black,1]\n" +
                        " L-[Black,0]\n" +
                        " R-[Black,4]\n" +
                        "   L-[Red,2]\n"
        );
    }

    @Test
    public void testRemoveBlackNodeWithLeftChildOnly() {
        assertTree(
                "[Black,1]\n" +
                        " L-[Black,0]\n" +
                        "   L-[Red,-1]\n" +
                        " R-[Black,2]\n" +
                        "   R-[Red,3]\n",
                0, 1, 2, 3, -1
        );
        removeRowWithValue(0);
        assertTree(
                "[Black,1]\n" +
                        " L-[Black,-1]\n" +
                        " R-[Black,2]\n" +
                        "   R-[Red,3]\n"
        );
    }

    @Test
    public void testRemoveBlackNodeWithNoChildren() {
        assertTree(
                "[Black,1]\n" +
                        " L-[Red,0]\n" +
                        " R-[Red,2]\n",
                0, 1, 2
        );
        removeRowWithValue(2);
        assertTree(
                "[Black,1]\n" +
                        " L-[Red,0]\n"
        );
    }

    @Test
    public void testRemoveBlackNodeWithRightChildOnly() {
        assertTree(
                "[Black,1]\n" +
                        " L-[Black,0]\n" +
                        " R-[Black,2]\n" +
                        "   R-[Red,3]\n",
                0, 1, 2, 3
        );
        removeRowWithValue(2);
        assertTree(
                "[Black,1]\n" +
                        " L-[Black,0]\n" +
                        " R-[Black,3]\n"
        );
    }

    @Test
    public void testRemoveRedNodeWithBothChildrenAndRightIsBlackSuccessor() {
        assertTree(
                "[Black,3]\n" +
                        " L-[Red,1]\n" +
                        "   L-[Black,0]\n" +
                        "   R-[Black,2]\n" +
                        " R-[Red,5]\n" +
                        "   L-[Black,4]\n" +
                        "   R-[Black,6]\n" +
                        "     R-[Red,7]\n",
                0, 1, 2, 3, 4, 5, 6, 7
        );
        removeRowWithValue(5);
        assertTree(
                "[Black,3]\n" +
                        " L-[Red,1]\n" +
                        "   L-[Black,0]\n" +
                        "   R-[Black,2]\n" +
                        " R-[Red,6]\n" +
                        "   L-[Black,4]\n" +
                        "   R-[Black,7]\n"
        );
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
