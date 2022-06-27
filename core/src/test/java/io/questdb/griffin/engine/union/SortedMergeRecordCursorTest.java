/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.griffin.engine.union;

import io.questdb.cairo.*;
import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.griffin.AbstractGriffinTest;
import io.questdb.griffin.engine.RecordComparator;
import io.questdb.std.Rnd;
import org.junit.Assert;
import org.junit.Test;

public final class SortedMergeRecordCursorTest extends AbstractGriffinTest {

    private static final GenericRecordMetadata METADATA = new GenericRecordMetadata()
            .add(0, new TableColumnMetadata("foo", 0, ColumnType.LONG));

    // 100 records with default seed
    private static final String SINGLE_RESULT = "foo\n0\n8\n11\n12\n13\n22\n24\n25\n31\n39\n45\n47\n51\n54\n55\n57\n63\n" +
            "70\n75\n79\n86\n93\n97\n104\n111\n111\n111\n119\n124\n128\n133\n134\n138\n147\n155\n164\n171\n179\n182\n190\n" +
            "190\n193\n194\n194\n195\n199\n202\n208\n213\n217\n219\n226\n229\n231\n231\n239\n242\n251\n259\n267\n274\n280\n" +
            "286\n290\n298\n300\n306\n306\n306\n310\n315\n318\n322\n330\n335\n338\n341\n344\n345\n352\n361\n368\n374\n382\n" +
            "388\n397\n402\n407\n410\n416\n417\n417\n420\n428\n432\n436\n441\n443\n447\n450\n";

    // 100 records with default seed merged with 100 records with swapped seed
    private static final String EQUAL_MERGED_RESULT = "foo\n0\n8\n8\n11\n12\n13\n14\n22\n22\n24\n25\n28\n31\n33\n37\n39\n41\n43\n" +
            "43\n45\n45\n47\n51\n52\n54\n55\n57\n58\n63\n63\n69\n70\n72\n75\n79\n81\n86\n87\n87\n87\n90\n90\n93\n94\n96\n97\n" +
            "100\n102\n104\n108\n110\n111\n111\n111\n115\n117\n119\n120\n124\n128\n128\n129\n131\n133\n134\n138\n139\n141\n147\n" +
            "147\n148\n155\n157\n164\n164\n171\n171\n171\n177\n179\n182\n184\n184\n185\n190\n190\n192\n193\n194\n194\n195\n199\n" +
            "200\n202\n204\n208\n209\n213\n213\n217\n219\n220\n226\n229\n229\n229\n231\n231\n237\n238\n239\n242\n244\n247\n248\n" +
            "251\n255\n259\n263\n263\n266\n267\n271\n273\n274\n278\n279\n280\n282\n286\n287\n287\n289\n290\n292\n298\n300\n300\n" +
            "306\n306\n306\n307\n307\n310\n315\n315\n318\n322\n322\n322\n329\n329\n330\n335\n335\n338\n340\n341\n341\n344\n345\n" +
            "345\n352\n352\n358\n361\n366\n368\n370\n373\n374\n376\n382\n384\n388\n391\n394\n397\n398\n402\n402\n404\n407\n408\n" +
            "410\n414\n415\n416\n417\n417\n420\n421\n424\n428\n432\n436\n441\n443\n447\n450\n";

    // 50 records with default seed merged with 100 records with swapped seed
    private static final String LEFT_SIDE_SMALLER_MERGED_RESULT = "foo\n0\n8\n8\n11\n12\n13\n14\n22\n22\n24\n25\n28\n31\n33\n37\n39\n41\n43\n43\n45\n45\n47\n" +
            "51\n52\n54\n55\n57\n58\n63\n63\n69\n70\n72\n75\n79\n81\n86\n87\n87\n87\n90\n90\n93\n94\n96\n97\n100\n102\n104\n108\n" +
            "110\n111\n111\n111\n115\n117\n119\n120\n124\n128\n128\n129\n131\n133\n134\n138\n139\n141\n147\n147\n148\n155\n157\n" +
            "164\n164\n171\n171\n171\n177\n179\n182\n184\n184\n185\n190\n190\n192\n193\n194\n194\n195\n199\n200\n202\n204\n208\n" +
            "209\n213\n213\n217\n220\n229\n229\n237\n238\n244\n247\n248\n255\n263\n263\n266\n271\n273\n278\n279\n282\n287\n287\n" +
            "289\n292\n300\n307\n307\n315\n322\n322\n329\n329\n335\n340\n341\n345\n352\n358\n366\n370\n373\n376\n384\n391\n394\n" +
            "398\n402\n404\n408\n414\n415\n421\n424\n";

    private static final String RIGHT_SIDE_SMALLER_MERGED_RESULT = "foo\n0\n8\n8\n11\n12\n13\n14\n22\n22\n24\n25\n28\n31\n33\n37\n39\n41\n43\n43\n45\n45\n47\n" +
            "51\n52\n54\n55\n57\n58\n63\n63\n69\n70\n72\n75\n79\n81\n86\n87\n87\n87\n90\n90\n93\n94\n96\n97\n100\n102\n104\n108\n" +
            "110\n111\n111\n111\n115\n117\n119\n120\n124\n128\n128\n129\n131\n133\n134\n138\n139\n141\n147\n147\n148\n155\n157\n" +
            "164\n164\n171\n171\n171\n177\n179\n182\n184\n184\n185\n190\n190\n192\n193\n194\n194\n195\n199\n200\n202\n204\n208\n" +
            "209\n213\n213\n217\n219\n226\n229\n231\n231\n239\n242\n251\n259\n267\n274\n280\n286\n290\n298\n300\n306\n306\n306\n" +
            "310\n315\n318\n322\n330\n335\n338\n341\n344\n345\n352\n361\n368\n374\n382\n388\n397\n402\n407\n410\n416\n417\n417\n" +
            "420\n428\n432\n436\n441\n443\n447\n450\n";
    private static final String EMPTY_RESULT = "foo\n";

    static class TestRecord implements Record {
        long value;
        private boolean ready;

        @Override
        public long getLong(int col) {
            if (!ready) {
                throw new IllegalStateException("cannot get a value out of uninitialized record");
            }
            return value;
        }

        public void ready() {
            ready = true;
        }

        public void notReady() {
            ready = false;
        }
    }

    static class TestRecordCursor implements NoRandomAccessRecordCursor {
        private final int recordCount;
        private final boolean knownSize;
        private int remaining;
        private final TestRecord leftRecord = new TestRecord();
        private final Rnd rnd;
        private final long s0;
        private final long s1;
        private boolean isClosed;

        public TestRecordCursor() {
            this(100, new Rnd(), true);
        }

        public TestRecordCursor(int recordCount, Rnd rnd, boolean knownSize) {
            this.rnd = rnd;
            this.remaining = recordCount;
            this.recordCount = recordCount;
            this.s0 = rnd.getS0();
            this.s1 = rnd.getS1();
            this.knownSize = knownSize;
        }

        @Override
        public void close() {
            isClosed = true;
        }

        public boolean isClosed() {
            return isClosed;
        }

        @Override
        public Record getRecord() {
            return leftRecord;
        }

        @Override
        public boolean hasNext() {
            if (remaining == 0) {
                return false;
            }
            if (remaining == recordCount) {
                leftRecord.ready();
            }
            remaining--;
            int nextInc = rnd.nextInt(10);
            leftRecord.value += nextInc;
            return true;
        }

        @Override
        public void toTop() {
            leftRecord.value = 0;
            remaining = recordCount;
            rnd.reset(s0, s1);
            leftRecord.notReady();
        }

        @Override
        public long size() {
            return knownSize ? recordCount : -1;
        }
    }

    private class TestRecordComparator implements RecordComparator {
        private long leftLong;

        @Override
        public int compare(Record record) {
            return Long.compare(leftLong, record.getLong(0));
        }

        @Override
        public void setLeft(Record record) {
            // mimic behaviour of RecordComparators produced by the RecordComparatorCompiler. The compiled comparators
            // cache left side. so let's do the same.
            this.leftLong = record.getLong(0);
        }
    }

    @Test
    public void testSanity() throws Exception {
        assertMemoryLeak(() -> {
            TestRecordCursor cursor = new TestRecordCursor();
            GenericRecordMetadata metadata = new GenericRecordMetadata();
            metadata.add(0, new TableColumnMetadata("foo", 0, ColumnType.LONG));
            assertCursor(SINGLE_RESULT, false, true, true, false, cursor, METADATA, true);
        });
    }

    @Test
    public void testHappyMerging() throws Exception {
        TestRecordCursor cursorA = new TestRecordCursor(100, new Rnd(0xdeadbeef, 0xdee4c0ed), true);
        TestRecordCursor cursorB = new TestRecordCursor(100, new Rnd(0xdee4c0ed, 0xdeadbeef), true);

        assertScenarios(cursorA, cursorB, EQUAL_MERGED_RESULT, 200);
    }

    @Test
    public void testBothEmpty() throws Exception {
        TestRecordCursor cursorA = new TestRecordCursor(0, new Rnd(), true);
        TestRecordCursor cursorB = new TestRecordCursor(0, new Rnd(), true);

        assertScenarios(cursorA, cursorB, EMPTY_RESULT, 0);
    }

    @Test
    public void testAEmpty() throws Exception {
        TestRecordCursor cursorA = new TestRecordCursor(0, new Rnd(), true);
        TestRecordCursor cursorB = new TestRecordCursor(100, new Rnd(), true);

        assertScenarios(cursorA, cursorB, SINGLE_RESULT, 100);
    }

    @Test
    public void testASmaller() throws Exception {
        TestRecordCursor cursorA = new TestRecordCursor(50, new Rnd(0xdeadbeef, 0xdee4c0ed), true);
        TestRecordCursor cursorB = new TestRecordCursor(100, new Rnd(0xdee4c0ed, 0xdeadbeef), true);

        assertScenarios(cursorA, cursorB, LEFT_SIDE_SMALLER_MERGED_RESULT, 150);
    }

    @Test
    public void testBSmaller() throws Exception {
        TestRecordCursor cursorA = new TestRecordCursor(100, new Rnd(0xdeadbeef, 0xdee4c0ed), true);
        TestRecordCursor cursorB = new TestRecordCursor(50, new Rnd(0xdee4c0ed, 0xdeadbeef), true);

        assertScenarios(cursorA, cursorB, RIGHT_SIDE_SMALLER_MERGED_RESULT, 150);
    }

    @Test
    public void testBEmpty() throws Exception {
        TestRecordCursor cursorA = new TestRecordCursor(100, new Rnd(), true);
        TestRecordCursor cursorB = new TestRecordCursor(0, new Rnd(), true);

        assertScenarios(cursorA, cursorB, SINGLE_RESULT, 100);
    }

    @Test
    public void testAUnknownSize()  throws Exception {
        TestRecordCursor cursorA = new TestRecordCursor(100, new Rnd(0xdeadbeef, 0xdee4c0ed), false);
        TestRecordCursor cursorB = new TestRecordCursor(100, new Rnd(0xdee4c0ed, 0xdeadbeef), true);

        assertScenarios(cursorA, cursorB, EQUAL_MERGED_RESULT, -1);
    }

    @Test
    public void testBUnknownSize() throws Exception {
        TestRecordCursor cursorA = new TestRecordCursor(100, new Rnd(0xdeadbeef, 0xdee4c0ed), true);
        TestRecordCursor cursorB = new TestRecordCursor(100, new Rnd(0xdee4c0ed, 0xdeadbeef), false);

        assertScenarios(cursorA, cursorB, EQUAL_MERGED_RESULT, -1);
    }

    @Test
    public void testBothUnknownSize() throws Exception {
        TestRecordCursor cursorA = new TestRecordCursor(100, new Rnd(0xdeadbeef, 0xdee4c0ed), false);
        TestRecordCursor cursorB = new TestRecordCursor(100, new Rnd(0xdee4c0ed, 0xdeadbeef), false);

        assertScenarios(cursorA, cursorB, EQUAL_MERGED_RESULT, -1);
    }
    
    private void assertScenarios(TestRecordCursor cursorA, TestRecordCursor cursorB, String expectedResult, int expectedSize) throws Exception {
        assertMemoryLeak(() -> {
            try (SortedMergeRecordCursor mergeSortRecordCursor = new SortedMergeRecordCursor()) {
                mergeSortRecordCursor.of(cursorA, cursorB, new TestRecordComparator());

                // check cursor indicates expected size()
                Assert.assertEquals(expectedSize, mergeSortRecordCursor.size());

                // check cursor yields expected records
                mergeSortRecordCursor.toTop();
                assertCursor(expectedResult, false, true, expectedSize != -1, false, mergeSortRecordCursor, METADATA, true);
                assertAllExhausted(cursorA, cursorB, mergeSortRecordCursor);

                // check toTop() rewinds cursor
                mergeSortRecordCursor.toTop();
                assertCursor(expectedResult, false, true, expectedSize != -1, false, mergeSortRecordCursor, METADATA, true);
                assertAllExhausted(cursorA, cursorB, mergeSortRecordCursor);

                // of() rewinds upstream cursors to top
                mergeSortRecordCursor.of(cursorA, cursorB, new TestRecordComparator());
                assertCursor(expectedResult, false, true, expectedSize != -1, false, mergeSortRecordCursor, METADATA, true);
                assertAllExhausted(cursorA, cursorB, mergeSortRecordCursor);
            }
            // assert upstream cursors are closed
            Assert.assertTrue(cursorA.isClosed);
            Assert.assertTrue(cursorB.isClosed);
        });
    }
    
    private static void assertAllExhausted(RecordCursor...cursors) {
        for (RecordCursor cursor : cursors) {
            Assert.assertFalse(cursor.hasNext());
        }
    }
}
