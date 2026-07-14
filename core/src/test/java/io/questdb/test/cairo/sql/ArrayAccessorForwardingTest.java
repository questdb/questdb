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

package io.questdb.test.cairo.sql;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.sql.DelegatingRecord;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.join.JoinRecord;
import io.questdb.griffin.engine.table.HorizonJoinRecord;
import io.questdb.griffin.engine.table.MultiHorizonJoinRecord;
import io.questdb.griffin.engine.table.SelectedRecord;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import org.junit.Assert;
import org.junit.Test;

/**
 * Pins the O(1) array-dimension path through the records that wrap a page frame.
 * <p>
 * {@code PageFrameMemoryRecord.getArrayDimLen}/{@code getArrayDouble1d2d} read the shape header
 * straight off the frame. A wrapper record that does not forward them silently falls back to
 * {@link Record}'s default, which calls {@code getArray()} and materializes an {@code ArrayView}
 * for every row - same answer, so no result assertion can tell the two apart, which is why the
 * optimization needs a test of its own.
 * <p>
 * The base record below refuses to hand out an {@code ArrayView} at all. A wrapper that forwards
 * reads through it; one that falls back trips the {@code getArray()} guard.
 * <p>
 * Narrow unit test: no native memory, so no assertMemoryLeak.
 */
public class ArrayAccessorForwardingTest {
    private static final int COL_TYPE = ColumnType.encodeArrayType(ColumnType.DOUBLE, 2);
    private static final int DIM_LEN_ANSWER = 7;
    private static final double DOUBLE_ANSWER = 1.5;

    @Test
    public void testDelegatingRecordForwards() {
        // A pass-through wrapper: it remaps nothing, so the only thing to pin is that it forwards the
        // direct accessors at all rather than falling through to Record's ArrayView default.
        final DelegatingRecord record = new DelegatingRecord();
        record.of(new DirectOnlyArrayRecord(0));
        assertForwards(record);
    }

    @Test
    public void testHorizonJoinRecordForwards() {
        final HorizonJoinRecord record = new HorizonJoinRecord();
        // Column 0 reads the master's column 2, so a forwarding override that dropped the column
        // mapping would read the wrong column. Column 1 has no source record at all.
        record.init(
                new int[]{HorizonJoinRecord.SOURCE_MASTER, HorizonJoinRecord.SOURCE_SEQUENCE},
                new int[]{2, 0}
        );
        record.of(new DirectOnlyArrayRecord(2), 0, 0, null);
        assertForwards(record);
        assertNullSource(record);
    }

    @Test
    public void testJoinRecordForwards() {
        // split = 1: column 0 is the master's column 0, column 1 the slave's column 0.
        final JoinRecord record = new JoinRecord(1);
        record.of(new DirectOnlyArrayRecord(0), new DirectOnlyArrayRecord(0));
        assertForwards(record);
        Assert.assertEquals(DIM_LEN_ANSWER, record.getArrayDimLen(1, COL_TYPE, 1));
        Assert.assertEquals(DOUBLE_ANSWER, record.getArrayDouble1d2d(1, COL_TYPE, 0, 0), 0.0);
    }

    @Test
    public void testMultiHorizonJoinRecordForwards() {
        final MultiHorizonJoinRecord record = new MultiHorizonJoinRecord(1);
        record.init(
                new int[]{MultiHorizonJoinRecord.SOURCE_MASTER, MultiHorizonJoinRecord.SOURCE_SEQUENCE},
                new int[]{2, 0}
        );
        final ObjList<Record> slaves = new ObjList<>();
        slaves.add(new DirectOnlyArrayRecord(2));
        record.of(new DirectOnlyArrayRecord(2), 0, 0, slaves);
        assertForwards(record);
        assertNullSource(record);
    }

    @Test
    public void testSelectedRecordForwards() {
        // A non-identity cross index: the projection's column 0 is the base's column 2, so an
        // override that forwarded the raw column index would read the wrong column.
        final IntList crossIndex = new IntList();
        crossIndex.add(2);
        final SelectedRecord record = new SelectedRecord(crossIndex);
        record.of(new DirectOnlyArrayRecord(2));
        assertForwards(record);
    }

    private static void assertForwards(Record record) {
        Assert.assertEquals(DIM_LEN_ANSWER, record.getArrayDimLen(0, COL_TYPE, 1));
        Assert.assertEquals(DOUBLE_ANSWER, record.getArrayDouble1d2d(0, COL_TYPE, 0, 0), 0.0);
    }

    /**
     * Column 1 of the horizon records has no source record behind it, which the direct accessors
     * must report as a NULL array, exactly as their getArray() reports a NULL ArrayView.
     */
    private static void assertNullSource(Record record) {
        Assert.assertEquals(Numbers.INT_NULL, record.getArrayDimLen(1, COL_TYPE, 1));
        Assert.assertTrue(Numbers.isNull(record.getArrayDouble1d2d(1, COL_TYPE, 0, 0)));
    }

    /**
     * Serves the two direct array accessors and nothing else, standing in for the page frame record
     * whose shape-header reads the wrappers must forward to. It answers only for {@code expectedCol},
     * so a wrapper that forwards the raw column index instead of the mapped one fails here.
     */
    private static class DirectOnlyArrayRecord implements Record {
        private final int expectedCol;

        private DirectOnlyArrayRecord(int expectedCol) {
            this.expectedCol = expectedCol;
        }

        @Override
        public ArrayView getArray(int col, int columnType) {
            throw new AssertionError("wrapper materialized an ArrayView instead of forwarding the"
                    + " direct array accessor, so the O(1) shape-header read is lost for every row");
        }

        @Override
        public int getArrayDimLen(int col, int columnType, int dim) {
            assertCol(col);
            return DIM_LEN_ANSWER;
        }

        @Override
        public double getArrayDouble1d2d(int col, int columnType, int idx0, int idx1) {
            assertCol(col);
            return DOUBLE_ANSWER;
        }

        private void assertCol(int col) {
            Assert.assertEquals("wrapper forwarded the wrong column index", expectedCol, col);
        }
    }
}
