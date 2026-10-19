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
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.engine.join.JoinRecord;
import io.questdb.griffin.engine.join.UnnestRecord;
import io.questdb.griffin.engine.table.ExtraNullColumnRecord;
import io.questdb.griffin.engine.table.HorizonJoinRecord;
import io.questdb.griffin.engine.table.MultiHorizonJoinRecord;
import io.questdb.griffin.engine.table.SelectedRecord;
import io.questdb.griffin.engine.union.UnionRecord;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;

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
    public void testExtraNullColumnRecordForwardsBaseColumn() {
        // columnSplit = 1: column 0 is a base column, so the direct array accessors must forward to
        // the base record; column 1 is a spliced NULL column and reports a NULL array instead.
        final ExtraNullColumnRecord record = new ExtraNullColumnRecord(1);
        record.of(new DirectOnlyArrayRecord(0));
        assertForwards(record);
        assertNullSource(record, 1);
    }

    @Test
    public void testHorizonJoinRecordForwards() {
        final HorizonJoinRecord record = new HorizonJoinRecord();
        // Column 0 reads the master's column 2, so a forwarding override that dropped the column
        // mapping would read the wrong column. Column 1 has no source record at all.
        record.init(
                new int[]{HorizonJoinRecord.SOURCE_MASTER, HorizonJoinRecord.SOURCE_SLAVE, HorizonJoinRecord.SOURCE_SEQUENCE},
                new int[]{2, 3, 0}
        );
        record.of(new DirectOnlyArrayRecord(2), 0, 0, new DirectOnlyArrayRecord(3));
        assertForwards(record);
        assertForwards(record, 1);
        assertNullSource(record, 2);
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
    public void testMaterializedRecordForwards() throws Exception {
        final Class<?> recordClass = Class.forName("io.questdb.griffin.engine.orderby.MaterializedRecord");
        final Constructor<?> constructor = recordClass.getDeclaredConstructor();
        constructor.setAccessible(true);
        final Record record = (Record) constructor.newInstance();
        final Method of = recordClass.getDeclaredMethod(
                "of",
                Record.class,
                int[].class,
                int[].class,
                MemoryCARW[].class
        );
        of.setAccessible(true);
        of.invoke(record, new DirectOnlyArrayRecord(0), new int[]{-1}, new int[]{0}, new MemoryCARW[0]);
        assertForwards(record);
    }

    @Test
    public void testMultiHorizonJoinRecordForwards() {
        final MultiHorizonJoinRecord record = new MultiHorizonJoinRecord(1);
        record.init(
                new int[]{MultiHorizonJoinRecord.SOURCE_MASTER, MultiHorizonJoinRecord.SOURCE_SLAVE_BASE, MultiHorizonJoinRecord.SOURCE_SEQUENCE},
                new int[]{2, 3, 0}
        );
        final ObjList<Record> slaves = new ObjList<>();
        slaves.add(new DirectOnlyArrayRecord(3));
        record.of(new DirectOnlyArrayRecord(2), 0, 0, slaves);
        assertForwards(record);
        assertForwards(record, 1);
        assertNullSource(record, 2);
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

    @Test
    public void testUnionRecordForwardsBothBranches() {
        final UnionRecord record = new UnionRecord();
        record.of(new DirectOnlyArrayRecord(0), new DirectOnlyArrayRecord(0));
        assertForwards(record);
        record.setAb(false);
        assertForwards(record);
    }

    @Test
    public void testUnnestRecordForwardsBaseColumn() throws Exception {
        // split = 1: column 0 is a base table column, so getArrayDimLen/getArrayDouble1d2d must
        // forward to the base record rather than fall through to Record's ArrayView default. The
        // sources list stays empty because only the base-column branch is under test here.
        final UnnestRecord record = new UnnestRecord(1, new ObjList<>());
        // of(Record) is package-private; reflect it in to seat the base record.
        final Method of = UnnestRecord.class.getDeclaredMethod("of", Record.class);
        of.setAccessible(true);
        of.invoke(record, new DirectOnlyArrayRecord(0));
        assertForwards(record);
    }

    @Test
    public void testWindowLightRecordForwardsBothMappings() throws Exception {
        final IntList sourceMap = new IntList();
        sourceMap.add(2);
        sourceMap.add(-4);
        final Class<?> recordClass = Class.forName("io.questdb.griffin.engine.window.WindowLightRecord");
        final Constructor<?> constructor = recordClass.getDeclaredConstructor(IntList.class);
        constructor.setAccessible(true);
        final Record record = (Record) constructor.newInstance(sourceMap);
        final Method of = recordClass.getDeclaredMethod("of", Record.class, Record.class, long.class);
        of.setAccessible(true);
        of.invoke(record, new DirectOnlyArrayRecord(2), new DirectOnlyArrayRecord(3), 0L);
        assertForwards(record);
        assertForwards(record, 1);
    }

    private static void assertForwards(Record record) {
        Assert.assertEquals(DIM_LEN_ANSWER, record.getArrayDimLen(0, COL_TYPE, 1));
        Assert.assertEquals(DOUBLE_ANSWER, record.getArrayDouble1d2d(0, COL_TYPE, 0, 0), 0.0);
    }

    private static void assertForwards(Record record, int col) {
        Assert.assertEquals(DIM_LEN_ANSWER, record.getArrayDimLen(col, COL_TYPE, 1));
        Assert.assertEquals(DOUBLE_ANSWER, record.getArrayDouble1d2d(col, COL_TYPE, 0, 0), 0.0);
    }

    /**
     * Column 2 of the horizon records has no source record behind it, which the direct accessors
     * must report as a NULL array, exactly as their getArray() reports a NULL ArrayView.
     */
    private static void assertNullSource(Record record, int col) {
        Assert.assertEquals(Numbers.INT_NULL, record.getArrayDimLen(col, COL_TYPE, 1));
        Assert.assertTrue(Numbers.isNull(record.getArrayDouble1d2d(col, COL_TYPE, 0, 0)));
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
