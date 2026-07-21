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

package io.questdb.test.griffin.engine.table;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.FullPartitionFrameCursorFactory;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.IntervalPartitionFrameCursorFactory;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.PartitionFrame;
import io.questdb.cairo.sql.PartitionFrameCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.idx.IndexReader;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.table.PageFrameRecordCursorFactory;
import io.questdb.griffin.engine.table.PageFrameRowCursorFactory;
import io.questdb.griffin.engine.table.SymbolIndexRowCursorFactory;
import io.questdb.griffin.model.RuntimeIntervalModel;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.Numbers;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_ASC;

/**
 * The bounded page-frame cursors a localized out-of-order live-view repair reads
 * through, in both directions.
 * <p>
 * Forward, the repair proves a convergence boundary {@code H} above which no output can
 * change; that guarantee buys nothing unless the source scan actually stops there, which
 * a record-level stop filter cannot deliver - it would still visit every partition above
 * {@code H} before rejecting its rows.
 * <p>
 * Backward, the repair does not know where its dependency floor {@code L} sits: it walks
 * down from the output floor counting qualifying predecessors per partition key and
 * stops on the row that satisfies the last key still short. Descending order is what
 * makes that stop meaningful - the row it stops on <i>is</i> {@code L}, and the
 * partitions below it stay unopened.
 * <p>
 * So these tests assert both halves in each direction: the exact row set of an inclusive
 * {@code [lo, hi]} range, and that the partitions outside it - or below the point the
 * caller stopped at - are never opened.
 * <p>
 * The bound is inclusive on both edges, matching every other QuestDB interval model.
 * A repair holding an exclusive {@code H} converts it, and carries an end-of-frame
 * {@code H} as a tag rather than as a timestamp - the highest timestamp a table can
 * hold is data, as {@link #testHighestTimestampIsDataNotInfinity} pins down.
 */
public class PageFrameTimestampRangeCursorTest extends AbstractCairoTest {
    private static final long HOUR = 3_600_000_000L;
    // The highest designated timestamp a MICROS table accepts: TableWriter rejects
    // anything at or beyond year 10000.
    private static final long MAX_TS = Micros.YEAR_10000 - 1;
    // 20 rows, one every 6 hours, so exactly 4 rows land in each of 5 DAY partitions.
    private static final int ROWS_PER_PARTITION = 4;
    private static final long STEP = 6 * HOUR;
    private static final int TOTAL_ROWS = 20;

    @Test
    public void testBackwardCursorIsReusedAcrossBounds() throws Exception {
        assertMemoryLeak(() -> {
            createSteppedTable("x", "DAY");
            // One factory serves every repair of one view and caches a single backward
            // interval cursor, so each call must re-stamp both bounds of its range.
            try (PageFrameRecordCursorFactory factory = newFullScanFactory("x")) {
                Assert.assertTrue(factory.isBackwardTimestampRangeSupported());
                assertBackwardTimestamps(factory, ts(4), ts(6), ts(6), ts(5), ts(4));
                assertBackwardTimestamps(factory, ts(16), ts(18), ts(18), ts(17), ts(16));
                // An empty range does not poison the next call.
                assertBackwardTimestamps(factory, ts(9), ts(8));
                assertBackwardTimestamps(factory, ts(0), ts(1), ts(1), ts(0));
            }
        });
    }

    @Test
    public void testBackwardEarlyStopLeavesLowerPartitionsUnopened() throws Exception {
        assertMemoryLeak(() -> {
            createSteppedTable("x", "DAY");
            // Predecessor discovery walks down until every key it waits for is satisfied
            // and then closes. This is the property that bounds it: the history below the
            // row it stopped on was never opened, however long that history is. A caller
            // that had to arrive in descending order through an ascending scan would have
            // opened all five partitions to produce the same three rows.
            assertBackwardPartitionFrames("x", ts(0), ts(19), 1, 1, 4, 4);
            // The unbounded walk over the same range does open all five.
            assertBackwardPartitionFrames("x", ts(0), ts(19), Integer.MAX_VALUE, 5, 4, 0);
        });
    }

    @Test
    public void testBackwardEmptyRangeOpensNoPartition() throws Exception {
        assertMemoryLeak(() -> {
            createSteppedTable("x", "DAY");
            assertBackwardPartitionFrames("x", ts(12), ts(5), Integer.MAX_VALUE, 0, -1, -1);
            assertBackwardRange("x", ts(12), ts(5));
            assertBackwardRange("x", ts(TOTAL_ROWS), ts(TOTAL_ROWS) + STEP);
            assertBackwardRange("x", Long.MIN_VALUE, ts(0) - 1);
        });
    }

    @Test
    public void testBackwardHighestTimestampIsDataNotInfinity() throws Exception {
        assertMemoryLeak(() -> {
            createTable("x", "NONE", 1, 100, MAX_TS);
            try (PageFrameRecordCursorFactory factory = newFullScanFactory("x")) {
                assertBackwardTimestamps(factory, 0, Long.MAX_VALUE, MAX_TS, 100, 1);
                assertBackwardTimestamps(factory, 0, MAX_TS, MAX_TS, 100, 1);
                assertBackwardTimestamps(factory, 0, MAX_TS - 1, 100, 1);
                assertBackwardTimestamps(factory, MAX_TS, MAX_TS, MAX_TS);
            }
        });
    }

    @Test
    public void testBackwardInclusiveOnBothEdges() throws Exception {
        assertMemoryLeak(() -> {
            createSteppedTable("x", "DAY");
            assertBackwardRange("x", ts(6), ts(9), 9, 8, 7, 6);
            // Neither boundary falls on a row: the cursor binary searches into the
            // partition rather than widening to the nearest row.
            assertBackwardRange("x", ts(6) + 1, ts(9) - 1, 8, 7);
            assertBackwardRange("x", ts(6), ts(6), 6);
        });
    }

    @Test
    public void testBackwardIsExactReverseOfForward() throws Exception {
        assertMemoryLeak(() -> {
            createSteppedTable("x", "DAY");
            try (PageFrameRecordCursorFactory factory = newFullScanFactory("x")) {
                // A dependency floor derived from a backward walk is only sound while the
                // two directions admit the same rows: a predecessor the descending scan
                // never yields is one the ascending warm-up would later read as state the
                // repair did not build.
                assertReversal(factory, Long.MIN_VALUE, Long.MAX_VALUE);
                assertReversal(factory, ts(4), ts(11));
                assertReversal(factory, ts(6) + 1, ts(9) - 1);
                assertReversal(factory, ts(19), ts(19));
                assertReversal(factory, ts(12), ts(5));
            }
        });
    }

    @Test
    public void testBackwardLowBoundCullsPartitionsBelowIt() throws Exception {
        assertMemoryLeak(() -> {
            createSteppedTable("x", "DAY");
            // Rows 4..11 live in partitions 1 and 2 of 5, and a descending walk that runs
            // to the bottom of the range still opens only those two.
            assertBackwardPartitionFrames("x", ts(4), ts(11), Integer.MAX_VALUE, 2, 2, 1);
            assertBackwardRange("x", ts(4), ts(11), 11, 10, 9, 8, 7, 6, 5, 4);
        });
    }

    @Test
    public void testBackwardRequiresEntityRowCursor() throws Exception {
        assertMemoryLeak(() -> {
            createSteppedTable("x", "DAY");
            final TableToken tableToken = engine.verifyTableName("x");
            final GenericRecordMetadata metadata = copyMetadata("x");
            final IntList columnIndexes = new IntList();
            final IntList columnSizeShifts = new IntList();
            populateColumnMapping(metadata, columnIndexes, columnSizeShifts);
            try (PageFrameRecordCursorFactory factory = new PageFrameRecordCursorFactory(
                    configuration,
                    metadata,
                    newFullFrameFactory(tableToken, metadata),
                    new SymbolIndexRowCursorFactory(0, 0, IndexReader.DIR_FORWARD, null),
                    false,
                    null,
                    true,
                    columnIndexes,
                    columnSizeShifts,
                    false,
                    false
            )) {
                // An index-backed row cursor yields rows in index order, not in timestamp
                // order. Substituting a descending entity cursor for it would silently
                // change which rows the scan reads, so the descending opener refuses it
                // rather than counting predecessors over the wrong rows.
                //
                // A planner that walks down to discover a bound asks before opening
                // anything: an index-backed factory is a legitimate compile, so it plans
                // a different bound rather than failing the query on the exception.
                Assert.assertFalse(factory.isBackwardTimestampRangeSupported());
                try {
                    factory.getCursorInTimestampRangeBackward(sqlExecutionContext, ts(1), ts(2));
                    Assert.fail();
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "backward timestamp range cursor requires an entity row cursor");
                }
            }
        });
    }

    @Test
    public void testBackwardRequiresFullPartitionScanFactory() throws Exception {
        assertMemoryLeak(() -> {
            createSteppedTable("x", "DAY");
            try (PageFrameRecordCursorFactory factory = newIntervalScanFactory("x", ts(0), ts(4))) {
                Assert.assertFalse(factory.isBackwardTimestampRangeSupported());
                try {
                    factory.getCursorInTimestampRangeBackward(sqlExecutionContext, ts(1), ts(2));
                    Assert.fail();
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "timestamp range cursor requires a full partition scan");
                }
            }
        });
    }

    @Test
    public void testBackwardTimestampTieAtBothEdges() throws Exception {
        assertMemoryLeak(() -> {
            // A predecessor count that admitted only part of a timestamp tie would put the
            // dependency floor inside it, and the warm-up would rebuild the boundary
            // timestamp from half its rows.
            createTable("x", "DAY", 10, 10, 10, 20, 30, 30, 30, 40);
            try (PageFrameRecordCursorFactory factory = newFullScanFactory("x")) {
                assertBackwardTimestamps(factory, 10, 30, 30, 30, 30, 20, 10, 10, 10);
                assertBackwardTimestamps(factory, 10, 29, 20, 10, 10, 10);
                assertBackwardTimestamps(factory, 11, 30, 30, 30, 30, 20);
            }
        });
    }

    @Test
    public void testDirectionsDoNotShareRangeState() throws Exception {
        assertMemoryLeak(() -> {
            createSteppedTable("x", "DAY");
            // The H -> Q -> L planning order collects output keys above the floor and
            // discovers the dependency floor below it, so both directions are open at
            // once. Each caches its own interval cursor; sharing one interval list would
            // let the second call silently re-point the first cursor's range.
            try (PageFrameRecordCursorFactory factory = newFullScanFactory("x")) {
                final RecordCursor forward = factory.getCursorInTimestampRange(sqlExecutionContext, ts(4), ts(7));
                try {
                    final Record forwardRecord = forward.getRecord();
                    Assert.assertTrue(forward.hasNext());
                    Assert.assertEquals(ts(4), forwardRecord.getTimestamp(1));

                    Assert.assertEquals(
                            timestamps(ts(15), ts(14), ts(13), ts(12)),
                            drain(factory.getCursorInTimestampRangeBackward(sqlExecutionContext, ts(12), ts(15)))
                    );

                    final LongList rest = new LongList();
                    while (forward.hasNext()) {
                        rest.add(forwardRecord.getTimestamp(1));
                    }
                    Assert.assertEquals(timestamps(ts(5), ts(6), ts(7)), rest);
                } finally {
                    forward.close();
                }
            }
        });
    }

    @Test
    public void testEmptyRangeOpensNoPartition() throws Exception {
        assertMemoryLeak(() -> {
            createSteppedTable("x", "DAY");
            // Inverted bounds: an empty range, not an error. An interval cursor handed
            // the inverted pair would binary search a range that cannot exist.
            assertPartitionFrames("x", ts(12), ts(5), 0, -1, -1);
            assertRange("x", ts(12), ts(5));
        });
    }

    @Test
    public void testHighBoundCullsPartitionsAboveIt() throws Exception {
        assertMemoryLeak(() -> {
            createSteppedTable("x", "DAY");
            // Rows 4..11 live in partitions 1 and 2 of 5. The three partitions outside
            // the range must never be opened - that is the cost the bound exists to
            // avoid, and the row assertion alone cannot see it.
            assertPartitionFrames("x", ts(4), ts(11), 2, 1, 2);
            assertRange("x", ts(4), ts(11), 4, 5, 6, 7, 8, 9, 10, 11);
        });
    }

    @Test
    public void testHighestTimestampIsDataNotInfinity() throws Exception {
        assertMemoryLeak(() -> {
            // The highest designated timestamp a table can hold is still data, so no
            // timestamp value can double as "no upper bound". Partition by NONE: the top
            // of the range has no representable DAY partition floor above it.
            createTable("x", "NONE", 1, 100, MAX_TS);
            try (PageFrameRecordCursorFactory factory = newFullScanFactory("x")) {
                // An inclusive Long.MAX_VALUE high - what an end-of-frame bound maps to -
                // admits every row, the topmost one included.
                assertTimestamps(factory, 0, Long.MAX_VALUE, 1, 100, MAX_TS);
                // The inclusive edge is exact on both sides of the top row.
                assertTimestamps(factory, 0, MAX_TS, 1, 100, MAX_TS);
                assertTimestamps(factory, 0, MAX_TS - 1, 1, 100);
                assertTimestamps(factory, MAX_TS, MAX_TS, MAX_TS);
            }
        });
    }

    @Test
    public void testInclusiveOnBothEdges() throws Exception {
        assertMemoryLeak(() -> {
            createSteppedTable("x", "DAY");
            // Both boundary rows exist, and both are admitted.
            assertRange("x", ts(6), ts(9), 6, 7, 8, 9);
            // Neither boundary falls on a row: the cursor binary searches into the
            // partition rather than widening to the nearest row.
            assertRange("x", ts(6) + 1, ts(9) - 1, 7, 8);
            // Degenerate single-row range.
            assertRange("x", ts(6), ts(6), 6);
        });
    }

    @Test
    public void testLowerBoundOverloadLeavesHighUnbounded() throws Exception {
        assertMemoryLeak(() -> {
            createSteppedTable("x", "DAY");
            try (PageFrameRecordCursorFactory factory = newFullScanFactory("x")) {
                final LongList bounded = drain(factory.getCursorInTimestampRange(sqlExecutionContext, ts(8), Long.MAX_VALUE));
                final LongList loOnly = drain(factory.getCursorFromTimestamp(sqlExecutionContext, ts(8)));
                Assert.assertEquals(TOTAL_ROWS - 8, loOnly.size());
                Assert.assertEquals(bounded, loOnly);
            }
        });
    }

    @Test
    public void testRangeCursorIsReusedAcrossBounds() throws Exception {
        assertMemoryLeak(() -> {
            createSteppedTable("x", "DAY");
            // One factory serves every repair of one view, and it caches a single
            // interval cursor. Each call must re-stamp BOTH bounds: a call that reset
            // only the low bound would inherit the previous call's ceiling.
            try (PageFrameRecordCursorFactory factory = newFullScanFactory("x")) {
                assertTimestamps(factory, ts(4), ts(6), ts(4), ts(5), ts(6));
                assertTimestamps(factory, ts(16), ts(18), ts(16), ts(17), ts(18));
                // Back to an unbounded high through the lower-bound overload: the
                // ts(6) ceiling of the first call must not survive.
                try (RecordCursor cursor = factory.getCursorFromTimestamp(sqlExecutionContext, ts(17))) {
                    Assert.assertEquals(3, drain(cursor).size());
                }
                // And an empty range does not poison the next call.
                assertTimestamps(factory, ts(9), ts(8));
                assertTimestamps(factory, ts(0), ts(1), ts(0), ts(1));
            }
        });
    }

    @Test
    public void testRangeOutsideTableData() throws Exception {
        assertMemoryLeak(() -> {
            createSteppedTable("x", "DAY");
            assertPartitionFrames("x", ts(TOTAL_ROWS), ts(TOTAL_ROWS) + STEP, 0, -1, -1);
            assertRange("x", ts(TOTAL_ROWS), ts(TOTAL_ROWS) + STEP);
            assertRange("x", Long.MIN_VALUE, ts(0) - 1);
            assertRange("x", Long.MIN_VALUE, Long.MAX_VALUE, allRows());
        });
    }

    @Test
    public void testRequiresFullPartitionScanFactory() throws Exception {
        assertMemoryLeak(() -> {
            createSteppedTable("x", "DAY");
            try (PageFrameRecordCursorFactory factory = newIntervalScanFactory("x", ts(0), ts(4))) {
                // A scan already narrowed by an intrinsic interval cannot take a second,
                // independent range without intersecting the two - so it is rejected
                // rather than silently widening the read the repair proved bounded.
                try {
                    factory.getCursorInTimestampRange(sqlExecutionContext, ts(1), ts(2));
                    Assert.fail();
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "timestamp range cursor requires a full partition scan");
                }
            }
        });
    }

    @Test
    public void testTimestampTieAtBothEdges() throws Exception {
        assertMemoryLeak(() -> {
            // Three rows share the low bound, three share the high bound. Every row of
            // both ties belongs to the range: a repair that dropped part of a tie would
            // leave the boundary timestamp half-recomputed.
            createTable("x", "DAY", 10, 10, 10, 20, 30, 30, 30, 40);
            try (PageFrameRecordCursorFactory factory = newFullScanFactory("x")) {
                assertTimestamps(factory, 10, 30, 10, 10, 10, 20, 30, 30, 30);
                assertTimestamps(factory, 10, 29, 10, 10, 10, 20);
                assertTimestamps(factory, 11, 30, 20, 30, 30, 30);
            }
        });
    }

    private static LongList allRows() {
        final LongList timestamps = new LongList();
        for (int i = 0; i < TOTAL_ROWS; i++) {
            timestamps.add(ts(i));
        }
        return timestamps;
    }

    private static GenericRecordMetadata copyMetadata(String tableName) {
        try (TableReader reader = engine.getReader(tableName)) {
            return GenericRecordMetadata.copyOf(reader.getMetadata());
        }
    }

    private static void createSteppedTable(String tableName, String partitionBy) throws SqlException {
        final long[] timestamps = new long[TOTAL_ROWS];
        for (int i = 0; i < TOTAL_ROWS; i++) {
            timestamps[i] = ts(i);
        }
        createTable(tableName, partitionBy, timestamps);
    }

    private static void createTable(String tableName, String partitionBy, long... timestamps) throws SqlException {
        execute("CREATE TABLE " + tableName + " (i INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY " + partitionBy);
        final StringSink sink = new StringSink();
        sink.put("INSERT INTO ").put(tableName).put(" VALUES ");
        for (int i = 0; i < timestamps.length; i++) {
            if (i > 0) {
                sink.put(',');
            }
            sink.put('(').put(i).put(", ").put(timestamps[i]).put("::timestamp)");
        }
        execute(sink.toString());
    }

    private static LongList drain(RecordCursor cursor) {
        try (RecordCursor c = cursor) {
            final LongList timestamps = new LongList();
            final Record record = c.getRecord();
            while (c.hasNext()) {
                timestamps.add(record.getTimestamp(1));
            }
            return timestamps;
        }
    }

    /**
     * Collects the {@code i} column, which is unique per row. Timestamps alone cannot
     * tell a reversal from a tie the scan re-ordered.
     */
    private static LongList drainRowIds(RecordCursor cursor) {
        try (RecordCursor c = cursor) {
            final LongList rowIds = new LongList();
            final Record record = c.getRecord();
            while (c.hasNext()) {
                rowIds.add(record.getInt(0));
            }
            return rowIds;
        }
    }

    private static FullPartitionFrameCursorFactory newFullFrameFactory(TableToken tableToken, RecordMetadata metadata) {
        return new FullPartitionFrameCursorFactory(
                tableToken,
                TableUtils.ANY_TABLE_VERSION,
                metadata,
                ORDER_ASC,
                null,
                0,
                false
        );
    }

    private static PageFrameRecordCursorFactory newFullScanFactory(String tableName) {
        final TableToken tableToken = engine.verifyTableName(tableName);
        final GenericRecordMetadata metadata = copyMetadata(tableName);
        final IntList columnIndexes = new IntList();
        final IntList columnSizeShifts = new IntList();
        populateColumnMapping(metadata, columnIndexes, columnSizeShifts);
        return new PageFrameRecordCursorFactory(
                configuration,
                metadata,
                newFullFrameFactory(tableToken, metadata),
                new PageFrameRowCursorFactory(ORDER_ASC),
                false,
                null,
                true,
                columnIndexes,
                columnSizeShifts,
                true,
                false
        );
    }

    private static PageFrameRecordCursorFactory newIntervalScanFactory(String tableName, long timestampLo, long timestampHi) {
        final TableToken tableToken = engine.verifyTableName(tableName);
        final GenericRecordMetadata metadata = copyMetadata(tableName);
        final IntList columnIndexes = new IntList();
        final IntList columnSizeShifts = new IntList();
        populateColumnMapping(metadata, columnIndexes, columnSizeShifts);
        final LongList intervals = new LongList();
        intervals.add(timestampLo);
        intervals.add(timestampHi);
        return new PageFrameRecordCursorFactory(
                configuration,
                metadata,
                new IntervalPartitionFrameCursorFactory(
                        tableToken,
                        TableUtils.ANY_TABLE_VERSION,
                        new RuntimeIntervalModel(
                                ColumnType.getTimestampDriver(metadata.getTimestampType()),
                                PartitionBy.DAY,
                                intervals
                        ),
                        metadata.getTimestampIndex(),
                        metadata,
                        ORDER_ASC,
                        null,
                        0,
                        false
                ),
                new PageFrameRowCursorFactory(ORDER_ASC),
                false,
                null,
                true,
                columnIndexes,
                columnSizeShifts,
                true,
                false
        );
    }

    private static void populateColumnMapping(RecordMetadata metadata, IntList columnIndexes, IntList columnSizeShifts) {
        for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
            columnIndexes.add(i);
            columnSizeShifts.add(Numbers.msb(ColumnType.sizeOf(metadata.getColumnType(i))));
        }
    }

    private static LongList timestamps(long... values) {
        final LongList timestamps = new LongList();
        for (long value : values) {
            timestamps.add(value);
        }
        return timestamps;
    }

    private static long ts(int rowIndex) {
        return rowIndex * STEP;
    }

    /**
     * The descending mirror of {@link #assertPartitionFrames}. {@code maxFrames} stops
     * the walk early, which is how a predecessor search behaves once the last key it
     * waits for is satisfied: the assertion is then about what the scan did <i>not</i>
     * open below that point.
     */
    private void assertBackwardPartitionFrames(
            String tableName,
            long timestampLo,
            long timestampHi,
            int maxFrames,
            int expectedOpenPartitions,
            int expectedFirstPartition,
            int expectedLastPartition
    ) throws SqlException {
        final TableToken tableToken = engine.verifyTableName(tableName);
        final GenericRecordMetadata metadata = copyMetadata(tableName);
        final IntList columnIndexes = new IntList();
        final IntList columnSizeShifts = new IntList();
        populateColumnMapping(metadata, columnIndexes, columnSizeShifts);
        try (FullPartitionFrameCursorFactory frameFactory = newFullFrameFactory(tableToken, metadata)) {
            try (PartitionFrameCursor cursor = frameFactory.getCursorBackward(sqlExecutionContext, columnIndexes, timestampLo, timestampHi)) {
                final TableReader reader = cursor.getTableReader();
                int firstPartition = -1;
                int lastPartition = -1;
                int frames = 0;
                PartitionFrame frame;
                while (frames < maxFrames && (frame = cursor.next()) != null) {
                    if (firstPartition == -1) {
                        firstPartition = frame.getPartitionIndex();
                    }
                    lastPartition = frame.getPartitionIndex();
                    frames++;
                }
                Assert.assertEquals(expectedFirstPartition, firstPartition);
                Assert.assertEquals(expectedLastPartition, lastPartition);
                Assert.assertEquals(expectedOpenPartitions, reader.getOpenPartitionCount());
            }
        }
    }

    private void assertBackwardRange(String tableName, long timestampLo, long timestampHi, int... expectedRowIndexes) throws SqlException {
        final LongList expected = new LongList();
        for (int rowIndex : expectedRowIndexes) {
            expected.add(ts(rowIndex));
        }
        try (PageFrameRecordCursorFactory factory = newFullScanFactory(tableName)) {
            Assert.assertEquals(expected, drain(factory.getCursorInTimestampRangeBackward(sqlExecutionContext, timestampLo, timestampHi)));
        }
    }

    private void assertBackwardTimestamps(
            PageFrameRecordCursorFactory factory,
            long timestampLo,
            long timestampHi,
            long... expectedTimestamps
    ) throws SqlException {
        Assert.assertEquals(
                timestamps(expectedTimestamps),
                drain(factory.getCursorInTimestampRangeBackward(sqlExecutionContext, timestampLo, timestampHi))
        );
    }

    /**
     * Drives the partition-frame cursor directly - one level below the record cursor -
     * so the assertions can see which partitions the scan touched at all, not merely
     * which rows survived it.
     */
    private void assertPartitionFrames(
            String tableName,
            long timestampLo,
            long timestampHi,
            int expectedOpenPartitions,
            int expectedFirstPartition,
            int expectedLastPartition
    ) throws SqlException {
        final TableToken tableToken = engine.verifyTableName(tableName);
        final GenericRecordMetadata metadata = copyMetadata(tableName);
        final IntList columnIndexes = new IntList();
        final IntList columnSizeShifts = new IntList();
        populateColumnMapping(metadata, columnIndexes, columnSizeShifts);
        try (FullPartitionFrameCursorFactory frameFactory = newFullFrameFactory(tableToken, metadata)) {
            try (PartitionFrameCursor cursor = frameFactory.getCursor(sqlExecutionContext, columnIndexes, timestampLo, timestampHi)) {
                final TableReader reader = cursor.getTableReader();
                Assert.assertEquals(TOTAL_ROWS / ROWS_PER_PARTITION, reader.getPartitionCount());
                int firstPartition = -1;
                int lastPartition = -1;
                PartitionFrame frame;
                while ((frame = cursor.next()) != null) {
                    if (firstPartition == -1) {
                        firstPartition = frame.getPartitionIndex();
                    }
                    lastPartition = frame.getPartitionIndex();
                }
                Assert.assertEquals(expectedFirstPartition, firstPartition);
                Assert.assertEquals(expectedLastPartition, lastPartition);
                Assert.assertEquals(expectedOpenPartitions, reader.getOpenPartitionCount());
            }
        }
    }

    private void assertRange(String tableName, long timestampLo, long timestampHi, int... expectedRowIndexes) throws SqlException {
        final LongList expected = new LongList();
        for (int rowIndex : expectedRowIndexes) {
            expected.add(ts(rowIndex));
        }
        assertRange(tableName, timestampLo, timestampHi, expected);
    }

    private void assertRange(String tableName, long timestampLo, long timestampHi, LongList expected) throws SqlException {
        try (PageFrameRecordCursorFactory factory = newFullScanFactory(tableName)) {
            Assert.assertEquals(expected, drain(factory.getCursorInTimestampRange(sqlExecutionContext, timestampLo, timestampHi)));
        }
    }

    private void assertReversal(PageFrameRecordCursorFactory factory, long timestampLo, long timestampHi) throws SqlException {
        final LongList forward = drainRowIds(factory.getCursorInTimestampRange(sqlExecutionContext, timestampLo, timestampHi));
        final LongList backward = drainRowIds(factory.getCursorInTimestampRangeBackward(sqlExecutionContext, timestampLo, timestampHi));
        final LongList reversed = new LongList();
        for (int i = forward.size() - 1; i > -1; i--) {
            reversed.add(forward.getQuick(i));
        }
        Assert.assertEquals(reversed, backward);
    }

    private void assertTimestamps(
            PageFrameRecordCursorFactory factory,
            long timestampLo,
            long timestampHi,
            long... expectedTimestamps
    ) throws SqlException {
        Assert.assertEquals(
                timestamps(expectedTimestamps),
                drain(factory.getCursorInTimestampRange(sqlExecutionContext, timestampLo, timestampHi))
        );
    }
}
