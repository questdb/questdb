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

package io.questdb.test.cairo;

import io.questdb.cairo.IntervalBwdPartitionFrameCursor;
import io.questdb.cairo.IntervalFwdPartitionFrameCursor;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.sql.PartitionFrame;
import io.questdb.griffin.model.RuntimeIntervalModel;
import io.questdb.cairo.ColumnType;
import io.questdb.std.LongList;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * UNIT-level coverage of the two interval cursors over a composite table: the cursors are constructed
 * and driven directly, and the FRAMES they emit are counted, rather than going through SQL.
 * <p>
 * The other composite interval tests are integration tests — they assert query results and infer that
 * the cursor did the right thing. That is the right primary check (it is what users see), but it cannot
 * distinguish "the cursor emitted the right frames" from "the cursor emitted the wrong frames and
 * something downstream compensated". This file closes that gap for the specific behaviour the
 * sibling-cell fixes changed: over a multi-cell day, an interval must produce a frame from EVERY cell
 * that holds matching rows, not just the first one visited.
 * <p>
 * It is also the layer the existing {@code IntervalFwdPartitionFrameCursorTest} and
 * {@code IntervalBwdPartitionFrameCursorTest} work at — 124 tests between them, none composite.
 */
public class CompositeIntervalCursorUnitTest extends AbstractCairoTest {

    /**
     * Forward cursor, the shape that broke it: cell E0 straddles the interval without matching, cell E1
     * holds the matching row. Exactly ONE frame must be produced, and it must be E1's.
     */
    @Test
    public void testForwardCursorVisitsSiblingCell() throws Exception {
        assertMemoryLeak(() -> {
            createAndFillStraddleShape();
            final LongList intervals = pointInterval("2023-01-02T02:00:00.000000Z");
            Assert.assertEquals("the matching sibling cell must produce a frame",
                    1, countForwardRows(intervals));
        });
    }

    /**
     * Backward cursor over the same shape. It walks cells highest-cellKey first, so it meets E1 (the
     * matching cell) first and E0 after; the frame count must still be exactly one.
     */
    @Test
    public void testBackwardCursorVisitsSiblingCell() throws Exception {
        assertMemoryLeak(() -> {
            createAndFillStraddleShape();
            final LongList intervals = pointInterval("2023-01-02T02:00:00.000000Z");
            Assert.assertEquals("the matching sibling cell must produce a frame",
                    1, countBackwardRows(intervals));
        });
    }

    /**
     * Several matching cells: the cursors must produce frames covering ALL of them. This is the count
     * that went wrong -- the old code stopped after the first cell that failed to match.
     */
    @Test
    public void testBothCursorsCoverEveryMatchingCell() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE c (ts TIMESTAMP, exch SYMBOL, px DOUBLE)"
                    + " TIMESTAMP(ts) PARTITION BY DAY, exch LAYOUT PLAIN WAL");
            final StringBuilder rows = new StringBuilder();
            // five cells that straddle 03:00 without matching it
            for (int i = 0; i < 5; i++) {
                rows.append("('2023-01-02T01:00:00.000000Z','E").append(i).append("',1.0),")
                        .append("('2023-01-02T05:00:00.000000Z','E").append(i).append("',5.0),");
            }
            // three cells that DO hold a row at 03:00
            rows.append("('2023-01-02T03:00:00.000000Z','X0',30.0),")
                    .append("('2023-01-02T03:00:00.000000Z','X1',31.0),")
                    .append("('2023-01-02T03:00:00.000000Z','X2',32.0)");
            execute("INSERT INTO c VALUES " + rows);
            drainWalQueue();

            final LongList intervals = pointInterval("2023-01-02T03:00:00.000000Z");
            Assert.assertEquals("forward cursor must reach every matching cell",
                    3, countForwardRows(intervals));
            Assert.assertEquals("backward cursor must reach every matching cell",
                    3, countBackwardRows(intervals));
        });
    }

    /**
     * An interval no cell matches must produce nothing from either direction -- the complement of the
     * tests above, so a cursor that simply emitted every cell would fail here.
     */
    @Test
    public void testBothCursorsEmptyWhenNothingMatches() throws Exception {
        assertMemoryLeak(() -> {
            createAndFillStraddleShape();
            final LongList intervals = pointInterval("2023-01-02T02:30:00.000000Z");
            Assert.assertEquals(0, countForwardRows(intervals));
            Assert.assertEquals(0, countBackwardRows(intervals));
        });
    }

    /**
     * The cursor takes OWNERSHIP of the reader: closing the cursor closes the reader. So each helper
     * opens its OWN reader and lets the cursor close it.
     * <p>
     * Two ways of getting this wrong were hit while writing this file, both worth naming because the
     * second looks like a product bug and is not:
     * <ul>
     *     <li>wrapping the reader in try-with-resources as well raises "double close" from the pool;</li>
     *     <li>REUSING one reader for a second cursor raises an NPE inside
     *     {@code resolveCellSegmentOrNullIfDormant}, because closing a reader nulls its composite
     *     dictionaries. That is a closed-reader-reuse bug in the test, NOT a composite defect --
     *     verified by constructing both a pooled and an off-pool reader on a routed composite table and
     *     confirming both report their dimensions and dictionaries present.</li>
     * </ul>
     */
    private long countBackwardRows(LongList intervals) throws Exception {
        final TableReader reader = engine.getReader("c");
        try (IntervalBwdPartitionFrameCursor cursor = new IntervalBwdPartitionFrameCursor(
                configuration,
                new RuntimeIntervalModel(
                        ColumnType.getTimestampDriver(reader.getMetadata().getTimestampType()),
                        reader.getPartitionedBy(),
                        intervals),
                reader.getMetadata().getTimestampIndex())
        ) {
            cursor.of(reader, sqlExecutionContext);
            return countRows(cursor);
        }
    }

    private long countForwardRows(LongList intervals) throws Exception {
        final TableReader reader = engine.getReader("c");
        try (IntervalFwdPartitionFrameCursor cursor = new IntervalFwdPartitionFrameCursor(
                configuration,
                new RuntimeIntervalModel(
                        ColumnType.getTimestampDriver(reader.getMetadata().getTimestampType()),
                        reader.getPartitionedBy(),
                        intervals),
                reader.getMetadata().getTimestampIndex())
        ) {
            cursor.of(reader, sqlExecutionContext);
            return countRows(cursor);
        }
    }

    /**
     * Total rows across every frame the cursor yields. Rows rather than frames: a cell may legitimately
     * be split across frames, and the contract under test is that no matching ROW is skipped.
     */
    private long countRows(io.questdb.cairo.sql.PartitionFrameCursor cursor) {
        long rows = 0;
        PartitionFrame frame;
        while ((frame = cursor.next()) != null) {
            rows += frame.getRowHi() - frame.getRowLo();
        }
        return rows;
    }

    private void createAndFillStraddleShape() throws Exception {
        execute("CREATE TABLE c (ts TIMESTAMP, exch SYMBOL, px DOUBLE)"
                + " TIMESTAMP(ts) PARTITION BY DAY, exch LAYOUT PLAIN WAL");
        execute("INSERT INTO c VALUES ('2023-01-02T01:00:00.000000Z','E0',1.0),"
                + "('2023-01-02T03:00:00.000000Z','E0',3.0),"
                + "('2023-01-02T02:00:00.000000Z','E1',2.0)");
        drainWalQueue();
    }

    /**
     * A single closed interval [t, t] -- the shape most cells of a day fail to match.
     */
    private LongList pointInterval(String timestamp) throws Exception {
        final LongList intervals = new LongList();
        final long t = ColumnType.getTimestampDriver(ColumnType.TIMESTAMP).parseFloorLiteral(timestamp);
        intervals.add(t);
        intervals.add(t);
        return intervals;
    }
}
