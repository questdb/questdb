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

package io.questdb.test.griffin.engine.table;

import io.questdb.PropertyKey;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * A scan that skips rows materializes a different set of page frames than a full scan does: the skip
 * discards whole frames and lands part-way into the one it stops at, so the frame that takes a given
 * frame index in one pass is not the frame that took it in the previous one. The page frame address
 * cache indexes frames by that position, so a pass that reuses an earlier pass's entry reads one
 * frame's rows through another frame's column addresses - NULLs where the addresses were a skeleton's
 * zeros, and a read past the end of the column ("string is outside of file boundary") where the cached
 * frame was shorter than the frame now sitting at that index.
 * <p>
 * Each test drives one cursor through both orders - skip then rescan, rescan then skip - because the
 * defect only shows when one pass inherits the other's frame numbering.
 */
public class PageFrameSkipRescanTest extends AbstractCairoTest {

    @Test
    public void testRescanAfterSkipReadsEveryRow() throws Exception {
        assertMemoryLeak(() -> {
            createThreePartitionTable();
            final String expected = """
                    i\ts
                    1\tone
                    2\ttwo
                    3\tthree
                    4\tfour
                    5\tfive
                    6\tsix
                    7\tseven
                    """;
            try (RecordCursorFactory factory = select("SELECT i, s FROM x")) {
                // A skip that consumes whole partitions leaves the address cache holding the skeletons
                // the skip walked over; the rescan behind it must still read real column addresses.
                for (int skip = 1; skip <= 7; skip++) {
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        final RecordCursor.Counter counter = new RecordCursor.Counter();
                        counter.set(skip);
                        cursor.skipRows(counter, RecordCursor.UNBOUNDED_ROW_COUNT);
                        //noinspection StatementWithEmptyBody
                        while (cursor.hasNext()) {
                        }
                        cursor.toTop();
                        assertCursorText("skip=" + skip, expected, cursor, factory);
                    }
                }
            }
        });
    }

    @Test
    public void testSkipAfterFullScanReadsTheLandingFrame() throws Exception {
        assertMemoryLeak(() -> {
            createThreePartitionTable();
            final String[] expectedTails = {
                    "i\ts\n1\tone\n2\ttwo\n3\tthree\n4\tfour\n5\tfive\n6\tsix\n7\tseven\n",
                    "i\ts\n2\ttwo\n3\tthree\n4\tfour\n5\tfive\n6\tsix\n7\tseven\n",
                    "i\ts\n3\tthree\n4\tfour\n5\tfive\n6\tsix\n7\tseven\n",
                    "i\ts\n4\tfour\n5\tfive\n6\tsix\n7\tseven\n",
                    "i\ts\n5\tfive\n6\tsix\n7\tseven\n",
                    "i\ts\n6\tsix\n7\tseven\n",
                    "i\ts\n7\tseven\n",
                    "i\ts\n",
            };
            try (RecordCursorFactory factory = select("SELECT i, s FROM x")) {
                for (int skip = 0; skip < expectedTails.length; skip++) {
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        // The full pass numbers every frame of the table; the skip pass that follows lands
                        // part-way into a partition, so its landing frame is shorter than the frame the full
                        // pass left at that index.
                        //noinspection StatementWithEmptyBody
                        while (cursor.hasNext()) {
                        }
                        cursor.toTop();
                        final RecordCursor.Counter counter = new RecordCursor.Counter();
                        counter.set(skip);
                        cursor.skipRows(counter, RecordCursor.UNBOUNDED_ROW_COUNT);
                        assertCursorText("skip=" + skip, expectedTails[skip], cursor, factory);
                    }
                }
            }
        });
    }

    @Test
    public void testSkipRowsAfterFullScanBackward() throws Exception {
        // Backward: the 20-row last partition is skipped wholesale, then the 48-row
        // partition (three fine frames on a plain scan) is read from its end.
        testSkipRowsAfterFullScan("SELECT x FROM t ORDER BY ts DESC", 68, -1, 50);
    }

    @Test
    public void testSkipRowsAfterFullScanForward() throws Exception {
        // Forward: the 48-row first partition is skipped wholesale, then the 20-row
        // partition is read.
        testSkipRowsAfterFullScan("SELECT x FROM t", 1, 1, 50);
    }

    private void testSkipRowsAfterFullScan(String sql, long first, long step, long skip) throws Exception {
        // 16-row frames: a 48-row partition is three fine frames on a plain scan, but one
        // coarse frame once the skip target covers the whole partition.
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MIN_ROWS, 1);
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 16);
        assertMemoryLeak(() -> {
            // The execution context caches the frame sizes it was created with.
            sqlExecutionContext.changePageFrameSizes(1, 16);
            execute("CREATE TABLE t (x LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t SELECT x, dateadd('s', x::int, '2024-01-01T00:00:00'::timestamp) FROM long_sequence(48)");
            execute("INSERT INTO t SELECT 48 + x, dateadd('s', x::int, '2024-01-02T00:00:00'::timestamp) FROM long_sequence(20)");

            try (RecordCursorFactory factory = select(sql);
                 RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                final Record record = cursor.getRecord();
                // Pass 1: a plain scan files every fine frame in the address cache.
                long expected = first;
                long rows = 0;
                while (cursor.hasNext()) {
                    Assert.assertEquals(expected, record.getLong(0));
                    expected += step;
                    rows++;
                }
                Assert.assertEquals(68, rows);

                // Pass 2: skip a whole partition plus a few rows of the next one.
                cursor.toTop();
                final RecordCursor.Counter counter = new RecordCursor.Counter();
                counter.set(skip);
                cursor.skipRows(counter, RecordCursor.UNBOUNDED_ROW_COUNT);
                Assert.assertEquals(0, counter.get());
                expected = first + skip * step;
                rows = 0;
                while (cursor.hasNext()) {
                    Assert.assertEquals("row after skipRows", expected, record.getLong(0));
                    expected += step;
                    rows++;
                }
                Assert.assertEquals(68 - skip, rows);
            }
        });
    }

    private static void assertCursorText(String message, String expected, RecordCursor cursor, RecordCursorFactory factory) {
        final StringSink actual = new StringSink();
        TestUtils.assertCursor(expected, cursor, factory.getMetadata(), true, actual);
        if (!expected.contentEquals(actual)) {
            TestUtils.assertEquals(message, expected, actual);
        }
    }

    /**
     * Three partitions of 3, 3 and 1 rows, so a skip lands part-way into a partition for most skip
     * counts and consumes whole partitions for the rest. The var-size column is what turns a read
     * through the wrong frame's addresses into a hard error rather than a wrong value.
     */
    private void createThreePartitionTable() throws Exception {
        execute("CREATE TABLE x (ts TIMESTAMP, i INT, s STRING) TIMESTAMP(ts) PARTITION BY DAY");
        execute("""
                INSERT INTO x VALUES
                ('2020-01-01T00:00:00.000000Z', 1, 'one'),
                ('2020-01-01T01:00:00.000000Z', 2, 'two'),
                ('2020-01-01T02:00:00.000000Z', 3, 'three'),
                ('2020-01-02T00:00:00.000000Z', 4, 'four'),
                ('2020-01-02T01:00:00.000000Z', 5, 'five'),
                ('2020-01-02T02:00:00.000000Z', 6, 'six'),
                ('2020-01-03T00:00:00.000000Z', 7, 'seven')
                """);
    }
}
