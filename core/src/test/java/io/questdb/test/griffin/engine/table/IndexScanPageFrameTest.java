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
import io.questdb.std.IntHashSet;
import io.questdb.std.Rows;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * A serial scan whose rows come from an index takes page frames of up to
 * {@code cairo.sql.index.scan.page.frame.max.rows} rows, while every other scan keeps
 * {@code cairo.sql.page.frame.max.rows}. The frame a record came from is part of its row id, so
 * the tests read the frames off the rows. Every index scan must return what the same query
 * returns on an unindexed copy of the data, whatever the frame sizes.
 */
public class IndexScanPageFrameTest extends AbstractCairoTest {
    private static final String[] INDEX_TYPES = {"BITMAP", "POSTING", "POSTING DELTA", "POSTING EF"};
    private static final String[] SYMBOLS = {"L", "S", "B", "H", "F3", "nope"};

    @Override
    public void setUp() {
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 1_000);
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MIN_ROWS, 100);
        super.setUp();
        // the execution context reads its frame sizes when the class starts, before setUp()
        sqlExecutionContext.changePageFrameSizes(100, 1_000);
    }

    @Override
    public void tearDown() throws Exception {
        sqlExecutionContext.restoreToDefaultPageFrameSizes();
        super.tearDown();
    }

    @Test
    public void testConfiguredIndexScanFramesAreUsed() throws Exception {
        // a configured value smaller than the default cuts index scans as it says
        setProperty(PropertyKey.CAIRO_SQL_INDEX_SCAN_PAGE_FRAME_MAX_ROWS, 2_500);
        assertMemoryLeak(() -> {
            createTables("POSTING");
            // 20,000 rows in day 1: 8 frames of 2,500 rows
            Assert.assertEquals(8, framesOf("SELECT * FROM t_idx WHERE sym = 'L' AND ts < '2024-01-02'"));
            Assert.assertEquals(8, framesOf("SELECT * FROM t_idx WHERE sym = 'L' AND ts < '2024-01-02' ORDER BY ts DESC"));
            // a full scan keeps cairo.sql.page.frame.max.rows
            Assert.assertEquals(20, framesOf("SELECT * FROM t_ref WHERE ts < '2024-01-02'"));
        });
    }

    @Test
    public void testFullScanFramesUnchanged() throws Exception {
        assertMemoryLeak(() -> {
            createTables("POSTING");
            Assert.assertEquals(20, framesOf("SELECT * FROM t_ref WHERE ts < '2024-01-02'"));
            Assert.assertEquals(20, framesOf("SELECT * FROM t_idx WHERE ts < '2024-01-02'"));
            // a filter that the index does not serve stays a full scan, cut in full-scan frames
            Assert.assertEquals(20, framesOf("SELECT * FROM t_idx WHERE qty = 5 AND ts < '2024-01-02'"));
        });
    }

    @Test
    public void testIdentityDefaultFrames() throws Exception {
        for (String indexType : INDEX_TYPES) {
            assertIdentity(indexType);
        }
    }

    @Test
    public void testIdentitySmallIndexFrames() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_INDEX_SCAN_PAGE_FRAME_MAX_ROWS, 1_000);
        for (String indexType : INDEX_TYPES) {
            assertIdentity(indexType);
        }
    }

    @Test
    public void testIndexScanFramesNeverBelowPageFrameMaxRows() throws Exception {
        // an index scan frame is never smaller than a full scan's: 20 frames of 1,000 rows, of
        // which the run of B (rows 5,000 to 7,000) fills two without an L
        setProperty(PropertyKey.CAIRO_SQL_INDEX_SCAN_PAGE_FRAME_MAX_ROWS, 100);
        assertMemoryLeak(() -> {
            createTables("POSTING");
            Assert.assertEquals(18, framesOf("SELECT * FROM t_idx WHERE sym = 'L' AND ts < '2024-01-02'"));
        });
    }

    @Test
    public void testIndexScanFramesSpanPartition() throws Exception {
        for (String indexType : INDEX_TYPES) {
            assertMemoryLeak(() -> {
                createTables(indexType);
                // 20,000 rows a day, 1,000-row frames for a full scan: an index scan reads each
                // partition as one frame, so the rows of day 1 all come from one frame
                Assert.assertEquals(indexType, 1, framesOf("SELECT * FROM t_idx WHERE sym = 'L' AND ts < '2024-01-02'"));
                Assert.assertEquals(indexType, 1, framesOf("SELECT * FROM t_idx WHERE sym = 'L' AND ts < '2024-01-02' ORDER BY ts DESC"));
                Assert.assertEquals(indexType, 1, framesOf("SELECT * FROM t_idx WHERE sym = 'L' AND qty > 10 AND ts < '2024-01-02'"));
                // the column top splits the partition it falls in, as it does any frame: the
                // NULL key's rows of day 2 come from both sides of it
                Assert.assertEquals(indexType, 2, framesOf("SELECT * FROM t_idx WHERE s2 IS NULL AND ts >= '2024-01-02' AND ts < '2024-01-03'"));
                execute("DROP TABLE t_idx");
                execute("DROP TABLE t_ref");
            });
        }
    }

    private static String rowsSql(long lo, long hi) {
        // B is bursty, L is ~25% of rows, S is rare, H only in day 1, Fn are filler keys
        return "SELECT" +
                " CASE" +
                "   WHEN x % 20_000 BETWEEN 5_000 AND 7_000 THEN 'B'" +
                "   WHEN (x * 2_654_435_761) % 100 < 25 THEN 'L'" +
                "   WHEN (x * 2_654_435_761) % 100_003 < 60 THEN 'S'" +
                "   WHEN x < 20_000 AND (x * 40_503) % 13 = 1 THEN 'H'" +
                "   ELSE 'F' || ((x * 97) % 31)" +
                " END sym," +
                " (x % 1_000) / 8.0 px," +
                " (x % 77)::int qty," +
                " ('2024-01-01'::timestamp + (x - 1) * 4_320_000)::timestamp ts" +
                " FROM long_sequence(" + hi + ") WHERE x >= " + lo;
    }

    private void assertIdentity(String indexType) throws Exception {
        assertMemoryLeak(() -> {
            createTables(indexType);
            for (String s : SYMBOLS) {
                final String[] queries = {
                        "SELECT * FROM %t WHERE sym = '" + s + "'",
                        "SELECT * FROM %t WHERE sym = '" + s + "' ORDER BY ts DESC",
                        "SELECT * FROM %t WHERE sym = '" + s + "' AND ts BETWEEN '2024-01-01T13:00' AND '2024-01-03T07:30'",
                        "SELECT * FROM %t WHERE sym = '" + s + "' AND ts BETWEEN '2024-01-01T13:00' AND '2024-01-03T07:30' ORDER BY ts DESC",
                        "SELECT * FROM %t WHERE sym = '" + s + "' AND qty > 40",
                        "SELECT * FROM %t WHERE sym IN ('" + s + "', 'S', 'F7')",
                        "SELECT * FROM %t WHERE sym IN ('" + s + "', 'S') ORDER BY ts DESC",
                        "SELECT * FROM %t WHERE s2 = '" + s + "'",
                        "SELECT * FROM %t WHERE s2 = '" + s + "' ORDER BY ts DESC",
                        "SELECT * FROM %t WHERE s2 IS NULL AND ts < '2024-01-02T06'",
                        "SELECT count(), sum(px), min(ts), max(ts), sum(qty) FROM %t WHERE sym = '" + s + "'",
                        "SELECT * FROM %t WHERE sym = '" + s + "' LIMIT 7",
                        "SELECT * FROM %t WHERE sym = '" + s + "' LIMIT -7",
                        "SELECT * FROM %t WHERE sym = '" + s + "' LIMIT 1000, 1010",
                };
                for (String q : queries) {
                    assertSqlCursors(q.replace("%t", "t_ref"), q.replace("%t", "t_idx"));
                }
            }
            execute("DROP TABLE t_idx");
            execute("DROP TABLE t_ref");
        });
    }

    private void createTables(String indexType) throws Exception {
        execute("CREATE TABLE t_idx (sym SYMBOL INDEX TYPE " + indexType + ", px DOUBLE, qty INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("CREATE TABLE t_ref (sym SYMBOL, px DOUBLE, qty INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        // day 1 and a quarter of day 2, then an indexed column added mid-partition (column top
        // at 2024-01-02T06:00), then the rest: 3 days of 20,000 rows each
        execute("INSERT INTO t_idx " + rowsSql(1, 25_000));
        execute("INSERT INTO t_ref " + rowsSql(1, 25_000));
        execute("ALTER TABLE t_idx ADD COLUMN s2 SYMBOL INDEX TYPE " + indexType);
        execute("ALTER TABLE t_ref ADD COLUMN s2 SYMBOL");
        final String rest = "SELECT sym, px, qty, ts, CASE WHEN qty % 3 = 0 THEN null ELSE sym END s2 FROM (" + rowsSql(25_001, 60_000) + ")";
        execute("INSERT INTO t_idx " + rest);
        execute("INSERT INTO t_ref " + rest);
        assertPlanContains("SELECT * FROM t_idx WHERE sym = 'S'", "Index forward scan on: sym");
        assertPlanContains("SELECT * FROM t_idx WHERE sym = 'S' ORDER BY ts DESC", "Index backward scan on: sym");
    }

    private void assertPlanContains(String query, String fragment) throws Exception {
        assertQuery(query).noLeakCheck().assertsPlanContaining(fragment);
    }

    // the number of distinct page frames the query's rows come from
    private int framesOf(String sql) throws Exception {
        final IntHashSet frames = new IntHashSet();
        try (
                RecordCursorFactory factory = select(sql);
                RecordCursor cursor = factory.getCursor(sqlExecutionContext)
        ) {
            final Record record = cursor.getRecord();
            while (cursor.hasNext()) {
                frames.add(Rows.toPartitionIndex(record.getRowId()));
            }
        }
        Assert.assertTrue("query returned no rows: " + sql, frames.size() > 0);
        return frames.size();
    }
}
