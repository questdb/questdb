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

package io.questdb.test.cairo.covering;

import io.questdb.PropertyKey;
import io.questdb.griffin.engine.table.CoveringIndexRecordCursorFactory;
import io.questdb.std.str.StringSink;
import org.junit.Assert;
import org.junit.Test;

/**
 * The tail of one symbol's rows under a covering index: {@code ORDER BY ts DESC LIMIT n},
 * {@code max(ts)} / {@code last(ts)} and a negative LIMIT read the last rows, not every row of the
 * symbol, and return what a full scan returns.
 */
public class CoveringIndexTailTest extends AbstractCoveringIndexQueryTest {

    @Test
    public void testFilteredNegativeLimitBeyondTheCapIsServed() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_MAX_NEGATIVE_LIMIT, 5);
        assertMemoryLeak(() -> {
            createTable(3, 40);
            // the async filter's backward path takes at most 5 rows; a longer tail is the serial
            // Limit's, not an error, as on the plain index (a full scan's async filter refuses it,
            // so the reference is the tail read in descending order, the timestamps being distinct)
            for (int n : new int[]{1, 5, 6, 37, 200}) {
                assertSameResult(
                        "SELECT ts, price FROM t WHERE sym = 'A' AND price > 3 LIMIT -" + n,
                        tailReference("ts, price", "sym = 'A' AND price > 3", n)
                );
            }
        });
    }

    @Test
    public void testMaxAndLastTimestampOfOneKey() throws Exception {
        assertMemoryLeak(() -> {
            createTable(4, 25);
            for (String sym : new String[]{"A", "B", "C", "none"}) {
                for (String f : new String[]{"max(ts)", "last(ts)", "min(ts)", "first(ts)"}) {
                    final String sql = "SELECT " + f + " FROM t WHERE sym = '" + sym + "'";
                    assertSameResult(sql, "SELECT /*+ no_index */ " + f + " FROM t WHERE sym = '" + sym + "'");
                }
            }
            final String plan = plan("SELECT max(ts) FROM t WHERE sym = 'A'");
            Assert.assertFalse(plan, plan.contains("sort"));
            Assert.assertTrue(plan, plan.contains("Index backward scan"));
        });
    }

    @Test
    public void testMaxTimestampKeepsTheQueryHints() throws Exception {
        assertMemoryLeak(() -> {
            createTable(2, 10);
            // the rewrite to ORDER BY ts DESC LIMIT 1 reads the table in a new model, which takes
            // the query's hints with it
            final String plan = plan("SELECT /*+ no_index */ max(ts) FROM t WHERE sym = 'A'");
            Assert.assertFalse(plan, plan.contains("Index"));
        });
    }

    @Test
    public void testNegativeLimitAcrossChunksAndPartitions() throws Exception {
        // frames of at most 4 postings: the first backward chunks (1024 postings) are capped to
        // frames, and a partition's postings go out over several of them
        CoveringIndexRecordCursorFactory.setMaxRowsPerFrameForTesting(4);
        try {
            assertMemoryLeak(() -> {
                createTable(3, 30);
                for (String sym : new String[]{"A", "B", "none"}) {
                    for (int n : new int[]{1, 3, 4, 5, 9, 29, 30, 31, 89, 90, 91, 500}) {
                        assertSameResult(
                                "SELECT ts, price, qty FROM t WHERE sym = '" + sym + "' LIMIT -" + n,
                                "SELECT /*+ no_index */ ts, price, qty FROM t WHERE sym = '" + sym + "' LIMIT -" + n
                        );
                        assertSameResult(
                                "SELECT ts, price FROM t WHERE sym = '" + sym + "' AND qty % 3 <> 0 LIMIT -" + n,
                                "SELECT /*+ no_index */ ts, price FROM t WHERE sym = '" + sym + "' AND qty % 3 <> 0 LIMIT -" + n
                        );
                    }
                }
            });
        } finally {
            CoveringIndexRecordCursorFactory.setMaxRowsPerFrameForTesting(-1);
        }
    }

    @Test
    public void testNegativeLimitAcrossGrowingChunks() throws Exception {
        assertMemoryLeak(() -> {
            // one partition of 5,000 rows per key: the backward chunks of 1024, 2048, ... postings
            // meet inside the partition
            createTable(1, 5_000);
            for (int n : new int[]{1, 1023, 1024, 1025, 3071, 3072, 3073, 4999, 5000, 5001}) {
                assertSameResult(
                        "SELECT ts, price FROM t WHERE sym = 'B' LIMIT -" + n,
                        "SELECT /*+ no_index */ ts, price FROM t WHERE sym = 'B' LIMIT -" + n
                );
            }
        });
    }

    @Test
    public void testNegativeLimitBeyondTheCapStaysSerial() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_MAX_NEGATIVE_LIMIT, 5);
        assertMemoryLeak(() -> {
            createTable(3, 40);
            Assert.assertTrue(plan("SELECT ts, price FROM t WHERE sym = 'A' LIMIT -5").contains("Async"));
            Assert.assertFalse(plan("SELECT ts, price FROM t WHERE sym = 'A' LIMIT -6").contains("Async"));
            for (int n : new int[]{5, 6, 77, 1000}) {
                assertSameResult(
                        "SELECT ts, price FROM t WHERE sym = 'A' LIMIT -" + n,
                        tailReference("ts, price", "sym = 'A'", n)
                );
            }
        });
    }

    @Test
    public void testNegativeLimitReadsTheTailBackward() throws Exception {
        assertMemoryLeak(() -> {
            createTable(3, 20);
            final String plan = plan("SELECT ts, price FROM t WHERE sym = 'A' LIMIT -3");
            Assert.assertTrue(plan, plan.contains("Async"));
            Assert.assertTrue(plan, plan.contains("limit: 3"));
            Assert.assertTrue(plan, plan.contains("CoveringIndex"));
            Assert.assertTrue(plan, plan.contains("Frame backward scan"));
            assertSameResult(
                    "SELECT ts, price FROM t WHERE sym = 'A' LIMIT -3",
                    "SELECT /*+ no_index */ ts, price FROM t WHERE sym = 'A' LIMIT -3"
            );
        });
    }

    @Test
    public void testOrderByTimestampDescReadsTheIndexBackward() throws Exception {
        assertMemoryLeak(() -> {
            createTable(3, 20);
            for (String limit : new String[]{" LIMIT 1", " LIMIT 7", " LIMIT 100", ""}) {
                for (String filter : new String[]{"", " AND price > 10"}) {
                    final String sql = "SELECT ts, price, qty FROM t WHERE sym = 'B'" + filter + " ORDER BY ts DESC" + limit;
                    final String plan = plan(sql);
                    Assert.assertFalse(plan, plan.contains("sort"));
                    Assert.assertTrue(plan, plan.contains("Index backward scan"));
                    assertSameResult(sql, sql.replace("SELECT ", "SELECT /*+ no_index */ "));
                }
            }
        });
    }

    // days partitions, each with rowsPerKey rows of each of A, B and C, interleaved
    private static void createTable(int days, int rowsPerKey) throws Exception {
        execute("CREATE TABLE t (ts TIMESTAMP, sym SYMBOL, price DOUBLE, qty LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
        final long rowsPerDay = 3L * rowsPerKey;
        execute("INSERT INTO t SELECT"
                + " dateadd('s', ((x - 1) / " + rowsPerDay + ")::INT * 86400 + ((x - 1) % " + rowsPerDay + ")::INT, '2024-01-01T00:00:00Z'::TIMESTAMP),"
                + " CASE x % 3 WHEN 0 THEN 'A' WHEN 1 THEN 'B' ELSE 'C' END::SYMBOL,"
                + " x::DOUBLE, x"
                + " FROM long_sequence(" + (days * rowsPerDay) + ")");
        execute("ALTER TABLE t ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, qty)");
    }

    // the last n rows of a full scan in ascending order, without a negative LIMIT
    private static String tailReference(String columns, String where, int n) {
        return "SELECT * FROM (SELECT /*+ no_index */ " + columns + " FROM t WHERE " + where + " ORDER BY ts DESC LIMIT " + n + ") ORDER BY ts";
    }

    private static String plan(String sql) throws Exception {
        final StringSink sink = new StringSink();
        printSql("EXPLAIN " + sql, sink);
        return sink.toString();
    }
}
