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
import io.questdb.griffin.TextPlanSink;
import io.questdb.std.Chars;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * The key-major ORDER BY symbol claims (SortedSymbolIndex, and FilterOnValues /
 * FilterOnExcludedValues with keyMajor: true) walk each key across every page frame of the scan.
 * With merge-append off no partition is composite, and each query here must return the rows the
 * same query returns through a full sort ({@code no_index}), across partitions, column tops and
 * small page frames. With merge-append on the planner must not claim the order at all, see
 * {@link SortedSymbolIndexOrderingTest}.
 */
public class KeyMajorSymbolOrderingTest extends AbstractCairoTest {

    @Override
    @Before
    public void setUp() {
        super.setUp();
        setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
    }

    @Test
    public void testCompositePartitionKeepsSortAfterMergeAppendCommits() throws Exception {
        // A partition that merge-append made composite must not be read key-major: its pieces overlap
        // in time, so a key walked across the partition's frames is not in timestamp order.
        setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 128);
        node1.setProperty(PropertyKey.CAIRO_O3_MID_PARTITION_MAX_SPLITS, 1000);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_PIECE_THRESHOLD, 100_000);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_AVG_ROWS_PIECE_LIM, 1);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (s SYMBOL INDEX CAPACITY 8, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO x
                    SELECT ('k' || ((x % 3) + 1))::SYMBOL, timestamp_sequence('2024-01-01', 3_600_000_000L)
                    FROM long_sequence(24)""");
            execute("INSERT INTO x VALUES ('k1', '2024-01-05T00:00:00.000000Z')");
            drainWalQueue();
            execute("INSERT INTO x VALUES ('k2', '2024-01-01T03:30:00.000000Z')");
            drainWalQueue();
            execute("INSERT INTO x VALUES ('k3', '2024-01-01T08:30:00.000000Z')");
            drainWalQueue();
            execute("INSERT INTO x VALUES ('k1', '2024-01-01T15:30:00.000000Z')");
            drainWalQueue();
            for (String query : new String[]{
                    "SELECT s, ts FROM x ORDER BY s, ts",
                    "SELECT s, ts FROM x ORDER BY s, ts DESC",
                    "SELECT s, ts FROM x WHERE s IN ('k1', 'k3') ORDER BY s, ts",
                    "SELECT s, ts FROM x WHERE s IN ('k1', 'k3') ORDER BY s DESC, ts",
                    "SELECT s, ts FROM x WHERE s NOT IN ('k2') ORDER BY s, ts",
                    "SELECT s, ts FROM x WHERE s = 'k2' ORDER BY s, ts DESC",
                    "SELECT s, ts FROM x ORDER BY s, ts LIMIT 5",
            }) {
                assertNoKeyMajor(query);
                assertMatchesFullSort(query, false);
            }
        });
    }

    @Test
    public void testManyPartitionsSmallFrames() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (s SYMBOL INDEX, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR");
            // 6 keys spread unevenly over 10 hourly partitions, some keys absent from some partitions
            execute("""
                    INSERT INTO x
                    SELECT ('k' || (((x * 7) % 11) % 6))::SYMBOL, timestamp_sequence('2024-01-01', 37_000_000L)
                    FROM long_sequence(970)""");
            sqlExecutionContext.changePageFrameSizes(3, 7);
            try {
                assertKeyMajorQueries();
            } finally {
                sqlExecutionContext.changePageFrameSizes(
                        configuration.getSqlPageFrameMinRows(),
                        configuration.getSqlPageFrameMaxRows()
                );
            }
        });
    }

    @Test
    public void testOutOfOrderSplitsAndColumnTops() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (s SYMBOL INDEX, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x
                    SELECT ('k' || (x % 5))::SYMBOL, timestamp_sequence('2024-01-01', 600_000_000L)
                    FROM long_sequence(500)""");
            execute("ALTER TABLE x ADD COLUMN v INT");
            // out-of-order rows land inside earlier partitions, and the new column leaves column tops
            execute("""
                    INSERT INTO x
                    SELECT ('k' || (x % 3))::SYMBOL, timestamp_sequence('2024-01-01T00:03', 1_200_000_000L), x::INT
                    FROM long_sequence(200)""");
            execute("""
                    INSERT INTO x
                    SELECT ('k' || (x % 7))::SYMBOL, timestamp_sequence('2024-01-03T00:01', 900_000_000L), x::INT
                    FROM long_sequence(150)""");
            sqlExecutionContext.changePageFrameSizes(5, 13);
            try {
                assertKeyMajorQueries();
                assertMatchesFullSort("SELECT s, ts, v FROM x ORDER BY s, ts", false);
                assertMatchesFullSort("SELECT s, ts, v FROM x WHERE ts IN '2024-01-02' ORDER BY s, ts DESC", true);
            } finally {
                sqlExecutionContext.changePageFrameSizes(
                        configuration.getSqlPageFrameMinRows(),
                        configuration.getSqlPageFrameMaxRows()
                );
            }
        });
    }

    @Test
    public void testSymbolOnlyOrderAcrossFrames() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (s SYMBOL INDEX, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x
                    SELECT ('k' || (x % 9))::SYMBOL, timestamp_sequence('2024-01-01', 300_000_000L)
                    FROM long_sequence(2_000)""");
            sqlExecutionContext.changePageFrameSizes(4, 16);
            try {
                assertSymbolOrder("SELECT s, ts FROM x WHERE s IN ('k8', 'k1', 'k4') ORDER BY s", true, 667);
                assertSymbolOrder("SELECT s, ts FROM x WHERE s NOT IN ('k8', 'k1') ORDER BY s DESC", false, 1_555);
            } finally {
                sqlExecutionContext.changePageFrameSizes(
                        configuration.getSqlPageFrameMinRows(),
                        configuration.getSqlPageFrameMaxRows()
                );
            }
        });
    }

    private void assertKeyMajorQueries() throws Exception {
        // these plans walk the keys key-major
        for (String query : new String[]{
                "SELECT s, ts FROM x WHERE s IN ('k1', 'k4', 'k2') ORDER BY s, ts",
                "SELECT s, ts FROM x WHERE s IN ('k1', 'k4', 'k2') ORDER BY s, ts DESC",
                "SELECT s, ts FROM x WHERE s NOT IN ('k0', 'k3') ORDER BY s, ts",
                "SELECT s, ts FROM x WHERE s = 'k2' ORDER BY s, ts DESC",
                "SELECT s, ts FROM x WHERE s IN ('k1', 'k2') ORDER BY s, ts LIMIT 11",
        }) {
            assertMatchesFullSort(query, true);
        }
        // these keep a sort or a top K, or walk key-major only when the interval hits one partition
        for (String query : new String[]{
                "SELECT s, ts FROM x WHERE ts IN '2024-01-01' ORDER BY s, ts",
                "SELECT s, ts FROM x WHERE ts IN '2024-01-01' ORDER BY s, ts DESC",
                "SELECT s, ts FROM x ORDER BY s, ts",
                "SELECT s, ts FROM x ORDER BY s DESC, ts",
                "SELECT s, ts FROM x ORDER BY s, ts DESC LIMIT 3, 40",
        }) {
            assertMatchesFullSort(query, false);
        }
    }

    private void assertNoKeyMajor(String query) throws Exception {
        try (RecordCursorFactory factory = select(query)) {
            final TextPlanSink sink = new TextPlanSink();
            sink.of(factory, sqlExecutionContext);
            Assert.assertFalse(query + "\n" + sink.getSink(), Chars.contains(sink.getSink(), "keyMajor"));
        }
    }

    /**
     * Compares the query with the same query planned through a full sort. With requireKeyMajor the
     * plan must also show a key-major scan, so the comparison is not vacuous.
     */
    private void assertMatchesFullSort(String query, boolean requireKeyMajor) throws Exception {
        if (requireKeyMajor) {
            assertQuery(query).assertsPlanContaining("keyMajor: true");
        }
        final String reference = query.replaceFirst("SELECT ", "SELECT /*+ no_index */ ");
        assertSqlCursors(reference, query);
    }

    private void assertSymbolOrder(String query, boolean isAscending, int expectedRowCount) throws Exception {
        assertQuery(query).assertsPlanContaining("keyMajor: true");
        try (RecordCursorFactory factory = select(query);
             RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            final Record record = cursor.getRecord();
            String previous = null;
            int row = 0;
            while (cursor.hasNext()) {
                final String symbol = record.getSymA(0).toString();
                if (previous != null) {
                    final int cmp = previous.compareTo(symbol);
                    Assert.assertTrue("ORDER BY violation at row " + row + ": " + previous + " -> " + symbol,
                            isAscending ? cmp <= 0 : cmp >= 0);
                }
                previous = symbol;
                row++;
            }
            Assert.assertEquals(expectedRowCount, row);
        }
    }
}
