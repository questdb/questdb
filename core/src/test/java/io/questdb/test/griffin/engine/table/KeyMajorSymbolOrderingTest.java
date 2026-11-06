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
import io.questdb.cairo.PartitionGeometry;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * The key-major ORDER BY symbol claims (SortedSymbolIndex, and FilterOnValues /
 * FilterOnExcludedValues with keyMajor: true) walk each key across every page frame of the scan.
 * Each query here must return the rows the same query returns through a full sort
 * ({@code no_index}), across partitions, column tops, small page frames and the pieces of
 * composite partitions. A composite piece's rows sit at a shifted, possibly relocated, range of
 * the partition's files, and the index reads file rows, so the walk must read each frame's index
 * range rather than its partition rows.
 */
public class KeyMajorSymbolOrderingTest extends AbstractCairoTest {

    @Test
    public void testCompositePartitionAfterMergeAppendCommits() throws Exception {
        setCompositeProperties();
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
            assertHasShiftedPieces("x", 0);
            for (String query : new String[]{
                    "SELECT s, ts FROM x WHERE s IN ('k1', 'k3') ORDER BY s, ts",
                    "SELECT s, ts FROM x WHERE s IN ('k1', 'k3') ORDER BY s DESC, ts",
                    "SELECT s, ts FROM x WHERE s NOT IN ('k2') ORDER BY s, ts",
                    "SELECT s, ts FROM x WHERE s = 'k2' ORDER BY s, ts DESC",
            }) {
                assertMatchesFullSort(query, true);
            }
            for (String query : new String[]{
                    "SELECT s, ts FROM x ORDER BY s, ts",
                    "SELECT s, ts FROM x ORDER BY s, ts DESC",
                    "SELECT s, ts FROM x ORDER BY s, ts LIMIT 5",
            }) {
                assertMatchesFullSort(query, false);
            }
        });
    }

    @Test
    public void testCompositePiecesRelocatedAndShifted() throws Exception {
        // Pieces merged at the files' tail sit far above their partition rows, and the pieces they
        // left behind sit below theirs (a negative shift), with dead rows in between. Two
        // partitions, and a column added after the first commit leaves column tops in both.
        setCompositeProperties();
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (s SYMBOL INDEX TYPE POSTING, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO x
                    SELECT ('k' || (x % 4))::SYMBOL, x, timestamp_sequence('2024-01-01', 86_400_000L)
                    FROM long_sequence(2_000)""");
            drainWalQueue();
            execute("ALTER TABLE x ADD COLUMN w INT");
            drainWalQueue();
            execute("""
                    INSERT INTO x VALUES
                    ('k1', 2_001, '2024-01-01T00:02:00.000001Z', 1),
                    ('k2', 2_002, '2024-01-01T00:23:00.000001Z', 2),
                    ('k0', 2_003, '2024-01-02T03:00:00.000001Z', 3)""");
            drainWalQueue();
            execute("""
                    INSERT INTO x VALUES
                    ('k3', 2_004, '2024-01-01T00:05:00.000001Z', 4),
                    ('k1', 2_005, '2024-01-02T11:00:00.000001Z', 5),
                    ('k2', 2_006, '2024-01-01T19:00:00.000001Z', 6)""");
            drainWalQueue();
            execute("""
                    INSERT INTO x VALUES
                    ('k0', 2_007, '2024-01-01T00:23:00.000002Z', 7),
                    ('k3', 2_008, '2024-01-02T03:00:00.000002Z', 8)""");
            drainWalQueue();
            assertHasShiftedPieces("x", 0);
            assertHasShiftedPieces("x", 1);
            for (int[] sizes : new int[][]{{3, 7}, {1, 1}, {
                    configuration.getSqlPageFrameMinRows(),
                    configuration.getSqlPageFrameMaxRows()
            }}) {
                sqlExecutionContext.changePageFrameSizes(sizes[0], sizes[1]);
                try {
                    for (String query : new String[]{
                            "SELECT s, ts, v, w FROM x WHERE s IN ('k1', 'k3') ORDER BY s, ts",
                            "SELECT s, ts, v, w FROM x WHERE s IN ('k1', 'k3', 'k0') ORDER BY s, ts DESC",
                            "SELECT s, ts, v, w FROM x WHERE s IN ('k1', 'k3') ORDER BY s DESC, ts",
                            "SELECT s, ts, v, w FROM x WHERE s NOT IN ('k2') ORDER BY s, ts",
                            "SELECT s, ts, v, w FROM x WHERE s NOT IN ('k0', 'k2') ORDER BY s, ts DESC",
                            "SELECT s, ts, v, w FROM x WHERE s IN ('k1', 'k2') ORDER BY s, ts LIMIT 11",
                            "SELECT s, ts, v, w FROM x WHERE s IN ('k1', 'k2') ORDER BY s, ts DESC LIMIT 3, 40",
                            "SELECT s, ts, v, w FROM x WHERE s IN ('k1', 'k2') AND ts IN '2024-01-01' ORDER BY s, ts",
                            "SELECT s, ts, v, w FROM x WHERE s IN ('k0', 'k3') AND v > 1_500 ORDER BY s, ts",
                    }) {
                        assertMatchesFullSort(query, true);
                    }
                    // a single key over both partitions keeps the single-symbol scan and its sort
                    for (String query : new String[]{
                            "SELECT s, ts, v, w FROM x ORDER BY s, ts",
                            "SELECT s, ts, v, w FROM x WHERE s = 'k2' ORDER BY s, ts",
                            "SELECT s, ts, v, w FROM x WHERE s = 'k1' ORDER BY s, ts DESC",
                    }) {
                        assertMatchesFullSort(query, false);
                    }
                    // a key-major window: each key's rows must arrive in timestamp order for its frame
                    sqlExecutionContext.setParallelWindowEnabled(true);
                    try {
                        final String window = "SELECT s, ts, v, sum(v) OVER (PARTITION BY s ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) sv, "
                                + "row_number() OVER (PARTITION BY s) rn FROM x WHERE s IN ('k1', 'k2', 'k3') ORDER BY s";
                        assertQuery(window).assertsPlanContaining("keyMajor: true");
                        assertSqlCursors(window.replaceFirst("SELECT ", "SELECT /*+ no_index */ "), window);
                    } finally {
                        sqlExecutionContext.setParallelWindowEnabled(false);
                    }
                } finally {
                    sqlExecutionContext.changePageFrameSizes(
                            configuration.getSqlPageFrameMinRows(),
                            configuration.getSqlPageFrameMaxRows()
                    );
                }
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

    // At least one piece of the partition sits above its partition rows and one below: a merge moved
    // the one to the files' tail and left the other behind it.
    private void assertHasShiftedPieces(String table, int partitionIndex) {
        try (TableReader reader = getReader(table)) {
            Assert.assertTrue(reader.getTxFile().isPartitionComposite(partitionIndex));
            final PartitionGeometry geometry = reader.getGeometry();
            geometry.resolve(partitionIndex);
            boolean isAbove = false;
            boolean isBelow = false;
            for (int i = 0, n = geometry.getPieceCount(partitionIndex); i < n; i++) {
                final long shift = geometry.getPieceShift(partitionIndex, i);
                isAbove |= shift > 0;
                isBelow |= shift < 0;
            }
            Assert.assertTrue("partition " + partitionIndex + " has no relocated piece", isAbove && isBelow);
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

    private static void setCompositeProperties() {
        // split and merge-append on a fixture small enough to assert row by row, and keep compaction off
        // the pieces, so the queries read the pieces the commits produced
        setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 128);
        node1.setProperty(PropertyKey.CAIRO_O3_MID_PARTITION_MAX_SPLITS, 1000);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_PIECE_THRESHOLD, 100_000);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_AVG_ROWS_PIECE_LIM, 1);
    }
}
