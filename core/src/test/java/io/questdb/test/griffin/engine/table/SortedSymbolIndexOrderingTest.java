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
import io.questdb.cairo.TableReader;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Key-ordered index scans must preserve symbol and timestamp order across page frames,
 * column tops, Parquet row groups, and composite partition pieces.
 */
public class SortedSymbolIndexOrderingTest extends AbstractCairoTest {

    private static final String ORDERED_QUERY = "SELECT s, ts FROM x WHERE ts IN '2024-01-01' ORDER BY s, ts";
    private static final String ORDERED_QUERY_PLAN = """
            SortedSymbolIndex
                Index forward scan on: s
                  symbolOrder: asc
                Interval forward scan on: x
                  intervals: [("2024-01-01T00:00:00.000000Z","2024-01-01T23:59:59.999999Z")]
            """;

    @Test
    public void testOrderBySymbolAcrossColumnTopFrames() throws Exception {
        assertMemoryLeak(() -> {
            createColumnTopSplitTable();

            final String query = "SELECT s, ts, v FROM x WHERE ts IN '2024-01-01' ORDER BY s, ts";
            assertQuery(query).assertsPlan(ORDERED_QUERY_PLAN);
            assertQuery(query).returns("""
                    s\tts\tv
                    k1\t2024-01-01T00:01:00.000000Z\tnull
                    k1\t2024-01-01T00:03:00.000000Z\tnull
                    k1\t2024-01-01T00:05:00.000000Z\t2
                    k1\t2024-01-01T00:07:00.000000Z\t4
                    k2\t2024-01-01T00:00:00.000000Z\tnull
                    k2\t2024-01-01T00:02:00.000000Z\tnull
                    k2\t2024-01-01T00:04:00.000000Z\t1
                    k2\t2024-01-01T00:06:00.000000Z\t3
                    """);
        });
    }

    /**
     * ORDER BY s, ts DESC compiled to the same factory with a BACKWARD index direction: the rows of one
     * symbol must descend by timestamp across the whole scan, which puts the LAST frame's rows first.
     */
    @Test
    public void testOrderBySymbolThenTimestampDescAcrossColumnTopFrames() throws Exception {
        assertMemoryLeak(() -> {
            createColumnTopSplitTable();

            final String query = "SELECT s, ts, v FROM x WHERE ts IN '2024-01-01' ORDER BY s, ts DESC";
            assertQuery(query).assertsPlan("""
                    SortedSymbolIndex
                        Index backward scan on: s
                          symbolOrder: asc
                        Interval backward scan on: x
                          intervals: [("2024-01-01T00:00:00.000000Z","2024-01-01T23:59:59.999999Z")]
                    """);
            assertQuery(query).returns("""
                    s\tts\tv
                    k1\t2024-01-01T00:07:00.000000Z\t4
                    k1\t2024-01-01T00:05:00.000000Z\t2
                    k1\t2024-01-01T00:03:00.000000Z\tnull
                    k1\t2024-01-01T00:01:00.000000Z\tnull
                    k2\t2024-01-01T00:06:00.000000Z\t3
                    k2\t2024-01-01T00:04:00.000000Z\t1
                    k2\t2024-01-01T00:02:00.000000Z\tnull
                    k2\t2024-01-01T00:00:00.000000Z\tnull
                    """);
        });
    }

    @Test
    public void testOrderBySymbolAcrossCompositePieces() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        // A production-sized partition pre-splits on its own at the 50MB default; shrink the threshold so a
        // fixture small enough to assert row by row splits the same way.
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 128);
        node1.setProperty(PropertyKey.CAIRO_O3_MID_PARTITION_MAX_SPLITS, 1000);
        // Keep compaction off the pieces, so the query sees the pieces the commits produced.
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_PIECE_THRESHOLD, 100_000);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_AVG_ROWS_PIECE_LIM, 1);
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE x (s SYMBOL INDEX CAPACITY 8, ts TIMESTAMP)
                    TIMESTAMP(ts) PARTITION BY DAY WAL""");
            execute("""
                    INSERT INTO x
                    SELECT ('k' || ((x % 2) + 1))::SYMBOL, timestamp_sequence('2024-01-01', 3_600_000_000L)
                    FROM long_sequence(24)""");
            // A later day, so 2024-01-01 is never the active partition and every write below is out of order.
            execute("INSERT INTO x VALUES ('k1', '2024-01-05T00:00:00.000000Z')");
            drainWalQueue();
            // Each backdated commit founds another piece of the 2024-01-01 partition.
            execute("INSERT INTO x VALUES ('k2', '2024-01-01T03:30:00.000000Z')");
            drainWalQueue();
            execute("INSERT INTO x VALUES ('k2', '2024-01-01T08:30:00.000000Z')");
            drainWalQueue();
            execute("INSERT INTO x VALUES ('k2', '2024-01-01T15:30:00.000000Z')");
            drainWalQueue();

            try (TableReader reader = engine.getReader(engine.verifyTableName("x"))) {
                Assert.assertTrue(
                        "test precondition: partition 2024-01-01 must be composite",
                        reader.getTxFile().isPartitionComposite(0)
                );
                final int pieceCount = reader.getGeometry().getPieceCount(0);
                Assert.assertTrue(
                        "test precondition: partition 2024-01-01 must hold several pieces, has " + pieceCount,
                        pieceCount > 1
                );
            }

            assertQuery(ORDERED_QUERY).assertsPlan(ORDERED_QUERY_PLAN);
            assertQuery(ORDERED_QUERY).returns("""
                    s\tts
                    k1\t2024-01-01T01:00:00.000000Z
                    k1\t2024-01-01T03:00:00.000000Z
                    k1\t2024-01-01T05:00:00.000000Z
                    k1\t2024-01-01T07:00:00.000000Z
                    k1\t2024-01-01T09:00:00.000000Z
                    k1\t2024-01-01T11:00:00.000000Z
                    k1\t2024-01-01T13:00:00.000000Z
                    k1\t2024-01-01T15:00:00.000000Z
                    k1\t2024-01-01T17:00:00.000000Z
                    k1\t2024-01-01T19:00:00.000000Z
                    k1\t2024-01-01T21:00:00.000000Z
                    k1\t2024-01-01T23:00:00.000000Z
                    k2\t2024-01-01T00:00:00.000000Z
                    k2\t2024-01-01T02:00:00.000000Z
                    k2\t2024-01-01T03:30:00.000000Z
                    k2\t2024-01-01T04:00:00.000000Z
                    k2\t2024-01-01T06:00:00.000000Z
                    k2\t2024-01-01T08:00:00.000000Z
                    k2\t2024-01-01T08:30:00.000000Z
                    k2\t2024-01-01T10:00:00.000000Z
                    k2\t2024-01-01T12:00:00.000000Z
                    k2\t2024-01-01T14:00:00.000000Z
                    k2\t2024-01-01T15:30:00.000000Z
                    k2\t2024-01-01T16:00:00.000000Z
                    k2\t2024-01-01T18:00:00.000000Z
                    k2\t2024-01-01T20:00:00.000000Z
                    k2\t2024-01-01T22:00:00.000000Z
                    """);
            bindVariableService.setStr("symbol", "k2");
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                for (String predicate : new String[]{"", " and s='k2'", " and s=:symbol", " and s in ('k1','k2')", " and s not in ('absent')"}) {
                    for (String direction : new String[]{"asc", "desc"}) {
                        for (String limit : new String[]{"", " limit 2,7", " limit -5"}) {
                            final String tail = "s, ts from x where ts in '2024-01-01'" + predicate
                                    + " order by s " + direction + ", ts " + direction + limit;
                            final StringSink expected = new StringSink();
                            TestUtils.printSql(compiler, sqlExecutionContext, "select /*+ no_index(x) */ " + tail, expected);
                            assertQuery("select " + tail).sizeMayVary().inferTimestamp().returns(expected);
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testOrderBySymbolAcrossPageFrameRowLimit() throws Exception {
        assertMemoryLeak(() -> {
            // cairo.sql.page.frame.max.rows feeds exactly this pair of context fields, which the context
            // caches when the test harness builds it - so set them on the context the query will run on.
            sqlExecutionContext.changePageFrameSizes(2, 4);
            try {
                execute("CREATE TABLE x (s SYMBOL INDEX, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
                execute("""
                        INSERT INTO x VALUES
                        ('k2', '2024-01-01T00:00:00.000000Z'),
                        ('k1', '2024-01-01T00:01:00.000000Z'),
                        ('k2', '2024-01-01T00:02:00.000000Z'),
                        ('k1', '2024-01-01T00:03:00.000000Z'),
                        ('k2', '2024-01-01T00:04:00.000000Z'),
                        ('k1', '2024-01-01T00:05:00.000000Z'),
                        ('k2', '2024-01-01T00:06:00.000000Z'),
                        ('k1', '2024-01-01T00:07:00.000000Z')""");

                assertQuery(ORDERED_QUERY).assertsPlan(ORDERED_QUERY_PLAN);
                assertQuery(ORDERED_QUERY).returns("""
                        s\tts
                        k1\t2024-01-01T00:01:00.000000Z
                        k1\t2024-01-01T00:03:00.000000Z
                        k1\t2024-01-01T00:05:00.000000Z
                        k1\t2024-01-01T00:07:00.000000Z
                        k2\t2024-01-01T00:00:00.000000Z
                        k2\t2024-01-01T00:02:00.000000Z
                        k2\t2024-01-01T00:04:00.000000Z
                        k2\t2024-01-01T00:06:00.000000Z
                        """);
            } finally {
                // the harness shares one execution context across the class's tests
                sqlExecutionContext.changePageFrameSizes(
                        configuration.getSqlPageFrameMinRows(),
                        configuration.getSqlPageFrameMaxRows()
                );
            }
        });
    }

    @Test
    public void testOrderBySymbolSingleFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (s SYMBOL INDEX, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    ('k2', '2024-01-01T00:00:00.000000Z'),
                    ('k1', '2024-01-01T00:01:00.000000Z'),
                    ('k2', '2024-01-01T00:02:00.000000Z'),
                    ('k1', '2024-01-01T00:03:00.000000Z')""");

            assertQuery(ORDERED_QUERY).assertsPlan(ORDERED_QUERY_PLAN);
            assertQuery(ORDERED_QUERY).returns("""
                    s\tts
                    k1\t2024-01-01T00:01:00.000000Z
                    k1\t2024-01-01T00:03:00.000000Z
                    k2\t2024-01-01T00:00:00.000000Z
                    k2\t2024-01-01T00:02:00.000000Z
                    """);
        });
    }

    /**
     * No configuration override at all: at the stock {@code cairo.sql.page.frame.max.rows} of 1,000,000 a
     * day partition of more rows than that splits into several page frames. The row count forbids a
     * literal expected result, so this one walks the
     * cursor and asserts the ordering contract directly.
     */
    @Test
    public void testOrderBySymbolAcrossDefaultPageFrameMaxRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE big (s SYMBOL INDEX, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO big
                    SELECT ('k' || ((x % 2) + 1))::SYMBOL, timestamp_sequence('2024-01-01', 60L)
                    FROM long_sequence(1_200_000)""");
            assertSymbolOrderAscending("SELECT s, ts FROM big WHERE ts IN '2024-01-01' ORDER BY s, ts", 1_200_000);
        });
    }

    @Test
    public void testOrderBySymbolNotInListAcrossDefaultPageFrameMaxRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE np_excluded (s SYMBOL INDEX, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO np_excluded
                    SELECT ('k' || ((x % 4) + 1))::SYMBOL, timestamp_sequence('2024-01-01', 60L)
                    FROM long_sequence(1_200_000)""");
            final String query = "SELECT s, ts FROM np_excluded WHERE s NOT IN ('k3', 'k4') ORDER BY s";
            assertQuery(query).assertsPlanContaining("FilterOnExcludedValues");
            assertSymbolOrderAscending(query, 600_000);
        });
    }

    @Test
    public void testOrderBySymbolInListAcrossDefaultPageFrameMaxRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE np_values (s SYMBOL INDEX, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO np_values
                    SELECT ('k' || ((x % 4) + 1))::SYMBOL, timestamp_sequence('2024-01-01', 60L)
                    FROM long_sequence(1_200_000)""");
            final String query = "SELECT s, ts FROM np_values WHERE s IN ('k1', 'k2') ORDER BY s";
            assertQuery(query).assertsPlanContaining("FilterOnValues");
            assertSymbolOrderAscending(query, 600_000);
        });
    }

    /**
     * A parquet partition emits one page frame per row group, so above
     * {@code cairo.partition.encoder.parquet.row.group.size} (stock 100,000) the same restart happens - and
     * with a LIMIT on top the query returns the wrong ROWS, not merely the wrong order: 'aa' lives entirely
     * in the second row group, so the first three rows of the misordered output are 'zz' rows.
     */
    @Test
    public void testOrderBySymbolWithLimitAcrossParquetRowGroups() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (s SYMBOL, i INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    ('zz', 0, '2024-01-01T00:00:00.000000Z'),
                    ('zz', 1, '2024-01-01T00:00:01.000000Z'),
                    ('zz', 2, '2024-01-01T00:00:02.000000Z'),
                    ('zz', 3, '2024-01-01T00:00:03.000000Z'),
                    ('zz', 4, '2024-01-01T00:00:04.000000Z'),
                    ('aa', 5, '2024-01-01T00:00:05.000000Z'),
                    ('aa', 6, '2024-01-01T00:00:06.000000Z'),
                    ('aa', 7, '2024-01-01T00:00:07.000000Z'),
                    ('mm', 8, '2024-01-02T00:00:00.000000Z')""");
            // 2024-01-01 stays historic so ADD INDEX routes it through indexParquetPartition
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts IN '2024-01-01'");
            execute("ALTER TABLE x ALTER COLUMN s ADD INDEX");

            assertQuery("SELECT s, i FROM x WHERE ts IN '2024-01-01' ORDER BY s LIMIT 3").returns("""
                    s\ti
                    aa\t5
                    aa\t6
                    aa\t7
                    """);
        });
    }

    /**
     * Control that must stay green: with a single key and ORDER BY key, ts ASC the claim is sound across
     * frames - every row carries the same symbol, the index cursor walks each frame forward, and frames
     * arrive in timestamp order - so the planner must keep emitting the bare index scan with no sort.
     */
    @Test
    public void testOrderBySingleKeyThenTimestampAscAcrossColumnTopFrames() throws Exception {
        assertMemoryLeak(() -> {
            createColumnTopSplitTable();
            final String query = "SELECT s, ts, v FROM x WHERE s = 'k1' AND ts IN '2024-01-01' ORDER BY s, ts";
            assertQuery(query).assertsPlan("""
                    DeferredSingleSymbolFilterPageFrame
                        Index forward scan on: s deferred: true
                          filter: s='k1'
                        Interval forward scan on: x
                          intervals: [("2024-01-01T00:00:00.000000Z","2024-01-01T23:59:59.999999Z")]
                    """);
            assertQuery(query).returns("""
                    s\tts\tv
                    k1\t2024-01-01T00:01:00.000000Z\tnull
                    k1\t2024-01-01T00:03:00.000000Z\tnull
                    k1\t2024-01-01T00:05:00.000000Z\t2
                    k1\t2024-01-01T00:07:00.000000Z\t4
                    """);
        });
    }

    /**
     * Both the index and page frames must run backward to preserve descending timestamp order.
     */
    @Test
    public void testOrderBySingleKeyThenTimestampDescAcrossColumnTopFrames() throws Exception {
        assertMemoryLeak(() -> {
            createColumnTopSplitTable();
            final String query = "SELECT s, ts, v FROM x WHERE s = 'k1' AND ts IN '2024-01-01' ORDER BY s, ts DESC";
            assertQuery(query).assertsPlan("""
                    DeferredSingleSymbolFilterPageFrame
                        Index backward scan on: s deferred: true
                          filter: s='k1'
                        Interval backward scan on: x
                          intervals: [("2024-01-01T00:00:00.000000Z","2024-01-01T23:59:59.999999Z")]
                    """);
            assertQuery(query).returns("""
                    s\tts\tv
                    k1\t2024-01-01T00:07:00.000000Z\t4
                    k1\t2024-01-01T00:05:00.000000Z\t2
                    k1\t2024-01-01T00:03:00.000000Z\tnull
                    k1\t2024-01-01T00:01:00.000000Z\tnull
                    """);
        });
    }

    private static void assertSymbolOrderAscending(String query, int expectedRowCount) throws Exception {
        try (RecordCursorFactory factory = select(query);
             RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            final Record record = cursor.getRecord();
            String previous = "";
            int row = 0;
            while (cursor.hasNext()) {
                final String symbol = record.getSymA(0).toString();
                Assert.assertTrue("ORDER BY violation at row " + row + ": " + previous + " -> " + symbol,
                        previous.compareTo(symbol) <= 0);
                previous = symbol;
                row++;
            }
            Assert.assertEquals(expectedRowCount, row);
        }
    }

    /**
     * Adding a column and writing into the same partition again leaves a column top, which cuts the
     * partition into two page frames for any query that projects the new column.
     */
    private static void createColumnTopSplitTable() throws Exception {
        execute("CREATE TABLE x (s SYMBOL INDEX, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("""
                INSERT INTO x VALUES
                ('k2', '2024-01-01T00:00:00.000000Z'),
                ('k1', '2024-01-01T00:01:00.000000Z'),
                ('k2', '2024-01-01T00:02:00.000000Z'),
                ('k1', '2024-01-01T00:03:00.000000Z')""");
        execute("ALTER TABLE x ADD COLUMN v INT");
        execute("""
                INSERT INTO x VALUES
                ('k2', '2024-01-01T00:04:00.000000Z', 1),
                ('k1', '2024-01-01T00:05:00.000000Z', 2),
                ('k2', '2024-01-01T00:06:00.000000Z', 3),
                ('k1', '2024-01-01T00:07:00.000000Z', 4)""");
    }
}
