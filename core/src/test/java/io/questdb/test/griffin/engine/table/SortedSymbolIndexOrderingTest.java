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
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * The deleted SortedSymbolIndexRecordCursorFactory claimed followedOrderByAdvice(), so the planner
 * emitted no enclosing Sort for ORDER BY sym[, ts]. Its row cursor walked the sorted symbol keys
 * INSIDE one page frame, so the ordering promise only held when the whole scanned interval was a
 * single page frame. A partition splits into several page frames for reasons that have nothing to do
 * with the query: the page-frame row limit, a column top, an O3 partition split, and (with
 * merge-append on) the pieces of a composite partition.
 * <p>
 * FilterOnValuesRecordCursorFactory, FilterOnExcludedValuesRecordCursorFactory (both through
 * SequentialRowCursorFactory, whose cursor resets its key walk per frame) and the single-key index scan for
 * ORDER BY key, ts DESC (a backward index walk inside forward-ordered frames) made the same claim from the
 * same compile-time single-frame assumption, so they are covered here too.
 * <p>
 * Every test asserts the globally ordered result the SQL asks for; the plan assertions pin the
 * corrective sort the planner now emits, and the two controls pin the claims that survive.
 */
public class SortedSymbolIndexOrderingTest extends AbstractCairoTest {

    private static final String ORDERED_QUERY = "SELECT s, ts FROM x WHERE ts IN '2024-01-01' ORDER BY s, ts";
    private static final String ORDERED_QUERY_PLAN = """
            Encode sort light
              keys: [s, ts]
                PageFrame
                    Row forward scan
                    Interval forward scan on: x
                      intervals: [("2024-01-01T00:00:00.000000Z","2024-01-01T23:59:59.999999Z")]
            """;

    @Test
    public void testOrderBySymbolAcrossColumnTopFrames() throws Exception {
        assertMemoryLeak(() -> {
            createColumnTopSplitTable();

            final String query = "SELECT s, ts, v FROM x WHERE ts IN '2024-01-01' ORDER BY s, ts";
            // no leaf claims the advice any more, so the planner sorts the whole interval scan
            assertQuery(query).assertsPlan("""
                    Encode sort light
                      keys: [s, ts]
                        PageFrame
                            Row forward scan
                            Interval forward scan on: x
                              intervals: [("2024-01-01T00:00:00.000000Z","2024-01-01T23:59:59.999999Z")]
                    """);
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
                    Encode sort light
                      keys: [s, ts desc]
                        PageFrame
                            Row forward scan
                            Interval forward scan on: x
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
     * day partition of more rows than that splits into several page frames, and the symbol sequence
     * restarts at the boundary. The row count forbids a literal expected result, so this one walks the
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

    /**
     * The sibling defect: FilterOnExcludedValuesRecordCursorFactory claimed the same order-by advice
     * for the key column while driving SequentialRowCursorFactory, whose cursor restarts its key walk
     * per page frame (SequentialRowCursorFactory.SequentialRowCursor#init). On a NON-partitioned table
     * SqlCodeGenerator grants intervalHitsOnlyOnePartition outright, so no interval filter is needed to
     * reach it.
     */
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

    /**
     * The sibling defect through FilterOnValuesRecordCursorFactory, which claimed the advice for the
     * key-column case while installing SequentialRowCursorFactory for it.
     */
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

            // the sort under the LIMIT knows its own size, which the misordered index cursor did not
            assertQuery("SELECT s, i FROM x WHERE ts IN '2024-01-01' ORDER BY s LIMIT 3").expectSize().returns("""
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
                        Index forward scan on: s
                          filter: s=2
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
     * The single-key sibling of the same defect: ORDER BY key, ts DESC asks the index cursor to walk each
     * frame BACKWARD while the page frames still arrive in ascending timestamp order, so the output is a
     * concatenation of per-frame descending runs. The claim comes from the {@code orderByKeyColumn ||
     * orderByTimestamp} arguments the single-key index scans receive in SqlCodeGenerator; orderByKeyColumn
     * no longer covers the ts-DESC shape.
     */
    @Test
    public void testOrderBySingleKeyThenTimestampDescAcrossColumnTopFrames() throws Exception {
        assertMemoryLeak(() -> {
            createColumnTopSplitTable();
            final String query = "SELECT s, ts, v FROM x WHERE s = 'k1' AND ts IN '2024-01-01' ORDER BY s, ts DESC";
            assertQuery(query).assertsPlan("""
                    Encode sort light
                      keys: [s, ts desc]
                        DeferredSingleSymbolFilterPageFrame
                            Index forward scan on: s
                              filter: s=2
                            Interval forward scan on: x
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
