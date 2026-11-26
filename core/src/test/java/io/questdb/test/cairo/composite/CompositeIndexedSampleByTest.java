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

package io.questdb.test.cairo.composite;

import io.questdb.PropertyKey;
import io.questdb.test.AbstractCairoTest;
import org.junit.Before;
import org.junit.Test;

/**
 * An index lists the FILE rows a key appears at; everything else about a page frame speaks in PARTITION
 * rows, and for a composite partition a relocated piece makes the two differ by that piece's shift. {@link
 * io.questdb.cairo.sql.PageFrame#getIndexRowLo()} / {@link io.questdb.cairo.sql.PageFrame#getIndexRowHi()}
 * exist to bridge exactly that, and every LATEST BY and symbol-index cursor was converted to them.
 * <p>
 * This class pins the consumers down from the outside instead: the same data is built twice, once with the
 * symbol column INDEXed and once without, so any query whose answer depends on which one it runs against
 * is reading the wrong file rows. The unindexed table is the oracle - it never consults an index, so its
 * answer is the partition-row answer by construction.
 */
public class CompositeIndexedSampleByTest extends AbstractCairoTest {

    @Before
    public void setUpMergeAppend() {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        // A production-sized partition pre-splits on its own at the 50MB default; shrink the threshold so
        // a fixture small enough to read reaches the same multi-piece, relocated-piece shape.
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 512);
        node1.setProperty(PropertyKey.CAIRO_O3_MID_PARTITION_MAX_SPLITS, 50);
    }

    /**
     * The interval form of {@link #testLatestByNullOverALateIndexedSymbol()}: an interval that ends inside
     * the pieces below the column top limits each frame's index row hi, so the clamp must use the frame's
     * bound rather than the directory-wide top.
     */
    @Test
    public void testLatestByNullInIntervalOverALateIndexedSymbol() throws Exception {
        configOverrideUseWithinLatestByOptimisation();
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1K");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_AVG_ROWS_PIECE_LIM, 8);
        assertMemoryLeak(() -> {
            createLateSymbolDay("indexed", " INDEX", false);
            createLateSymbolDay("plain", "", false);

            final String query = "SELECT late_sym, ts, price FROM %s WHERE ts < '2020-02-03T09:00' LATEST ON ts PARTITION BY late_sym ORDER BY late_sym";
            final String expected = """
                    late_sym\tts\tprice
                    \t2020-02-03T08:59:45.000000Z\t2160.0
                    """;
            assertQuery(String.format(query, "plain")).expectSize().returns(expected);
            assertQuery(String.format(query, "indexed"))
                    .expectSize()
                    .withPlanContaining("LatestByAllIndexed")
                    .returns(expected);
        });
    }

    /**
     * A symbol column added after the day turned composite has a column top: every row below it is an
     * implicit NULL the index does not list. {@code LatestByAllIndexedRecordCursor} passes that top to the
     * native latest-by as the unindexed NULL count, and the native code answers NULL's latest row as
     * {@code top - 1}. A piece that ends below the top must clamp that to its own index row hi; otherwise
     * the frame-relative row lands outside the piece and NULL's latest row comes back stale.
     */
    @Test
    public void testLatestByNullOverALateIndexedSymbol() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1K");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_AVG_ROWS_PIECE_LIM, 8);
        assertMemoryLeak(() -> {
            createLateSymbolDay("indexed", " INDEX", false);
            createLateSymbolDay("plain", "", false);

            final String query = "SELECT late_sym, ts, price FROM %s LATEST ON ts PARTITION BY late_sym ORDER BY late_sym";
            final String expected = """
                    late_sym\tts\tprice
                    \t2020-02-03T23:59:45.000000Z\t5760.0
                    A\t2020-02-03T23:59:59.000000Z\t9999.0
                    """;
            assertQuery(String.format(query, "plain")).expectSize().returns(expected);
            assertQuery(String.format(query, "indexed"))
                    .expectSize()
                    .withPlanContaining("LatestByAllIndexed")
                    .returns(expected);
        });
    }

    /**
     * The geohash WITHIN form of {@link #testLatestByNullOverALateIndexedSymbol()}, which runs the native
     * latest-by with a prefix filter. Every row carries the same geohash, so the expected rows are the ones
     * that test checks against the unindexed oracle.
     */
    @Test
    public void testLatestByNullWithinOverALateIndexedSymbol() throws Exception {
        configOverrideUseWithinLatestByOptimisation();
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1K");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_AVG_ROWS_PIECE_LIM, 8);
        assertMemoryLeak(() -> {
            createLateSymbolDay("indexed", " INDEX", true);

            assertQuery("SELECT late_sym, ts, price FROM indexed WHERE g WITHIN(#u3) LATEST ON ts PARTITION BY late_sym ORDER BY late_sym")
                    .expectSize()
                    .withPlanContaining("LatestByAllIndexed")
                    .returns("""
                            late_sym\tts\tprice
                            \t2020-02-03T23:59:45.000000Z\t5760.0
                            A\t2020-02-03T23:59:59.000000Z\t9999.0
                            """);
        });
        // EXPLAIN leaves WITHIN prefixes in the pooled generator. Later tests disable the
        // setting, so discard these compilers before they can reuse that scratch state.
        engine.getSqlCompilerPool().releaseAll();
    }

    /**
     * {@code SampleByFirstLastRecordCursorFactory} bounds its index cursor - and rebases the timestamp page
     * address it reads through - with the frame's index row bounds. Bounded with partition rows instead, on
     * a relocated piece it asks the index for one row range and reads the answer out of another.
     */
    @Test
    public void testSampleByFirstLastOverAnIndexedSymbol() throws Exception {
        assertMemoryLeak(() -> {
            createCompositeDay("indexed", ", INDEX(s CAPACITY 8)");
            createCompositeDay("plain", "");

            final String expected = """
                    ts\tfirst\tlast
                    2024-01-01T00:01:00.000000Z\t2\t810001
                    2024-01-01T01:01:00.000000Z\t62\t800001
                    2024-01-01T02:01:00.000000Z\t122\t180
                    2024-01-01T03:01:00.000000Z\t182\t240
                    """;
            // The oracle first, so a fixture that stops producing a composite partition fails here rather
            // than silently making the assertion below vacuous.
            assertQuery(sampleBy("plain")).timestamp("ts").noRandomAccess().returns(expected);
            assertQuery(sampleBy("indexed")).timestamp("ts").noRandomAccess().returns(expected);
        });
    }

    /**
     * The same consumer over the ACTIVE partition, which the writer is still appending to and whose index
     * is therefore still being written. Not a composite shape - the day is one piece - so this is the
     * pre-existing behaviour the composite fix must leave intact.
     */
    @Test
    public void testSampleByFirstLastOverAnIndexedSymbolOnTheActivePartition() throws Exception {
        assertMemoryLeak(() -> {
            for (String table : new String[]{"indexed", "plain"}) {
                execute("CREATE TABLE " + table + " AS (" +
                        " SELECT x::INT v, ('k' || ((x % 3) + 1))::SYMBOL s," +
                        " timestamp_sequence('2024-01-01', 60_000_000L) ts" +
                        " FROM long_sequence(240))" + (table.equals("indexed") ? ", INDEX(s CAPACITY 8)" : "") +
                        " TIMESTAMP(ts) PARTITION BY DAY WAL");
                drainWalQueue();
                // Appends to the active partition, so the writer's own index is what the query reads.
                execute("INSERT INTO " + table + " SELECT x::INT + 90_000, 'k3'," +
                        " timestamp_sequence('2024-01-01T04:00:00', 60_000_000L) FROM long_sequence(10)");
                drainWalQueue();
            }

            final String query = "SELECT ts, first(v), last(v) FROM %s WHERE s = 'k3' SAMPLE BY 1h ALIGN TO FIRST OBSERVATION";
            // k3 is every x with x % 3 == 2: v = 2, 5, ..., 239 at minute x - 1, then the ten appended rows.
            final String expected = """
                    ts\tfirst\tlast
                    2024-01-01T00:01:00.000000Z\t2\t59
                    2024-01-01T01:01:00.000000Z\t62\t119
                    2024-01-01T02:01:00.000000Z\t122\t179
                    2024-01-01T03:01:00.000000Z\t182\t90001
                    2024-01-01T04:01:00.000000Z\t90002\t90010
                    """;
            assertQuery(String.format(query, "plain")).timestamp("ts").noRandomAccess().returns(expected);
            assertQuery(String.format(query, "indexed")).timestamp("ts").noRandomAccess().returns(expected);
        });
    }

    /**
     * The plain per-row index scan, for contrast: it goes through the page-frame row cursors that were
     * converted first, so it is the control that shows the fixture itself is sound.
     */
    @Test
    public void testSymbolIndexScanOverACompositePartition() throws Exception {
        assertMemoryLeak(() -> {
            createCompositeDay("indexed", ", INDEX(s CAPACITY 8)");
            createCompositeDay("plain", "");

            final String expected = "count\tsum\n140\t16114630\n";
            assertQuery("SELECT count(), sum(v) FROM plain WHERE s = 'k1' AND ts IN '2024-01-01'")
                    .expectSize()
                    .noRandomAccess()
                    .returns(expected);
            assertQuery("SELECT count(), sum(v) FROM indexed WHERE s = 'k1' AND ts IN '2024-01-01'")
                    .expectSize()
                    .noRandomAccess()
                    .returns(expected);
        });
    }

    /**
     * 2024-01-01T00:00 .. 03:59 at one row a minute, then two backdated strides. Each stride is rewritten
     * at the shared files' tail, which is what leaves the day several pieces whose file rows no longer
     * line up with their partition rows - {@code shift != 0}.
     */
    static void createCompositeDay(String table, String index) throws Exception {
        execute("CREATE TABLE " + table + " AS (" +
                " SELECT x::INT v, ('k' || ((x % 2) + 1))::SYMBOL s," +
                " timestamp_sequence('2024-01-01', 60_000_000L) ts" +
                " FROM long_sequence(240))" + index + " TIMESTAMP(ts) PARTITION BY DAY WAL");
        // A later day, so 2024-01-01 is never the active partition and both strides below are O3.
        execute("INSERT INTO " + table + " VALUES (90_000, 'k1', '2024-01-03T00:00:00.000000Z')");
        drainWalQueue();
        execute("INSERT INTO " + table + " SELECT x::INT + 800_000, 'k1'," +
                " timestamp_sequence('2024-01-01T02:00:00', 60_000_000L) FROM long_sequence(10)");
        drainWalQueue();
        execute("INSERT INTO " + table + " SELECT x::INT + 810_000, 'k1'," +
                " timestamp_sequence('2024-01-01T01:00:00', 60_000_000L) FROM long_sequence(10)");
        drainWalQueue();
    }

    /**
     * A day of 15-second rows, made composite by two backdated strides, then given a {@code late_sym} column
     * whose column top lies above the relocated pieces, plus one row for key 'A' past the top.
     */
    private static void createLateSymbolDay(String table, String index, boolean hasGeoHash) throws Exception {
        final String geoHashColumn = hasGeoHash ? ", g GEOHASH(2c)" : "";
        final String geoHashValue = hasGeoHash ? ", #u3" : "";
        execute("CREATE TABLE " + table + " (ts TIMESTAMP, sym SYMBOL, price DOUBLE" + geoHashColumn + ") TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("INSERT INTO " + table + " SELECT timestamp_sequence('2020-02-03', 15 * 1_000_000L), 'F', x * 1.0" + geoHashValue + " FROM long_sequence(5_760)");
        drainWalQueue();
        execute("INSERT INTO " + table + " SELECT timestamp_sequence('2020-02-03T02:00:07', 5 * 1_000_000L), 'G', x * 1.0" + geoHashValue + " FROM long_sequence(120)");
        execute("INSERT INTO " + table + " SELECT timestamp_sequence('2020-02-03T08:00:11', 5 * 1_000_000L), 'H', x * 1.0" + geoHashValue + " FROM long_sequence(120)");
        drainWalQueue();
        execute("ALTER TABLE " + table + " ADD COLUMN late_sym SYMBOL" + index);
        drainWalQueue();
        execute("INSERT INTO " + table + " VALUES ('2020-02-03T23:59:59', 'A', 9999.0" + geoHashValue + ", 'A')");
        drainWalQueue();
    }

    private static String sampleBy(String table) {
        return "SELECT ts, first(v), last(v) FROM " + table +
                " WHERE s = 'k1' AND ts IN '2024-01-01' SAMPLE BY 1h ALIGN TO FIRST OBSERVATION";
    }
}
