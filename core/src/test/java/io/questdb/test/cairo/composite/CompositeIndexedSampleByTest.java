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

    private static String sampleBy(String table) {
        return "SELECT ts, first(v), last(v) FROM " + table +
                " WHERE s = 'k1' AND ts IN '2024-01-01' SAMPLE BY 1h ALIGN TO FIRST OBSERVATION";
    }
}
