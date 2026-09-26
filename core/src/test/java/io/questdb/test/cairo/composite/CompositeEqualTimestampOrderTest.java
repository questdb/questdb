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
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * QuestDB preserves INSERTION order among rows sharing a timestamp: an O3 merge takes the existing row
 * before the incoming one - {@code binary_merge_ts_long_index} compares {@code <=} on the partition
 * side. These tests assert a composite partition keeps that promise, on the shape
 * {@code FuzzTableFactory.applyCompositePartition} builds: a handful of rows backdated onto timestamps
 * a non-active day already holds.
 * <p>
 * The invariant is checked through a monotonically increasing marker column {@code i}: whatever the
 * geometry, a forward scan must never return a larger {@code i} before a smaller one at the same
 * timestamp. The equal timestamps also have to survive the round trip a
 * {@code QueryFuzzTest} shadow makes - copied out to a plain sibling, and converted to parquet.
 * <p>
 * Note what these tests do NOT cover, because no storage layer decides it: which of two equal-timestamp
 * rows a QUERY returns first. A cursor that reads through a symbol index emits rows grouped by symbol
 * key rather than in physical order, so {@code row_number() OVER (ORDER BY ts DESC)} numbers a tied
 * pair differently on an indexed table than on a non-indexed one. That is long-standing behaviour, has
 * nothing to do with partition geometry, and is why the fuzz generator keeps timestamps unique.
 */
public class CompositeEqualTimestampOrderTest extends AbstractCairoTest {

    private static final long DAY_1 = MicrosTimestampDriver.floor("2024-01-01T00:00:00.000000Z");
    private static final Log LOG = LogFactory.getLog(CompositeEqualTimestampOrderTest.class);
    // The fuzz generator's own step: 30 minutes, 48 rows to a DAY partition.
    private static final long STEP = 30L * 60L * 1_000_000L;

    /**
     * 136 rows over three DAY partitions, then 1..3 rows backdated onto the first timestamps of the
     * first, non-active day - a single MERGE relocating the whole day to the shared files' tail.
     */
    @Test
    public void testFuzzBackdateShapeKeepsTieOrder() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
            for (int overlapRows = 1; overlapRows <= 3; overlapRows++) {
                execute("DROP TABLE IF EXISTS x");
                createBase(136);
                backdate(1000, DAY_1, overlapRows);
                drainWalQueue();
                assertTieOrderPreserved("overlapRows=" + overlapRows, "x");
            }
        });
    }

    /**
     * The same shape with the pre-split knobs turned down far enough that a 48-row partition is big
     * enough to cut ({@code computeCuts} needs {@code rows >= 4 * avgRowsPieceLim}), and the batch
     * landing at offsets across the day so a cut falls before, inside and after the shared timestamps.
     * A cut resolves through {@code BIN_SEARCH_SCAN_UP}, so it lands on the FIRST row carrying its
     * timestamp and can never split a run of equal timestamps between two pieces.
     */
    @Test
    public void testPreSplitKeepsTieOrder() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_AVG_ROWS_PIECE_LIM, 4);
            for (int offsetRows = 0; offsetRows < 48; offsetRows += 7) {
                for (int overlapRows = 1; overlapRows <= 3; overlapRows++) {
                    execute("DROP TABLE IF EXISTS x");
                    createBase(136);
                    backdate(1000, DAY_1 + offsetRows * STEP, overlapRows);
                    drainWalQueue();
                    assertTieOrderPreserved("offsetRows=" + offsetRows + ", overlapRows=" + overlapRows, "x");
                }
            }
        });
    }

    /**
     * Successive backdates onto the SAME timestamps. Each commit leaves the day composite, so the next
     * one routes its rows past pieces whose bounds already touch at the shared timestamp.
     */
    @Test
    public void testRepeatedBackdatesKeepTieOrder() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_AVG_ROWS_PIECE_LIM, 4);
            createBase(136);
            for (int round = 1; round <= 6; round++) {
                backdate(1000 * round, DAY_1 + (round % 3) * 5 * STEP, 3);
                drainWalQueue();
                assertTieOrderPreserved("round=" + round, "x");
            }
        });
    }

    /**
     * A plain sibling filled by {@code INSERT ... SELECT * FROM} the composite primary must read back
     * identically, and must still do so once either side converts to parquet - forwards and in
     * descending timestamp order.
     */
    @Test
    public void testShadowCopyAgreesWithCompositePrimary() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
            createBase(136);
            backdate(1000, DAY_1, 3);
            drainWalQueue();

            execute("CREATE TABLE y (i INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO y SELECT * FROM x");
            drainWalQueue();
            assertTieOrderPreserved("plain copy", "y");
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, "x", "y", LOG);
            TestUtils.assertSqlCursors(engine, sqlExecutionContext,
                    "(x ORDER BY ts DESC)", "(y ORDER BY ts DESC)", LOG);

            // Parquet on the plain copy: the arrangement QueryFuzzTest's shadow oracle compares.
            execute("ALTER TABLE y CONVERT PARTITION TO PARQUET WHERE ts >= 0");
            drainWalQueue();
            assertTieOrderPreserved("plain copy, parquet", "y");
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, "x", "y", LOG);
            TestUtils.assertSqlCursors(engine, sqlExecutionContext,
                    "(x ORDER BY ts DESC)", "(y ORDER BY ts DESC)", LOG);

            // ... and on the composite primary itself.
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");
            drainWalQueue();
            assertTieOrderPreserved("composite primary, parquet", "x");
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, "y", "x", LOG);
            TestUtils.assertSqlCursors(engine, sqlExecutionContext,
                    "(y ORDER BY ts DESC)", "(x ORDER BY ts DESC)", LOG);
        });
    }

    /**
     * Stability across the piece boundaries the plan draws. A tie run must never be split between two
     * actions in the wrong order, so the batch is aimed at every offset of the day - including a
     * piece's own tsLo and tsHi, where {@code computeActions} decides between claiming a tied row for
     * the piece's MERGE and sparing it for the NEW_PIECE above.
     */
    @Test
    public void testTiesAcrossPieceBoundariesKeepArrivalOrder() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_AVG_ROWS_PIECE_LIM, 4);
            for (int offsetRows = 0; offsetRows < 48; offsetRows += 5) {
                for (int copies = 2; copies <= 3; copies++) {
                    execute("DROP TABLE IF EXISTS x");
                    createBase(136);
                    // First commit makes the day composite, the second aims at the pieces it left.
                    backdateTied(1000, DAY_1 + offsetRows * STEP, 3, copies);
                    drainWalQueue();
                    assertTieOrderPreserved("first, offsetRows=" + offsetRows + ", copies=" + copies, "x");
                    backdateTied(5000, DAY_1 + offsetRows * STEP, 3, copies);
                    drainWalQueue();
                    assertTieOrderPreserved("second, offsetRows=" + offsetRows + ", copies=" + copies, "x");
                }
            }
        });
    }

    /**
     * Ties INSIDE one commit, on a composite partition. The batch arrives round-robin, so it is not
     * sorted by timestamp and the WAL apply has to sort it; a stable sort leaves the markers of each
     * tied run ascending, and the merge into the piece has to keep them that way.
     */
    @Test
    public void testWithinCommitTiesKeepArrivalOrder() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
            for (int copies = 2; copies <= 4; copies++) {
                execute("DROP TABLE IF EXISTS x");
                createBase(136);
                backdateTied(1000, DAY_1, 3, copies);
                drainWalQueue();
                assertTieOrderPreserved("backdated, copies=" + copies, "x");

                // The same shape at the table's own maximum: an append that ties with the last row.
                backdateTied(5000, DAY_1 + 135 * STEP, 2, copies);
                drainWalQueue();
                assertTieOrderPreserved("appended, copies=" + copies, "x");
            }
        });
    }

    private static void assertTieOrderPreserved(String label, String table) throws SqlException {
        long prevTs = Long.MIN_VALUE;
        int prevI = Integer.MIN_VALUE;
        long row = 0;
        long ties = 0;
        try (
                RecordCursorFactory factory = select("SELECT i, ts FROM " + table);
                RecordCursor cursor = factory.getCursor(sqlExecutionContext)
        ) {
            final Record record = cursor.getRecord();
            while (cursor.hasNext()) {
                final int i = record.getInt(0);
                final long ts = record.getTimestamp(1);
                Assert.assertTrue(
                        label + ": timestamps went backwards at row " + row + " [ts=" + ts + ", prevTs=" + prevTs + ']',
                        ts >= prevTs
                );
                if (ts == prevTs) {
                    ties++;
                    Assert.assertTrue(
                            label + ": equal-timestamp rows out of insertion order at row " + row
                                    + " [ts=" + ts + ", i=" + i + " came after i=" + prevI + ']',
                            i > prevI
                    );
                }
                prevTs = ts;
                prevI = i;
                row++;
            }
        }
        // Nothing here is worth asserting if the shape produced no tied rows to begin with.
        Assert.assertTrue(label + ": no equal-timestamp rows in " + table, ties > 0);
    }

    private static void backdate(int markerBase, long startMicros, int rowCount) throws SqlException {
        execute(
                "INSERT INTO x SELECT (" + markerBase + " + x)::INT i, ("
                        + startMicros + " + (x - 1) * " + STEP + ")::TIMESTAMP ts"
                        + " FROM long_sequence(" + rowCount + ')'
        );
    }

    /**
     * Inserts {@code copies} rows on each of {@code timestamps} consecutive timestamps from
     * {@code startMicros}, round-robin: row {@code x} carries marker {@code markerBase + x} and lands
     * on timestamp {@code (x - 1) % timestamps}, so markers ascend in ARRIVAL order while the batch
     * itself is out of timestamp order.
     */
    private static void backdateTied(int markerBase, long startMicros, int timestamps, int copies) throws SqlException {
        execute(
                "INSERT INTO x SELECT (" + markerBase + " + x)::INT i, ("
                        + startMicros + " + ((x - 1) % " + timestamps + ") * " + STEP + ")::TIMESTAMP ts"
                        + " FROM long_sequence(" + ((long) timestamps * copies) + ')'
        );
    }

    private static void createBase(int rowCount) throws SqlException {
        execute(
                "CREATE TABLE x AS (" +
                        "SELECT x::INT i, timestamp_sequence('2024-01-01', " + STEP + "L) ts" +
                        " FROM long_sequence(" + rowCount + ')' +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL"
        );
        drainWalQueue();
    }
}
