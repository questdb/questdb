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

import io.questdb.griffin.engine.table.CoveringIndexRecordCursorFactory;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Pins the PERFORMANCE crossover on the per-key (unordered) covering scan: below roughly 30
 * rows per (key, partition) pair, per-key mode must hand back to the timestamp-ordered merge.
 * <p>
 * Per-key costs a fixed 1.2-1.8 us per FRAME regardless of how many rows that frame carries,
 * and it emits one frame per non-empty (key, partition) pair. So on identical data, with only
 * the partition count varying, it measured 2.9x FASTER at P=1, parity at P=4 and 3.0x SLOWER at
 * P=32 -- a straight inversion of the 12x win the mode was added for. The crossover is where
 * the fixed per-frame cost stops being amortised: ~30 rows per pair measured, 31.6 by analytic
 * fit, {@code PER_KEY_MIN_ROWS_PER_PAIR = 32} as shipped.
 * <p>
 * The two UNIFORM fixtures differ in exactly one thing. Same four keys, same
 * {@link #PARTITIONS} partitions, same columns, same query -- only the number of rows per pair
 * moves, from {@link #SPARSE_ROWS_PER_PAIR} (well under the crossover) to
 * {@link #DENSE_ROWS_PER_PAIR} (well over it). Anything else that could flip the mode is held
 * constant, so a passing pair of arms cannot be explained by the key count, the partition
 * count, the frame-count ceiling or the plan shape.
 * <p>
 * <b>The remaining fixtures are deliberately NON-uniform, because a uniform one cannot see the
 * estimate at all.</b> Rows per pair is a mean; on a table that is flat along both axes any
 * sample of it, taken anywhere, is the right answer, so a suite built only from uniform
 * fixtures passes whatever the estimate samples and however it divides. Every defect the
 * estimate has had so far -- a prefix of the partition axis being scored as if it were the
 * whole of it, empty pairs entering the denominator -- is invisible to a flat table and shows
 * up the moment density varies along one axis. The four shapes below vary it: a leading sparse
 * partition, keys commissioned partway through history, a dense head above a sparse tail, and
 * keys absent from most partitions.
 * <p>
 * <b>Both arms assert the MODE, not just the answer.</b> Merged and per-key return identical
 * rows at this size -- that is the whole point of the fallback -- so a result comparison alone
 * would pass whichever mode ran, and a gate that silently rejected everything would look
 * healthy. {@code getPerKeyModeOpensForTesting()} /
 * {@code getMergedModeOpensForTesting()} are the only observables of the decision: the plan
 * prints the plan-stable PERMISSION and says nothing about the mode an execution picked.
 */
public class CoveringIndexPerKeyDensityTest extends AbstractCoveringIndexQueryTest {

    private static final int DENSE_ROWS_PER_PAIR = 200;
    private static final int KEYS = 4;
    private static final int PARTITIONS = 40;
    private static final int SPARSE_ROWS_PER_PAIR = 5;
    private static final String QUERY =
            "SELECT param_id, avg(value) a, count() c FROM telemetry" +
                    " WHERE param_id IN ('SFID','HOTMIC','KCAS','CALT') ORDER BY param_id";

    /**
     * A dense head above a long sparse tail must fall back to the MERGE.
     * <p>
     * This is the shape the gate exists to catch, and the one a prefix sample gets exactly
     * backwards: four partitions at 1000 rows per pair followed by 600 at 2 leaves an overall
     * 8.6 rows per pair, well under the crossover, but any sample taken from the front of the
     * scan sees only the head and admits per-key. Measured on this fixture with the mode forced
     * either way, per-key runs 1.25x slower than the merge; lengthening the tail to 2000
     * partitions takes that to 1.72x.
     */
    @Test(timeout = 300_000)
    public void testDenseHeadAboveSparseTailFallsBackToMerge() throws Exception {
        assertMemoryLeak(() -> {
            createTelemetryTable();
            insertUniformBlock(0, 4, 1000);
            insertUniformBlock(4, 600, 2);
            assertPartitionCount(604);
            CoveringIndexRecordCursorFactory.resetModeSelectionsForTesting();
            assertSameResult(QUERY, referenceOf(QUERY));
            Assert.assertEquals(
                    "a dense HEAD admitted per-key on a table whose overall density (8.6 rows per"
                            + " pair) is a quarter of the crossover. The density estimate is scoring the"
                            + " front of the scan instead of the whole of it, which is the exact"
                            + " inversion this gate exists to prevent: per-key measures 1.25x SLOWER"
                            + " than the merge on this fixture.",
                    0,
                    CoveringIndexRecordCursorFactory.getPerKeyModeOpensForTesting()
            );
            Assert.assertTrue(
                    "the gate never ran, so this test proved nothing",
                    CoveringIndexRecordCursorFactory.getMergedModeOpensForTesting() > 0
            );
        });
    }

    /**
     * Pairs the index holds no rows for must not enter the denominator.
     * <p>
     * Eight keys round-robined one per partition at 100 rows each: seven of every eight (key,
     * partition) pairs are EMPTY, and per-key emits no frame for an empty pair, so every frame
     * it does emit carries 100 rows -- 3.1x the crossover. Dividing the matched rows by ALL
     * sampled pairs instead of the non-empty ones scores this shape 12 and rejects it.
     */
    @Test(timeout = 300_000)
    public void testEmptyPairsDoNotDragTheDensityDown() throws Exception {
        assertMemoryLeak(() -> {
            createTelemetryTable();
            // Partition p holds key 'K' + (p % 8) and nothing else, 100 rows.
            execute("INSERT INTO telemetry SELECT" +
                    " ((((x - 1) / 100) * 86400000000L) + (((x - 1) % 100) * 1000L))::timestamp," +
                    " concat('K', (((x - 1) / 100) % 8)::string)," +
                    " x::double" +
                    " FROM long_sequence(8000)");
            assertPartitionCount(80);
            final String sql = "SELECT param_id, avg(value) a, count() c FROM telemetry" +
                    " WHERE param_id IN ('K0','K1','K2','K3','K4','K5','K6','K7') ORDER BY param_id";
            CoveringIndexRecordCursorFactory.resetModeSelectionsForTesting();
            assertSameResult(sql, referenceOf(sql));
            Assert.assertTrue(
                    "100 rows per EMITTED frame -- 3.1x the crossover -- was rejected. The density"
                            + " estimate is dividing by every sampled (key, partition) pair rather than"
                            + " by the non-empty ones per-key actually emits a frame for, so a shape is"
                            + " scored by how many of its keys are ABSENT from a partition.",
                    CoveringIndexRecordCursorFactory.getPerKeyModeOpensForTesting() > 0
            );
        });
    }

    /**
     * A symbol that starts being recorded partway through history must still take per-key.
     * <p>
     * This is the motivating workload for the whole per-key mode -- a telemetry parameter
     * commissioned mid-history is the norm -- and the one a prefix sample scores ZERO on,
     * because every partition the sample looks at predates the symbol. Here the four queried
     * keys appear only from partition 50 onwards and then run at 200 rows per pair, 6.2x the
     * crossover.
     */
    @Test(timeout = 300_000)
    public void testKeysCommissionedPartwayThroughHistoryTakePerKey() throws Exception {
        assertMemoryLeak(() -> {
            createTelemetryTable();
            execute("INSERT INTO telemetry SELECT" +
                    " ((((x - 1) / 100) * 86400000000L) + (((x - 1) % 100) * 1000L))::timestamp," +
                    " 'PREDECESSOR'," +
                    " x::double" +
                    " FROM long_sequence(5000)");
            insertUniformBlock(50, 100, 200);
            assertPartitionCount(150);
            CoveringIndexRecordCursorFactory.resetModeSelectionsForTesting();
            assertSameResult(QUERY, referenceOf(QUERY));
            Assert.assertTrue(
                    "four keys recorded only from partition 50 onwards, at 200 rows per pair (6.2x"
                            + " the crossover), fell back to the merge. The density estimate is"
                            + " sampling the front of the scan, where the keys do not exist yet, and"
                            + " scoring them zero. A parameter commissioned partway through history is"
                            + " the motivating workload for per-key mode, not a corner case.",
                    CoveringIndexRecordCursorFactory.getPerKeyModeOpensForTesting() > 0
            );
        });
    }

    /**
     * One leading partition holding eight rows must not change the decision on an otherwise
     * uniform table.
     * <p>
     * Both arms are the SAME 100-partition, 16 000-row, 40-rows-per-pair table; the only
     * difference is a single extra leading partition of 8 rows, 0.05% of the data. A four-frame
     * prefix sample scores the two 30 and 40 -- opposite sides of the crossover -- so the
     * decision inverts on a partition holding a twentieth of a percent of the rows.
     */
    @Test(timeout = 300_000)
    public void testLeadingSparsePartitionDoesNotFlipAUniformTable() throws Exception {
        assertMemoryLeak(() -> {
            createTelemetryTable();
            insertUniformBlock(1, 100, 40);
            assertPartitionCount(100);
            CoveringIndexRecordCursorFactory.resetModeSelectionsForTesting();
            assertSameResult(QUERY, referenceOf(QUERY));
            final boolean perKeyWithoutLeader =
                    CoveringIndexRecordCursorFactory.getPerKeyModeOpensForTesting() > 0;
            Assert.assertTrue(
                    "the gate never ran on the control arm, so the comparison below proves nothing",
                    perKeyWithoutLeader || CoveringIndexRecordCursorFactory.getMergedModeOpensForTesting() > 0
            );

            execute("DROP TABLE telemetry");
            createTelemetryTable();
            // Two rows per key in the leading partition, then the identical uniform block.
            insertUniformBlock(0, 1, 2);
            insertUniformBlock(1, 100, 40);
            assertPartitionCount(101);
            CoveringIndexRecordCursorFactory.resetModeSelectionsForTesting();
            assertSameResult(QUERY, referenceOf(QUERY));
            final boolean perKeyWithLeader =
                    CoveringIndexRecordCursorFactory.getPerKeyModeOpensForTesting() > 0;

            Assert.assertEquals(
                    "adding ONE leading partition of 8 rows -- 0.05% of the table -- flipped the"
                            + " per-key decision on an otherwise uniform 40-rows-per-pair table. The"
                            + " density estimate is reading a prefix of the partition axis, which on a"
                            + " full scan is its OLDEST end, so the least representative partition in"
                            + " the table gets a quarter of the vote.",
                    perKeyWithoutLeader,
                    perKeyWithLeader
            );
        });
    }

    @Test(timeout = 300_000)
    public void testDenseShapeStillTakesPerKey() throws Exception {
        assertMemoryLeak(() -> {
            createTelemetryAtDensity(DENSE_ROWS_PER_PAIR);
            CoveringIndexRecordCursorFactory.resetModeSelectionsForTesting();
            assertSameResult(QUERY, referenceOf(QUERY));
            Assert.assertTrue(
                    "at " + DENSE_ROWS_PER_PAIR + " rows per (key, partition) pair -- " +
                            (DENSE_ROWS_PER_PAIR / 32) + "x the crossover -- the merge was chosen."
                            + " The density estimate is rejecting shapes per-key is supposed to win,"
                            + " which is the 12x win this whole mode exists for.",
                    CoveringIndexRecordCursorFactory.getPerKeyModeOpensForTesting() > 0
            );
            Assert.assertEquals(
                    "a dense open fell back to the merge",
                    0,
                    CoveringIndexRecordCursorFactory.getMergedModeOpensForTesting()
            );
        });
    }

    /**
     * Headroom canary on the shared multi-partition fixture: 10,000 rows over 4 keys and 70
     * daily partitions is ~35.7 rows per (key, partition) pair against a crossover of 32 -- a
     * margin of about 12%. That is the tightest realistic shape in the suite, and it is the one
     * that says what raising {@code PER_KEY_MIN_ROWS_PER_PAIR} would cost: at 36 this fixture
     * flips to the merge and every test built on it stops exercising per-key mode at all, while
     * still passing, because merged returns the same rows.
     * <p>
     * Sibling suites over this fixture ({@code CoveringIndexTelemetryShapeTest},
     * {@code CoveringIndexOrderSensitiveTest}) assert only the PLAN, which prints the
     * plan-stable permission and is therefore blind to the flip. This is the arm that is not.
     */
    @Test(timeout = 300_000)
    public void testSharedMultiPartitionFixtureKeepsPerKey() throws Exception {
        assertMemoryLeak(() -> {
            createTelemetryMultiPartition();
            CoveringIndexRecordCursorFactory.resetModeSelectionsForTesting();
            assertSameResult(QUERY, referenceOf(QUERY));
            Assert.assertTrue(
                    "the shared 70-partition fixture (~35.7 rows per (key, partition) pair) fell back"
                            + " to the merge. It sits only ~12% above the 32-row crossover, so either the"
                            + " constant moved or the density estimate is under-counting. Every sibling"
                            + " suite over this fixture asserts the plan only and would not have noticed.",
                    CoveringIndexRecordCursorFactory.getPerKeyModeOpensForTesting() > 0
            );
        });
    }

    @Test(timeout = 300_000)
    public void testSparseShapeFallsBackToMerge() throws Exception {
        assertMemoryLeak(() -> {
            createTelemetryAtDensity(SPARSE_ROWS_PER_PAIR);
            CoveringIndexRecordCursorFactory.resetModeSelectionsForTesting();
            assertSameResult(QUERY, referenceOf(QUERY));
            Assert.assertEquals(
                    "at " + SPARSE_ROWS_PER_PAIR + " rows per (key, partition) pair, far below the"
                            + " ~30-row crossover, per-key mode was chosen. Per-key pays a fixed"
                            + " 1.2-1.8 us per frame and emits one frame per pair, so at this density"
                            + " it is several times SLOWER than the merge it replaced.",
                    0,
                    CoveringIndexRecordCursorFactory.getPerKeyModeOpensForTesting()
            );
            Assert.assertTrue(
                    "the gate never ran: no open was permitted to choose a mode, so this test proved"
                            + " nothing. Check the query still routes through the covering index and"
                            + " still receives the unordered offer.",
                    CoveringIndexRecordCursorFactory.getMergedModeOpensForTesting() > 0
            );
        });
    }

    private static String referenceOf(String indexedSql) {
        return indexedSql.replaceFirst("SELECT ", "SELECT /*+ no_index */ ");
    }

    private void assertPartitionCount(int expected) throws Exception {
        assertScalar("SELECT count() c FROM table_partitions('telemetry')", "c\n" + expected + "\n");
    }

    private void assertScalar(String sql, String expected) throws Exception {
        final StringSink sink = new StringSink();
        printSql(sql, sink);
        TestUtils.assertEquals(expected, sink);
    }

    private void createTelemetryTable() throws Exception {
        execute("CREATE TABLE telemetry (" +
                "  ts TIMESTAMP," +
                "  param_id SYMBOL INDEX TYPE POSTING INCLUDE (ts, value)," +
                "  value DOUBLE" +
                ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
    }

    /**
     * {@link #KEYS} keys spread evenly over {@link #PARTITIONS} daily partitions at exactly
     * {@code rowsPerPair} rows per (key, partition) pair. Row x lands in partition
     * {@code (x - 1) / (KEYS * rowsPerPair)}, so timestamps ascend and the loader stays on the
     * append path.
     */
    private void createTelemetryAtDensity(int rowsPerPair) throws Exception {
        createTelemetryTable();
        insertUniformBlock(0, PARTITIONS, rowsPerPair);
        // Non-vacuity guard: both arms are calibrated on EXACTLY this partition count. If a
        // spacing edit collapsed the fixture to one partition, the sparse arm's density would
        // jump by 40x and it would silently start asserting the opposite of what it claims.
        assertPartitionCount(PARTITIONS);
    }

    /**
     * {@code partitions} consecutive daily partitions starting at day {@code firstDay}, holding
     * {@link #KEYS} keys at exactly {@code rowsPerPair} rows per (key, partition) pair.
     * Timestamps ascend within and across calls, so the loader stays on the append path.
     */
    private void insertUniformBlock(int firstDay, int partitions, int rowsPerPair) throws Exception {
        final int rowsPerPartition = KEYS * rowsPerPair;
        final long rows = (long) rowsPerPartition * partitions;
        execute("INSERT INTO telemetry SELECT" +
                " ((" + firstDay + "L + ((x - 1) / " + rowsPerPartition + ")) * 86400000000L" +
                "   + (((x - 1) % " + rowsPerPartition + ") * 1000L))::timestamp," +
                " CASE WHEN x % 4 = 0 THEN 'SFID' WHEN x % 4 = 1 THEN 'HOTMIC'" +
                "      WHEN x % 4 = 2 THEN 'KCAS' ELSE 'CALT' END," +
                " x::double" +
                " FROM long_sequence(" + rows + ")");
    }
}
