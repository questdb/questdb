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
 * The two fixtures differ in exactly one thing. Same four keys, same
 * {@link #PARTITIONS} partitions, same columns, same query -- only the number of rows per pair
 * moves, from {@link #SPARSE_ROWS_PER_PAIR} (well under the crossover) to
 * {@link #DENSE_ROWS_PER_PAIR} (well over it). Anything else that could flip the mode is held
 * constant, so a passing pair of arms cannot be explained by the key count, the partition
 * count, the frame-count ceiling or the plan shape.
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

    private void assertScalar(String sql, String expected) throws Exception {
        final StringSink sink = new StringSink();
        printSql(sql, sink);
        TestUtils.assertEquals(expected, sink);
    }

    /**
     * {@link #KEYS} keys spread evenly over {@link #PARTITIONS} daily partitions at exactly
     * {@code rowsPerPair} rows per (key, partition) pair. Row x lands in partition
     * {@code (x - 1) / (KEYS * rowsPerPair)}, so timestamps ascend and the loader stays on the
     * append path.
     */
    private void createTelemetryAtDensity(int rowsPerPair) throws Exception {
        final int rowsPerPartition = KEYS * rowsPerPair;
        final long rows = (long) rowsPerPartition * PARTITIONS;
        execute("CREATE TABLE telemetry (" +
                "  ts TIMESTAMP," +
                "  param_id SYMBOL INDEX TYPE POSTING INCLUDE (ts, value)," +
                "  value DOUBLE" +
                ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
        execute("INSERT INTO telemetry SELECT" +
                " ((((x - 1) / " + rowsPerPartition + ") * 86400000000L)" +
                "   + (((x - 1) % " + rowsPerPartition + ") * 1000L))::timestamp," +
                " CASE WHEN x % 4 = 0 THEN 'SFID' WHEN x % 4 = 1 THEN 'HOTMIC'" +
                "      WHEN x % 4 = 2 THEN 'KCAS' ELSE 'CALT' END," +
                " x::double" +
                " FROM long_sequence(" + rows + ")");
        // Non-vacuity guard: both arms are calibrated on EXACTLY this partition count. If a
        // spacing edit collapsed the fixture to one partition, the sparse arm's density would
        // jump by 40x and it would silently start asserting the opposite of what it claims.
        assertScalar("SELECT count() c FROM table_partitions('telemetry')", "c\n" + PARTITIONS + "\n");
    }
}
