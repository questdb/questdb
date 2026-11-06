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
 * Pins the PERFORMANCE crossover on the per-key (unordered) covering scan: below it, per-key mode
 * must hand back to the timestamp-ordered merge.
 * <p>
 * Per-key costs a fixed cost per FRAME regardless of how many rows that frame carries, and it
 * emits one frame per non-empty (key, partition) pair, so its win is entirely a function of how
 * many rows each pair holds. The crossover is where that fixed cost stops being amortised:
 * {@code fixedFrameCost / perRowSaving}.
 * <p>
 * The numerator is the CONSUMER's, which is why the shipped number is
 * {@code PER_KEY_MIN_ROWS_PER_PAIR_BASE} times the passes the consumer makes over each frame
 * rather than a single constant. The vectorized group by dispatches one task per
 * (frame, aggregate) pair, so its per-frame cost scales with the aggregate count; the async group
 * bys walk each frame once and update every aggregate per row, so theirs does not. Swept directly
 * in {@code CoveringIndexPerKeyCrossoverPerfTest} across all three offer sites at one and four
 * aggregates -- mode DRIVEN through the override rather than inferred from the data, both arms
 * over the same bytes, bookended -- the crossover per PASS lands in 45-90 rows per pair on every
 * one of the five reachable shapes, against a base of 64.
 * <p>
 * It read 32 until the first of those sweeps, on the strength of a partition-count sweep rather
 * than a density sweep, and then 256 -- which was this base times the four passes of the single
 * shape that sweep measured, with the x4 hidden inside the constant. At 32 rows per pair per-key
 * measures 3.2-5.2x SLOWER than the merge; at 256 the gate declined per-key on the other four
 * shapes where it measured 2.2-3.4x FASTER. Both are the same mistake in opposite directions:
 * a per-consumer quantity written down as a global one.
 * <p>
 * {@link #QUERY} carries TWO aggregate values on the vectorized site, so the crossover these
 * fixtures actually face is {@link #CROSSOVER} = base x 2.
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

    // QUERY's aggregate count, which on the vectorized site IS its passes per frame: the Rosti
    // dispatch loop publishes one task per (frame, aggregate) pair. avg(value) and count().
    private static final int QUERY_FRAME_PASSES = 2;
    // The crossover QUERY actually faces, DERIVED from the shipped base rather than copied from
    // it. The base is package-private in io.questdb.griffin.engine.table, so the @TestOnly getter
    // is what couples them -- and coupling is the point: every density in this file is stated as
    // a multiple of this, and a hard-coded 256 here silently stopped meaning what the javadoc
    // said the last two times the crossover moved.
    private static final int CROSSOVER =
            CoveringIndexRecordCursorFactory.getPerKeyMinRowsPerPairBaseForTesting() * QUERY_FRAME_PASSES;
    private static final int DENSE_ROWS_PER_PAIR = 1000;
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
     * 8.6 rows per pair, a fifteenth of the crossover, but any sample taken from the front of the
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
                            + " pair) is a fifteenth of the " + CROSSOVER + "-row crossover. The density"
                            + " estimate is scoring the"
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
     * Eight keys round-robined one per partition at 1000 rows each: seven of every eight (key,
     * partition) pairs are EMPTY, and per-key emits no frame for an empty pair, so every frame
     * it does emit carries 1000 rows -- 7.8x the crossover. Dividing the matched rows by ALL
     * sampled pairs instead of the non-empty ones scores this shape 125 and rejects it.
     */
    @Test(timeout = 300_000)
    public void testEmptyPairsDoNotDragTheDensityDown() throws Exception {
        assertMemoryLeak(() -> {
            createTelemetryTable();
            // Partition p holds key 'K' + (p % 8) and nothing else, 1000 rows.
            execute("INSERT INTO telemetry SELECT" +
                    " ((((x - 1) / 1000) * 86400000000L) + (((x - 1) % 1000) * 1000L))::timestamp," +
                    " concat('K', (((x - 1) / 1000) % 8)::string)," +
                    " x::double" +
                    " FROM long_sequence(80000)");
            assertPartitionCount(80);
            final String sql = "SELECT param_id, avg(value) a, count() c FROM telemetry" +
                    " WHERE param_id IN ('K0','K1','K2','K3','K4','K5','K6','K7') ORDER BY param_id";
            CoveringIndexRecordCursorFactory.resetModeSelectionsForTesting();
            assertSameResult(sql, referenceOf(sql));
            Assert.assertTrue(
                    "1000 rows per EMITTED frame -- " + String.format("%.1f", 1000.0 / CROSSOVER)
                            + "x the crossover -- was rejected. The density"
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
     * keys appear only from partition 50 onwards and then run at 1000 rows per pair, 7.8x the
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
            insertUniformBlock(50, 100, DENSE_ROWS_PER_PAIR);
            assertPartitionCount(150);
            CoveringIndexRecordCursorFactory.resetModeSelectionsForTesting();
            assertSameResult(QUERY, referenceOf(QUERY));
            Assert.assertTrue(
                    "four keys recorded only from partition 50 onwards, at 1000 rows per pair ("
                            + String.format("%.1f", 1000.0 / CROSSOVER)
                            + "x the crossover), fell back to the merge. The density estimate is"
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
     * Both arms are the SAME 100-partition, 160 000-row, 400-rows-per-pair table; the only
     * difference is a single extra leading partition of 8 rows, 0.005% of the data. A
     * four-frame prefix sample scores the two on opposite sides of the crossover, so the
     * decision inverts on a partition holding a twentieth of a percent of the rows.
     * <p>
     * The uniform density has to sit ABOVE the crossover for this to mean anything: if both
     * arms decline on density the assertion below compares false against false and passes
     * having tested nothing. 400 rows per pair is 3.1x the crossover, and the leading partition
     * drags the mean only to 396 -- still above it, which is the whole claim.
     */
    @Test(timeout = 300_000)
    public void testLeadingSparsePartitionDoesNotFlipAUniformTable() throws Exception {
        assertMemoryLeak(() -> {
            createTelemetryTable();
            insertUniformBlock(1, 100, 400);
            assertPartitionCount(100);
            CoveringIndexRecordCursorFactory.resetModeSelectionsForTesting();
            assertSameResult(QUERY, referenceOf(QUERY));
            final boolean perKeyWithoutLeader =
                    CoveringIndexRecordCursorFactory.getPerKeyModeOpensForTesting() > 0;
            Assert.assertTrue(
                    "the gate never ran on the control arm, so the comparison below proves nothing",
                    perKeyWithoutLeader || CoveringIndexRecordCursorFactory.getMergedModeOpensForTesting() > 0
            );
            Assert.assertTrue(
                    "the control arm is a UNIFORM table at 400 rows per pair, "
                            + String.format("%.1f", 400.0 / CROSSOVER) + "x the crossover,"
                            + " and it declined per-key. Both arms of the comparison below now"
                            + " answer false and it passes having tested nothing -- either the"
                            + " crossover moved above this fixture's density or the estimate is"
                            + " under-counting a flat table.",
                    perKeyWithoutLeader
            );

            execute("DROP TABLE telemetry");
            createTelemetryTable();
            // Two rows per key in the leading partition, then the identical uniform block.
            insertUniformBlock(0, 1, 2);
            insertUniformBlock(1, 100, 400);
            assertPartitionCount(101);
            CoveringIndexRecordCursorFactory.resetModeSelectionsForTesting();
            assertSameResult(QUERY, referenceOf(QUERY));
            final boolean perKeyWithLeader =
                    CoveringIndexRecordCursorFactory.getPerKeyModeOpensForTesting() > 0;

            Assert.assertEquals(
                    "adding ONE leading partition of 8 rows -- 0.005% of the table -- flipped the"
                            + " per-key decision on an otherwise uniform 400-rows-per-pair table. The"
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
                    // Formatted, not integer-divided: 1000 / 256 renders as "3" and read "3x the
                    // crossover" in a file whose every javadoc said 3.9x.
                    "at " + DENSE_ROWS_PER_PAIR + " rows per (key, partition) pair -- " +
                            String.format("%.1f", (double) DENSE_ROWS_PER_PAIR / CROSSOVER)
                            + "x the crossover of " + CROSSOVER + " -- the merge was chosen."
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
     * Headroom canary on the shared multi-partition fixture: 200,000 rows over 4 keys and 70
     * daily partitions is ~714 rows per (key, partition) pair. If that fixture ever falls under
     * the crossover, every test built on it stops exercising per-key mode at all while still
     * passing, because merged returns the same rows.
     * <p>
     * This arm has already earned its keep once. The fixture held 10,000 rows -- ~35 per pair,
     * three above the then-crossover of 32 -- and this was the ONLY test that failed when the
     * crossover was re-measured and the constant moved to 256. The two sibling suites over this
     * fixture are {@code CoveringIndexOrderSensitiveTest} and
     * {@code CoveringIndexScanDirectionTest} -- NOT {@code CoveringIndexTelemetryShapeTest},
     * which builds {@code createTelemetryWithNulls()}, a single-partition fixture, and was named
     * here in error. {@code CoveringIndexScanDirectionTest} still asserts only the PLAN, which
     * prints the plan-stable permission and is therefore blind to the flip: it would go green and
     * silently stop covering the mode. {@code CoveringIndexOrderSensitiveTest} no longer is --
     * its three arms over this fixture and over {@code dec_tel} now assert the mode from the
     * counters, which is what this canary was covering for them.
     * <p>
     * Note what this arm is NOT. Its headroom is ~5.6x the crossover, so it trips only on a gross
     * under-count; the tight density tripwire in this file is
     * {@link #testLeadingSparsePartitionDoesNotFlipAUniformTable}, whose control arm sits at
     * 400 against 128 and which fails at a 1.56x under-count. This one exists to keep the shared
     * fixture honest, not to pin the estimate.
     */
    @Test(timeout = 300_000)
    public void testSharedMultiPartitionFixtureKeepsPerKey() throws Exception {
        assertMemoryLeak(() -> {
            createTelemetryMultiPartition();
            CoveringIndexRecordCursorFactory.resetModeSelectionsForTesting();
            assertSameResult(QUERY, referenceOf(QUERY));
            Assert.assertTrue(
                    "the shared 70-partition fixture (~714 rows per (key, partition) pair) fell back"
                            + " to the merge. It sits " + String.format("%.1f", 714.0 / CROSSOVER)
                            + "x above the " + CROSSOVER + "-row crossover, so"
                            + " either the constant moved or the density estimate is under-counting."
                            + " Every sibling suite over this fixture asserts the plan only and would"
                            + " not have noticed: they would stay green and stop covering per-key mode.",
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
                            + " " + CROSSOVER + "-row crossover, per-key mode was chosen. Per-key pays"
                            + " a fixed 3.9-5.3 us per frame and emits one frame per pair, so at this density"
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
