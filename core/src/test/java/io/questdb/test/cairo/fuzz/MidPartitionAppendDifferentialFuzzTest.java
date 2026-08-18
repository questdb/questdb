/*+*****************************************************************************
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

package io.questdb.test.cairo.fuzz;

import io.questdb.PropertyKey;
import io.questdb.cairo.idx.PostingIndexWriter;
import io.questdb.std.Rnd;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Differential equivalence proof for the mid-partition covered append path: a
 * pure O3 append into an existing, non-last partition now indexes only the
 * appended range and publishes an incremental covered fragment
 * (TableWriter#sealPostingIndexForPartition) instead of resealing the whole
 * partition.
 * <p>
 * A single deterministic stream is precomputed per seed and replayed twice into
 * two identically-defined covering-indexed tables:
 * <ul>
 *   <li>{@code reseal} - {@code COVERING_MIDPART_APPEND_DISABLED = true}: O3
 *       indexes the rows and the seal full-reseals, as before;</li>
 *   <li>{@code append} - the flag off: the covered append path is active.</li>
 * </ul>
 * Both must produce identical base content and identical covered reads. The run
 * also asserts the two modes genuinely differed (the append run takes the append
 * path and reseals strictly less), so a silently-disabled path cannot pass.
 * <p>
 * The stream writes into SEVERAL days at once with a day always present after
 * the one being written, which is what makes the writes mid-partition appends;
 * it also mixes in out-of-order dips (real O3 merges) and truncates so the two
 * paths are compared across the transitions between them, not just in steady
 * state.
 */
public class MidPartitionAppendDifferentialFuzzTest extends AbstractFuzzTest {

    private static final long DAY_MICROS = 24L * 60 * 60 * 1_000_000L;
    // Day 0 is the anchor: every other day is written into while day (DAYS-1)
    // already holds rows, so those writes land in a non-last partition.
    private static final int DAYS = 4;
    private static final long T0 = 1_700_000_000_000_000L / DAY_MICROS * DAY_MICROS;

    @Before
    public void enableCoveringCounters() {
        PostingIndexWriter.COVERING_COUNTERS_ENABLED = true;
        resetCoveringCounters();
    }

    @After
    public void disableCoveringCountersAndAppendPath() {
        PostingIndexWriter.COVERING_COUNTERS_ENABLED = false;
        PostingIndexWriter.COVERING_MIDPART_APPEND_DISABLED = false;
    }

    @Test
    public void testMidPartitionAppendDifferentialFuzz() throws Exception {
        runDifferential(generateRandom(LOG), false);
    }

    @Test
    public void testMidPartitionAppendDifferentialFuzzRegression() throws Exception {
        runDifferential(generateRandom(LOG, 0x51e3d7a9c4b206L, 0x1f8c62b0da3945L), false);
    }

    /**
     * Var-size covered columns (STRING and VARCHAR). Every other test here covers
     * only fixed-width DOUBLE, but the append path publishes an INCREMENTAL
     * covered fragment: it hands the aux (index) addresses and mapped sizes to
     * writeSidecarGenData and appends a slice, where the reseal it replaces
     * rewrote the sidecar wholesale. The offset arithmetic for the incremental
     * case is therefore genuinely different code, and it was the largest piece of
     * new write logic with no coverage. Both var-size layouts are included
     * because STRING and VARCHAR do not share an aux representation.
     */
    @Test
    public void testMidPartitionAppendDifferentialFuzzVarSize() throws Exception {
        runDifferential(generateRandom(LOG), true);
    }

    @Test
    public void testMidPartitionAppendDifferentialFuzzVarSizeRegression() throws Exception {
        runDifferential(generateRandom(LOG, 0x2c9b41f7e05a83L, 0x6d13ea82c4f507L), true);
    }

    private void applyStream(String table, List<Op> ops, int symbolCardinality, boolean varSize) throws Exception {
        for (int i = 0, n = ops.size(); i < n; i++) {
            Op op = ops.get(i);
            if (op.truncate) {
                execute("TRUNCATE TABLE " + table);
            } else {
                final String valueExpr = "(" + op.v0 + " + x)::DOUBLE";
                // Lengths must VARY (and include NULL and empty), or every aux
                // offset is uniform and an incremental-offset error still lines up.
                // Three length classes plus the digit-count drift of the id, so
                // consecutive rows differ in width; the empty-string class shares
                // an offset with its neighbour, which is where an off-by-one in
                // the incremental aux write shows up.
                final String pad = "CASE WHEN ((" + op.v0 + " + x) % 3) = 0 THEN 'aaaaaaaaaaaaaaaaaaaa'"
                        + " WHEN ((" + op.v0 + " + x) % 3) = 1 THEN 'bb' ELSE '' END";
                final String varCols = varSize
                        ? ", CASE WHEN ((" + op.v0 + " + x) % " + op.nullMod + ") = 1 THEN cast(NULL AS STRING)"
                          + " ELSE (" + pad + ") || (" + op.v0 + " + x)::STRING END AS s"
                          + ", CASE WHEN ((" + op.v0 + " + x) % " + op.nullMod + ") = 2 THEN cast(NULL AS VARCHAR)"
                          + " ELSE (('\u00e9' || (" + pad + ")) || (" + op.v0 + " + x)::STRING)::VARCHAR END AS v"
                        : "";
                execute("INSERT INTO " + table
                        + " SELECT (" + op.startTs + " + x * " + op.step + ")::TIMESTAMP AS ts,"
                        + " 'S' || ((" + op.v0 + " + x) % " + symbolCardinality + ") AS sym,"
                        + " CASE WHEN ((" + op.v0 + " + x) % " + op.nullMod + ") = 0 THEN cast(NULL AS DOUBLE) ELSE " + valueExpr + " END AS value"
                        + varCols
                        + " FROM long_sequence(" + op.rows + ")");
            }
            if (op.drainAfter) {
                drainWalQueue();
            }
        }
    }

    private void assertTablesIdentical(int symbolCardinality, boolean varSize) throws Exception {
        final String cols = varSize ? "ts, sym, value, s, v" : "ts, sym, value";
        assertSqlCursors(
                "SELECT " + cols + " FROM reseal ORDER BY ts, sym, value",
                "SELECT " + cols + " FROM append ORDER BY ts, sym, value"
        );
        for (int s = 0; s < symbolCardinality; s++) {
            final String sym = "S" + s;
            // Covered reads through the covering index, per symbol.
            assertSqlCursors(
                    "SELECT " + cols + " FROM reseal WHERE sym = '" + sym + "' ORDER BY value",
                    "SELECT " + cols + " FROM append WHERE sym = '" + sym + "' ORDER BY value"
            );
            assertSqlCursors(
                    "SELECT ts, sym FROM reseal WHERE sym = '" + sym + "' AND value IS NULL ORDER BY ts",
                    "SELECT ts, sym FROM append WHERE sym = '" + sym + "' AND value IS NULL ORDER BY ts"
            );
        }
        assertSqlCursors(
                "SELECT sym, sum(value), count(value), count(*), min(value), max(value) FROM reseal ORDER BY sym",
                "SELECT sym, sum(value), count(value), count(*), min(value), max(value) FROM append ORDER BY sym"
        );
    }

    /**
     * Per-day append cursors: each op appends after the chosen day's current
     * max, so writes into days 0..DAYS-2 are pure appends into a partition that
     * is not the last one. 15% of ops dip behind the day's max instead, which
     * is a genuine O3 merge and must take the unchanged reseal path.
     */
    private List<Op> precomputeStream(Rnd rnd, int symbolCardinality) {
        final List<Op> ops = new ArrayList<>();
        final long[] dayCursor = new long[DAYS];
        for (int d = 0; d < DAYS; d++) {
            dayCursor[d] = T0 + d * DAY_MICROS;
        }
        long valueCursor = 0;

        // Seed every day (ascending) so all DAYS partitions exist before the
        // measured ops: without the later day present, a write into day d would
        // be a last-partition append, which is a different path.
        for (int d = 0; d < DAYS; d++) {
            final int rows = 200 + rnd.nextInt(800);
            ops.add(new Op(false, dayCursor[d], valueCursor, rows, 1 + rnd.nextInt(50), 3 + rnd.nextInt(20), d == DAYS - 1));
            valueCursor += rows;
            dayCursor[d] += (long) rows * 50 + 1;
        }

        final int rounds = 30 + rnd.nextInt(30);
        for (int round = 0; round < rounds; round++) {
            final int txnsInRound = 1 + rnd.nextInt(5);
            for (int t = 0; t < txnsInRound; t++) {
                final int day = rnd.nextInt(DAYS);
                final int rows = 20 + rnd.nextInt(500);
                final long step = 1 + rnd.nextInt(200);
                final int nullMod = 3 + rnd.nextInt(20);
                final boolean dip = rnd.nextInt(100) < 15;
                final long dayStart = T0 + (long) day * DAY_MICROS;
                long startTs = dayCursor[day];
                if (dip) {
                    // Land behind the day's max (but inside the day) -> O3 merge.
                    final long back = 1 + rnd.nextInt(1 + (int) Math.min(Integer.MAX_VALUE, startTs - dayStart));
                    startTs = Math.max(dayStart, startTs - back);
                }
                final boolean drainAfter = t == txnsInRound - 1;
                ops.add(new Op(false, startTs, valueCursor, rows, step, nullMod, drainAfter));
                valueCursor += rows;
                // Keep the whole batch inside its day so the op cannot spill into
                // the next partition (which would create/extend a later day and
                // change which partition is last).
                final long batchMax = startTs + (long) rows * step;
                final long dayEnd = dayStart + DAY_MICROS - 1;
                dayCursor[day] = Math.max(dayCursor[day], Math.min(batchMax + 1, dayEnd));
            }
            if (rnd.nextInt(100) < 4) {
                ops.add(new Op(true, 0, 0, 0, 0, 0, true));
                // After a truncate the days must be re-seeded, else subsequent
                // writes recreate partitions in arbitrary order.
                for (int d = 0; d < DAYS; d++) {
                    dayCursor[d] = T0 + (long) d * DAY_MICROS;
                    final int rows = 100 + rnd.nextInt(300);
                    ops.add(new Op(false, dayCursor[d], valueCursor, rows, 40, 7, d == DAYS - 1));
                    valueCursor += rows;
                    dayCursor[d] += (long) rows * 40 + 1;
                }
            }
        }
        return ops;
    }

    private void resetCoveringCounters() {
        PostingIndexWriter.COVERING_FULL_RESEAL_COUNT.set(0);
        PostingIndexWriter.COVERING_FASTLAG_COMMIT_COUNT.set(0);
        PostingIndexWriter.COVERING_AUTOSEAL_COUNT.set(0);
        PostingIndexWriter.COVERING_MIDPART_APPEND_COUNT.set(0);
    }

    private void runDifferential(Rnd rnd, boolean varSize) throws Exception {
        setProperty(PropertyKey.CAIRO_WAL_SEGMENT_ROLLOVER_ROW_COUNT, 10_000_000);
        setProperty(PropertyKey.CAIRO_SQL_SORT_KEY_MAX_BYTES, 134_217_728);
        setProperty(PropertyKey.CAIRO_SQL_SORT_LIGHT_VALUE_MAX_BYTES, 134_217_728);

        final int symbolCardinality = 3 + rnd.nextInt(14);
        final List<Op> ops = precomputeStream(rnd, symbolCardinality);

        assertMemoryLeak(() -> {
            final String schema = varSize
                    ? " (ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING INCLUDE (value, s, v),"
                      + " value DOUBLE, s STRING, v VARCHAR)"
                    : " (ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING INCLUDE (value), value DOUBLE)";
            execute("CREATE TABLE reseal" + schema + " TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE append" + schema + " TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();

            // Replay 1: append path FORCED OFF -> index in O3 + full reseal.
            PostingIndexWriter.COVERING_MIDPART_APPEND_DISABLED = true;
            resetCoveringCounters();
            applyStream("reseal", ops, symbolCardinality, varSize);
            final long resealRuns = PostingIndexWriter.COVERING_FULL_RESEAL_COUNT.get();
            final long resealAppends = PostingIndexWriter.COVERING_MIDPART_APPEND_COUNT.get();

            // Replay 2: append path ACTIVE.
            PostingIndexWriter.COVERING_MIDPART_APPEND_DISABLED = false;
            resetCoveringCounters();
            applyStream("append", ops, symbolCardinality, varSize);
            final long appendRuns = PostingIndexWriter.COVERING_FULL_RESEAL_COUNT.get();
            final long appendAppends = PostingIndexWriter.COVERING_MIDPART_APPEND_COUNT.get();

            Assert.assertFalse("reseal suspended", engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("reseal")));
            Assert.assertFalse("append suspended", engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("append")));

            // THE proof: identical results regardless of path.
            assertTablesIdentical(symbolCardinality, varSize);

            // ... and the two runs really did take different paths.
            Assert.assertEquals("forced-off run must never take the append path", 0, resealAppends);
            Assert.assertTrue("append run must take the append path, appends=" + appendAppends, appendAppends > 0);
            Assert.assertTrue("forced-off run must full-reseal, reseals=" + resealRuns, resealRuns > 0);
            Assert.assertTrue("append run must reseal less than the forced-off run"
                            + " (append=" + appendRuns + ", reseal=" + resealRuns + ')',
                    appendRuns < resealRuns);
        });
    }

    // One replayed operation. Absolute startTs / v0 make the two replays a pure
    // function of this list.
    private static final class Op {
        final boolean drainAfter;
        final int nullMod;
        final int rows;
        final long startTs;
        final long step;
        final boolean truncate;
        final long v0;

        Op(boolean truncate, long startTs, long v0, int rows, long step, int nullMod, boolean drainAfter) {
            this.truncate = truncate;
            this.startTs = startTs;
            this.v0 = v0;
            this.rows = rows;
            this.step = step;
            this.nullMod = nullMod;
            this.drainAfter = drainAfter;
        }
    }
}
