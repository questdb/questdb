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
 * Test B — the differential equivalence proof for the covering block-apply fast
 * path. The single most important covering-index test: it proves the fast path
 * (tryFastAppendInOrderBlock / tryFastAppendSortedBlock) produces results
 * BYTE-IDENTICAL to the unchanged O3 + rebuildSidecars path.
 * <p>
 * For each seed a single deterministic, ascending-biased stream (same shape as
 * Test A, so the fast path fires — the vanilla FuzzTransactionGenerator does not
 * reach it, see CoveringIndexBlockApplyFuzzTest) is precomputed once, then
 * replayed twice into two covering-indexed tables:
 * <ul>
 *   <li>{@code o3}  — replayed with {@code COVERING_FASTPATH_DISABLED = true}, so
 *       every block goes through O3 + rebuildSidecars (fastLag == 0, reseals &gt; 0);</li>
 *   <li>{@code fast} — replayed with {@code COVERING_FASTPATH_DISABLED = false}, so
 *       the fast path is active (fastLag &gt; 0).</li>
 * </ul>
 * Both receive byte-identical input (the SAME precomputed batches, not a
 * regenerated stream). We then assert the two tables are byte-identical: full
 * base content (SELECT *, ordered) and covered reads (per-symbol ordered by the
 * unique value, covered aggregates, IS NULL) match exactly, AND that the two
 * modes genuinely differed (fast: fastLag &gt; 0; o3: fastLag == 0 &amp;&amp;
 * reseals &gt; 0).
 * <p>
 * A divergence for ANY seed means the fast path is not result-equivalent to O3
 * — a serious bug and a do-not-merge signal. Reproduce a failing seed with the
 * {@code random seeds:} log line via {@code generateRandom(LOG, s0, s1)}.
 */
public class CoveringIndexFastPathDifferentialFuzzTest extends AbstractFuzzTest {

    // Encodes one precomputed operation replayed identically into both tables.
    // truncate ops reset the table; insert ops carry absolute, already-evolved
    // startTs / v0 so the two replays are a pure function of this list.
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

    @Before
    public void enableCoveringCounters() {
        PostingIndexWriter.COVERING_COUNTERS_ENABLED = true;
        resetCoveringCounters();
    }

    @After
    public void disableCoveringCountersAndFastPath() {
        PostingIndexWriter.COVERING_COUNTERS_ENABLED = false;
        PostingIndexWriter.COVERING_FASTPATH_DISABLED = false;
    }

    @Test
    public void testFastPathDifferentialFuzz() throws Exception {
        runDifferential(generateRandom(LOG));
    }

    @Test
    public void testFastPathDifferentialFuzzRegression() throws Exception {
        runDifferential(generateRandom(LOG, 0x2c7a1f9b3e5d84L, 0x6b0d9e2a4c1f37L));
    }

    private void applyStream(String table, List<Op> ops, int symbolCardinality) throws Exception {
        for (int i = 0, n = ops.size(); i < n; i++) {
            Op op = ops.get(i);
            if (op.truncate) {
                execute("TRUNCATE TABLE " + table);
            } else {
                final String valueExpr = "(" + op.v0 + " + x)::DOUBLE";
                execute("INSERT INTO " + table
                        + " SELECT (" + op.startTs + " + x * " + op.step + ")::TIMESTAMP AS ts,"
                        + " 'S' || ((" + op.v0 + " + x) % " + symbolCardinality + ") AS sym,"
                        + " CASE WHEN ((" + op.v0 + " + x) % " + op.nullMod + ") = 0 THEN cast(NULL AS DOUBLE) ELSE " + valueExpr + " END AS value"
                        + " FROM long_sequence(" + op.rows + ")");
            }
            if (op.drainAfter) {
                drainWalQueue();
            }
        }
    }

    private void assertTablesIdentical(int symbolCardinality) throws Exception {
        // Full base-table content.
        assertSqlCursors(
                "SELECT ts, sym, value FROM o3 ORDER BY ts, sym, value",
                "SELECT ts, sym, value FROM fast ORDER BY ts, sym, value"
        );
        // Covered reads through the covering index, per symbol.
        for (int s = 0; s < symbolCardinality; s++) {
            final String sym = "S" + s;
            assertSqlCursors(
                    "SELECT ts, sym, value FROM o3 WHERE sym = '" + sym + "' ORDER BY value",
                    "SELECT ts, sym, value FROM fast WHERE sym = '" + sym + "' ORDER BY value"
            );
            assertSqlCursors(
                    "SELECT ts, sym FROM o3 WHERE sym = '" + sym + "' AND value IS NULL ORDER BY ts",
                    "SELECT ts, sym FROM fast WHERE sym = '" + sym + "' AND value IS NULL ORDER BY ts"
            );
        }
        // Covered aggregates.
        assertSqlCursors(
                "SELECT sym, sum(value), count(value), count(*), min(value), max(value) FROM o3 ORDER BY sym",
                "SELECT sym, sum(value), count(value), count(*), min(value), max(value) FROM fast ORDER BY sym"
        );
    }

    private List<Op> precomputeStream(Rnd rnd, int symbolCardinality) {
        final List<Op> ops = new ArrayList<>();
        long tsCursor = 1_700_000_000_000_000L;
        long valueCursor = 0;
        final int rounds = 40 + rnd.nextInt(40); // 40..79
        for (int round = 0; round < rounds; round++) {
            final int txnsInRound = 2 + rnd.nextInt(7); // 2..8
            for (int t = 0; t < txnsInRound; t++) {
                final int rows = 50 + rnd.nextInt(4000);
                final long step = 1 + rnd.nextInt(1_000_000);
                final int nullMod = 3 + rnd.nextInt(20);
                final boolean dip = rnd.nextInt(100) < 15;
                final long backOff = dip ? (long) (1 + rnd.nextInt(50)) * step * rows : 0;
                final long startTs = dip ? tsCursor - backOff : tsCursor;
                final boolean drainAfter = t == txnsInRound - 1;
                ops.add(new Op(false, startTs, valueCursor, rows, step, nullMod, drainAfter));
                valueCursor += rows;
                final long batchMaxTs = startTs + (long) rows * step;
                if (batchMaxTs > tsCursor) {
                    tsCursor = batchMaxTs;
                }
            }
            if (rnd.nextInt(100) < 6) {
                ops.add(new Op(true, 0, 0, 0, 0, 0, true));
                tsCursor += 2L * 24 * 60 * 60 * 1_000_000L;
            }
        }
        return ops;
    }

    private void resetCoveringCounters() {
        PostingIndexWriter.COVERING_FASTLAG_COMMIT_COUNT.set(0);
        PostingIndexWriter.COVERING_FULL_RESEAL_COUNT.set(0);
        PostingIndexWriter.COVERING_AUTOSEAL_COUNT.set(0);
        PostingIndexWriter.COVERING_MAX_GENCOUNT_OBSERVED.set(0);
        PostingIndexWriter.COVERING_MAX_SEGCOUNT_OBSERVED.set(0);
    }

    private void runDifferential(Rnd rnd) throws Exception {
        setProperty(PropertyKey.CAIRO_WAL_SEGMENT_ROLLOVER_ROW_COUNT, 10_000_000);
        setProperty(PropertyKey.CAIRO_WAL_APPLY_LOOK_AHEAD_TXN_COUNT, 2000);
        setProperty(PropertyKey.CAIRO_WAL_APPLY_TABLE_TIME_QUOTA, 600_000);
        // assertTablesIdentical() sorts the whole table (ORDER BY ts, sym, value) through
        // EncodedSort, which charges 32 bytes per row against these two caps combined. The
        // corpus can retain up to 79 rounds x 8 txns x 4049 rows = ~2.56M rows when truncates
        // land early or never, or ~82MB of sort entries - the test-default combined cap of
        // ~22.4MB overflows at ~699k rows. 128MB each (256MB combined) covers the worst case
        // three times over; the sort commits memory as rows arrive, so typical runs stay small.
        setProperty(PropertyKey.CAIRO_SQL_SORT_KEY_MAX_BYTES, 134_217_728);
        setProperty(PropertyKey.CAIRO_SQL_SORT_LIGHT_VALUE_MAX_BYTES, 134_217_728);

        final int symbolCardinality = 3 + rnd.nextInt(14); // 3..16
        final List<Op> ops = precomputeStream(rnd, symbolCardinality);

        assertMemoryLeak(() -> {
            // Both covering-indexed, identical schema.
            execute("CREATE TABLE o3 (ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING INCLUDE (value), value DOUBLE)"
                    + " TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE fast (ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING INCLUDE (value), value DOUBLE)"
                    + " TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();

            // Replay 1: fast path FORCED OFF -> pure O3 + rebuildSidecars.
            PostingIndexWriter.COVERING_FASTPATH_DISABLED = true;
            resetCoveringCounters();
            applyStream("o3", ops, symbolCardinality);
            final long o3FastLag = PostingIndexWriter.COVERING_FASTLAG_COMMIT_COUNT.get();
            final long o3Reseals = PostingIndexWriter.COVERING_FULL_RESEAL_COUNT.get();

            // Replay 2: fast path ACTIVE.
            PostingIndexWriter.COVERING_FASTPATH_DISABLED = false;
            resetCoveringCounters();
            applyStream("fast", ops, symbolCardinality);
            final long fastFastLag = PostingIndexWriter.COVERING_FASTLAG_COMMIT_COUNT.get();
            final long fastReseals = PostingIndexWriter.COVERING_FULL_RESEAL_COUNT.get();

            Assert.assertFalse("o3 suspended", engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("o3")));
            Assert.assertFalse("fast suspended", engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("fast")));

            // THE proof: identical results regardless of path.
            assertTablesIdentical(symbolCardinality);

            // Prove the two modes actually differed on the BLOCK-apply path.
            // (COVERING_FASTPATH_DISABLED only forces BLOCK applies through O3; the
            // pre-existing single-txn fast-lag path is unaffected, so o3FastLag is
            // not necessarily 0. The block-path difference shows up as: the o3 run
            // full-reseals its blocks, while the fast run fast-lags them instead --
            // strictly MORE fast-lag commits and strictly FEWER full reseals.)
            Assert.assertTrue("o3 run must full-reseal blocks (fast path forced off), o3Reseals=" + o3Reseals,
                    o3Reseals > 0);
            Assert.assertTrue("fast run must fast-lag block-applies the o3 run resealed"
                            + " (fastFastLag=" + fastFastLag + ", o3FastLag=" + o3FastLag + ")",
                    fastFastLag > o3FastLag);
            Assert.assertTrue("fast run must avoid reseals via the fast path"
                            + " (fastReseals=" + fastReseals + ", o3Reseals=" + o3Reseals + ")",
                    fastReseals < o3Reseals);
        });
    }
}
