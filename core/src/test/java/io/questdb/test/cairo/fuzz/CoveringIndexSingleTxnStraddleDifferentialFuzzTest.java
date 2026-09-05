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
 * Differential equivalence proof for the SINGLE-TRANSACTION covering fast path.
 * <p>
 * {@link CoveringIndexFastPathDifferentialFuzzTest} is the sibling of this test
 * and covers block-apply. It cannot cover this path: it drains only once per
 * round, so several WAL transactions are pending per drain and
 * {@code calculateInsertTransactionBlock} forms a BLOCK. Verified empirically --
 * with the single-txn call site instrumented, that suite reaches it ZERO times,
 * so running it against a single-txn change is vacuous.
 * <p>
 * This test drains after EVERY transaction, so every apply takes the
 * {@code processWalCommit} single-transaction route, and partitions by HOUR with
 * batch spans that routinely exceed an hour, so transactions STRADDLE a partition
 * boundary -- the shape whose prefix the fast path must split off and whose
 * overflow must still go through O3.
 * <p>
 * The same precomputed stream is replayed into two identically-shaped tables, one
 * with {@code COVERING_FASTPATH_DISABLED = true} (everything through O3 +
 * rebuildSidecars) and one with it active, then the two are asserted identical.
 * The counters additionally prove the two modes genuinely diverged, so a green
 * run cannot mean "the fast path never fired".
 */
public class CoveringIndexSingleTxnStraddleDifferentialFuzzTest extends AbstractFuzzTest {

    @Before
    public void enableCoveringCounters() {
        PostingIndexWriter.COVERING_COUNTERS_ENABLED = true;
        resetCounters();
    }

    @After
    public void disableCoveringCountersAndFastPath() {
        PostingIndexWriter.COVERING_COUNTERS_ENABLED = false;
        PostingIndexWriter.COVERING_FASTPATH_DISABLED = false;
    }

    @Test
    public void testSingleTxnStraddleDifferentialFuzz() throws Exception {
        runDifferential(generateRandom(LOG));
    }

    @Test
    public void testSingleTxnStraddleDifferentialFuzzRegression() throws Exception {
        runDifferential(generateRandom(LOG, 0x51e3c7a90b4d26L, 0x1fa60c8b3d5e97L));
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
            // Drain after EVERY op: one pending WAL transaction per drain, so the
            // apply job forms no block and takes the single-transaction route.
            drainWalQueue();
        }
    }

    private void assertTablesIdentical(int symbolCardinality) throws Exception {
        assertSqlCursors(
                "SELECT ts, sym, value FROM o3 ORDER BY ts, sym, value",
                "SELECT ts, sym, value FROM fast ORDER BY ts, sym, value"
        );
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
        assertSqlCursors(
                "SELECT sym, sum(value), count(value), count(*), min(value), max(value) FROM o3 ORDER BY sym",
                "SELECT sym, sum(value), count(value), count(*), min(value), max(value) FROM fast ORDER BY sym"
        );
    }

    private List<Op> precomputeStream(Rnd rnd, int symbolCardinality) {
        final List<Op> ops = new ArrayList<>();
        long tsCursor = 1_700_000_000_000_000L;
        long valueCursor = 0;
        final int rounds = 60 + rnd.nextInt(40); // 60..99 transactions
        for (int round = 0; round < rounds; round++) {
            final int rows = 200 + rnd.nextInt(2000);
            // Span is rows*step; with HOUR partitions (3.6e9 us) a step in this
            // range puts most batches across at least one boundary, which is the
            // case under test. Some land inside one partition, which exercises
            // the whole-block prefix branch.
            final long step = 1 + rnd.nextInt(4_000_000);
            final int nullMod = 3 + rnd.nextInt(20);
            // Occasional backward dip -> late data, disqualifies the fast path and
            // must fall through to O3 unchanged.
            final boolean dip = rnd.nextInt(100) < 12;
            final long backOff = dip ? (long) (1 + rnd.nextInt(20)) * step * rows : 0;
            final long startTs = dip ? tsCursor - backOff : tsCursor;
            ops.add(new Op(false, startTs, valueCursor, rows, step, nullMod));
            valueCursor += rows;
            final long batchMaxTs = startTs + (long) rows * step;
            if (batchMaxTs > tsCursor) {
                tsCursor = batchMaxTs;
            }
            if (rnd.nextInt(100) < 5) {
                ops.add(new Op(true, 0, 0, 0, 0, 0));
                tsCursor += 2L * 24 * 60 * 60 * 1_000_000L;
            }
        }
        return ops;
    }

    private void resetCounters() {
        PostingIndexWriter.COVERING_FASTLAG_COMMIT_COUNT.set(0);
        PostingIndexWriter.COVERING_FULL_RESEAL_COUNT.set(0);
        PostingIndexWriter.COVERING_AUTOSEAL_COUNT.set(0);
        PostingIndexWriter.COVERING_BLOCK_FASTPATH_COUNT.set(0);
    }

    private void runDifferential(Rnd rnd) throws Exception {
        setProperty(PropertyKey.CAIRO_WAL_SEGMENT_ROLLOVER_ROW_COUNT, 10_000_000);
        setProperty(PropertyKey.CAIRO_SQL_SORT_KEY_MAX_BYTES, 134_217_728);
        setProperty(PropertyKey.CAIRO_SQL_SORT_LIGHT_VALUE_MAX_BYTES, 134_217_728);

        final int symbolCardinality = 3 + rnd.nextInt(14); // 3..16
        final List<Op> ops = precomputeStream(rnd, symbolCardinality);

        assertMemoryLeak(() -> {
            execute("CREATE TABLE o3 (ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING INCLUDE (value), value DOUBLE)"
                    + " TIMESTAMP(ts) PARTITION BY HOUR WAL");
            execute("CREATE TABLE fast (ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING INCLUDE (value), value DOUBLE)"
                    + " TIMESTAMP(ts) PARTITION BY HOUR WAL");
            drainWalQueue();

            // Replay 1: fast path FORCED OFF.
            PostingIndexWriter.COVERING_FASTPATH_DISABLED = true;
            resetCounters();
            applyStream("o3", ops, symbolCardinality);
            final long o3FastPath = PostingIndexWriter.COVERING_BLOCK_FASTPATH_COUNT.get();
            final long o3Reseals = PostingIndexWriter.COVERING_FULL_RESEAL_COUNT.get();

            // Replay 2: fast path ACTIVE.
            PostingIndexWriter.COVERING_FASTPATH_DISABLED = false;
            resetCounters();
            applyStream("fast", ops, symbolCardinality);
            final long fastFastPath = PostingIndexWriter.COVERING_BLOCK_FASTPATH_COUNT.get();
            final long fastReseals = PostingIndexWriter.COVERING_FULL_RESEAL_COUNT.get();

            Assert.assertFalse("o3 suspended", engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("o3")));
            Assert.assertFalse("fast suspended", engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("fast")));

            // THE proof: identical results regardless of path.
            assertTablesIdentical(symbolCardinality);

            // Non-vacuity: the fast path must actually have fired, and only when
            // enabled. Every apply here is single-transaction (drained one at a
            // time), so any increment is this change's call site, not block-apply.
            Assert.assertEquals("fast path fired while disabled", 0, o3FastPath);
            Assert.assertTrue("single-txn fast path never fired -- test is vacuous", fastFastPath > 0);
            Assert.assertTrue("fast run must avoid reseals the o3 run paid"
                            + " (fastReseals=" + fastReseals + ", o3Reseals=" + o3Reseals + ")",
                    fastReseals < o3Reseals);
        });
    }

    // Encodes one precomputed operation replayed identically into both tables.
    private static final class Op {
        final int nullMod;
        final int rows;
        final long startTs;
        final long step;
        final boolean truncate;
        final long v0;

        Op(boolean truncate, long startTs, long v0, int rows, long step, int nullMod) {
            this.truncate = truncate;
            this.startTs = startTs;
            this.v0 = v0;
            this.rows = rows;
            this.step = step;
            this.nullMod = nullMod;
        }
    }
}
