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

/**
 * Fuzz Test A for the covering-index WAL block-apply fast path.
 * <p>
 * Reachability note: the generic {@link io.questdb.test.fuzz.FuzzTransactionGenerator}
 * back-dates every transaction ({@code startTs = lastTimestamp - writeInterval}),
 * which makes {@code getCommitToTimestamp} return {@code FORCE_FULL_COMMIT} for
 * almost every txn, so the applier never batches into a multi-txn block
 * ({@code processWalCommitBlock} is never reached) and the covering fast-lag path
 * is not exercised (measured: {@code fastLag=0}, {@code maxSeg=0}, everything on
 * the single-txn O3 reseal path). Table-level {@code DEDUP} is likewise a
 * dead-end for this path: {@code isCommitPlainInsert} excludes dedup-mode commits
 * by design. So this test drives an ascending-biased random stream that actually
 * reaches the fast path while keeping randomised chaos: variable batch sizes and
 * per-batch txn counts (backlog depth -&gt; block size), variable symbol
 * cardinality, NULL-dense covered values, occasional intra-batch out-of-order
 * "dips" (sorted fast path / O3 merge), and periodic TRUNCATE resets, across
 * DAY partition boundaries. Dedup / replace-range / covered-column DDL bypass the
 * fast path by gate design and are deferred to Tests B/C/D.
 * <p>
 * Every batch is applied by identical deterministic SQL to two tables that differ
 * only in the {@code sym} index: {@code blk} carries a covering POSTING index
 * INCLUDE(value) (covered reads serve {@code value} from the sidecar); {@code ctl}
 * carries a plain POSTING index (the ORACLE, serving {@code value} from the base
 * column). After each round we assert per-symbol covered reads (ordered by the
 * unique value, plus covered aggregates and IS NULL) match the control, so any
 * covered-read divergence in the fast path fails the seed. The fast path MUST
 * fire: {@code COVERING_COUNTERS_ENABLED} is set in {@code @Before} and each run
 * asserts {@code COVERING_FASTLAG_COMMIT_COUNT > 0}. Reproduce a failing seed with
 * the {@code random seeds:} log line via {@code generateRandom(LOG, s0, s1)}.
 */
public class CoveringIndexBlockApplyFuzzTest extends AbstractFuzzTest {

    private long tsCursor;
    private long valueCursor;

    @Before
    public void enableCoveringCounters() {
        PostingIndexWriter.COVERING_COUNTERS_ENABLED = true;
        resetCoveringCounters();
    }

    @After
    public void disableCoveringCounters() {
        PostingIndexWriter.COVERING_COUNTERS_ENABLED = false;
    }

    @Test
    public void testCoveringBlockApplyFuzz() throws Exception {
        runCoveringBlockApplyFuzz(generateRandom(LOG));
    }

    @Test
    public void testCoveringBlockApplyFuzzRegression() throws Exception {
        runCoveringBlockApplyFuzz(generateRandom(LOG, 0x51e6d0f2a3b4c5L, 0x9f8e7d6c5b4a39L));
    }

    private void assertCoveredMatchesControl(int symbolCardinality) throws Exception {
        for (int s = 0; s < symbolCardinality; s++) {
            final String sym = "S" + s;
            assertSqlCursors(
                    "SELECT ts, sym, value FROM ctl WHERE sym = '" + sym + "' ORDER BY value",
                    "SELECT ts, sym, value FROM blk WHERE sym = '" + sym + "' ORDER BY value"
            );
            assertSqlCursors(
                    "SELECT ts, sym FROM ctl WHERE sym = '" + sym + "' AND value IS NULL ORDER BY ts",
                    "SELECT ts, sym FROM blk WHERE sym = '" + sym + "' AND value IS NULL ORDER BY ts"
            );
        }
        assertSqlCursors(
                "SELECT sym, sum(value), count(value), count(*), min(value), max(value) FROM ctl ORDER BY sym",
                "SELECT sym, sum(value), count(value), count(*), min(value), max(value) FROM blk ORDER BY sym"
        );
    }

    // One INSERT batch, applied to BOTH tables by identical deterministic SQL
    // (no rnd_* functions), so blk and ctl stay byte-identical. Rows ascend from
    // tsCursor by stepMicros; when {@code dip} the batch starts backOffMicros
    // before tsCursor to force an out-of-order / merge shape. NULLs are placed
    // deterministically by nullMod. Values are globally unique (valueCursor).
    private void insertBatch(int rows, long stepMicros, int symbolCardinality, int nullMod, boolean dip, long backOffMicros) throws Exception {
        final long startTs = dip ? tsCursor - backOffMicros : tsCursor;
        final long v0 = valueCursor;
        final String valueExpr = "(" + v0 + " + x)::DOUBLE";
        final String select = " SELECT (" + startTs + " + x * " + stepMicros + ")::TIMESTAMP AS ts,"
                + " 'S' || ((" + v0 + " + x) % " + symbolCardinality + ") AS sym,"
                + " CASE WHEN ((" + v0 + " + x) % " + nullMod + ") = 0 THEN cast(NULL AS DOUBLE) ELSE " + valueExpr + " END AS value"
                + " FROM long_sequence(" + rows + ")";
        execute("INSERT INTO blk" + select);
        execute("INSERT INTO ctl" + select);
        valueCursor += rows;
        final long batchMaxTs = startTs + (long) rows * stepMicros;
        if (batchMaxTs > tsCursor) {
            tsCursor = batchMaxTs;
        }
    }

    private void resetCoveringCounters() {
        PostingIndexWriter.COVERING_FASTLAG_COMMIT_COUNT.set(0);
        PostingIndexWriter.COVERING_FULL_RESEAL_COUNT.set(0);
        PostingIndexWriter.COVERING_AUTOSEAL_COUNT.set(0);
        PostingIndexWriter.COVERING_MAX_GENCOUNT_OBSERVED.set(0);
        PostingIndexWriter.COVERING_MAX_SEGCOUNT_OBSERVED.set(0);
    }

    private void runCoveringBlockApplyFuzz(Rnd rnd) throws Exception {
        // Keep many small txns inside one WAL segment and let the applier look
        // far ahead so a backlog batches into few, big blocks (block apply).
        setProperty(PropertyKey.CAIRO_WAL_SEGMENT_ROLLOVER_ROW_COUNT, 10_000_000);
        setProperty(PropertyKey.CAIRO_WAL_APPLY_LOOK_AHEAD_TXN_COUNT, 2000);
        setProperty(PropertyKey.CAIRO_WAL_APPLY_TABLE_TIME_QUOTA, 600_000);

        final int symbolCardinality = 3 + rnd.nextInt(14);   // 3..16
        final int rounds = 40 + rnd.nextInt(40);             // 40..79

        assertMemoryLeak(() -> {
            execute("CREATE TABLE blk (ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING INCLUDE (value), value DOUBLE)"
                    + " TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE ctl (ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING, value DOUBLE)"
                    + " TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();
            resetCoveringCounters();

            tsCursor = 1_700_000_000_000_000L; // 2023-11-14T…, micros
            valueCursor = 0;

            for (int round = 0; round < rounds; round++) {
                // Backlog depth: several insert txns before a single drain so the
                // applier batches them into a multi-txn block.
                final int txnsInRound = 2 + rnd.nextInt(7); // 2..8
                for (int t = 0; t < txnsInRound; t++) {
                    final int rows = 50 + rnd.nextInt(4000);
                    final long step = 1 + rnd.nextInt(1_000_000); // sub-second..~1s spacing
                    final int nullMod = 3 + rnd.nextInt(20);      // ~5%..33% NULLs
                    // ~15% of batches dip out-of-order to exercise the sorted
                    // fast path / O3 merge; the rest are pure appends.
                    final boolean dip = rnd.nextInt(100) < 15;
                    final long backOff = dip ? (long) (1 + rnd.nextInt(50)) * step * rows : 0;
                    insertBatch(rows, step, symbolCardinality, nullMod, dip, backOff);
                }
                drainWalQueue();

                // Occasional TRUNCATE reset (both tables), then keep appending.
                if (rnd.nextInt(100) < 6) {
                    execute("TRUNCATE TABLE blk");
                    execute("TRUNCATE TABLE ctl");
                    drainWalQueue();
                    tsCursor += 2L * 24 * 60 * 60 * 1_000_000L; // jump a couple of days after a reset
                }
            }

            Assert.assertFalse("blk suspended",
                    engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("blk")));
            Assert.assertFalse("ctl suspended",
                    engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("ctl")));

            assertCoveredMatchesControl(symbolCardinality);

            Assert.assertTrue("block-apply fast path must fire (COVERING_FASTLAG_COMMIT_COUNT="
                            + PostingIndexWriter.COVERING_FASTLAG_COMMIT_COUNT.get()
                            + ", fullReseals=" + PostingIndexWriter.COVERING_FULL_RESEAL_COUNT.get()
                            + ", maxSeg=" + PostingIndexWriter.COVERING_MAX_SEGCOUNT_OBSERVED.get() + ")",
                    PostingIndexWriter.COVERING_FASTLAG_COMMIT_COUNT.get() > 0);
        });
    }
}
