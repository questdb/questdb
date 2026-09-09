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
import io.questdb.cairo.CairoError;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.idx.PostingIndexWriter;
import io.questdb.std.FilesFacade;
import io.questdb.std.Rnd;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8s;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Test C — crash / failure-injection fuzz over the covering block-apply fast
 * path. A seeded {@link FilesFacade} fails a chosen filesystem op (open / mmap /
 * allocate) on the APPLY-path index files of the covering table — the covered
 * sidecar {@code .pc} (the fast-lag covered publish), the posting values
 * {@code .pv}, or the partition {@code value.d} (the fast-lag base-column
 * append) — after N matching calls, deterministically by seed. Faults never
 * touch WAL segment files, so the transaction log stays durable.
 * <p>
 * A non-covering control table {@code ctl} receives the SAME committed stream
 * with NO faults (the oracle). The covering table {@code t} receives the
 * identical stream while faults are armed per round; after each fault the writer
 * is reopened (running rollbackConditionally) and the suspended table resumed and
 * re-drained until it converges.
 * <p>
 * Invariant after recovery: {@code t} is at a consistent transaction boundary
 * (not suspended) and its COVERED reads equal the base column of the control for
 * every committed row — per symbol ordered by the unique value, covered
 * aggregates, and IS NULL. WAL durability guarantees {@code t} eventually applies
 * the whole stream, so it must converge byte-identical to {@code ctl}: a
 * transient IO fault mid fast-lag must never leave partial/garbage covered
 * fragments, crash a reader, return rows past sealed coverage, or permanently
 * suspend the table.
 * <p>
 * The fast path must actually be exercised: {@code COVERING_FASTLAG_COMMIT_COUNT
 * > 0} over the run. Reproduce a failing seed with the {@code random seeds:} log
 * line. A fault that yields covered != base, a crash on reopen/read, rows past
 * sealed coverage, or an unrecoverable table is a real crash-safety bug — the
 * whole point of this test.
 */
public class CoveringIndexFastPathFailureFuzzTest extends AbstractFuzzTest {

    private static final int MODE_ALLOC = 2;
    private static final int MODE_MMAP = 1;
    private static final int MODE_OPEN = 0;
    private final FaultFilesFacade faultFf = new FaultFilesFacade();

    @Before
    public void enableCoveringCounters() {
        PostingIndexWriter.COVERING_COUNTERS_ENABLED = true;
        resetCoveringCounters();
        faultFf.disarm();
    }

    @After
    public void disableCoveringCounters() {
        PostingIndexWriter.COVERING_COUNTERS_ENABLED = false;
        faultFf.disarm();
    }

    @Test
    public void testFastPathFailureFuzz() throws Exception {
        runFailureFuzz(generateRandom(LOG), true);
    }

    @Test
    public void testFastPathFailureFuzzNoFaultControl() throws Exception {
        // Harness self-check: with faults disabled the run must converge cleanly.
        runFailureFuzz(generateRandom(LOG), false);
    }

    @Test
    public void testFastPathFailureFuzzRegression() throws Exception {
        runFailureFuzz(generateRandom(LOG, 0x7ab3e10c9d2f45L, 0x3e9c7b16a80d52L), true);
    }

    private void applyBatch(String table, Op op, int symbolCardinality) throws Exception {
        final String valueExpr = "(" + op.v0 + " + x)::DOUBLE";
        execute("INSERT INTO " + table
                + " SELECT (" + op.startTs + " + x * " + op.step + ")::TIMESTAMP AS ts,"
                + " 'S' || ((" + op.v0 + " + x) % " + symbolCardinality + ") AS sym,"
                + " CASE WHEN ((" + op.v0 + " + x) % " + op.nullMod + ") = 0 THEN cast(NULL AS DOUBLE) ELSE " + valueExpr + " END AS value"
                + " FROM long_sequence(" + op.rows + ")");
    }

    private void assertCoveredMatchesControl(int symbolCardinality) throws Exception {
        for (int s = 0; s < symbolCardinality; s++) {
            final String sym = "S" + s;
            assertSqlCursors(
                    "SELECT ts, sym, value FROM ctl WHERE sym = '" + sym + "' ORDER BY value",
                    "SELECT ts, sym, value FROM t WHERE sym = '" + sym + "' ORDER BY value"
            );
            assertSqlCursors(
                    "SELECT ts, sym FROM ctl WHERE sym = '" + sym + "' AND value IS NULL ORDER BY ts",
                    "SELECT ts, sym FROM t WHERE sym = '" + sym + "' AND value IS NULL ORDER BY ts"
            );
        }
        assertSqlCursors(
                "SELECT sym, sum(value), count(value), count(*), min(value), max(value) FROM ctl ORDER BY sym",
                "SELECT sym, sum(value), count(value), count(*), min(value), max(value) FROM t ORDER BY sym"
        );
    }

    private void drainQuietly() {
        try {
            drainWalQueue();
        } catch (CairoException | CairoError ignore) {
            // A fault surfaced as an exception rather than a suspend; recovery loop handles it.
        }
    }

    private List<Op> precomputeStream(Rnd rnd, int symbolCardinality) {
        final List<Op> ops = new ArrayList<>();
        long tsCursor = 1_700_000_000_000_000L;
        long valueCursor = 0;
        final int rounds = 25 + rnd.nextInt(25); // 25..49
        for (int round = 0; round < rounds; round++) {
            final int rows = 50 + rnd.nextInt(3000);
            final long step = 1 + rnd.nextInt(500_000);
            final int nullMod = 3 + rnd.nextInt(20);
            final boolean dip = rnd.nextInt(100) < 12;
            final long backOff = dip ? (long) (1 + rnd.nextInt(30)) * step * rows : 0;
            final long startTs = dip ? tsCursor - backOff : tsCursor;
            ops.add(new Op(startTs, valueCursor, rows, step, nullMod));
            valueCursor += rows;
            final long batchMaxTs = startTs + (long) rows * step;
            if (batchMaxTs > tsCursor) {
                tsCursor = batchMaxTs;
            }
        }
        return ops;
    }

    private void recover(TableToken tt) {
        faultFf.disarm();
        int guard = 0;
        while (guard++ < 200) {
            engine.releaseInactive(); // reopen writers -> rollbackConditionally evicts orphan fragments
            if (engine.getTableSequencerAPI().isSuspended(tt)) {
                try {
                    execute("ALTER TABLE " + tt.getTableName() + " RESUME WAL");
                } catch (Exception ignore) {
                    // fall through and retry the drain
                }
            }
            drainQuietly();
            if (!engine.getTableSequencerAPI().isSuspended(tt)) {
                return;
            }
        }
    }

    private void resetCoveringCounters() {
        PostingIndexWriter.COVERING_FASTLAG_COMMIT_COUNT.set(0);
        PostingIndexWriter.COVERING_FULL_RESEAL_COUNT.set(0);
        PostingIndexWriter.COVERING_AUTOSEAL_COUNT.set(0);
        PostingIndexWriter.COVERING_MAX_GENCOUNT_OBSERVED.set(0);
        PostingIndexWriter.COVERING_MAX_SEGCOUNT_OBSERVED.set(0);
    }

    private void runFailureFuzz(Rnd rnd, boolean injectFaults) throws Exception {
        setProperty(PropertyKey.CAIRO_WAL_SEGMENT_ROLLOVER_ROW_COUNT, 10_000_000);
        setProperty(PropertyKey.CAIRO_WAL_APPLY_LOOK_AHEAD_TXN_COUNT, 2000);
        setProperty(PropertyKey.CAIRO_WAL_APPLY_TABLE_TIME_QUOTA, 600_000);

        final int symbolCardinality = 3 + rnd.nextInt(14);
        final List<Op> ops = precomputeStream(rnd, symbolCardinality);
        // Pre-roll fault parameters per round from the seed (varying fault points).
        final boolean[] armRound = new boolean[ops.size()];
        final int[] modeRound = new int[ops.size()];
        final int[] targetRound = new int[ops.size()];
        final int[] skipRound = new int[ops.size()];
        for (int i = 0; i < ops.size(); i++) {
            armRound[i] = injectFaults && rnd.nextInt(100) < 70;
            modeRound[i] = rnd.nextInt(3);
            targetRound[i] = rnd.nextInt(3); // 0=.pc, 1=.pv, 2=value.d
            skipRound[i] = rnd.nextInt(3);   // fail after this many matching ops
        }

        assertMemoryLeak(faultFf, () -> {
            execute("CREATE TABLE t (ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING INCLUDE (value), value DOUBLE)"
                    + " TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE ctl (ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING, value DOUBLE)"
                    + " TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();
            resetCoveringCounters();

            final TableToken tToken = engine.verifyTableName("t");

            // Phase 1: clean oracle.
            for (int i = 0; i < ops.size(); i++) {
                applyBatch("ctl", ops.get(i), symbolCardinality);
            }
            drainWalQueue();

            // Phase 2: covering table under fault injection + recovery.
            for (int i = 0; i < ops.size(); i++) {
                applyBatch("t", ops.get(i), symbolCardinality);
                if (armRound[i]) {
                    faultFf.arm(modeRound[i], targetRound[i], skipRound[i]);
                }
                drainQuietly();
                recover(tToken); // disarms, reopens (rollbackConditionally), resumes, re-drains
            }
            // Final convergence drain.
            recover(tToken);

            Assert.assertFalse("covering table must recover (not suspended)",
                    engine.getTableSequencerAPI().isSuspended(tToken));

            // THE invariant: covered reads == base column of the control, for every committed row.
            assertCoveredMatchesControl(symbolCardinality);

            if (injectFaults) {
                Assert.assertTrue("fast path must be exercised over the run (fastLag="
                                + PostingIndexWriter.COVERING_FASTLAG_COMMIT_COUNT.get() + ")",
                        PostingIndexWriter.COVERING_FASTLAG_COMMIT_COUNT.get() > 0);
            }
        });
    }

    // Encodes one precomputed insert batch, replayed into both tables.
    private static final class Op {
        final int nullMod;
        final int rows;
        final long startTs;
        final long step;
        final long v0;

        Op(long startTs, long v0, int rows, long step, int nullMod) {
            this.startTs = startTs;
            this.v0 = v0;
            this.rows = rows;
            this.step = step;
            this.nullMod = nullMod;
        }
    }

    // Seeded fault injector: fails one chosen fs op on an apply-path index file of
    // the covering table after {@code skip} matching calls, then disarms (one-shot).
    // Never targets WAL segment files (paths containing "wal").
    private static final class FaultFilesFacade extends TestFilesFacadeImpl {
        private volatile boolean armed = false;
        private volatile int mode;
        private volatile int skip;
        private volatile long targetFd = -1;
        private volatile int targetKind;

        void arm(int mode, int targetKind, int skip) {
            this.mode = mode;
            this.targetKind = targetKind;
            this.skip = skip;
            this.targetFd = -1;
            this.armed = true;
        }

        void disarm() {
            this.armed = false;
            this.targetFd = -1;
        }

        @Override
        public boolean allocate(long fd, long size) {
            if (armed && mode == MODE_ALLOC && fd == targetFd && targetFd >= 0) {
                disarm();
                return false;
            }
            return super.allocate(fd, size);
        }

        @Override
        public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
            if (armed && mode == MODE_MMAP && fd == targetFd && targetFd >= 0) {
                disarm();
                return -1;
            }
            return super.mmap(fd, len, offset, flags, memoryTag);
        }

        @Override
        public long openRW(LPSZ name, int opts) {
            if (armed && matches(name)) {
                if (mode == MODE_OPEN) {
                    if (skip-- <= 0) {
                        disarm();
                        return -1;
                    }
                } else if (targetFd < 0 && skip-- <= 0) {
                    long fd = super.openRW(name, opts);
                    if (fd >= 0) {
                        targetFd = fd; // arm the fd-based (mmap/alloc) fault; stays armed until it fires
                    }
                    return fd;
                }
            }
            return super.openRW(name, opts);
        }

        private boolean matches(LPSZ name) {
            if (Utf8s.containsAscii(name, "wal")) {
                return false; // never fault WAL segments
            }
            switch (targetKind) {
                case 0:
                    return Utf8s.endsWithAscii(name, ".pc");
                case 1:
                    return Utf8s.endsWithAscii(name, ".pv");
                default:
                    return Utf8s.endsWithAscii(name, "value.d");
            }
        }
    }
}
