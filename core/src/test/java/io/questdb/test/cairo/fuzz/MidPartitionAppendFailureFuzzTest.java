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
import io.questdb.cairo.TableToken;
import io.questdb.cairo.idx.PostingIndexWriter;
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
 * Failure-injection fuzz for the mid-partition covered append path. A seeded
 * {@link io.questdb.std.FilesFacade} fails one filesystem op (open / mmap /
 * allocate) on the covering table's apply-path index files - the covered sidecar
 * {@code .pc}, the posting values {@code .pv}, or the partition {@code value.d}
 * - while commits append into an existing, non-last partition. Faults never
 * touch WAL segment files, so the transaction log stays durable and the stream
 * must eventually apply in full.
 * <p>
 * A non-covering control table receives the same stream with no faults. After
 * each fault the writer is reopened and the table resumed until it converges;
 * the invariant is that the covering table ends unsuspended with covered reads
 * equal to the control's base column for every committed row. The append path
 * publishes into a LIVE .pc rather than rotating it, so a fault mid-publish must
 * still leave no partial or garbage covered fragment behind.
 */
public class MidPartitionAppendFailureFuzzTest extends AbstractFuzzTest {

    private static final long DAY_MICROS = 24L * 60 * 60 * 1_000_000L;
    private static final int MODE_ALLOC = 2;
    private static final int MODE_MMAP = 1;
    private static final int MODE_OPEN = 0;
    // Fails the read the append path issues over the symbol column it is
    // indexing, which no other mode can reach.
    private static final int MODE_READ = 3;
    private static final int TARGET_SYM_D = 4;
    private static final long T0 = 1_700_000_000_000_000L / DAY_MICROS * DAY_MICROS;
    // Batches land in day 1; day 3 exists from the start, so day 1 is never last.
    private static final long TARGET_DAY = T0 + DAY_MICROS;
    private final FaultFilesFacade faultFf = new FaultFilesFacade();
    // when set, the table dedups, which disqualifies the fast-lag gate and routes
    // the appends into the LAST partition through O3
    private boolean dedup;

    @Before
    public void enableCoveringCounters() {
        PostingIndexWriter.COVERING_COUNTERS_ENABLED = true;
        resetCoveringCounters();
        faultFf.disarm();
    }

    @After
    public void disableCoveringCounters() {
        dedup = false;
        PostingIndexWriter.COVERING_COUNTERS_ENABLED = false;
        PostingIndexWriter.COVERING_SEAL_APPEND_DISABLED = false;
        faultFf.disarm();
    }

    @Test
    public void testMidPartitionAppendFailureFuzz() throws Exception {
        runFailureFuzz(generateRandom(LOG), true);
    }

    /**
     * The same fault injection against the LAST partition. DEDUP disqualifies the
     * WAL fast-lag gate, so the appends take the O3 route into the last partition
     * - the route whose index is maintained through the writer's LIVE indexer,
     * which the seal closes and reopens. A fault there must still converge.
     */
    @Test
    public void testLastPartitionAppendFailureFuzzDedup() throws Exception {
        dedup = true;
        runFailureFuzz(generateRandom(LOG), true);
    }

    @Test
    public void testLastPartitionAppendFailureFuzzDedupRegression() throws Exception {
        dedup = true;
        runFailureFuzz(generateRandom(LOG, 0x7c05b3e1d9a462L, 0x28fa61c7e0b539L), true);
    }

    @Test
    public void testMidPartitionAppendFailureFuzzNoFaultControl() throws Exception {
        // Harness self-check: with faults disabled the run must converge cleanly.
        runFailureFuzz(generateRandom(LOG), false);
    }

    @Test
    public void testMidPartitionAppendFailureFuzzRegression() throws Exception {
        runFailureFuzz(generateRandom(LOG, 0x18b7d3f0a29e64L, 0x5c02af71e6d938L), true);
    }

    /**
     * Same seed, same faults, but the append path is forced OFF so the run takes
     * the unchanged index-in-O3 + reseal route. Discriminates a genuine
     * crash-safety regression in the append path from a property of the O3
     * mid-partition path (or of this harness) that predates it.
     */
    @Test
    public void testMidPartitionAppendFailureFuzzResealPathControl() throws Exception {
        PostingIndexWriter.COVERING_SEAL_APPEND_DISABLED = true;
        runFailureFuzz(generateRandom(LOG, 0x18b7d3f0a29e64L, 0x5c02af71e6d938L), true);
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
        } catch (Throwable ignore) {
            // A fault surfaces as a failed apply; recovery is asserted below.
        }
    }

    /**
     * Ascending batches confined to the target day, with an occasional dip
     * behind its max (a real O3 merge, which must keep taking the reseal path).
     */
    private List<Op> precomputeStream(Rnd rnd) {
        final List<Op> ops = new ArrayList<>();
        long tsCursor = TARGET_DAY + 1;
        long valueCursor = 0;
        final int rounds = 20 + rnd.nextInt(20);
        for (int round = 0; round < rounds; round++) {
            final int rows = 50 + rnd.nextInt(500);
            final long step = 1 + rnd.nextInt(500);
            final int nullMod = 3 + rnd.nextInt(20);
            final boolean dip = rnd.nextInt(100) < 12;
            final long backOff = dip ? Math.min(tsCursor - TARGET_DAY - 1, (long) (1 + rnd.nextInt(5)) * step * rows) : 0;
            final long startTs = tsCursor - backOff;
            ops.add(new Op(startTs, valueCursor, rows, step, nullMod));
            valueCursor += rows;
            final long batchMaxTs = startTs + (long) rows * step;
            if (batchMaxTs > tsCursor) {
                tsCursor = batchMaxTs;
            }
            Assert.assertTrue("stream left the target partition", tsCursor < TARGET_DAY + DAY_MICROS);
        }
        return ops;
    }

    private void recover(TableToken tt) {
        faultFf.disarm();
        int guard = 0;
        while (guard++ < 200) {
            engine.releaseInactive();
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
        PostingIndexWriter.COVERING_FULL_RESEAL_COUNT.set(0);
        PostingIndexWriter.COVERING_AUTOSEAL_COUNT.set(0);
        PostingIndexWriter.COVERING_SEAL_APPEND_COUNT.set(0);
    }

    private void runFailureFuzz(Rnd rnd, boolean injectFaults) throws Exception {
        setProperty(PropertyKey.CAIRO_WAL_SEGMENT_ROLLOVER_ROW_COUNT, 10_000_000);
        setProperty(PropertyKey.CAIRO_WAL_APPLY_LOOK_AHEAD_TXN_COUNT, 2000);
        setProperty(PropertyKey.CAIRO_WAL_APPLY_TABLE_TIME_QUOTA, 600_000);

        final int symbolCardinality = 3 + rnd.nextInt(14);
        final List<Op> ops = precomputeStream(rnd);
        final boolean[] armRound = new boolean[ops.size()];
        final int[] modeRound = new int[ops.size()];
        final int[] targetRound = new int[ops.size()];
        final int[] skipRound = new int[ops.size()];
        for (int i = 0; i < ops.size(); i++) {
            armRound[i] = injectFaults && rnd.nextInt(100) < 70;
            modeRound[i] = rnd.nextInt(4);
            // 3 = _txn fails the commit AFTER the seal has already published the
            // appended generation - the one window this path made structurally
            // different from the reseal it replaces, since a mid partition has no
            // reopen-time recovery walk to undo the publish.
            targetRound[i] = rnd.nextInt(5); // 0=.pc, 1=.pv, 2=value.d, 3=_txn, 4=sym.d
            // Only sym.d is consumed through ff.read (SymbolColumnIndexer#index);
            // every other target is mmapped, so pairing MODE_READ with them would
            // latch an fd whose read fault can never fire.
            if (modeRound[i] == MODE_READ) {
                targetRound[i] = TARGET_SYM_D;
            }
            skipRound[i] = rnd.nextInt(3);
        }
        int armed = 0;
        for (boolean a : armRound) {
            if (a) {
                armed++;
            }
        }
        final int armedRounds = armed;

        assertMemoryLeak(faultFf, () -> {
            final String dedupClause = dedup ? " DEDUP UPSERT KEYS(ts, sym)" : "";
            execute("CREATE TABLE t (ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING INCLUDE (value), value DOUBLE)"
                    + " TIMESTAMP(ts) PARTITION BY DAY WAL" + dedupClause);
            execute("CREATE TABLE ctl (ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING, value DOUBLE)"
                    + " TIMESTAMP(ts) PARTITION BY DAY WAL" + dedupClause);
            if (!dedup) {
                // Anchor the table max in a LATER day so every batch below appends
                // into a partition that is not the last one. With dedup there is
                // deliberately no anchor: the target day IS the last partition.
                for (String table : new String[]{"t", "ctl"}) {
                    execute("INSERT INTO " + table + " SELECT (" + (T0 + 3 * DAY_MICROS) + ")::TIMESTAMP,"
                            + " 'S0', cast(NULL AS DOUBLE) FROM long_sequence(1)");
                }
            }
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
                recover(tToken);
            }
            recover(tToken);

            Assert.assertFalse("covering table must recover (not suspended)",
                    engine.getTableSequencerAPI().isSuspended(tToken));

            // Localise a divergence before comparing against the control: is the
            // covering table's INDEX short (rows the index scan cannot find) or
            // is its DATA short (rows never applied)?
            //
            // Filtered and per-symbol, because an unfiltered keyed group-by
            // ignores no_index: both sides compile to the same vectorized GroupBy
            // over a full scan (verified by EXPLAIN), so the unfiltered form
            // compared a plan with itself and could never localise anything.
            for (int s = 0; s < symbolCardinality; s++) {
                assertSqlCursors(
                        "SELECT /*+ no_index */ ts, sym, value FROM t WHERE sym = 'S" + s + "' ORDER BY ts",
                        "SELECT ts, sym, value FROM t WHERE sym = 'S" + s + "' ORDER BY ts"
                );
            }
            assertSqlCursors("SELECT count() FROM ctl", "SELECT count() FROM t");

            assertCoveredMatchesControl(symbolCardinality);

            if (!PostingIndexWriter.COVERING_SEAL_APPEND_DISABLED) {
                Assert.assertTrue("the mid-partition append path must be exercised (appends="
                                + PostingIndexWriter.COVERING_SEAL_APPEND_COUNT.get() + ')',
                        PostingIndexWriter.COVERING_SEAL_APPEND_COUNT.get() > 0);
            }

            // Without this the run silently degrades into the no-fault control:
            // arming is only an intent, and every match is by file name, so a
            // rename or a mode that never reaches its callsite would leave the
            // whole crash-safety premise untested while everything above stays
            // green. Require a real fraction of the armed rounds to have fired,
            // not merely one - a single fire would satisfy ">0" while the other
            // modes were all inert (which is exactly how the MODE_READ latch bug
            // hid).
            if (injectFaults) {
                Assert.assertTrue("faults were armed but never fired (armed=" + armedRounds
                                + ", fired=" + faultFf.fired() + ')',
                        faultFf.fired() >= Math.max(1, armedRounds / 4));
            }
        });
    }

    // One precomputed insert batch, replayed into both tables.
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

    // Seeded one-shot fault injector over the covering table's index files.
    // Versioned seal files carry a .{txn} suffix, so match on "contains".
    private static final class FaultFilesFacade extends TestFilesFacadeImpl {
        private final java.util.concurrent.atomic.AtomicInteger firedCount = new java.util.concurrent.atomic.AtomicInteger();
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

        // Arming is not firing. Every arm site below can silently fail to match -
        // a renamed file, a mode that never reaches its callsite, a stale latch -
        // and the run then degrades into the no-fault control while still passing
        // every assertion. Count the fires so that cannot happen unnoticed.
        void fire() {
            firedCount.incrementAndGet();
            disarm();
        }

        int fired() {
            return firedCount.get();
        }

        @Override
        public boolean allocate(long fd, long size) {
            if (armed && mode == MODE_ALLOC && fd == targetFd && targetFd >= 0) {
                fire();
                return false;
            }
            return super.allocate(fd, size);
        }

        @Override
        public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
            if (armed && mode == MODE_MMAP && fd == targetFd && targetFd >= 0) {
                fire();
                return -1;
            }
            return super.mmap(fd, len, offset, flags, memoryTag);
        }

        // The covered append path reads the symbol column through openRO + read
        // (SymbolColumnIndexer#index) rather than the openRW/mmap the reseal
        // used, so faulting only openRW/mmap/allocate would leave the new I/O
        // untested.
        @Override
        public long openRO(LPSZ name) {
            if (armed && matches(name)) {
                if (mode == MODE_OPEN && skip-- <= 0) {
                    fire();
                    return -1;
                }
                if (mode == MODE_READ && targetFd < 0 && skip-- <= 0) {
                    long fd = super.openRO(name);
                    if (fd >= 0) {
                        targetFd = fd; // stays armed until the read fires
                    }
                    return fd;
                }
            }
            return super.openRO(name);
        }

        @Override
        public long openRW(LPSZ name, int opts) {
            if (armed && matches(name)) {
                if (mode == MODE_OPEN) {
                    if (skip-- <= 0) {
                        fire();
                        return -1;
                    }
                } else if ((mode == MODE_MMAP || mode == MODE_ALLOC) && targetFd < 0 && skip-- <= 0) {
                    // Restricted to the mmap/alloc modes. Latching here for
                    // MODE_READ too meant the writer's own openRW of sym.d (which
                    // happens long before the seal's openRO) claimed the latch,
                    // openRO's MODE_READ branch was then skipped, and the read
                    // fault could no longer fire - so the SymbolColumnIndexer
                    // short-read path was armed far more often than it was hit.
                    long fd = super.openRW(name, opts);
                    if (fd >= 0) {
                        targetFd = fd;
                    }
                    return fd;
                }
            }
            return super.openRW(name, opts);
        }

        @Override
        public boolean close(long fd) {
            // Release the latch: descriptor numbers are recycled, and a stale
            // targetFd would fault an unrelated file - possibly a WAL segment
            // that matches() is written to protect.
            if (fd == targetFd) {
                targetFd = -1;
            }
            return super.close(fd);
        }

        @Override
        public long read(long fd, long buf, long len, long offset) {
            if (armed && mode == MODE_READ && fd == targetFd && targetFd >= 0) {
                fire();
                return -1;
            }
            return super.read(fd, buf, len, offset);
        }

        private boolean matches(LPSZ name) {
            if (Utf8s.containsAscii(name, "wal")) {
                return false; // never fault WAL segments
            }
            switch (targetKind) {
                case 0:
                    return Utf8s.containsAscii(name, ".pc");
                case 1:
                    return Utf8s.containsAscii(name, ".pv");
                case 2:
                    return Utf8s.containsAscii(name, "value.d");
                case 3:
                    // The TABLE's _txn only. "_txn" as a substring also matches
                    // the sequencer log (txn_seq/_txnlog, _txnlog.meta.*) and
                    // _txn_scoreboard; faulting those would break the durability
                    // premise this whole test rests on - that the transaction log
                    // survives, so the stream must eventually apply in full.
                    return Utf8s.endsWithAscii(name, "/_txn");
                default:
                    // the symbol column the append path re-reads to index its tail
                    return Utf8s.containsAscii(name, "sym.d");
            }
        }
    }
}
