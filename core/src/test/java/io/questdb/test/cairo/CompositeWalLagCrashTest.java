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

package io.questdb.test.cairo;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.wal.ApplyWal2TableJob;
import io.questdb.griffin.SqlException;
import io.questdb.mp.Job;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Composite-partitioning Plan #5, Task 4 -- CRASH / POWER-LOSS suite for the flag-gated cell-aware WAL lag.
 * <p>
 * A composite lag that loses / tears / duplicates a row on a crash is worse than no lag, so every window in
 * which a flush touches disk must recover to a consistent state -- identical to a plain twin fed the same
 * stream -- after a simulated power loss. This suite injects a {@link FilesFacade} fault at a precise flush
 * point, drives the WAL apply job into the failure, then reopens the writer (discarding the RAM lag buffer,
 * exactly as a crash / writer eviction would) and replays the WAL, asserting the recovered composite table
 * equals a plain twin (row count AND full ordered scan AND per-cell / LATEST ON / SAMPLE BY shapes) and is
 * not left permanently suspended.
 * <p>
 * <b>The crash-safety invariant under test.</b> The composite lag buffer is RAM-only, and {@code seqTxn} is
 * advanced / persisted to {@code _txn} only AFTER a flush's {@code commit00} durably lands. So any crash
 * before a flush's {@code commit00} loses the RAM buffer with {@code seqTxn} un-advanced, the WAL replays
 * the un-acked transactions, and the result equals the twin. There are two flush sites, both exercised here:
 * <ul>
 *     <li><b>Threshold flush</b> inside {@code processWalCommit} ({@code flushCompositeLag}) -- crash points
 *     A, C, D below.</li>
 *     <li><b>Drain-interrupt flush</b> inside {@code TableWriter.commitSeqTxn()} (the Plan #5 Task 3 Critical
 *     fix) -- crash point B; this is the fix's crash-safety re-review.</li>
 * </ul>
 * The happy path of the interrupt flush (flush completes, writer released, reopen loses nothing) is already
 * covered by {@link CompositeWalLagInterruptTest}. This suite adds the crash-DURING-flush windows.
 * <p>
 * <b>Crash points.</b>
 * <ul>
 *     <li><b>A -- mid-flush, threshold path</b> ({@link #testCrashDuringThresholdFlushReplaysEqualsTwin}):
 *     a cell column ({@code &lt;day&gt;/&lt;cell&gt;/px.d}) memory-map fails while {@code flushCompositeLag}'s
 *     {@code processO3Block} is writing it, before {@code commit00}. The cell segment is torn (its data
 *     never became live because {@code _txn} was not advanced); restart replays the un-acked transactions.</li>
 *     <li><b>B -- mid-flush, INTERRUPT path</b> ({@link #testCrashDuringInterruptFlushReplaysEqualsTwin}):
 *     the drain is interrupted ({@code isTerminating}) with the buffer non-empty, routing into
 *     {@code commitSeqTxn()}; a cell map then fails inside that flush, before it can fold {@code seqTxn} and
 *     {@code commit00}. Because the fix flushes FIRST and only advances the persisted {@code seqTxn} after
 *     {@code commit00}, the crash leaves {@code seqTxn} un-advanced and the transactions replay.</li>
 *     <li><b>C -- after {@code processO3Block}, before {@code _txn}</b>
 *     ({@link #testCrashAfterCellsWrittenBeforeTxnReplaysEqualsTwin}): every cell is fully written and synced
 *     and the flush reaches {@code commit00}, but a symbol-map {@code msync} in {@code commit00}'s
 *     {@code syncColumns} step -- which runs after every cell is written and before {@code txWriter.commit}
 *     bumps the {@code _txn} version -- fails. {@code _txn} is never rewritten, so {@code seqTxn} stays
 *     un-advanced; the fully-written-but-uncommitted per-cell bytes (in new version-suffixed dirs) are
 *     dropped and replayed with no double-apply.</li>
 *     <li><b>D -- crash on a cell MERGE ({@code O3_BLOCK_MERGE})</b>
 *     ({@link #testCrashDuringMergeIntoPopulatedCellReplaysEqualsTwin}): every cell is pre-populated with a
 *     committed row so the flush EXTENDS populated cells ({@code srcDataMax &gt; 0}); the fault then lands on
 *     the cell-merge path (a new version dir is written from the old one), not a fresh-cell append.</li>
 * </ul>
 * All four assert the recovered composite equals the plain twin, empirically confirming the T3 report's
 * claim that the composite flush's crash window is identical to plain's full-commit window (O3 writes each
 * cell into a NEW version-suffixed directory and the previous version stays live until {@code _txn} swaps
 * it, so a crash before {@code _txn} cannot tear committed data).
 */
public class CompositeWalLagCrashTest extends AbstractCairoTest {

    private static final String[] EXCH = {"A", "B", "C"};
    // 12 rows/txn, 8 txns: two 12-row transactions (24) overflow getWalMaxLagRows (=20 under
    // maxUncommittedRows=1), so the apply job processes each transaction one at a time (block==1) -- the
    // path the cell-aware lag lives on -- and the lag flushes after ~2 transactions.
    private static final int ROWS_PER_TXN = 12;
    private static final int TOTAL_ROWS = 96;
    private static final int TXN_COUNT = TOTAL_ROWS / ROWS_PER_TXN;
    // A single seed row (distinct, late timestamp) drained before the accumulating stream makes the
    // composite table non-dormant (a fresh composite table's very first commit full-commits, bypassing the
    // lag), so the stream that follows actually accumulates into the RAM buffer.
    private static final String SEED_ROW = "('2020-01-02T12:00:00.000000Z','A',-1.0)";

    @Override
    public void setUp() {
        // Config must be in place BEFORE super.setUp() rebuilds the engine's configuration.
        setProperty(PropertyKey.CAIRO_WAL_COMPOSITE_LAG_ENABLED, "true");
        // getWalMaxLagRows = walLagRowsMultiplier(20) * maxUncommittedRows(1) = 20: a single 12-row
        // out-of-order transaction stays sub-threshold and ACCUMULATES; two overflow it and flush.
        setProperty(PropertyKey.CAIRO_MAX_UNCOMMITTED_ROWS, "1");
        // Neutralise the commit-latency force-commit clause so transactions accumulate on a slow CI box.
        setProperty(PropertyKey.CAIRO_COMMIT_LATENCY, "600000000");
        // Power-loss fidelity: SYNC mode makes column / _cv / _txn durability call ff.msync, so crash
        // point C can fail the _cv msync (the last durable step before the _txn write). Under the default
        // NOSYNC there is no durability syscall to intercept at that point.
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "sync");
        super.setUp();
    }

    /**
     * Crash point A -- mid-flush on the THRESHOLD path. A cell column map fails inside
     * {@code flushCompositeLag}'s {@code processO3Block}, before {@code commit00}. Restart replays the
     * un-acked transactions; the recovered composite equals the plain twin, no torn committed segment.
     */
    @Test
    public void testCrashDuringThresholdFlushReplaysEqualsTwin() throws Exception {
        final AtomicBoolean cellArmed = new AtomicBoolean(false);
        final AtomicBoolean faultHit = new AtomicBoolean(false);
        final AtomicLong targetFd = new AtomicLong(-1);

        final FilesFacade ff = cellMapFaultFacade(cellArmed, faultHit, targetFd);

        assertMemoryLeak(ff, () -> {
            Assert.assertTrue(configuration.isWalCompositeLagEnabled());
            createCompositeAndPlain();
            seedBothNonDormant();

            ingestShuffledTransactions("c");
            cellArmed.set(true);                 // arm only for the composite flush (post-seed)
            drainWalQueue();                     // the threshold flush's cell map faults -> apply aborts
            cellArmed.set(false);

            recoverAndAssertEqualsTwin(faultHit);
        });
    }

    /**
     * Crash point B -- mid-flush on the drain-INTERRUPT path (the Plan #5 Task 3 Critical fix's re-review).
     * The apply loop is interrupted with the RAM buffer non-empty, routing into {@code commitSeqTxn()}; a
     * cell map then fails inside that flush, before it can fold {@code seqTxn} and call {@code commit00}.
     * Because the fix flushes FIRST and advances the persisted {@code seqTxn} only after {@code commit00}
     * durably lands, the crash leaves {@code seqTxn} un-advanced and the transactions replay == twin.
     */
    @Test
    public void testCrashDuringInterruptFlushReplaysEqualsTwin() throws Exception {
        final AtomicBoolean isTerminating = new AtomicBoolean(false);
        final AtomicBoolean interruptArmed = new AtomicBoolean(false);
        final AtomicBoolean cellArmed = new AtomicBoolean(false);
        final AtomicBoolean faultHit = new AtomicBoolean(false);
        final AtomicLong targetFd = new AtomicLong(-1);

        final Job.WorkerContext runStatus = new Job.WorkerContext() {
            @Override
            public int carrierId() {
                return 0;
            }

            @Override
            public boolean isTerminating() {
                return isTerminating.get();
            }
        };

        final FilesFacade ff = new TestFilesFacadeImpl() {
            // Trip isTerminating the moment a WAL segment data file is read while armed -- after the first
            // stream transaction has accumulated into the RAM buffer but before any clean forced flush -- so
            // the apply loop breaks into commitSeqTxn() with the buffer non-empty. Arm the cell fault at the
            // same instant so it can only fire inside that interrupt flush, never on an earlier threshold flush.
            @Override
            public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
                if (cellArmed.get() && fd == targetFd.get()) {
                    cellArmed.set(false);
                    faultHit.set(true);
                    return -1;
                }
                return super.mmap(fd, len, offset, flags, memoryTag);
            }

            @Override
            public long openRO(LPSZ name) {
                if (interruptArmed.get() && Utf8s.containsAscii(name, "wal") && Utf8s.endsWithAscii(name, "px.d")) {
                    isTerminating.set(true);
                    interruptArmed.set(false);
                    cellArmed.set(true);
                }
                return super.openRO(name);
            }

            @Override
            public long openRW(LPSZ name, int opts) {
                long fd = super.openRW(name, opts);
                if (cellArmed.get() && isCompositeCellData(name)) {
                    targetFd.compareAndSet(-1, fd);
                }
                return fd;
            }
        };

        assertMemoryLeak(ff, () -> {
            Assert.assertTrue(configuration.isWalCompositeLagEnabled());
            createCompositeAndPlain();
            seedBothNonDormant();

            ingestShuffledTransactions("c");
            interruptArmed.set(true);
            try (ApplyWal2TableJob walApplyJob = createWalApplyJob()) {
                // Accumulates the first stream transaction, trips isTerminating, breaks into commitSeqTxn()
                // with the buffer non-empty, and the cell map faults inside that interrupt flush -> the apply
                // job suspends the table (the crash-during-fix window).
                walApplyJob.run(runStatus);
            }
            isTerminating.set(false);
            interruptArmed.set(false);
            cellArmed.set(false);

            recoverAndAssertEqualsTwin(faultHit);
        });
    }

    /**
     * Crash point C -- after {@code processO3Block}, before {@code _txn}. Every cell is fully written and
     * synced, and the flush reaches {@code commit00}, but a durable {@code msync} of the {@code exch} symbol
     * map ({@code exch.o}) in {@code commit00}'s {@code syncColumns} step -- which runs AFTER
     * {@code processO3Block} has written every cell and strictly BEFORE {@code txWriter.commit} bumps the
     * {@code _txn} version -- fails. {@code _txn} is therefore never rewritten and {@code seqTxn} stays
     * un-advanced, so the fully-written-but-uncommitted per-cell bytes (in new version-suffixed dirs) are
     * dropped and the transactions replayed, with no double-apply.
     * <p>
     * (A composite flush produces no {@code _cv} change -- columns exist from creation and per-cell
     * versioning lives in {@code _txn} -- so {@code columnVersionWriter.commit()} early-returns without an
     * msync; the symbol map sync is the reliable durable step in {@code commit00} before the {@code _txn}
     * write. Requires {@code commitMode=sync} so the sync is a real syscall.)
     */
    @Test
    public void testCrashAfterCellsWrittenBeforeTxnReplaysEqualsTwin() throws Exception {
        final AtomicBoolean armed = new AtomicBoolean(false);
        final AtomicBoolean faultHit = new AtomicBoolean(false);
        final AtomicLong symFd = new AtomicLong(-1);
        final AtomicLong symAddr = new AtomicLong(-1);

        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public boolean close(long fd) {
                if (fd == symFd.get()) {
                    symFd.set(-1);
                    symAddr.set(-1);
                }
                return super.close(fd);
            }

            @Override
            public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
                long addr = super.mmap(fd, len, offset, flags, memoryTag);
                if (addr != -1 && fd == symFd.get()) {
                    symAddr.set(addr);
                }
                return addr;
            }

            @Override
            public long mremap(long fd, long addr, long previousSize, long newSize, long offset, int mode, int memoryTag) {
                // Keep the tracked address current if the symbol map remaps (grows) via mremap, not mmap.
                long newAddr = super.mremap(fd, addr, previousSize, newSize, offset, mode, memoryTag);
                if (newAddr != -1 && fd == symFd.get()) {
                    symAddr.set(newAddr);
                }
                return newAddr;
            }

            @Override
            public void msync(long addr, long len, boolean async) {
                if (armed.get() && symAddr.get() != -1 && addr == symAddr.get()) {
                    armed.set(false);
                    faultHit.set(true);
                    throw CairoException.critical(5).put("simulated power loss: symbol msync failed before _txn write");
                }
                super.msync(addr, len, async);
            }

            @Override
            public long openRW(LPSZ name, int opts) {
                long fd = super.openRW(name, opts);
                if (Utf8s.endsWithAscii(name, "exch.o")) {
                    symFd.set(fd);
                }
                return fd;
            }
        };

        assertMemoryLeak(ff, () -> {
            Assert.assertTrue(configuration.isWalCompositeLagEnabled());
            createCompositeAndPlain();
            seedBothNonDormant();

            ingestShuffledTransactions("c");
            armed.set(true);
            drainWalQueue();                     // flush writes+syncs all cells, then the symbol msync faults
            armed.set(false);

            recoverAndAssertEqualsTwin(faultHit);
        });
    }

    /**
     * Crash point D -- crash on a cell MERGE ({@code O3_BLOCK_MERGE}, {@code srcDataMax &gt; 0}). Every cell is
     * pre-populated with a committed row so the flush EXTENDS populated cells rather than appending fresh
     * ones; the fault then lands on the cell-merge path (writing a new version dir derived from the old one),
     * exercising the historically corruption-prone {@code srcDataMax &gt; 0} bookkeeping under a crash.
     */
    @Test
    public void testCrashDuringMergeIntoPopulatedCellReplaysEqualsTwin() throws Exception {
        final AtomicBoolean cellArmed = new AtomicBoolean(false);
        final AtomicBoolean faultHit = new AtomicBoolean(false);
        final AtomicLong targetFd = new AtomicLong(-1);

        final FilesFacade ff = cellMapFaultFacade(cellArmed, faultHit, targetFd);

        assertMemoryLeak(ff, () -> {
            Assert.assertTrue(configuration.isWalCompositeLagEnabled());
            createCompositeAndPlain();
            // Pre-populate EVERY (day, exch) cell so that every flush of the accumulating stream merges into
            // an already-populated cell (srcDataMax > 0), guaranteeing the crash lands on O3_BLOCK_MERGE.
            prePopulateAllCells();

            ingestShuffledTransactions("c");
            cellArmed.set(true);
            drainWalQueue();                     // the merge flush's cell map faults
            cellArmed.set(false);

            recoverAndAssertEqualsTwin(faultHit);
        });
    }

    // ------------------------------------------------------------------------------------------------------

    private static boolean isCompositeCellData(LPSZ name) {
        // A composite cell data file lives under <tableRoot>/<date>/<cellSegment>.<nameTxn>/px.d -- it ends
        // with px.d, sits under a 2020-01-0* date directory, and (unlike the WAL segment copy) is not under
        // a wal directory. Matching px.d alone (not ts.d / the symbol column) targets a single data column.
        return Utf8s.endsWithAscii(name, "px.d")
                && Utf8s.containsAscii(name, "2020-01-0")
                && !Utf8s.containsAscii(name, "wal");
    }

    /**
     * A one-shot FilesFacade that arms on the first composite cell {@code px.d} {@code openRW} and fails its
     * memory-map, tearing that cell's write inside {@code processO3Block}, before {@code commit00}.
     */
    private FilesFacade cellMapFaultFacade(AtomicBoolean cellArmed, AtomicBoolean faultHit, AtomicLong targetFd) {
        return new TestFilesFacadeImpl() {
            @Override
            public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
                if (cellArmed.get() && fd == targetFd.get()) {
                    cellArmed.set(false);
                    faultHit.set(true);
                    return -1;
                }
                return super.mmap(fd, len, offset, flags, memoryTag);
            }

            @Override
            public long openRW(LPSZ name, int opts) {
                long fd = super.openRW(name, opts);
                if (cellArmed.get() && isCompositeCellData(name)) {
                    targetFd.compareAndSet(-1, fd);
                }
                return fd;
            }
        };
    }

    private void assertShapesMatch(String ref, String actual) throws SqlException {
        assertSqlCursors(
                "select ts, exch, px from " + ref + " order by ts, exch, px",
                "select ts, exch, px from " + actual + " order by ts, exch, px");
        assertSqlCursors("select count() from " + ref, "select count() from " + actual);
        for (String exch : EXCH) {
            String pred = " where exch = '" + exch + "' order by ts, px";
            assertSqlCursors(
                    "select ts, px from " + ref + pred,
                    "select ts, px from " + actual + pred);
        }
        assertSqlCursors(
                "select ts, exch, px from " + ref + " latest on ts partition by exch order by exch",
                "select ts, exch, px from " + actual + " latest on ts partition by exch order by exch");
        assertSqlCursors(
                "select ts, count(), sum(px) from " + ref + " sample by 1d",
                "select ts, count(), sum(px) from " + actual + " sample by 1d");
    }

    private void assertWalTableNotSuspended(String tableName) {
        Assert.assertFalse(
                tableName + " must not be suspended after recovery",
                engine.getTableSequencerAPI().isSuspended(engine.verifyTableName(tableName)));
    }

    private void createCompositeAndPlain() throws SqlException {
        execute("create table c (ts timestamp, exch symbol, px double) timestamp(ts) partition by day, exch wal");
        execute("create table p (ts timestamp, exch symbol, px double) timestamp(ts) partition by day wal");
    }

    private void ingestShuffledTransactions(String table) throws SqlException {
        // Deterministic permutation of 0..TOTAL_ROWS-1 (gcd(17,96)=1) grouped into TXN_COUNT transactions of
        // ROWS_PER_TXN rows each -- each transaction carries a spread of timestamps while later transactions
        // backfill earlier ones, so commit-to-timestamp stays below each transaction's max lag timestamp and
        // the transaction accumulates (and later flushes merge into cells earlier flushes already wrote).
        for (int t = 0; t < TXN_COUNT; t++) {
            StringBuilder sb = new StringBuilder("insert into ").append(table).append(" values ");
            for (int i = 0; i < ROWS_PER_TXN; i++) {
                int k = ((t * ROWS_PER_TXN + i) * 17) % TOTAL_ROWS;
                if (i > 0) {
                    sb.append(", ");
                }
                sb.append(rowValues(k));
            }
            execute(sb.toString());
        }
    }

    /**
     * Populate every (day, exch) cell with one committed row, on both tables, drained cleanly. The seed
     * timestamps (hour 23) never collide with the stream (hours 0..1), so the twin comparison stays exact.
     */
    private void prePopulateAllCells() throws SqlException {
        StringBuilder c = new StringBuilder("insert into c values ");
        StringBuilder p = new StringBuilder("insert into p values ");
        boolean first = true;
        for (int d = 1; d <= 2; d++) {
            for (int e = 0; e < EXCH.length; e++) {
                String row = "('2020-01-0" + d + "T23:0" + e + ":00.000000Z','" + EXCH[e] + "'," + (-100 - d * 10 - e) + ".0)";
                if (!first) {
                    c.append(", ");
                    p.append(", ");
                }
                c.append(row);
                p.append(row);
                first = false;
            }
        }
        execute(c.toString());
        execute(p.toString());
        drainWalQueue();
        engine.releaseInactive();
        assertWalTableNotSuspended("c");
        assertWalTableNotSuspended("p");
    }

    /**
     * Reopen the writer (discarding the RAM lag buffer, exactly as a crash / writer eviction would), resume
     * the table if the aborted flush suspended it, replay the WAL, then feed a plain twin the identical
     * stream via a clean drain and assert the recovered composite equals it across every query shape.
     */
    private void recoverAndAssertEqualsTwin(AtomicBoolean faultHit) throws Exception {
        // The injection MUST have fired -- otherwise a clean drain would trivially "match" the twin and the
        // test would be a false pass (it would not actually exercise a crash window).
        Assert.assertTrue("fault injection must have fired at the flush point", faultHit.get());

        // Cold restart: drop the writer and its RAM buffer, so recovery is purely from what is durable.
        engine.releaseInactive();

        TableToken ct = engine.verifyTableName("c");
        if (engine.getTableSequencerAPI().isSuspended(ct)) {
            execute("alter table c resume wal");
        }
        drainWalQueue();
        assertWalTableNotSuspended("c");

        // Plain twin fed the identical stream via an uninterrupted drain.
        ingestShuffledTransactions("p");
        drainWalQueue();
        assertWalTableNotSuspended("p");

        assertShapesMatch("p", "c");
    }

    // Row k (0..TOTAL_ROWS-1): day = 2020-01-0(1+k%2), exch = EXCH[k%3], ts minute-of-hour = k (distinct
    // within any cell), px = k+0.5 (globally unique -> no ordering ties).
    private String rowValues(int k) {
        String day = "2020-01-0" + (1 + k % 2);
        String exch = EXCH[k % 3];
        int hour = k / 60;
        int minute = k % 60;
        String hh = (hour < 10 ? "0" : "") + hour;
        String mm = (minute < 10 ? "0" : "") + minute;
        return "('" + day + 'T' + hh + ':' + mm + ":00.000000Z','" + exch + "'," + (k + 0.5) + ')';
    }

    private void seedBothNonDormant() throws SqlException {
        execute("insert into c values " + SEED_ROW);
        execute("insert into p values " + SEED_ROW);
        drainWalQueue();
        engine.releaseInactive();
        assertWalTableNotSuspended("c");
        assertWalTableNotSuspended("p");
    }
}
