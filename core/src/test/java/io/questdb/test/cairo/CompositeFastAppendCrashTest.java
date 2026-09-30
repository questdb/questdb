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
import io.questdb.cairo.TableWriter;
import io.questdb.griffin.SqlException;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Composite single-cell fast-append (composite-partitioning fast-append spec 1, Task 3) -- CRASH /
 * POWER-LOSS suite. Task 2 built a synchronous fast-append that appends an ordered commit's rows onto a
 * kept-open {@code <day>/<cell>} column segment PAST the cell's committed size, then bumps that cell's
 * {@code (ts, cellKey)} {@code _txn} size; the caller advances {@code seqTxn} and durably persists the
 * {@code _txn} record (size + seqTxn together, atomically) only afterwards, in {@code commit00}. This
 * suite PROVES with fault injection that a crash at any point BEFORE that durable {@code _txn} write
 * recovers to a consistent state -- identical to a plain twin fed the same rows -- never a torn cell, a
 * lost row, or a duplicated row.
 * <p>
 * <b>The crash-safety invariant under test</b> (Task 2 report). {@code applyCompositeSingleCellFastAppend}
 * appends the rows past the cell's committed size and fsyncs them (commitMode-gated) BEFORE it bumps the
 * cell's {@code _txn} size in memory; that bump increments {@code recordStructureVersion}, so the caller's
 * {@code commit00 -> txWriter.commit()} takes the SLOW {@code commitFullRecord} path that serializes the
 * whole attached-partitions array + {@code seqTxn} + fixed/transient/max ATOMICALLY. {@code seqTxn}
 * advances (durably) only inside that write. So the appended bytes live PAST the recorded cell size until
 * the {@code _txn} write lands: a crash before it leaves them ignored on reopen, the WAL replays the
 * un-acked transaction, and the recovered table equals the twin. A half-write sets {@code distressed} and
 * the pool rebuilds the writer from durable state.
 * <p>
 * <b>Method.</b> Each test injects a {@link FilesFacade} fault at a precise point of the fast-append, drives
 * the WAL apply job into the failure (inserting only into the composite table {@code c} so the fault can
 * never touch the twin), then reopens the writer ({@link io.questdb.cairo.CairoEngine#releaseInactive()} --
 * discarding any in-memory writer state exactly as a crash / eviction would), resumes the table if the
 * aborted commit suspended it, replays the WAL, and asserts the recovered {@code c} equals a plain twin
 * {@code p} fed the identical rows (count AND full ordered scan AND per-cell / {@code LATEST ON} /
 * {@code SAMPLE BY}). {@code commitMode=sync} makes the durability syncs real syscalls so crash points B and
 * C can intercept them.
 * <p>
 * <b>Crash points.</b>
 * <ul>
 *     <li><b>A -- mid fast-append, fresh cell open</b>
 *     ({@link #testCrashMidFastAppendReplaysEqualsTwin}): a cell column ({@code <day>/<cell>/px.d})
 *     memory-map fails while {@code applyCompositeSingleCellFastAppend} is opening / positioning the
 *     kept-open handle, strictly before the {@code _txn} size bump. The cell's committed size never
 *     changed; restart replays the un-acked transaction.</li>
 *     <li><b>B -- at the {@code commit00} / {@code _txn} write</b>
 *     ({@link #testCrashAtTxnCommitReplaysEqualsTwin}): the fast-append writes + syncs its cell and returns,
 *     the caller advances {@code seqTxn} in memory, but a durable {@code msync} of the {@code exch} symbol
 *     map ({@code exch.o}) in {@code commit00} -- which runs strictly BEFORE {@code txWriter.commit()} bumps
 *     the durable {@code _txn} -- fails. The durable {@code seqTxn} stays un-advanced, so the fully-written
 *     cell bytes past the still-committed size are dropped and the transaction replayed, with no
 *     double-apply.</li>
 *     <li><b>C -- extending an already-populated, already-open cell</b>
 *     ({@link #testCrashExtendingOpenCellReplaysEqualsTwin}): one fast-append into the cell has already
 *     committed durably (its handle stays open, its page stays mapped); a SECOND fast-append then crashes
 *     when the cell column {@code msync} (after the append memcpy, before the {@code _txn} bump) fails. The
 *     first fast-append's rows must survive and the second's must not be half-applied.</li>
 *     <li><b>Cross-day reposition</b> ({@link #testCrossDayRepositionMatchesTwin}, no crash injection --
 *     the Task 2 untested edge): fast-append into a day-1 cell (handle open at day-1), then commit into a
 *     day-2 cell (the partition changes, so the day-1 handle is closed / repositioned), fast-append into
 *     day-2, then backfill day-1 again (no longer the last partition -> full path). Asserts the recovered
 *     data equals the twin across both days -- the reposition produces correct data.</li>
 * </ul>
 */
public class CompositeFastAppendCrashTest extends AbstractCairoTest {

    @Before
    public void setUp() {
        // Config must be in place BEFORE super.setUp() rebuilds the engine configuration.
        setProperty(PropertyKey.CAIRO_WAL_COMPOSITE_FASTAPPEND_ENABLED, "true");
        // Power-loss fidelity: SYNC makes the cell-column and symbol-map durability calls real ff.msync
        // syscalls, so crash points B (exch.o msync in commit00) and C (cell px.d msync in the fast-append)
        // have a syscall to intercept. Under the default NOSYNC there would be none.
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "sync");
        super.setUp();
    }

    /**
     * Crash point A -- a cell column ({@code px.d}) memory-map fails inside
     * {@code applyCompositeSingleCellFastAppend} (opening / positioning the kept-open handle), strictly
     * before the {@code _txn} size bump. Restart replays the un-acked transaction; the recovered composite
     * equals the plain twin, with no torn committed cell.
     */
    @Test
    public void testCrashMidFastAppendReplaysEqualsTwin() throws Exception {
        final AtomicBoolean cellArmed = new AtomicBoolean(false);
        final AtomicBoolean faultHit = new AtomicBoolean(false);
        final AtomicLong targetFd = new AtomicLong(-1);

        final FilesFacade ff = new TestFilesFacadeImpl() {
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

        assertMemoryLeak(ff, () -> {
            Assert.assertTrue(configuration.isWalCompositeFastAppendEnabled());
            createCompositeAndPlain();

            // Warm c's day-1 cell 'A': commit 1 routes the table + creates the cell, commit 2 warms the
            // per-cell max cache (both full-path), so commit 3 (the armed one) fast-appends.
            insCell("c", 1, 0, "A", 1.0);
            drainWalQueue();
            insCell("c", 1, 1, "A", 2.0);
            drainWalQueue();

            // Arm only for the fast-append commit: its cell handle open faults on the px.d map. Drift-guard:
            // the armed commit must route to the fast-append path (eligible counter +1) so a future routing
            // change that sent it down the full O3 path -- which would also open px.d and still recover
            // == twin -- cannot silently stop exercising the fast-append. (The committed counter can't be
            // used here: the fault throws inside applyCompositeSingleCellFastAppend, before that increment.)
            long eligibleBefore = TableWriter.getCompositeFastAppendEligibleCount();
            insCell("c", 1, 2, "A", 3.0);
            cellArmed.set(true);
            drainWalQueue();
            cellArmed.set(false);
            Assert.assertEquals("the armed commit must be routed to the composite fast-append path",
                    eligibleBefore + 1, TableWriter.getCompositeFastAppendEligibleCount());

            recoverAndBuildTwin(faultHit, () -> {
                insCell("p", 1, 0, "A", 1.0);
                insCell("p", 1, 1, "A", 2.0);
                insCell("p", 1, 2, "A", 3.0);
            });
        });
    }

    /**
     * Crash point B -- the fast-append writes + syncs its cell and returns, the caller advances
     * {@code seqTxn} in memory, but a durable {@code msync} of the {@code exch} symbol map ({@code exch.o})
     * in {@code commit00} -- strictly BEFORE {@code txWriter.commit()} makes the {@code _txn} durable --
     * fails. The durable {@code seqTxn} stays un-advanced; the fully-written cell bytes past the committed
     * size are dropped and the transaction replays, with no double-apply.
     */
    @Test
    public void testCrashAtTxnCommitReplaysEqualsTwin() throws Exception {
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
            Assert.assertTrue(configuration.isWalCompositeFastAppendEnabled());
            createCompositeAndPlain();

            insCell("c", 1, 0, "A", 1.0);
            drainWalQueue();
            insCell("c", 1, 1, "A", 2.0);
            drainWalQueue();

            // Arm the exch.o msync fault for the fast-append commit's commit00. Drift-guard: the fast-append
            // itself completes (committed counter +1) before the commit00 msync faults, so a future routing
            // change that took the full O3 path would fail this assertion rather than silently stop
            // exercising the fast-append (the full path would still fault on exch.o and recover == twin).
            long committedBefore = TableWriter.getCompositeFastAppendCommittedCount();
            insCell("c", 1, 2, "A", 3.0);
            armed.set(true);
            drainWalQueue();
            armed.set(false);
            Assert.assertEquals("the fast-append must have completed before the commit00 msync fault",
                    committedBefore + 1, TableWriter.getCompositeFastAppendCommittedCount());

            recoverAndBuildTwin(faultHit, () -> {
                insCell("p", 1, 0, "A", 1.0);
                insCell("p", 1, 1, "A", 2.0);
                insCell("p", 1, 2, "A", 3.0);
            });
        });
    }

    /**
     * Crash point C -- a first fast-append into the cell has already committed durably (its handle stays
     * open, its page mapped); a SECOND fast-append then crashes when the cell column {@code msync} (after
     * the append memcpy, strictly before the {@code _txn} bump) fails. The first fast-append's rows must
     * survive and the second's must not be half-applied.
     */
    @Test
    public void testCrashExtendingOpenCellReplaysEqualsTwin() throws Exception {
        final AtomicBoolean armed = new AtomicBoolean(false);
        final AtomicBoolean faultHit = new AtomicBoolean(false);
        final AtomicLong cellFd = new AtomicLong(-1);
        final AtomicLong cellAddr = new AtomicLong(-1);

        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public boolean close(long fd) {
                if (fd == cellFd.get()) {
                    cellFd.set(-1);
                    cellAddr.set(-1);
                }
                return super.close(fd);
            }

            @Override
            public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
                long addr = super.mmap(fd, len, offset, flags, memoryTag);
                if (addr != -1 && fd == cellFd.get()) {
                    cellAddr.set(addr);
                }
                return addr;
            }

            @Override
            public void msync(long addr, long len, boolean async) {
                if (armed.get() && cellAddr.get() != -1 && addr == cellAddr.get()) {
                    armed.set(false);
                    faultHit.set(true);
                    throw CairoException.critical(5).put("simulated power loss: cell msync failed before _txn bump");
                }
                super.msync(addr, len, async);
            }

            @Override
            public long openRW(LPSZ name, int opts) {
                long fd = super.openRW(name, opts);
                // Track the composite cell's kept-open px.d handle (last wins: the full-path warm-up
                // commits open + close a transient copy first; the fast-append handle opened afterwards is
                // the one that stays open and gets msync'd on every fast-append).
                if (isCompositeCellData(name)) {
                    cellFd.set(fd);
                }
                return fd;
            }
        };

        assertMemoryLeak(ff, () -> {
            Assert.assertTrue(configuration.isWalCompositeFastAppendEnabled());
            createCompositeAndPlain();

            insCell("c", 1, 0, "A", 1.0);
            drainWalQueue();
            insCell("c", 1, 1, "A", 2.0);
            drainWalQueue();

            // First fast-append: commits durably, opens + maps the kept-open cell handle.
            long before = TableWriter.getCompositeFastAppendCommittedCount();
            insCell("c", 1, 2, "A", 3.0);
            drainWalQueue();
            Assert.assertEquals("first commit into the warm cell must fast-append",
                    before + 1, TableWriter.getCompositeFastAppendCommittedCount());

            // Second fast-append into the SAME open cell: crash on its cell msync, before the _txn bump.
            insCell("c", 1, 3, "A", 4.0);
            armed.set(true);
            drainWalQueue();
            armed.set(false);

            recoverAndBuildTwin(faultHit, () -> {
                insCell("p", 1, 0, "A", 1.0);
                insCell("p", 1, 1, "A", 2.0);
                insCell("p", 1, 2, "A", 3.0);
                insCell("p", 1, 3, "A", 4.0);
            });
        });
    }

    /**
     * Cross-day reposition (correctness, no crash injection -- the Task 2 untested edge). Fast-append into a
     * day-1 cell (the kept-open handle opens at day-1), then commit into a day-2 cell: the partition changes,
     * so the day-1 handle is closed / repositioned. Fast-append into day-2, then backfill day-1 (no longer
     * the last partition -> full O3 path, no fast-append). The recovered composite must equal the plain twin
     * across both days.
     */
    @Test
    public void testCrossDayRepositionMatchesTwin() throws Exception {
        assertMemoryLeak(() -> {
            Assert.assertTrue(configuration.isWalCompositeFastAppendEnabled());
            createCompositeAndPlain();

            // Day 1 cell 'A': warm (2 commits), then fast-append -> handle open at day-1.
            insBoth(1, 0, "A", 1.0);
            drainWalQueue();
            insBoth(1, 1, "A", 2.0);
            drainWalQueue();
            long before = TableWriter.getCompositeFastAppendCommittedCount();
            insBoth(1, 2, "A", 3.0);
            drainWalQueue();

            // Day 2 cell 'A': the first commit is a brand-new cell -> full path, which CLOSES the open
            // day-1 handle and folds the (cellKey-keyed) per-cell max cache to day-2. The second day-2
            // commit then fast-appends, reopening the handle at day-2 (the cross-day reposition).
            insBoth(2, 0, "A", 4.0);
            drainWalQueue();
            insBoth(2, 1, "A", 5.0);
            drainWalQueue();
            insBoth(2, 2, "A", 6.0);
            drainWalQueue();
            long afterDay2 = TableWriter.getCompositeFastAppendCommittedCount();
            Assert.assertTrue("fast-append must fire in BOTH days across the cross-day reposition",
                    afterDay2 >= before + 2);

            // Backfill day-1 (no longer the last partition): fast-append declines it -> full O3 path. Both
            // tables take it; the counter stays flat.
            long beforeBackfill = TableWriter.getCompositeFastAppendCommittedCount();
            insBoth(1, 3, "A", 7.0);
            drainWalQueue();
            Assert.assertEquals("a commit into a non-last partition must NOT fast-append",
                    beforeBackfill, TableWriter.getCompositeFastAppendCommittedCount());

            engine.releaseInactive();
            assertNotSuspended("c");
            assertNotSuspended("p");
            assertShapesMatch("p", "c");
        });
    }

    // ------------------------------------------------------------------------------------------------------

    // A composite cell data file lives under <tableRoot>/<day>[.<nameTxn>]/<cellSegment>/px.d -- it ends
    // with px.d, sits under a 2020-01-0* day directory, and (unlike the WAL segment copy) is not under a
    // wal directory. Only the composite table 'c' is ever pending while a fault is armed, so this cannot
    // match the plain twin's px.d.
    private static boolean isCompositeCellData(LPSZ name) {
        return Utf8s.endsWithAscii(name, "px.d")
                && Utf8s.containsAscii(name, "2020-01-0")
                && !Utf8s.containsAscii(name, "wal");
    }

    private void assertNotSuspended(String tableName) {
        Assert.assertFalse(
                tableName + " must not be suspended after recovery",
                engine.getTableSequencerAPI().isSuspended(engine.verifyTableName(tableName)));
    }

    private void assertShapesMatch(String ref, String actual) throws SqlException {
        assertSqlCursors(
                "select ts, exch, px from " + ref + " order by ts, exch, px",
                "select ts, exch, px from " + actual + " order by ts, exch, px");
        assertSqlCursors("select count() from " + ref, "select count() from " + actual);
        assertSqlCursors(
                "select ts, px from " + ref + " where exch = 'A' order by ts, px",
                "select ts, px from " + actual + " where exch = 'A' order by ts, px");
        assertSqlCursors(
                "select ts, exch, px from " + ref + " latest on ts partition by exch order by exch",
                "select ts, exch, px from " + actual + " latest on ts partition by exch order by exch");
        assertSqlCursors(
                "select ts, count(), sum(px) from " + ref + " sample by 1d",
                "select ts, count(), sum(px) from " + actual + " sample by 1d");
    }

    private void createCompositeAndPlain() throws SqlException {
        execute("create table c (ts timestamp, exch symbol, px double) timestamp(ts) partition by day, exch wal");
        execute("create table p (ts timestamp, exch symbol, px double) timestamp(ts) partition by day wal");
    }

    // One single-cell row into BOTH the composite table c and the plain twin p (identical literal) -- used
    // by the no-fault cross-day test where a shared drain is safe.
    private void insBoth(int day, int hour, String exch, double px) throws SqlException {
        insCell("c", day, hour, exch, px);
        insCell("p", day, hour, exch, px);
    }

    // One single-cell row (all same exch) into the named table.
    private void insCell(String table, int day, int hour, String exch, double px) throws SqlException {
        execute("insert into " + table + " values ('" + tsOf(day, hour) + "','" + exch + "'," + px + ")");
    }

    /**
     * Reopen the writer (discarding any in-memory writer state, exactly as a crash / eviction would), resume
     * the composite table if the aborted commit suspended it, replay the WAL, then build the plain twin with
     * the identical rows via a clean drain and assert the recovered composite equals it across every shape.
     */
    private void recoverAndBuildTwin(AtomicBoolean faultHit, TwinBuilder buildTwin) throws Exception {
        // The injection MUST have fired -- otherwise a clean drain would trivially "match" the twin and the
        // test would be a false pass that never exercised the crash window.
        Assert.assertTrue("fault injection must have fired at the fast-append point", faultHit.get());

        // Cold restart: drop the (possibly distressed) writer and any in-memory state, so recovery is purely
        // from what is durable.
        engine.releaseInactive();

        TableToken ct = engine.verifyTableName("c");
        if (engine.getTableSequencerAPI().isSuspended(ct)) {
            execute("alter table c resume wal");
        }
        drainWalQueue();
        assertNotSuspended("c");

        // Plain twin fed the identical rows via an uninterrupted drain (never touched by the armed fault).
        buildTwin.build();
        drainWalQueue();
        assertNotSuspended("p");

        assertShapesMatch("p", "c");
    }

    private static String tsOf(int day, int hour) {
        return String.format("2020-01-0%dT%02d:00:00.000000Z", day, hour);
    }

    @FunctionalInterface
    private interface TwinBuilder {
        void build() throws SqlException;
    }
}
