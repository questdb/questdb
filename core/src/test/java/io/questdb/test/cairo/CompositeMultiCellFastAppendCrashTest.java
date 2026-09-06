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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Composite MULTI-cell fast-append (composite-partitioning fast-append spec 2, Task 4) -- CRASH /
 * POWER-LOSS suite. Task 3 built {@code applyCompositeMultiCellFastAppend}: an eligible multi-cell commit
 * appends its rows onto EACH touched {@code <day>/<cell>} kept-open segment PAST that cell's committed
 * size (APPEND PASS), syncs EVERY touched cell (SYNC PASS), bumps each cell's {@code (ts, cellKey)}
 * {@code _txn} size in memory (N-FOLD BUMP), then takes the cheap early return -- the caller advances
 * {@code seqTxn} and durably persists the {@code _txn} record (all N size bumps + {@code seqTxn} together,
 * atomically) only afterwards, ONCE, in {@code commit00 -> txWriter.commit()}. This suite PROVES with
 * fault injection that a crash at ANY point before that single durable {@code _txn} write recovers to a
 * state identical to a plain twin fed the same rows -- never a torn cell, a partial-N half-commit, a lost
 * row, a duplicated row, or a cross-contaminated cell.
 * <p>
 * <b>The crash-safety invariant under test.</b> The whole multi-cell fast-append writes NOTHING durable to
 * {@code _txn}: it appends every cell's rows past that cell's still-committed size, syncs, and bumps
 * {@code _txn} sizes only IN MEMORY (the bump increments {@code recordStructureVersion}, so the caller's
 * {@code commit00 -> txWriter.commit()} takes the slow {@code commitFullRecord} path that serializes the
 * whole attached-partitions array + {@code seqTxn} + fixed/transient/max ATOMICALLY). {@code seqTxn}
 * advances durably only inside that one write. So EVERY touched cell's appended bytes live PAST its
 * recorded size until the single {@code _txn} write lands: a crash before it leaves them all ignored on
 * reopen, the WAL replays the un-acked transaction, and the recovered table equals the twin. A half-write
 * sets {@code distressed} and the pool rebuilds the writer from durable state, closing every kept-open
 * cell handle NON-TRUNCATING (spec-1's T3 fix) so a committed cell column is never shrunk below its
 * committed size.
 * <p>
 * <b>The new window vs spec 1</b> (single-cell). A multi-cell commit appends N cells before the single
 * {@code _txn} write, so there is a PARTIAL-N crash window that single-cell has no analog for: cell #1 of N
 * fully appended, then cell #2's open faults. BOTH cells' extra byte-runs past their committed sizes must
 * be ignored on reopen -- there is no cell-#1 half-commit, because no {@code _txn} bump has landed for ANY
 * cell (the bump loop runs strictly after the whole append+sync). {@link
 * #testCrashPartialNSecondCellReplaysEqualsTwin} is that proof.
 * <p>
 * <b>Method</b> (spec-1's {@link CompositeFastAppendCrashTest} idiom). Each test injects a
 * {@link FilesFacade} fault at a precise point of the multi-cell fast-append, drives the WAL apply into the
 * failure (inserting only into the composite table {@code c} so the fault can never touch the twin), then
 * reopens the writer ({@link io.questdb.cairo.CairoEngine#releaseInactive()} -- discarding all in-memory
 * writer state exactly as a crash / eviction would), resumes the table if the aborted commit suspended it,
 * replays the WAL, and asserts the recovered {@code c} equals a plain twin {@code p} fed the identical rows
 * (count AND full ordered scan AND per-cell A/B AND {@code LATEST ON} AND {@code SAMPLE BY}).
 * {@code commitMode=sync} makes the durability syncs real syscalls so crash points C and D can intercept
 * them. Each armed commit's drift-guard (the eligible / committed counter must advance by exactly one)
 * proves the commit really routed to the multi-cell fast-append path, so a future routing change that sent
 * it down the full O3 path -- which also recovers {@code == twin} -- cannot silently stop exercising the
 * fast-append.
 * <p>
 * <b>Warm-up.</b> The first commit into any cell ROUTES the composite table but is not itself dispatched
 * through the fast-append hook ({@code isRoutedComposite()} is still false), so its cell max is never
 * folded into the shared per-cell max cache. Every test therefore routes with a throwaway {@code R0}
 * commit first, THEN seeds cells {@code A} and {@code B} (each a full-path create that folds its max via
 * the universal multi-cell predicate), so a later multi-cell commit spanning {@code A,B} is
 * append-only-eligible -- exactly the warm-up {@link CompositeMultiCellFastAppendTest} uses.
 * <p>
 * <b>Crash points.</b>
 * <ul>
 *     <li><b>A -- mid append of cell #1 of N</b> ({@link #testCrashMidAppendFirstCellReplaysEqualsTwin}):
 *     the FIRST touched cell's {@code px.d} memory-map fails while the APPEND PASS is opening that cell's
 *     kept-open handle, strictly before ANY {@code _txn} bump. No cell's committed size changed; restart
 *     replays the un-acked transaction.</li>
 *     <li><b>B -- partial-N (the new multi-cell window)</b>
 *     ({@link #testCrashPartialNSecondCellReplaysEqualsTwin}): cell #1 is fully appended, then the SECOND
 *     touched cell's {@code px.d} memory-map fails while the APPEND PASS opens it -- after cell #1's bytes
 *     landed past its committed size but before the bump loop. BOTH cells' extra bytes are ignored on
 *     reopen (no {@code _txn} bump landed for either), so the WAL replays the whole commit -- no cell-#1
 *     half-commit, no partial multi-cell commit.</li>
 *     <li><b>C -- at the single {@code commit00} / {@code _txn} write</b>
 *     ({@link #testCrashAtTxnCommitReplaysEqualsTwin}): the multi-cell fast-append appends + syncs + bumps
 *     all cells in memory and returns, the caller advances {@code seqTxn} in memory, but the durable
 *     {@code msync} of the {@code exch} symbol map ({@code exch.o}) in {@code commit00} -- strictly BEFORE
 *     {@code txWriter.commit()} makes the {@code _txn} durable -- fails. The durable {@code seqTxn} stays
 *     un-advanced, so ALL N cells' bytes past their still-committed sizes are dropped and the transaction
 *     replays, with no double-apply.</li>
 *     <li><b>D -- extending already-populated cells</b>
 *     ({@link #testCrashExtendingOpenCellsReplaysEqualsTwin}): a FIRST multi-cell commit into {@code A,B}
 *     has already committed durably (both handles stay open, both pages mapped); a SECOND multi-cell commit
 *     into the SAME open cells then crashes when the second cell's {@code msync} in the SYNC PASS (after
 *     every cell's append memcpy, strictly before the {@code _txn} bump loop) fails. The first commit's
 *     rows must survive and the second's must not be half-applied to either cell.</li>
 * </ul>
 */
public class CompositeMultiCellFastAppendCrashTest extends AbstractCairoTest {

    // Warm-up + commit timestamps. R0 routes; A/B seed at :05; the multi-cell commits land strictly after
    // each cell's seeded max (append-only per cell) at :10 (commit 1) and :11 (commit 2, crash D only).
    private static final String T_A1 = "2020-01-01T00:10:00.000000Z"; // cell A, multi-commit 1
    private static final String T_A2 = "2020-01-01T00:11:00.000000Z"; // cell A, multi-commit 2 (crash D)
    private static final String T_B1 = "2020-01-01T00:10:01.000000Z"; // cell B, multi-commit 1
    private static final String T_B2 = "2020-01-01T00:11:01.000000Z"; // cell B, multi-commit 2 (crash D)
    private static final String T_SEED = "2020-01-01T00:05:00.000000Z";
    private static final String T_ROUTE = "2020-01-01T00:00:00.000000Z";

    @Before
    public void setUp() {
        // Config must be in place BEFORE super.setUp() rebuilds the engine configuration.
        setProperty(PropertyKey.CAIRO_WAL_COMPOSITE_FASTAPPEND_ENABLED, "true");
        // Power-loss fidelity: SYNC makes the cell-column and symbol-map durability calls real ff.msync
        // syscalls, so crash points C (exch.o msync in commit00) and D (cell px.d msync in the SYNC PASS)
        // have a syscall to intercept. Under the default NOSYNC there would be none.
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "sync");
        super.setUp();
    }

    /**
     * Crash point A -- the FIRST touched cell's {@code px.d} memory-map fails inside the multi-cell
     * fast-append's APPEND PASS (opening / positioning the kept-open handle), strictly before any
     * {@code _txn} size bump. Restart replays the un-acked transaction; the recovered composite equals the
     * plain twin, with no torn committed cell.
     */
    @Test
    public void testCrashMidAppendFirstCellReplaysEqualsTwin() throws Exception {
        final AtomicBoolean armed = new AtomicBoolean(false);
        final AtomicBoolean faultHit = new AtomicBoolean(false);
        final AtomicLong targetFd = new AtomicLong(-1);

        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
                if (armed.get() && fd == targetFd.get()) {
                    armed.set(false);
                    faultHit.set(true);
                    return -1;
                }
                return super.mmap(fd, len, offset, flags, memoryTag);
            }

            @Override
            public long openRW(LPSZ name, int opts) {
                long fd = super.openRW(name, opts);
                // First composite cell data file (px.d) opened while armed == cell #1 of the commit.
                if (armed.get() && isCompositeCellData(name)) {
                    targetFd.compareAndSet(-1, fd);
                }
                return fd;
            }
        };

        assertMemoryLeak(ff, () -> {
            Assert.assertTrue(configuration.isWalCompositeFastAppendEnabled());
            createCompositeAndPlain();
            warmCellsAB();

            // Arm only for the multi-cell commit: cell #1's px.d open faults on its mmap, before any _txn
            // bump. Drift-guard on the ELIGIBLE counter: the fault throws inside
            // applyCompositeMultiCellFastAppend, before the committed increment, so committed cannot be
            // used -- but eligible (+1 at the hook, before the action) proves the armed commit routed to
            // the multi-cell fast-append path.
            long eligibleBefore = TableWriter.getCompositeMultiCellFastAppendEligibleCount();
            insMulti("c", T_A1, 3.0, T_B1, 4.0);
            armed.set(true);
            drainWalQueue();
            armed.set(false);
            Assert.assertEquals("the armed commit must be routed to the composite MULTI-cell fast-append path",
                    eligibleBefore + 1, TableWriter.getCompositeMultiCellFastAppendEligibleCount());

            recoverAndBuildTwin(faultHit, () -> buildTwinCommit1("p"));
        });
    }

    /**
     * Crash point B -- the PARTIAL-N window (no single-cell analog). Cell #1 of the commit is fully
     * appended (its bytes past its committed size), then the SECOND touched cell's {@code px.d} memory-map
     * fails while the APPEND PASS opens it -- before the bump loop. Neither cell's {@code _txn} size
     * changed, so BOTH extra byte-runs are ignored on reopen and the WAL replays the whole multi-cell
     * transaction: no cell-#1 half-commit, no partial multi-cell commit.
     */
    @Test
    public void testCrashPartialNSecondCellReplaysEqualsTwin() throws Exception {
        final AtomicBoolean armed = new AtomicBoolean(false);
        final AtomicBoolean faultHit = new AtomicBoolean(false);
        final AtomicInteger cellOpens = new AtomicInteger(0);
        final AtomicLong targetFd = new AtomicLong(-1);

        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
                if (armed.get() && fd == targetFd.get()) {
                    armed.set(false);
                    faultHit.set(true);
                    return -1;
                }
                return super.mmap(fd, len, offset, flags, memoryTag);
            }

            @Override
            public long openRW(LPSZ name, int opts) {
                long fd = super.openRW(name, opts);
                if (armed.get() && isCompositeCellData(name)) {
                    // The APPEND PASS opens cell #1's px.d first, then cell #2's. Fault only the SECOND, so
                    // cell #1 is fully appended before the fault -- the partial-N window. (Each cell opens
                    // its px.d exactly once; the fast-append is the only thing opening a composite px.d
                    // during the armed drain, the seeds having already committed unarmed.)
                    if (cellOpens.incrementAndGet() == 2) {
                        targetFd.set(fd);
                    }
                }
                return fd;
            }
        };

        assertMemoryLeak(ff, () -> {
            Assert.assertTrue(configuration.isWalCompositeFastAppendEnabled());
            createCompositeAndPlain();
            warmCellsAB();

            long eligibleBefore = TableWriter.getCompositeMultiCellFastAppendEligibleCount();
            insMulti("c", T_A1, 3.0, T_B1, 4.0);
            armed.set(true);
            drainWalQueue();
            armed.set(false);
            Assert.assertEquals("the armed commit must be routed to the composite MULTI-cell fast-append path",
                    eligibleBefore + 1, TableWriter.getCompositeMultiCellFastAppendEligibleCount());
            Assert.assertTrue("the partial-N fault must fire on the SECOND cell (cell #1 fully appended first)",
                    cellOpens.get() >= 2);

            recoverAndBuildTwin(faultHit, () -> buildTwinCommit1("p"));
        });
    }

    /**
     * Crash point C -- the multi-cell fast-append appends + syncs + bumps ALL cells in memory and returns,
     * the caller advances {@code seqTxn} in memory, but a durable {@code msync} of the {@code exch} symbol
     * map ({@code exch.o}) in {@code commit00} -- strictly BEFORE {@code txWriter.commit()} makes the single
     * {@code _txn} durable -- fails. The durable {@code seqTxn} stays un-advanced; every cell's bytes past
     * its still-committed size are dropped and the transaction replays, with no double-apply.
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
            warmCellsAB();

            // Arm the exch.o msync fault for the multi-cell commit's commit00. Drift-guard on the COMMITTED
            // counter: the whole multi-cell fast-append completes (committed +1) BEFORE the commit00 msync
            // faults, so a future routing change taking the full O3 path would fail this assertion rather
            // than silently stop exercising the fast-append (the full path would still fault on exch.o and
            // recover == twin).
            long committedBefore = TableWriter.getCompositeMultiCellFastAppendCommittedCount();
            insMulti("c", T_A1, 3.0, T_B1, 4.0);
            armed.set(true);
            drainWalQueue();
            armed.set(false);
            Assert.assertEquals("the multi-cell fast-append must have completed before the commit00 msync fault",
                    committedBefore + 1, TableWriter.getCompositeMultiCellFastAppendCommittedCount());

            recoverAndBuildTwin(faultHit, () -> buildTwinCommit1("p"));
        });
    }

    /**
     * Crash point D -- a first multi-cell commit into {@code A,B} has already committed durably (both
     * handles stay open, both pages mapped); a SECOND multi-cell commit into the SAME open cells then
     * crashes when the second cell's column {@code msync} in the SYNC PASS (after every cell's append
     * memcpy, strictly before the {@code _txn} bump loop) fails. The first commit's rows must survive and
     * the second's must not be half-applied to either cell.
     */
    @Test
    public void testCrashExtendingOpenCellsReplaysEqualsTwin() throws Exception {
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
            public long mremap(long fd, long addr, long previousSize, long newSize, long offset, int mode, int memoryTag) {
                long newAddr = super.mremap(fd, addr, previousSize, newSize, offset, mode, memoryTag);
                if (newAddr != -1 && fd == cellFd.get()) {
                    cellAddr.set(newAddr);
                }
                return newAddr;
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
                // Track the LAST composite cell px.d handle opened by the first multi-cell fast-append: it
                // opens cell A then cell B, so last-wins captures cell B's kept-open handle -- the one the
                // SECOND commit's SYNC PASS msyncs (after cell A) and we fault. (The full-path seeds open +
                // close transient copies first; the fast-append handles opened afterwards stay open.)
                if (isCompositeCellData(name)) {
                    cellFd.set(fd);
                }
                return fd;
            }
        };

        assertMemoryLeak(ff, () -> {
            Assert.assertTrue(configuration.isWalCompositeFastAppendEnabled());
            createCompositeAndPlain();
            warmCellsAB();

            // First multi-cell commit: commits durably, opens + maps both kept-open cell handles (A, B).
            long before = TableWriter.getCompositeMultiCellFastAppendCommittedCount();
            insMulti("c", T_A1, 3.0, T_B1, 4.0);
            drainWalQueue();
            Assert.assertEquals("first multi-cell commit into the warm cells must fast-append",
                    before + 1, TableWriter.getCompositeMultiCellFastAppendCommittedCount());

            // Second multi-cell commit into the SAME open cells: crash on cell B's msync in the SYNC PASS,
            // before the _txn bump loop. Drift-guard on the ELIGIBLE counter (the fault throws inside the
            // action, before the committed increment).
            long eligibleBefore = TableWriter.getCompositeMultiCellFastAppendEligibleCount();
            insMulti("c", T_A2, 5.0, T_B2, 6.0);
            armed.set(true);
            drainWalQueue();
            armed.set(false);
            Assert.assertEquals("the second multi-cell commit must route to the fast-append path",
                    eligibleBefore + 1, TableWriter.getCompositeMultiCellFastAppendEligibleCount());

            recoverAndBuildTwin(faultHit, () -> buildTwinCommit2("p"));
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

    private static String row(String ts, String exch, double px) {
        return "('" + ts + "','" + exch + "'," + px + ')';
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
                "select ts, px from " + ref + " where exch = 'B' order by ts, px",
                "select ts, px from " + actual + " where exch = 'B' order by ts, px");
        assertSqlCursors(
                "select exch, count() from " + ref + " group by exch order by exch",
                "select exch, count() from " + actual + " group by exch order by exch");
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

    // One row into the named table.
    private void ins(String table, String ts, String exch, double px) throws SqlException {
        execute("insert into " + table + " values " + row(ts, exch, px));
    }

    // One multi-cell commit (two distinct cells A and B, buffer order A then B) into the named table.
    private void insMulti(String table, String tsA, double pxA, String tsB, double pxB) throws SqlException {
        execute("insert into " + table + " values " + row(tsA, "A", pxA) + "," + row(tsB, "B", pxB));
    }

    /**
     * Reopen the writer (discarding any in-memory writer state, exactly as a crash / eviction would),
     * resume the composite table if the aborted commit suspended it, replay the WAL, then build the plain
     * twin with the identical rows via a clean drain and assert the recovered composite equals it across
     * every shape.
     */
    private void recoverAndBuildTwin(AtomicBoolean faultHit, TwinBuilder buildTwin) throws Exception {
        // The injection MUST have fired -- otherwise a clean drain would trivially "match" the twin and the
        // test would be a false pass that never exercised the crash window.
        Assert.assertTrue("fault injection must have fired at the multi-cell fast-append point", faultHit.get());

        // Cold restart: drop the (possibly distressed) writer and all in-memory state, so recovery is
        // purely from what is durable.
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

    // Warm cells A and B so a later multi-cell commit spanning them is append-only-eligible: route the
    // composite table with a throwaway R0 commit (whose own cell max is not folded -- it routes but is not
    // dispatched through the fast-append hook), then seed A and B (each a full-path create that folds its
    // max via the universal multi-cell predicate). Composite table c only (the twin is built post-recovery).
    private void warmCellsAB() throws SqlException {
        ins("c", T_ROUTE, "R0", 0.0);
        drainWalQueue();
        ins("c", T_SEED, "A", 1.0);
        drainWalQueue();
        ins("c", T_SEED, "B", 2.0);
        drainWalQueue();
    }

    // The plain twin's rows for a single armed multi-cell commit (crash A/B/C): route + seeds + commit 1.
    private void buildTwinCommit1(String table) throws SqlException {
        ins(table, T_ROUTE, "R0", 0.0);
        ins(table, T_SEED, "A", 1.0);
        ins(table, T_SEED, "B", 2.0);
        insMulti(table, T_A1, 3.0, T_B1, 4.0);
    }

    // The plain twin's rows for two multi-cell commits (crash D): route + seeds + commit 1 + commit 2.
    private void buildTwinCommit2(String table) throws SqlException {
        buildTwinCommit1(table);
        insMulti(table, T_A2, 5.0, T_B2, 6.0);
    }

    @FunctionalInterface
    private interface TwinBuilder {
        void build() throws SqlException;
    }
}
