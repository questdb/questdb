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
import io.questdb.cairo.TableToken;
import io.questdb.std.FilesFacade;
import io.questdb.std.Rnd;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.fuzz.CompositeFuzzRunner;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * SP8 Task 8 — crash injection driven by the FUZZ workload rather than a hand-written one.
 * <p>
 * {@code CompositeFastAppendCrashTest} already proves crash safety at three hand-placed points, each
 * over a workload written to reach exactly that point. This test asks the complementary question: does
 * a composite table survive a crash in the middle of an ARBITRARY generated commit — many cells, mixed
 * dimension values, NULLs, unassigned columns — and recover to something indistinguishable from a plain
 * twin fed the identical rows?
 * <p>
 * <b>Method.</b> A {@link FilesFacade} fails the memory-map of a composite CELL data file during an
 * armed window, aborting a WAL-apply pass mid-write. The table is then recovered exactly as a restart
 * would: {@code releaseInactive()} discards all in-memory writer state, the suspended table is resumed,
 * and the WAL replays. The recovered composite table must then equal its plain twin, which was fed the
 * same rows and never touched by the fault.
 * <p>
 * <b>What the earlier attempts got wrong</b> (recorded so this is not rediscovered a fourth time): a
 * facade handed to {@code assertMemoryLeak(ff, ...)} from a test in package
 * {@code io.questdb.test.cairo.fuzz} logged ZERO opens, so no predicate could ever match. This class
 * lives in {@code io.questdb.test.cairo} beside the crash test whose facade demonstrably works, and
 * copies its wiring: the same {@code @Before} property order (commit mode SYNC installed BEFORE
 * {@code super.setUp()}, so the durability calls are real syscalls to intercept), the same capture of a
 * target fd in {@code openRW} followed by a failing {@code mmap} on that fd.
 * <p>
 * <b>Non-vacuity.</b> Every test asserts the fault ACTUALLY FIRED. A crash test that cannot prove it
 * crashed is a clean run wearing a crash test's name, and would pass no matter how broken recovery was.
 */
public class CompositeFuzzCrashTest extends AbstractCairoTest {

    @Before
    public void setUp() {
        // Both properties must be in place BEFORE super.setUp() rebuilds the engine configuration.
        setProperty(PropertyKey.CAIRO_WAL_COMPOSITE_FASTAPPEND_ENABLED, "true");
        // SYNC makes the cell-column durability calls real syscalls, matching the working crash test's
        // power-loss fidelity. Under the default NOSYNC there would be nothing to intercept.
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "sync");
        super.setUp();
    }

    /**
     * Fault during a generated commit's cell write, after the table already holds committed data.
     */
    @Test
    public void testCrashMidGeneratedCommitRecoversEqualToTwin() throws Exception {
        assertRecoversFromFaultDuringApply(1234L, 5678L, 400, 20, 200, 10);
    }

    /**
     * A different seed, so the fault lands in a different composite shape (dimension set, layout,
     * clustering and cardinality are all drawn from the seed).
     */
    @Test
    public void testCrashMidGeneratedCommitOtherShapeRecoversEqualToTwin() throws Exception {
        assertRecoversFromFaultDuringApply(99L, 42L, 300, 15, 300, 15);
    }

    /**
     * @param warmRows/warmTxns  applied cleanly first, so the fault lands on a table that already has
     *                           committed cells rather than on an empty one
     * @param armedRows/armedTxns applied with the fault armed
     */
    private void assertRecoversFromFaultDuringApply(
            long seed0, long seed1, int warmRows, int warmTxns, int armedRows, int armedTxns
    ) throws Exception {
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
                final long fd = super.openRW(name, opts);
                if (armed.get() && isCompositeCellData(name)) {
                    targetFd.compareAndSet(-1, fd);
                }
                return fd;
            }
        };

        assertMemoryLeak(ff, () -> {
            final CompositeFuzzRunner runner = CompositeFuzzRunner.of(engine, new Rnd(seed0, seed1));
            runner.createTables("crash");

            // Clean warm-up: the fault must land on a table that already holds committed cells, which is
            // the interesting recovery case (a torn extension of an existing cell, not a first write).
            runner.applyGeneratedTransactions(warmRows, warmTxns);

            armed.set(true);
            try {
                runner.applyGeneratedTransactions(armedRows, armedTxns);
            } catch (Throwable expected) {
                // The abort may surface synchronously through the apply call or asynchronously as a
                // suspended table; both are recovered identically below. What must NOT be tolerated is
                // the fault never firing, which is asserted next.
            } finally {
                armed.set(false);
            }

            Assert.assertTrue("fault injection must have fired -- otherwise this is a clean run, not a crash test",
                    faultHit.get());
            // ... and it must have ABORTED AN APPLY, not been absorbed harmlessly. Without this, a
            // future change that swallowed the failed map would leave a test that still "fires" its
            // fault, still passes, and no longer exercises any crash window at all. Measured: the armed
            // transactions are unapplied at this point (row count is still the warm-up's) and the table
            // is suspended -- recovery below is what replays them.
            Assert.assertTrue("the fault must have aborted a WAL apply (table suspended), not been absorbed",
                    engine.getTableSequencerAPI().isSuspended(engine.verifyTableName(runner.compositeName())));

            // Cold restart: drop the (possibly distressed) writer and every scrap of in-memory state, so
            // recovery proceeds purely from what was made durable.
            engine.releaseInactive();

            final TableToken composite = engine.verifyTableName(runner.compositeName());
            if (engine.getTableSequencerAPI().isSuspended(composite)) {
                execute("alter table " + runner.compositeName() + " resume wal");
            }
            drainWalQueue();
            Assert.assertFalse("composite table must not still be suspended after resume + drain",
                    engine.getTableSequencerAPI().isSuspended(composite));

            final TableToken plain = engine.verifyTableName(runner.plainName());
            if (engine.getTableSequencerAPI().isSuspended(plain)) {
                execute("alter table " + runner.plainName() + " resume wal");
            }
            drainWalQueue();

            // The whole point: after a crash mid-apply and a restart, the composite table is
            // indistinguishable from the plain table fed the same rows -- no lost row, no duplicated
            // row, no torn cell. assertTwinEqual() compares every shape the oracle knows.
            runner.assertTwinEqual();
        });
    }

    /**
     * A composite CELL's data file: a {@code px.d} under a dated partition directory of the COMPOSITE
     * table. The {@code wal} exclusion matters — WAL segment files also end in {@code .d} and carry the
     * date — and so does the table-name check: the plain twin's files match everything else, and
     * faulting it would corrupt the reference this test compares against.
     */
    private static boolean isCompositeCellData(LPSZ name) {
        return Utf8s.endsWithAscii(name, "px.d")
                && Utf8s.containsAscii(name, "2023-01-0")
                && Utf8s.containsAscii(name, "_composite")
                && !Utf8s.containsAscii(name, "wal");
    }
}
