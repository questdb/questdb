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

package io.questdb.test.cairo.lv;

import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8s;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.Assert;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Fails one read of a base table's WAL {@code created_at} column after skipping a given number,
 * which is what puts a live view refresh turn's failure between two commits it drains in one
 * pass. Install {@link #facade()} through {@code assertMemoryLeak(FilesFacade, ...)}, name the
 * base table's directory with {@link #of}, and {@link #arm} the fault right before the turn that
 * should fail.
 * <p>
 * {@link #armAppliedScan} fails the whole-view rebuild instead: the next open of the base's own
 * {@code amount} column outside the WAL, which is what the rebuild's scan of the applied base
 * reads and the raw-WAL drain never does. {@link #armTimelineOpen} fails the restore ahead of
 * both: the next open of the view's checkpoint timeline, which is the first file a restore maps.
 * Arming all three fails a recovery outright - neither the restore nor the rebuild behind it puts
 * the accumulators back - which is what leaves the window-state debt for a later turn's gate.
 * <p>
 * {@link #armBreach} arms the same WAL read as {@link #arm}, but has it breach the view's own
 * refresh memory limit instead of failing the open, so the turn fails mid-drain with the error the
 * view's tracker raises for any allocation over that limit.
 */
final class LiveViewMidDrainFault {
    private final AtomicBoolean appliedScanArmed = new AtomicBoolean();
    private final AtomicBoolean appliedScanFired = new AtomicBoolean();
    // -1 disarmed; otherwise the number of reads still to skip before the one to fail.
    private final AtomicInteger countdown = new AtomicInteger(-1);
    private final AtomicBoolean fired = new AtomicBoolean();
    // Set by a failed WAL read that reports readErrno, until the errno read that reports it.
    private final AtomicBoolean isReadErrnoPending = new AtomicBoolean();
    private final AtomicBoolean timelineOpenArmed = new AtomicBoolean();
    private volatile String baseDir;
    // The tracker the armed WAL read breaches instead of failing its open; null fails the open.
    private volatile MemoryTracker breachTracker;
    // The errno the failed WAL read reports; 0 leaves whatever errno the thread last saw.
    private volatile int readErrno;

    void arm(int skip) {
        breachTracker = null;
        fired.set(false);
        countdown.set(skip);
    }

    void armAppliedScan() {
        appliedScanFired.set(false);
        appliedScanArmed.set(true);
    }

    /**
     * Arms the WAL read {@link #arm} would fail, and has it charge {@code tracker} - the view's
     * own refresh memory tracker - one byte past what the tracker has left, which the tracker
     * refuses with its "query memory limit exceeded" error.
     */
    void armBreach(int skip, MemoryTracker tracker) {
        Assert.assertNotNull("the view must run under a refresh memory limit", tracker);
        Assert.assertTrue("the view must run under a refresh memory limit", tracker.getLimit() > 0);
        arm(skip);
        breachTracker = tracker;
    }

    void armTimelineOpen() {
        timelineOpenArmed.set(true);
    }

    FilesFacade facade() {
        return new TestFilesFacadeImpl() {
            @Override
            public int errno() {
                return isReadErrnoPending.compareAndSet(true, false) ? readErrno : super.errno();
            }

            @Override
            public long openRO(LPSZ name) {
                final String dir = baseDir;
                if (appliedScanArmed.get()
                        && dir != null
                        && Utf8s.containsAscii(name, dir)
                        && !Utf8s.containsAscii(name, "wal")
                        && Utf8s.endsWithAscii(name, "amount.d")
                        && appliedScanArmed.compareAndSet(true, false)) {
                    appliedScanFired.set(true);
                    return -1;
                }
                if (countdown.get() >= 0
                        && dir != null
                        && Utf8s.containsAscii(name, dir)
                        && Utf8s.containsAscii(name, "wal")
                        && Utf8s.endsWithAscii(name, "created_at.d")) {
                    if (countdown.getAndDecrement() == 0) {
                        fired.set(true);
                        final MemoryTracker tracker = breachTracker;
                        if (tracker != null) {
                            breachTracker = null;
                            breach(tracker);
                        }
                        isReadErrnoPending.set(readErrno != 0);
                        return -1;
                    }
                }
                return super.openRO(name);
            }

            @Override
            public long openRW(LPSZ name, int opts) {
                if (timelineOpenArmed.get()
                        && Utf8s.endsWithAscii(name, LiveViewCheckpointLayout.TIMELINE_FILE_NAME)
                        && timelineOpenArmed.compareAndSet(true, false)) {
                    return -1;
                }
                return super.openRW(name, opts);
            }
        };
    }

    boolean hasAppliedScanFired() {
        return appliedScanFired.get();
    }

    boolean hasFired() {
        return fired.get() && countdown.get() < 0;
    }

    boolean isTimelineOpenArmed() {
        return timelineOpenArmed.get();
    }

    void of(String baseDir) {
        this.baseDir = baseDir;
    }

    /**
     * Makes the failed WAL read report {@code errno}, so the fault reads as the failure it
     * models - a lost segment file, say - rather than as whatever errno the thread last saw.
     */
    void reportReadErrno(int errno) {
        readErrno = errno;
    }

    private static void breach(MemoryTracker tracker) {
        try (MemoryCARW overflow = Vm.getCARWInstance(4096, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
            overflow.setMemoryTracker(tracker);
            overflow.extend(tracker.getLimit() - tracker.getUsed() + 1);
        }
        throw new AssertionError("the view's tracker admitted a charge past its limit");
    }
}
