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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.lv.LiveViewState;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Os;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8s;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.Assert;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

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
 * both: the next open of the view's checkpoint timeline, which is the first file a restore maps,
 * reporting a missing file. No retry gets past that, so the recovery falls back to the rebuild.
 * Arming all three fails a recovery outright - neither the restore nor the rebuild behind it puts
 * the accumulators back - which is what leaves the window-state debt for a later turn's gate.
 * {@link #armTimelineOpen(int)} fails the same open with the errno it names. An operating system
 * error a retry may get past - EMFILE or EIO, say - has the recovery retry the restore on a later
 * turn instead of falling back to the rebuild. {@link #armTimelineOpenUntilDisarmed} fails every
 * such open, not just the next one, until {@link #disarmAll()}: a timeline file the process can no
 * longer open for writing, which every restore and every seal meets.
 * <p>
 * {@link #armAnchorRestoreOpen} fails the same open, but only the one an out-of-order repair makes
 * when it restores the root it resumes from: the open made while {@code replayFromAnchor} and
 * {@code restoreAnchorRoot} or {@code restoreAnchorRootKeys} are on the stack. Every other
 * restore, and every seal, opens the timeline unharmed. {@link #armAnchorRestoreOpenUntilDisarmed}
 * fails every such open until {@link #disarmAll()}. {@link #armFrontierRootRestoreOpen} fails the
 * open the same restore makes for a segment loop instead, from {@code restoreRuntimeAtFrontierRoot}.
 * <p>
 * {@link #armRuntimeRestore} fails nothing itself: it runs an action once, right before a
 * running recovery's restore from the timeline reads the view's table, so a case can stage what
 * that read meets - a reader pool every reader of the view's table is taken from, say.
 * <p>
 * {@link #armBreach} arms the same WAL read as {@link #arm}, but has it breach the view's own
 * refresh memory limit instead of failing the open, so the turn fails mid-drain with the error the
 * view's tracker raises for any allocation over that limit.
 * <p>
 * {@link #armViewStateOpen} fails nothing mid-drain: it fails the next open for writing of a live
 * view's state file, once. A lead flush opens that file first to persist the consumed watermark
 * its apply earned, and again to persist the rest of its state when that fails, so the fault
 * fails the first of the two and lets the second through. A turn of a view over a deduplicating
 * base opens it in the same order.
 * <p>
 * {@link #armViewSymbolKeyOpen} fails nothing mid-drain either: it fails the next open of the
 * {@code account_id} symbol's key file in a view's own table, outside its WAL, once. A pooled
 * reader of that table opens the file again each time it reloads to a newer transaction, and
 * only an apply of the view's own WAL moves the table there. So armed right before a lead
 * flush, the fault fails the first reader that reloads behind the flush's apply: the one the
 * flush checks its symbol ids with.
 * <p>
 * The column names above are the defaults. {@link #LiveViewMidDrainFault(String, String, String)}
 * names the timestamp, value and key columns of another schema.
 * <p>
 * Every open a fault fails reports a missing file, unless the fault names another errno
 * ({@link #reportReadErrno}, {@link #armTimelineOpen(int)}, {@link #armAnchorRestoreOpen}). An
 * open that fails without a system call leaves the thread's errno where the last failed call put
 * it, which differs between operating systems, and the refresh job routes a failure by its errno.
 */
final class LiveViewMidDrainFault {
    // EIO, a read error a retry may get past, for reportReadErrno. Windows has no EIO and reads 5
    // as ERROR_ACCESS_DENIED, which Files.isErrnoFileCannotRead() takes for a lost file, so the
    // fault reports ERROR_READ_FAULT there.
    static final int ERRNO_EIO = Os.isWindows() ? 30 : 5;
    // The anchor restore opens the armed fault failed.
    private final AtomicInteger anchorRestoreOpenFailures = new AtomicInteger();
    private final AtomicBoolean appliedScanArmed = new AtomicBoolean();
    // The file name, with its leading separator, of the base column armAppliedScan fails.
    private final String appliedScanFileName;
    private final AtomicBoolean appliedScanFired = new AtomicBoolean();
    // The opens armAppliedScan still lets through before the one it fails.
    private final AtomicInteger appliedScanSkip = new AtomicInteger();
    // -1 disarmed; otherwise the number of reads still to skip before the one to fail.
    private final AtomicInteger countdown = new AtomicInteger(-1);
    private final AtomicBoolean fired = new AtomicBoolean();
    private final AtomicBoolean hasViewStateOpenFired = new AtomicBoolean();
    private final AtomicBoolean hasViewSymbolKeyOpenFired = new AtomicBoolean();
    private final AtomicBoolean isAnchorRestoreOpenArmed = new AtomicBoolean();
    // Set by a failed anchor restore open, until the errno read that reports anchorRestoreOpenErrno.
    private final AtomicBoolean isAnchorRestoreOpenErrnoPending = new AtomicBoolean();
    // Set by armAnchorRestoreOpenUntilDisarmed: the armed fault fails every anchor restore open, not one.
    private final AtomicBoolean isAnchorRestoreOpenPersistent = new AtomicBoolean();
    // Set by a failed open that names no errno of its own - the applied scan's, the view symbol
    // key file's or the view state file's - until the errno read that reports a missing file.
    private final AtomicBoolean isMissingFileErrnoPending = new AtomicBoolean();
    // Set by a failed WAL read that reports readErrno, until the errno read that reports it.
    private final AtomicBoolean isReadErrnoPending = new AtomicBoolean();
    // Set by a failed timeline open that reports timelineOpenErrno, until the errno read that reports it.
    private final AtomicBoolean isTimelineOpenErrnoPending = new AtomicBoolean();
    // Set by armTimelineOpenUntilDisarmed: an armed timeline open fault fails every open, not one.
    private final AtomicBoolean isTimelineOpenPersistent = new AtomicBoolean();
    private final AtomicBoolean isViewStateOpenArmed = new AtomicBoolean();
    private final AtomicBoolean isViewSymbolKeyOpenArmed = new AtomicBoolean();
    // Run once by the check for the view's timeline that a running recovery's restore opens with;
    // null when nothing is armed.
    private final AtomicReference<Runnable> runtimeRestoreAction = new AtomicReference<>();
    private final AtomicBoolean timelineOpenArmed = new AtomicBoolean();
    // The timeline opens the armed fault failed.
    private final AtomicInteger timelineOpenFailures = new AtomicInteger();
    // The file name, with its leading separator, of the view symbol key file
    // armViewSymbolKeyOpen fails.
    private final String viewSymbolKeyFileName;
    // The file name, with its leading separator, of the base WAL column arm fails.
    private final String walReadFileName;
    // The refresh job method whose anchor restore the armed fault fails: replayFromAnchor or
    // restoreRuntimeAtFrontierRoot.
    private volatile String anchorRestoreCaller;
    // The errno the failed anchor restore open reports.
    private volatile int anchorRestoreOpenErrno;
    // The partition the open armAppliedScan fails must be in; null fails it in any partition.
    private volatile String appliedScanPartition;
    private volatile String baseDir;
    // The tracker the armed WAL read breaches instead of failing its open; null fails the open.
    private volatile MemoryTracker breachTracker;
    // The errno the failed WAL read reports: a missing file unless reportReadErrno names another.
    private volatile int readErrno = CairoException.ERRNO_FILE_DOES_NOT_EXIST;
    // The errno the failed timeline open reports.
    private volatile int timelineOpenErrno;
    // The directory of the view table whose symbol key file armViewSymbolKeyOpen fails.
    private volatile String viewDir;

    LiveViewMidDrainFault() {
        this("created_at", "amount", "account_id");
    }

    /**
     * The same faults over another schema: {@link #arm} fails a WAL read of
     * {@code timestampColumn}, {@link #armAppliedScan} an open of {@code valueColumn} outside
     * the WAL, and {@link #armViewSymbolKeyOpen} an open of {@code keyColumn}'s key file in the
     * view's table.
     */
    LiveViewMidDrainFault(String timestampColumn, String valueColumn, String keyColumn) {
        walReadFileName = Files.SEPARATOR + timestampColumn + ".d";
        appliedScanFileName = Files.SEPARATOR + valueColumn + ".d";
        viewSymbolKeyFileName = Files.SEPARATOR + keyColumn + ".k";
    }

    void arm(int skip) {
        breachTracker = null;
        fired.set(false);
        countdown.set(skip);
    }

    /**
     * Arms the failure of the next open of a live view's checkpoint timeline that an
     * out-of-order repair makes to restore the root it resumes from, reporting {@code errno}.
     */
    void armAnchorRestoreOpen(int errno) {
        armAnchorRestoreOpen("replayFromAnchor", errno, false);
    }

    /**
     * Arms the open {@link #armAnchorRestoreOpen(int)} fails for every such open until
     * {@link #disarmAll()}, each reporting {@code errno}.
     */
    void armAnchorRestoreOpenUntilDisarmed(int errno) {
        armAnchorRestoreOpen("replayFromAnchor", errno, true);
    }

    void armAppliedScan() {
        armAppliedScan(null);
    }

    /**
     * Arms the open {@link #armAppliedScan()} fails, in the partition named {@code partition}
     * alone. The applied scan of a deduplicating base reaches a later partition's file only
     * after it appended every row of the one before, and the raw-WAL drain never opens it.
     */
    void armAppliedScan(String partition) {
        armAppliedScan(partition, 0);
    }

    /**
     * Arms the open {@link #armAppliedScan(String)} fails, letting the first {@code skip} of
     * those opens through. A reader opens a partition's files once and keeps them, so each
     * open the fault skips is another reader's first look at the partition.
     */
    void armAppliedScan(String partition, int skip) {
        appliedScanPartition = partition;
        appliedScanSkip.set(skip);
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

    /**
     * Arms the failure of the next open of a live view's checkpoint timeline that the restore of
     * the root at the runtime frontier makes ahead of a segment loop, reporting {@code errno}.
     */
    void armFrontierRootRestoreOpen(int errno) {
        armAnchorRestoreOpen("restoreRuntimeAtFrontierRoot", errno, false);
    }

    void armTimelineOpen() {
        armTimelineOpen(CairoException.ERRNO_FILE_DOES_NOT_EXIST);
    }

    /**
     * Arms the open {@link #armTimelineOpen()} fails, and has it report {@code errno}.
     */
    void armTimelineOpen(int errno) {
        timelineOpenErrno = errno;
        isTimelineOpenPersistent.set(false);
        timelineOpenArmed.set(true);
    }

    /**
     * Arms the open {@link #armTimelineOpen(int)} fails for every open of a live view's checkpoint
     * timeline until {@link #disarmAll()}, each reporting {@code errno}.
     */
    void armTimelineOpenUntilDisarmed(int errno) {
        timelineOpenErrno = errno;
        isTimelineOpenPersistent.set(true);
        timelineOpenArmed.set(true);
    }

    /**
     * Arms {@code action} to run once, at the next check for a live view's checkpoint timeline
     * that a running recovery's restore makes ({@code tryRestoreRuntimeFromTimeline}), which goes
     * ahead afterwards. The restore reads the view's table right after that check.
     */
    void armRuntimeRestore(Runnable action) {
        runtimeRestoreAction.set(action);
    }

    void armViewStateOpen() {
        hasViewStateOpenFired.set(false);
        isViewStateOpenArmed.set(true);
    }

    /**
     * Arms the failure of the next open of the {@code account_id} symbol's key file in the table
     * under {@code viewDir}, a live view's own directory.
     */
    void armViewSymbolKeyOpen(String viewDir) {
        this.viewDir = viewDir;
        hasViewSymbolKeyOpenFired.set(false);
        isViewSymbolKeyOpenArmed.set(true);
    }

    /**
     * Withdraws a WAL read {@link #arm} armed and that no read has met yet, which is how a test
     * ends a fault it kept re-arming turn after turn.
     */
    void disarm() {
        breachTracker = null;
        countdown.set(-1);
    }

    /**
     * Withdraws every fault this class arms, fired or not. A fuzz test that arms one fault per
     * pass calls it once the pass is over, so a fault no read or open reached cannot fire later,
     * outside the pass that armed it.
     */
    void disarmAll() {
        disarm();
        appliedScanArmed.set(false);
        isViewStateOpenArmed.set(false);
        isViewSymbolKeyOpenArmed.set(false);
        runtimeRestoreAction.set(null);
        timelineOpenArmed.set(false);
        isTimelineOpenPersistent.set(false);
        isTimelineOpenErrnoPending.set(false);
        isAnchorRestoreOpenArmed.set(false);
        isAnchorRestoreOpenPersistent.set(false);
        isAnchorRestoreOpenErrnoPending.set(false);
    }

    FilesFacade facade() {
        return new TestFilesFacadeImpl() {
            @Override
            public int errno() {
                if (isAnchorRestoreOpenErrnoPending.compareAndSet(true, false)) {
                    return anchorRestoreOpenErrno;
                }
                if (isTimelineOpenErrnoPending.compareAndSet(true, false)) {
                    return timelineOpenErrno;
                }
                if (isMissingFileErrnoPending.compareAndSet(true, false)) {
                    return CairoException.ERRNO_FILE_DOES_NOT_EXIST;
                }
                return isReadErrnoPending.compareAndSet(true, false) ? readErrno : super.errno();
            }

            @Override
            public boolean exists(LPSZ path) {
                if (runtimeRestoreAction.get() != null
                        && Utf8s.endsWithAscii(path, LiveViewCheckpointLayout.TIMELINE_FILE_NAME)
                        && isRuntimeRestore()) {
                    final Runnable action = runtimeRestoreAction.getAndSet(null);
                    if (action != null) {
                        action.run();
                    }
                }
                return super.exists(path);
            }

            @Override
            public long openRO(LPSZ name) {
                final String dir = baseDir;
                if (appliedScanArmed.get()
                        && dir != null
                        && Utf8s.containsAscii(name, dir)
                        && !Utf8s.containsAscii(name, "wal")
                        && isInAppliedScanPartition(name)
                        && Utf8s.endsWithAscii(name, appliedScanFileName)
                        && appliedScanSkip.getAndDecrement() <= 0
                        && appliedScanArmed.compareAndSet(true, false)) {
                    appliedScanFired.set(true);
                    isMissingFileErrnoPending.set(true);
                    return -1;
                }
                if (countdown.get() >= 0
                        && dir != null
                        && Utf8s.containsAscii(name, dir)
                        && Utf8s.containsAscii(name, "wal")
                        && Utf8s.endsWithAscii(name, walReadFileName)) {
                    if (countdown.getAndDecrement() == 0) {
                        fired.set(true);
                        final MemoryTracker tracker = breachTracker;
                        if (tracker != null) {
                            breachTracker = null;
                            breach(tracker);
                        }
                        isReadErrnoPending.set(true);
                        return -1;
                    }
                }
                final String viewTableDir = viewDir;
                if (isViewSymbolKeyOpenArmed.get()
                        && viewTableDir != null
                        && Utf8s.containsAscii(name, viewTableDir)
                        && !Utf8s.containsAscii(name, "wal")
                        && Utf8s.endsWithAscii(name, viewSymbolKeyFileName)
                        && isViewSymbolKeyOpenArmed.compareAndSet(true, false)) {
                    hasViewSymbolKeyOpenFired.set(true);
                    isMissingFileErrnoPending.set(true);
                    return -1;
                }
                return super.openRO(name);
            }

            @Override
            public long openRW(LPSZ name, int opts) {
                if (isAnchorRestoreOpenArmed.get()
                        && Utf8s.endsWithAscii(name, LiveViewCheckpointLayout.TIMELINE_FILE_NAME)
                        && isAnchorRestore(anchorRestoreCaller)
                        && (isAnchorRestoreOpenPersistent.get() || isAnchorRestoreOpenArmed.compareAndSet(true, false))) {
                    anchorRestoreOpenFailures.incrementAndGet();
                    isAnchorRestoreOpenErrnoPending.set(true);
                    return -1;
                }
                if (timelineOpenArmed.get()
                        && Utf8s.endsWithAscii(name, LiveViewCheckpointLayout.TIMELINE_FILE_NAME)
                        && (isTimelineOpenPersistent.get() || timelineOpenArmed.compareAndSet(true, false))) {
                    timelineOpenFailures.incrementAndGet();
                    isTimelineOpenErrnoPending.set(true);
                    return -1;
                }
                if (isViewStateOpenArmed.get()
                        && Utf8s.endsWithAscii(name, LiveViewState.LIVE_VIEW_STATE_FILE_NAME)
                        && isViewStateOpenArmed.compareAndSet(true, false)) {
                    hasViewStateOpenFired.set(true);
                    isMissingFileErrnoPending.set(true);
                    return -1;
                }
                return super.openRW(name, opts);
            }
        };
    }

    int getAnchorRestoreOpenFailures() {
        return anchorRestoreOpenFailures.get();
    }

    int getTimelineOpenFailures() {
        return timelineOpenFailures.get();
    }

    boolean hasAppliedScanFired() {
        return appliedScanFired.get();
    }

    boolean hasFired() {
        return fired.get() && countdown.get() < 0;
    }

    boolean hasViewStateOpenFired() {
        return hasViewStateOpenFired.get();
    }

    boolean hasViewSymbolKeyOpenFired() {
        return hasViewSymbolKeyOpenFired.get();
    }

    boolean isAnchorRestoreOpenArmed() {
        return isAnchorRestoreOpenArmed.get();
    }

    boolean isTimelineOpenArmed() {
        return timelineOpenArmed.get();
    }

    void of(String baseDir) {
        this.baseDir = baseDir;
    }

    /**
     * Makes the failed WAL read report {@code errno} rather than a missing file, so the fault
     * reads as the failure it models - a read error a retry may get past, say.
     */
    void reportReadErrno(int errno) {
        readErrno = errno;
    }

    /**
     * Charges {@code tracker} one byte past what it has left, which it refuses with its
     * "query memory limit exceeded" error.
     */
    static void breach(MemoryTracker tracker) {
        try (MemoryCARW overflow = Vm.getCARWInstance(4096, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
            overflow.setMemoryTracker(tracker);
            overflow.extend(tracker.getLimit() - tracker.getUsed() + 1);
        }
        throw new AssertionError("the view's tracker admitted a charge past its limit");
    }

    // A restore of a timeline root by its exact key, the whole-runtime one or the key-scoped
    // one, made from the refresh job method named caller.
    private static boolean isAnchorRestore(String caller) {
        final List<String> methods = StackWalker.getInstance().walk(frames -> frames
                .filter(frame -> LiveViewRefreshJob.class.getName().equals(frame.getClassName()))
                .map(StackWalker.StackFrame::getMethodName)
                .toList());
        return methods.contains(caller)
                && (methods.contains("restoreAnchorRoot") || methods.contains("restoreAnchorRootKeys"));
    }

    private static boolean isRuntimeRestore() {
        return StackWalker.getInstance().walk(frames -> frames.anyMatch(
                frame -> LiveViewRefreshJob.class.getName().equals(frame.getClassName())
                        && "tryRestoreRuntimeFromTimeline".equals(frame.getMethodName())
        ));
    }

    // Sets the caller and the errno before it arms, so an open that sees the arm reads both.
    private void armAnchorRestoreOpen(String caller, int errno, boolean isPersistent) {
        anchorRestoreCaller = caller;
        anchorRestoreOpenErrno = errno;
        isAnchorRestoreOpenPersistent.set(isPersistent);
        isAnchorRestoreOpenArmed.set(true);
    }

    // Read behind the armed check: armAppliedScan sets the partition before it arms.
    private boolean isInAppliedScanPartition(LPSZ name) {
        final String partition = appliedScanPartition;
        return partition == null || Utf8s.containsAscii(name, partition);
    }
}
