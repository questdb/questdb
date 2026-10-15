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
import io.questdb.cairo.lv.LiveViewRefreshHooks;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.Nullable;

import java.util.function.Consumer;

/**
 * The refresh job's fault, race and behaviour points, armed by tests. One-shot points disarm
 * as they fire; sticky ones stay until a test clears them. Each fault throws what the code it
 * stands in for throws at that point.
 */
public final class LiveViewTestRefreshHooks implements LiveViewRefreshHooks {
    private Runnable baseApplyDuringRederive;
    private Runnable baseCommitBetweenAheadGuardReads;
    private QuietCloseable baseMetadataCloseFault;
    private boolean isColdKeyedTimelineFaultArmed;
    private boolean isForwardCommitDedupCollapsed;
    private boolean isKeyedTransplantFaultArmed;
    private boolean isOpenSegmentKeyedReplayForced;
    private boolean isRepairApplySkipped;
    private boolean isRepairUnwindCleanupFaultArmed;
    private boolean isStagingBufferCloseFaultArmed;
    private boolean isStoredSuffixCountFaultArmed;
    private boolean isStrandedIdLeadPublished;
    private Runnable keyedTransplantEndObserver;
    // The view tracker the armed hand-back entry breaches; null throws a critical error instead.
    private MemoryTracker keyedTransplantEntryBreachTracker;
    // The hand-back position whose restore throws; -1 is disarmed.
    private int keyedTransplantEntryFaultIndex = -1;
    private Runnable keyedTransplantStartObserver;
    private Runnable peerTurnBeforeRefreshLatch;
    private Consumer<Throwable> refreshFaultObserver;
    // Repair cleanup chains to let through before the next one throws; -1 is disarmed.
    private int repairCleanupFaultCountdown = -1;
    private Runnable resumeReplayStart;

    /**
     * Returns the hooks installed on {@code job}, installing new ones first if it runs others.
     */
    public static LiveViewTestRefreshHooks install(LiveViewRefreshJob job) {
        if (job.getHooksForTest() instanceof LiveViewTestRefreshHooks hooks) {
            return hooks;
        }
        final LiveViewTestRefreshHooks hooks = new LiveViewTestRefreshHooks();
        job.setHooksForTest(hooks);
        return hooks;
    }

    @Override
    public void afterStagingBufferClose() {
        if (isStagingBufferCloseFaultArmed) {
            isStagingBufferCloseFaultArmed = false;
            // Stands in for a native-memory or tracker-balance assert in
            // LiveViewInMemoryBuffer.close() under -ea.
            throw new AssertionError("injected staging-buffer close fault");
        }
    }

    @Override
    public void beforeColdKeyedRowPositionRebase() {
        if (isColdKeyedTimelineFaultArmed) {
            isColdKeyedTimelineFaultArmed = false;
            // What RepairCapture.collectEffectiveRowPositions raises over a missing or torn
            // metadata page. No FilesFacade fault reaches that call: the repair capture drops
            // itself on an I/O failure, and the call reads no file when the pinned generation
            // carries no row-position delta.
            throw CairoException.critical(0).put("simulated live view checkpoint row position fault");
        }
    }

    @Override
    public void beforeKeyedTransplantEntry(int entryIndex) {
        if (keyedTransplantEntryFaultIndex >= 0 && entryIndex == keyedTransplantEntryFaultIndex) {
            keyedTransplantEntryFaultIndex = -1;
            final MemoryTracker tracker = keyedTransplantEntryBreachTracker;
            keyedTransplantEntryBreachTracker = null;
            if (tracker != null) {
                // What the view's own tracker raises for a restore that grows the primary's
                // map past the view's refresh memory limit.
                LiveViewMidDrainFault.breach(tracker);
            }
            // Stands in for any other failure of one key's restore, which the refresh
            // failure handler retries.
            throw CairoException.critical(0).put("simulated live view keyed repair transplant entry fault");
        }
    }

    @Override
    public void beforeKeyedTransplantRestore() {
        if (isKeyedTransplantFaultArmed) {
            isKeyedTransplantFaultArmed = false;
            // Stands in for a critical failure after the freeze and before any key moves,
            // which the refresh failure handler retries. It is not a breach of the view's
            // refresh memory limit: a breach invalidates the view, and
            // setSimulateKeyedTransplantEntryFault arms that one.
            throw CairoException.critical(0).put("simulated live view keyed repair transplant fault");
        }
    }

    @Override
    public void beforeRederiveReplay() {
        final Runnable action = baseApplyDuringRederive;
        if (action != null) {
            baseApplyDuringRederive = null;
            action.run();
        }
    }

    @Override
    public void beforeRefreshLatch() {
        final Runnable action = peerTurnBeforeRefreshLatch;
        if (action != null) {
            peerTurnBeforeRefreshLatch = null;
            action.run();
        }
    }

    @Override
    public void beforeResumeReplay() {
        final Runnable action = resumeReplayStart;
        if (action != null) {
            resumeReplayStart = null;
            action.run();
        }
    }

    @Override
    public void beforeStoredRowCount() {
        if (isStoredSuffixCountFaultArmed) {
            isStoredSuffixCountFaultArmed = false;
            // No FilesFacade fault reaches this count deterministically: whether it opens a file
            // depends on what the pooled view reader and the pinned base reader hold open.
            throw CairoException.critical(0).put("simulated live view stored row count fault");
        }
    }

    @Override
    public void betweenAheadGuardReads() {
        final Runnable action = baseCommitBetweenAheadGuardReads;
        if (action != null) {
            baseCommitBetweenAheadGuardReads = null;
            action.run();
        }
    }

    /**
     * Withdraws every fault this class can arm, fired or not, and leaves the behaviour
     * toggles, the observers and the one-shot actions as they are. A fuzz test that arms
     * one fault per pass calls it once the pass is over, so a fault no turn reached cannot
     * fire later, outside the pass that armed it.
     */
    public void clearFaults() {
        isColdKeyedTimelineFaultArmed = false;
        isKeyedTransplantFaultArmed = false;
        keyedTransplantEntryBreachTracker = null;
        keyedTransplantEntryFaultIndex = -1;
        isRepairApplySkipped = false;
        isRepairUnwindCleanupFaultArmed = false;
        isStagingBufferCloseFaultArmed = false;
        isStoredSuffixCountFaultArmed = false;
        repairCleanupFaultCountdown = -1;
    }

    public boolean isColdKeyedTimelineFaultArmed() {
        return isColdKeyedTimelineFaultArmed;
    }

    @Override
    public boolean isForwardCommitDedupCollapsed() {
        return isForwardCommitDedupCollapsed;
    }

    public boolean isKeyedTransplantEntryFaultArmed() {
        return keyedTransplantEntryFaultIndex >= 0;
    }

    public boolean isKeyedTransplantFaultArmed() {
        return isKeyedTransplantFaultArmed;
    }

    @Override
    public boolean isOpenSegmentKeyedReplayForced() {
        return isOpenSegmentKeyedReplayForced;
    }

    @Override
    public boolean isRepairApplySkipped() {
        return isRepairApplySkipped;
    }

    /**
     * @return whether the fault {@link #setSimulateRepairCleanupFault(int)} armed is still
     * waiting; false proves the named chain ran
     */
    public boolean isRepairCleanupFaultArmed() {
        return repairCleanupFaultCountdown >= 0;
    }

    public boolean isRepairUnwindCleanupFaultArmed() {
        return isRepairUnwindCleanupFaultArmed;
    }

    public boolean isStagingBufferCloseFaultArmed() {
        return isStagingBufferCloseFaultArmed;
    }

    public boolean isStoredSuffixCountFaultArmed() {
        return isStoredSuffixCountFaultArmed;
    }

    @Override
    public boolean isStrandedIdLeadPublished() {
        return isStrandedIdLeadPublished;
    }

    @Override
    public @Nullable Throwable onBaseMetadataClose() {
        final QuietCloseable closeFault = baseMetadataCloseFault;
        if (closeFault == null) {
            return null;
        }
        baseMetadataCloseFault = null;
        return Misc.freeBestEffort(null, closeFault);
    }

    @Override
    public void onKeyedTransplantEnd() {
        if (keyedTransplantEndObserver != null) {
            keyedTransplantEndObserver.run();
        }
    }

    @Override
    public void onKeyedTransplantStart() {
        if (keyedTransplantStartObserver != null) {
            keyedTransplantStartObserver.run();
        }
    }

    @Override
    public void onRefreshFault(Throwable t) {
        final Consumer<Throwable> observer = refreshFaultObserver;
        if (observer != null) {
            observer.accept(t);
        }
    }

    @Override
    public void onRepairCleanupTail() {
        if (repairCleanupFaultCountdown >= 0 && repairCleanupFaultCountdown-- == 0) {
            throw new AssertionError("injected repair cleanup fault");
        }
    }

    @Override
    public void onRepairUnwindCleanup() {
        if (isRepairUnwindCleanupFaultArmed) {
            isRepairUnwindCleanupFaultArmed = false;
            throw new AssertionError("injected repair unwind cleanup fault");
        }
    }

    public void setForceOpenSegmentKeyedReplay(boolean force) {
        isOpenSegmentKeyedReplayForced = force;
    }

    /**
     * Sticky observers for the keyed repair's hand-back. Nulls remove them.
     */
    public void setKeyedTransplantObserver(@Nullable Runnable onStart, @Nullable Runnable onEnd) {
        keyedTransplantStartObserver = onStart;
        keyedTransplantEndObserver = onEnd;
    }

    /**
     * One-shot. The action runs with the refresh latch held, inside the try whose catch logs
     * and refuses, so it must catch its own throwables, and it must not reach DROP LIVE VIEW
     * or a checkpoint freeze, which wait for that latch.
     */
    /**
     * Sticky: {@code observer} sees every throwable a turn of this job faults with, in the
     * order the job counts them. It runs inside the job's failure handler, so it must record
     * and return, never throw. Null removes it.
     */
    public void setRefreshFaultObserver(@Nullable Consumer<Throwable> observer) {
        refreshFaultObserver = observer;
    }

    public void setSimulateBaseApplyDuringRederive(Runnable action) {
        baseApplyDuringRederive = action;
    }

    /**
     * One-shot. The action runs with no latch held and no enclosing catch.
     */
    public void setSimulateBaseCommitBetweenAheadGuardReads(Runnable action) {
        baseCommitBetweenAheadGuardReads = action;
    }

    /**
     * One-shot: the closeable's close() failure becomes the pooled base metadata close's own.
     */
    public void setSimulateBaseMetadataCloseFailure(QuietCloseable closeFault) {
        baseMetadataCloseFault = closeFault;
    }

    public void setSimulateColdKeyedTimelineFault(boolean simulate) {
        isColdKeyedTimelineFaultArmed = simulate;
    }

    public void setSimulateForwardCommitDedupCollapse(boolean simulate) {
        isForwardCommitDedupCollapsed = simulate;
    }

    /**
     * One-shot: the next hand-back fails restoring the key at {@code entryIndex}, with every
     * key below it already restored into the primary runtime. With {@code breachTracker} - the
     * view's own refresh memory tracker - the restore breaches it; without one it throws a
     * critical error.
     */
    public void setSimulateKeyedTransplantEntryFault(int entryIndex, @Nullable MemoryTracker breachTracker) {
        keyedTransplantEntryBreachTracker = breachTracker;
        keyedTransplantEntryFaultIndex = entryIndex;
    }

    public void setSimulateKeyedTransplantFault(boolean simulate) {
        isKeyedTransplantFaultArmed = simulate;
    }

    /**
     * One-shot. The action runs with no latch held and no enclosing catch, so it may drive
     * another job's turn over the same view.
     */
    public void setSimulatePeerTurnBeforeRefreshLatch(Runnable action) {
        peerTurnBeforeRefreshLatch = action;
    }

    public void setSimulateRepairApplyFailure(boolean simulate) {
        isRepairApplySkipped = simulate;
    }

    /**
     * One-shot. {@code skipChains} names the chain that throws: 0 is the next one the job runs,
     * 1 the one after it.
     */
    public void setSimulateRepairCleanupFault(int skipChains) {
        repairCleanupFaultCountdown = skipChains;
    }

    public void setSimulateRepairUnwindCleanupFault() {
        isRepairUnwindCleanupFaultArmed = true;
    }

    public void setSimulateResumeReplayStart(Runnable action) {
        resumeReplayStart = action;
    }

    public void setSimulateStagingBufferCloseFault() {
        isStagingBufferCloseFaultArmed = true;
    }

    public void setSimulateStoredSuffixCountFault() {
        isStoredSuffixCountFaultArmed = true;
    }

    public void setSimulateStrandedIdLead(boolean simulate) {
        isStrandedIdLeadPublished = simulate;
    }
}
