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

package io.questdb.cairo.lv;

import org.jetbrains.annotations.Nullable;

/**
 * Named points inside {@link LiveViewRefreshJob} where a test injects a fault, a race or a
 * behaviour change. Each method is called at exactly one position, or at the positions its
 * javadoc names, and a test relies on that position, so a call must not move. Production runs
 * {@link #NONE}: every point does nothing, and every toggle reads false.
 */
public interface LiveViewRefreshHooks {
    LiveViewRefreshHooks NONE = new LiveViewRefreshHooks() {
    };

    /**
     * Called by the refresh turn's finally right after it frees the staging buffer, inside
     * the try whose finally releases the refresh latch.
     */
    default void afterStagingBufferClose() {
    }

    /**
     * Called by a cold keyed head-miss right before it rebases its captured boundaries' row
     * positions, ahead of the replay's own try.
     */
    default void beforeColdKeyedRowPositionRebase() {
    }

    /**
     * Called by a keyed repair's hand-back right before it restores each frozen key into the
     * primary runtime, with that key's position in the hand-back. Position 0 follows
     * {@link #beforeKeyedTransplantRestore()}; at a higher position every key below it is
     * already restored.
     */
    default void beforeKeyedTransplantEntry(int entryIndex) {
    }

    /**
     * Called by a keyed repair's hand-back after it froze the isolated runtime's keys and
     * before it restores the first of them into the primary runtime.
     */
    default void beforeKeyedTransplantRestore() {
    }

    /**
     * Called by the WAL-loss re-derive after its entry broken-dependency check and before
     * its replay, with the refresh latch held, inside the try whose catch logs and refuses.
     */
    default void beforeRederiveReplay() {
    }

    /**
     * Called by a refresh turn after every guard it evaluates before the refresh latch and
     * before it tries the latch, with no latch held.
     */
    default void beforeRefreshLatch() {
    }

    /**
     * Called by a predecessor resume once its anchor is restored and its cursors are open,
     * immediately before its replay pulls the first row.
     */
    default void beforeResumeReplay() {
    }

    /**
     * Called by an open-segment keyed route's count of the view's stored rows once it holds
     * the view's reader and before it counts.
     */
    default void beforeStoredRowCount() {
    }

    /**
     * Called by the fallback scan's ahead guard between its read of the view watermark and
     * its two reads of the base head, with no latch held.
     */
    default void betweenAheadGuardReads() {
    }

    /**
     * @return true to commit a forward live-view block at the default dedup mode instead of
     * {@code NO_DEDUP} on a dedup-keyed view
     */
    default boolean isForwardCommitDedupCollapsed() {
        return false;
    }

    /**
     * @return true to take the open-segment keyed route whatever its scan-row price says;
     * every other eligibility check still applies
     */
    default boolean isOpenSegmentKeyedReplayForced() {
        return false;
    }

    /**
     * @return true to skip the inline apply of an out-of-order repair's own replacement
     * block, leaving it committed but unapplied
     */
    default boolean isRepairApplySkipped() {
        return false;
    }

    /**
     * @return true to publish a lead drain whose symbol-id rewind left stranded ids in place
     * as a reader-visible lead, instead of sending it to disk or keeping it from readers
     */
    default boolean isStrandedIdLeadPublished() {
        return false;
    }

    /**
     * Called twice by the re-derive's broken-dependency check: as the primary failure of the
     * statement that closes the pooled base metadata, and in the catch of a failed metadata
     * read, which never reaches that statement.
     *
     * @return the failure to report as the close's own, or null
     */
    default @Nullable Throwable onBaseMetadataClose() {
        return null;
    }

    /**
     * Called by a keyed repair's hand-back as its last step, after its retention trims and
     * its count of handed-back keys. A hand-back that throws does not reach it.
     */
    default void onKeyedTransplantEnd() {
    }

    /**
     * Called by a keyed repair's hand-back once it has both windows to move state between,
     * before it clears its lists and arenas.
     */
    default void onKeyedTransplantStart() {
    }

    /**
     * Called by the refresh failure handler right after it counts a faulting turn on the
     * view, before any of its branches swallows, recovers from or charges the fault, with
     * the throwable the turn failed with. It must not throw.
     */
    default void onRefreshFault(Throwable t) {
    }

    /**
     * Called at the tail of every repair cleanup chain, after every borrowed resource is
     * back and immediately before the session release the chain ends with. A head-miss turn
     * runs two chains, its replay cleanup and its publication tail, and so does an anchor
     * resume.
     */
    default void onRepairCleanupTail() {
    }

    /**
     * Called by the cleanup of an unwinding head-miss turn after it freed the staged capture
     * and retired the descriptor, inside the try whose catch folds its failure.
     */
    default void onRepairUnwindCleanup() {
    }
}
