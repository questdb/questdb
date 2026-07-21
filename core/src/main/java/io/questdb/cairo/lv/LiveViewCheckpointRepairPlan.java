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

package io.questdb.cairo.lv;

import io.questdb.std.Numbers;
import org.jetbrains.annotations.NotNull;

/**
 * Everything one out-of-order repair decides before it touches anything: the
 * pinned base snapshot it works against, the change-set coordinates derived from
 * that snapshot, and which executor runs. This is the plan half of design
 * section 12.1 ("pin and classify"), split out of the execution so both repair
 * executors read one decision instead of re-deriving their own.
 * <p>
 * The caller pins exactly one applied base reader for the whole repair and hands
 * its {@code seqTxn} in as {@code pinnedSeqTxn} ({@code E}). Planning against one
 * pinned snapshot is what makes the coordinates below mutually consistent: a
 * second reader opened later could sit at a different {@code seqTxn}, and the
 * bounds derived here would no longer describe the data the replay reads.
 * <p>
 * The coordinates, in the design's terms:
 * <ul>
 *     <li>{@code C} - {@link #getCorrectionTs() correctionTs}, the earliest
 *     timestamp whose existing live-view output may have changed: the triggering
 *     commit's lowest touched timestamp clamped up to the view's {@code START
 *     FROM} boundary {@code S}. {@link Numbers#LONG_NULL} for a non-DATA or
 *     recovery trigger, which authorises no deletion at all.</li>
 *     <li>{@code E} - {@link #getPinnedSeqTxn() pinnedSeqTxn}, the applied base
 *     {@code seqTxn} the repair materialises and commits at.</li>
 *     <li>{@link #getRetireLowTs() retireLowTs} - the floor at or above which
 *     this repair unseals retained checkpoints. Equal to {@code C} on the common
 *     path; lowered to the apply-ahead range's minimum in-view timestamp when
 *     {@code ApplyWal2TableJob} raced the reader past the trigger, and
 *     {@code LONG_NULL} (retire everything) when that range is unclassifiable.</li>
 *     <li>{@link #getReplayLowTs() replayLowTs} - the inclusive timestamp the
 *     replay scans and replaces from. A resume floors it at the anchor's
 *     {@code maxTs + 1}; a boundary rebuild floors it at {@code S}.</li>
 * </ul>
 * Steps 4-6 of the design's phase 5 extend this plan with the tagged high bound
 * {@code H}, the dependency floor {@code L}, the output floor {@code R}, and the
 * affected/output key domains {@code A}/{@code Q}. Until then the high bound is
 * implicitly {@code EOF}: both executors replace through positive infinity.
 * <p>
 * One instance per refresh job, reused across repairs - {@link #of} overwrites
 * every field, so no reset is needed between plans.
 */
public final class LiveViewCheckpointRepairPlan {
    /**
     * The repair recomputes the whole view from the {@code START FROM} boundary.
     * The residual O(view age) fallback: no sealed anchor sits below the change,
     * the trigger carries no timestamp to search with, or the apply-ahead range
     * cannot be classified.
     */
    public static final int DISPOSITION_BOUNDARY_REBUILD = 1;
    /**
     * The repair rolls state back to a sealed anchor strictly below the change
     * and replays only the tail above it.
     */
    public static final int DISPOSITION_RESUME_FROM_ANCHOR = 2;
    private long anchorLvSeqTxn;
    private long anchorMaxTs;
    private long applyAheadMinTs;
    private long correctionTs;
    private int disposition;
    private long pinnedSeqTxn;
    private long replayLowTs;
    private long retireLowTs;
    private long triggerSeqTxn;

    /**
     * Whether the caller must classify the apply-ahead range
     * {@code (triggerSeqTxn, pinnedSeqTxn]} before planning. Classification reads
     * the base WAL-E files, so {@link #of} takes its result rather than performing
     * it; this predicate keeps the two in step.
     * <p>
     * The range matters only when {@code ApplyWal2TableJob} has raced the pinned
     * reader past the trigger AND the trigger carries a timestamp: a non-DATA or
     * recovery trigger already retires every anchor and rebuilds, so nothing the
     * classification could report changes the plan.
     */
    public static boolean isApplyAheadClassificationRequired(long lateRowTs, long triggerSeqTxn, long pinnedSeqTxn) {
        return pinnedSeqTxn != triggerSeqTxn && lateRowTs != Numbers.LONG_NULL;
    }

    /**
     * @return the {@code lvSeqTxn} key of the checkpoint the resume restores from.
     * Meaningful only for {@link #DISPOSITION_RESUME_FROM_ANCHOR}.
     */
    public long getAnchorLvSeqTxn() {
        return anchorLvSeqTxn;
    }

    /**
     * @return the maximum base-row timestamp the resume anchor covers. Meaningful
     * only for {@link #DISPOSITION_RESUME_FROM_ANCHOR}.
     */
    public long getAnchorMaxTs() {
        return anchorMaxTs;
    }

    /**
     * @return the minimum in-view timestamp the apply-ahead range holds,
     * {@link Numbers#LONG_NULL} when the range is unclassifiable or was not
     * classified. Diagnostic; the planning result it feeds is
     * {@link #getRetireLowTs()}.
     */
    public long getApplyAheadMinTs() {
        return applyAheadMinTs;
    }

    /**
     * @return the base {@code seqTxn} the repair commits at and advances its
     * watermarks to. Always the pinned snapshot's {@code seqTxn}: the replay
     * materialises everything that snapshot holds, including any transaction
     * apply raced past the trigger.
     */
    public long getCommitSeqTxn() {
        return pinnedSeqTxn;
    }

    /**
     * @return {@code C}: the earliest timestamp whose existing output may have
     * changed, clamped up to the view's {@code START FROM} boundary, or
     * {@link Numbers#LONG_NULL} for a non-DATA / recovery trigger.
     */
    public long getCorrectionTs() {
        return correctionTs;
    }

    public int getDisposition() {
        return disposition;
    }

    /**
     * @return {@code E}: the applied base {@code seqTxn} of the pinned reader.
     */
    public long getPinnedSeqTxn() {
        return pinnedSeqTxn;
    }

    /**
     * @return the inclusive timestamp the replay scans and replaces from. Never
     * overflows for a resume: every anchor sits strictly below a real timestamp,
     * so {@code anchorMaxTs + 1} stays representable.
     */
    public long getReplayLowTs() {
        return replayLowTs;
    }

    /**
     * @return the floor at or above which this repair unseals retained
     * checkpoints, or {@link Numbers#LONG_NULL} to retire all of them.
     */
    public long getRetireLowTs() {
        return retireLowTs;
    }

    /**
     * @return the base {@code seqTxn} that triggered the repair. Below
     * {@link #getPinnedSeqTxn()} exactly when apply raced ahead of it.
     */
    public long getTriggerSeqTxn() {
        return triggerSeqTxn;
    }

    /**
     * @return true when apply advanced the base table past the trigger before the
     * repair pinned its reader.
     */
    public boolean isApplyAhead() {
        return pinnedSeqTxn != triggerSeqTxn;
    }

    public boolean isResumeFromAnchor() {
        return disposition == DISPOSITION_RESUME_FROM_ANCHOR;
    }

    /**
     * Classifies one out-of-order change against one pinned base snapshot and
     * selects the executor.
     * <p>
     * Anchor selection runs in two steps, mirroring the two ways a resume can be
     * defeated:
     * <ol>
     *     <li>The newest sealed anchor is the head, so a head strictly below the
     *     late row anchors the resume directly (a "head hit"). A head at or above
     *     the late row cannot - its state already incorporates rows the change
     *     invalidates - so the search falls back to the older retained anchors
     *     below the raw trigger timestamp.</li>
     *     <li>When apply raced ahead, the anchor must additionally sit below every
     *     timestamp those un-examined transactions hold: the resume only re-reads
     *     base above the anchor, so a back-dated row below it would be dropped and
     *     advancing the watermark over it would lose it permanently. The plan
     *     therefore re-searches below {@code min(C, applyAheadMinTs)} and rebuilds
     *     from the boundary when nothing qualifies.</li>
     * </ol>
     * A repair with no usable anchor rebuilds from the boundary, which reads the
     * whole snapshot and therefore needs no such guarantee.
     *
     * @param anchors                  sealed checkpoints the resume may roll back
     *                                 to, newest last
     * @param lateRowTs                lowest timestamp the triggering DATA commit
     *                                 touched, or {@link Numbers#LONG_NULL} for a
     *                                 non-DATA / recovery trigger
     * @param viewLowerBoundTimestamp  the view's {@code START FROM} boundary
     *                                 {@code S}
     * @param triggerSeqTxn            base {@code seqTxn} that triggered the repair
     * @param pinnedSeqTxn             {@code seqTxn} of the pinned base reader
     *                                 ({@code E}); never below
     *                                 {@code triggerSeqTxn}
     * @param headLvSeqTxn             head checkpoint key, or
     *                                 {@link Numbers#LONG_NULL} when there is no head
     * @param headMaxTs                head checkpoint maximum timestamp, or
     *                                 {@link Numbers#LONG_NULL}
     * @param applyAheadMinTs          minimum in-view timestamp of the apply-ahead
     *                                 range, or {@link Numbers#LONG_NULL} when that
     *                                 range is unclassifiable; ignored unless
     *                                 {@link #isApplyAheadClassificationRequired}
     *                                 holds
     */
    public void of(
            @NotNull AnchorSource anchors,
            long lateRowTs,
            long viewLowerBoundTimestamp,
            long triggerSeqTxn,
            long pinnedSeqTxn,
            long headLvSeqTxn,
            long headMaxTs,
            long applyAheadMinTs
    ) {
        assert pinnedSeqTxn >= triggerSeqTxn : "pinned base snapshot is below the trigger";
        this.triggerSeqTxn = triggerSeqTxn;
        this.pinnedSeqTxn = pinnedSeqTxn;
        // C: the trigger's authority to delete and to unseal, expressed in the
        // view's own coordinate space. A commit routinely reaches below the view's
        // boundary - those rows are simply not the view's - so the raw trigger is
        // clamped up; for a BEGINNING view the clamp is an identity. A non-DATA /
        // recovery trigger carries no timestamp and authorises no deletion.
        correctionTs = lateRowTs == Numbers.LONG_NULL
                ? Numbers.LONG_NULL
                : Math.max(lateRowTs, viewLowerBoundTimestamp);
        final boolean applyAhead = pinnedSeqTxn != triggerSeqTxn;
        final boolean classified = isApplyAheadClassificationRequired(lateRowTs, triggerSeqTxn, pinnedSeqTxn);
        this.applyAheadMinTs = classified ? applyAheadMinTs : Numbers.LONG_NULL;
        // The retire floor doubles as the resume ceiling: an anchor below it is
        // sealed against everything this repair incorporates. Without apply-ahead
        // it is C. With it, an unclassifiable range (a structural or non-DATA
        // commit, or no DATA commit at all) leaves LONG_NULL, which retires the
        // whole ring and denies every anchor.
        if (classified) {
            retireLowTs = this.applyAheadMinTs == Numbers.LONG_NULL
                    ? Numbers.LONG_NULL
                    : Math.min(correctionTs, this.applyAheadMinTs);
        } else {
            retireLowTs = correctionTs;
        }

        // A head with no maxTs cannot anchor anything: the resume floors at
        // maxTs + 1, and LONG_NULL + 1 would admit every base row, including rows
        // below the view's boundary. The strict comparison against the late row is
        // load-bearing too - the head covers rows up to AND INCLUDING its maxTs
        // while the resume starts above it, so a late row at exactly headMaxTs
        // would be neither covered nor re-read.
        final boolean headHit = headLvSeqTxn != Numbers.LONG_NULL
                && headMaxTs != Numbers.LONG_NULL
                && lateRowTs != Numbers.LONG_NULL
                && headMaxTs < lateRowTs;
        boolean hasAnchor = headHit;
        anchorLvSeqTxn = headLvSeqTxn;
        anchorMaxTs = headMaxTs;
        if (!headHit && lateRowTs != Numbers.LONG_NULL) {
            // Bounded miss: the head sits at or above the late row, but an older
            // sealed anchor may still be strictly below it. Search on the raw
            // trigger timestamp - the boundary clamp governs deletion authority,
            // not which state a resume may trust.
            final int index = anchors.findAnchorBelow(lateRowTs);
            if (index >= 0) {
                hasAnchor = true;
                anchorLvSeqTxn = anchors.getAnchorLvSeqTxn(index);
                anchorMaxTs = anchors.getAnchorMaxTs(index);
            }
        }
        if (hasAnchor && applyAhead) {
            // Re-anchor below the ahead range's floor, which is at or below C, so
            // this can only move the anchor down or reject it outright.
            final int index = retireLowTs == Numbers.LONG_NULL ? -1 : anchors.findAnchorBelow(retireLowTs);
            hasAnchor = index >= 0;
            if (hasAnchor) {
                anchorLvSeqTxn = anchors.getAnchorLvSeqTxn(index);
                anchorMaxTs = anchors.getAnchorMaxTs(index);
            }
        }
        if (hasAnchor) {
            disposition = DISPOSITION_RESUME_FROM_ANCHOR;
            // The anchor's state already covers rows up to and including its
            // maxTs, so the replay starts strictly above it. Floored at the view's
            // boundary so the resume applies the same row predicate as the seed,
            // the forward drain and the boundary rebuild.
            replayLowTs = Math.max(anchorMaxTs + 1, viewLowerBoundTimestamp);
        } else {
            disposition = DISPOSITION_BOUNDARY_REBUILD;
            anchorLvSeqTxn = Numbers.LONG_NULL;
            anchorMaxTs = Numbers.LONG_NULL;
            replayLowTs = viewLowerBoundTimestamp;
        }
    }

    /**
     * The sealed checkpoints a resume may roll back to, ordered by ascending
     * {@code maxTs} with the newest (the head) last.
     * <p>
     * {@link LiveViewInstance} implements this over the retained-checkpoint ring.
     * The design's versioned timeline replaces that ring with a logarithmic
     * predecessor lookup over permanently retained roots; it substitutes behind
     * these three methods.
     */
    public interface AnchorSource {
        /**
         * @return the index of the newest anchor whose {@code maxTs} is strictly
         * below {@code ceilTs}, or {@code -1} when every anchor sits at or above
         * it. The strict inequality preserves a complete timestamp tie: an anchor
         * at exactly {@code ceilTs} covers only part of the rows at that
         * timestamp.
         */
        int findAnchorBelow(long ceilTs);

        long getAnchorLvSeqTxn(int index);

        long getAnchorMaxTs(int index);
    }
}
