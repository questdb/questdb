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

import io.questdb.cairo.lv.LiveViewCheckpointContracts.HighBoundTag;
import io.questdb.griffin.SqlException;
import io.questdb.std.Numbers;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Everything one out-of-order repair decides before it touches anything: the
 * pinned base snapshot it works against, the change-set coordinates derived
 * from that snapshot, and which executor runs. This is the pin-and-classify
 * half of a repair, split out of the execution so both repair executors read
 * one decision instead of re-deriving their own.
 * <p>
 * The caller pins exactly one applied base reader for the whole repair and hands
 * its {@code seqTxn} in as {@code pinnedSeqTxn} ({@code E}). Planning against one
 * pinned snapshot is what makes the coordinates below mutually consistent: a
 * second reader opened later could sit at a different {@code seqTxn}, and the
 * bounds derived here would no longer describe the data the replay reads.
 * <p>
 * The coordinates:
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
 *     <li>{@code L} - {@link #getReplayLowTs() replayLowTs}, the inclusive
 *     timestamp the replay <b>scans</b> from: the earliest row needed to
 *     reconstruct state immediately before the output floor. A resume floors it at
 *     the anchor's {@code maxTs + 1}; a boundary rebuild floors it at whatever the
 *     view's finite dependency proves - {@code max(S, R - W)} for a bounded RANGE
 *     view, a discovered per-key floor for a bounded ROWS one, the start of
 *     {@code R}'s segment for an anchored one - and at {@code S} when the view
 *     carries none.</li>
 *     <li>{@code R} - {@link #getOutputLowTs() outputLowTs}, the inclusive
 *     timestamp the replay <b>emits and replaces</b> from. Never below {@code L}:
 *     rows in {@code [L, R)} are fed to warm the window state up and produce no
 *     output, because their durable output is already correct.</li>
 *     <li>{@code H} - {@link #getHighBoundTag() highBoundTag} plus
 *     {@link #getHighTsExclusive() highTsExclusive}, the tagged exclusive bound
 *     after which every eligible function has converged. Finite for a view whose
 *     dependency proves one and whose pre-repair runtime state survives the repair -
 *     {@code changeMaxTs + W + 1} for a bounded RANGE view, the discovered
 *     convergence timestamp for a bounded ROWS one, the end of
 *     {@code changeMaxTs}'s segment for an anchored one - and
 *     {@link HighBoundTag#EOF} otherwise.</li>
 * </ul>
 * The high bound is tagged rather than a bare {@code long} because no timestamp
 * value can also mean infinity: an exclusive bound one past
 * {@code Long.MAX_VALUE} is not representable, and spelling it
 * {@code Long.MAX_VALUE} would exclude a row sitting there. Its one consumer
 * today is {@link #getScanHighTsInclusive()}, which both executors hand to the
 * bounded forward page-frame cursor; an {@code EOF} plan therefore scans
 * through positive infinity exactly as an unbounded scan did.
 * <p>
 * The {@code L}/{@code R} split is what makes a correction older than every
 * sealed anchor local. A boundary rebuild has no anchor to restore from, but a
 * bounded {@code RANGE W PRECEDING ... CURRENT ROW} view needs none: the state
 * a row at {@code R} sees is exactly the rows in {@code [R - W, R]}, so
 * replaying from {@code L = R - W} reconstructs it without reading a single row
 * below that. The finite dependency, rather than checkpoint availability,
 * provides the lower bound. {@link #isLocalized()} reports when that applies;
 * without it the two floors collapse to {@code S} and the rebuild reads the
 * whole view history as before.
 * <p>
 * The same width bounds the repair from above. A row at {@code m} sits in the frame
 * of every row in {@code [m, m + W]} and in no other, so a change whose highest
 * touched timestamp is {@code changeMaxTs} cannot reach output at or above
 * {@code changeMaxTs + W + 1}. That is {@code H}, and because every incorporated
 * change sits in {@code [R, H)} by construction, the durable output above {@code H}
 * stays correct while the watermark advances over the whole pinned snapshot.
 * <p>
 * A bounded {@code ROWS N PRECEDING ... CURRENT ROW} view reaches the same two
 * bounds by a different route. {@code Nmax} counts rows of one partition key, so
 * neither bound has a closed form: how far back a key's {@code Nmax} predecessors
 * sit, and how far above the change its output stops moving, are properties of
 * where that key's rows actually are. The plan therefore hands the floor {@code R}
 * to a {@link RowsBoundSource} and reads {@code L} and {@code H} off the discovery
 * it runs over the same pinned snapshot. Two conditions the RANGE path does not
 * carry gate that call:
 * <ul>
 *     <li>the incorporated change set must be provably insert-only. The discovery
 *     reads the affected key domain {@code A} off the post-change snapshot, so a
 *     deletion that emptied a key out of the change interval would leave it
 *     invisible there while its later rows still pull older history into their
 *     frames. RANGE needs no such proof - its bound is key-independent arithmetic
 *     over an interval a deletion cannot escape.</li>
 *     <li>{@code H} must come back {@link HighBoundTag#FINITE}. A ROWS frame never
 *     expires by time, so a key whose rows all sit below {@code L} holds state a
 *     replay from {@code L} cannot reconstruct. A finite {@code H} keeps the
 *     pre-repair runtime state (see {@link #isRuntimeStatePreserved()}) and the
 *     question does not arise; an {@code EOF} one would promote what the replay
 *     ends on, losing exactly those keys. RANGE promotes safely because its frame
 *     at any row at or above {@code R} spans no further back than {@code L}.</li>
 * </ul>
 * <p>
 * An anchored view reaches the same two bounds without a frame at all. Its anchor
 * resets every function on it the moment the anchor value changes, so one segment -
 * a maximal run of rows sharing an anchor value - is a wall in both directions, and
 * {@link LiveViewCheckpointAnchorPlan} puts both walls where timestamp arithmetic
 * alone says they are: {@code L} is the start of the segment holding {@code R} and
 * {@code H} the end of the segment holding {@code changeMaxTs}. Both are
 * key-independent, so the anchor path needs no insert-only proof - it is the RANGE
 * path's shape with the segment standing in for the width. The one thing it does
 * need is that every window function on the view is actually reset by that anchor,
 * which the compiler decides before it hands the plan over.
 * <p>
 * A factory may carry more than one of the three shapes, and then the plan takes
 * their <b>union</b>: the earliest {@code L} and the latest {@code H} any of them
 * proves. Both directions are safe to widen. A warm-up that starts below a
 * function's own {@code L} feeds it rows that leave the frame again - or that a
 * later anchor reset throws away - before the output floor is reached, so its
 * state at {@code R} is what a whole-history replay would hold; and output above
 * a function's own {@code H} has converged, so re-emitting it reproduces what is
 * already stored there. What may not be widened is the <i>tag</i>. {@code EOF}
 * sits above every timestamp, so one arm that proves no finite bound sinks the
 * union to {@code EOF} - and a ROWS or anchored function cannot survive that
 * (see {@link #isRuntimeStatePreserved()}), so a factory holding one declines the
 * whole localization rather than promoting a runtime that has lost its keys. The
 * caller is responsible for handing over a complete set: every window function
 * must be covered by one of the three, or none of them describes the view.
 * <p>
 * A later extension adds the affected/output key domains {@code A}/{@code Q} to
 * the plan itself. For the timestamp-global RANGE replacement they are degenerate:
 * {@code Q} is every key with a qualifying row in {@code [R, H)}, which is exactly
 * what the replay emits when it re-evaluates the whole interval, and {@code A} does
 * not enter the bounds at all because {@code L} and {@code H} are key-independent
 * timestamp arithmetic. Both are load-bearing for the ROWS shapes, and the
 * discovery behind {@link RowsBoundSource} already derives them; only the bounds
 * they produce cross back into the plan.
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
    private long changeMaxTs;
    private long correctionTs;
    private int disposition;
    private HighBoundTag highBoundTag = HighBoundTag.EOF;
    private long highTsExclusive;
    private boolean localized;
    private long outputLowTs;
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
     * Copies every derived coordinate out of {@code other}. A repair that yields
     * on its turn budget keeps its own copy: the refresh worker refills its plan
     * instance on the next repair it runs, while the suspended one must keep the
     * bounds it derived against the snapshot it pinned.
     */
    public void copyFrom(@NotNull LiveViewCheckpointRepairPlan other) {
        this.anchorLvSeqTxn = other.anchorLvSeqTxn;
        this.anchorMaxTs = other.anchorMaxTs;
        this.applyAheadMinTs = other.applyAheadMinTs;
        this.changeMaxTs = other.changeMaxTs;
        this.correctionTs = other.correctionTs;
        this.disposition = other.disposition;
        this.highBoundTag = other.highBoundTag;
        this.highTsExclusive = other.highTsExclusive;
        this.localized = other.localized;
        this.outputLowTs = other.outputLowTs;
        this.pinnedSeqTxn = other.pinnedSeqTxn;
        this.replayLowTs = other.replayLowTs;
        this.retireLowTs = other.retireLowTs;
        this.triggerSeqTxn = other.triggerSeqTxn;
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
     * @return the highest designated timestamp any change this repair incorporates
     * touched, or {@link Numbers#LONG_NULL} when the caller could not bound it.
     * This is the input {@code H} is derived from, and it is the whole reason a
     * repair may stop short of the end of the base table.
     */
    public long getChangeMaxTs() {
        return changeMaxTs;
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
     * @return whether {@code H} is a concrete exclusive timestamp
     * ({@link HighBoundTag#FINITE}) or pinned to end-of-frame
     * ({@link HighBoundTag#EOF}).
     */
    public HighBoundTag getHighBoundTag() {
        return highBoundTag;
    }

    /**
     * @return {@code H}: the exclusive timestamp after which every eligible
     * function has converged. Meaningful only for {@link HighBoundTag#FINITE};
     * {@link Numbers#LONG_NULL} under {@link HighBoundTag#EOF}, where no timestamp
     * can express the bound.
     */
    public long getHighTsExclusive() {
        return highTsExclusive;
    }

    /**
     * @return {@code R}: the inclusive timestamp the replay emits output and
     * replaces durable output from. At or above {@link #getReplayLowTs()}; equal to
     * it whenever the repair needs no warm-up.
     */
    public long getOutputLowTs() {
        return outputLowTs;
    }

    /**
     * @return {@code E}: the applied base {@code seqTxn} of the pinned reader.
     */
    public long getPinnedSeqTxn() {
        return pinnedSeqTxn;
    }

    /**
     * @return {@code L}: the inclusive timestamp the replay scans from. Never
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
     * {@code H} restated as the <b>inclusive</b> high bound the bounded forward
     * page-frame cursor takes, so the source scan stops at the convergence boundary
     * instead of running to the end of the base table.
     * <p>
     * {@link HighBoundTag#EOF} maps to {@code Long.MAX_VALUE}, which as an
     * inclusive bound admits every row, up to and including one at the very top of
     * the timestamp range. That is the whole reason the bound is tagged: the same
     * value as an <i>exclusive</i> bound would silently drop that row.
     *
     * @return the inclusive timestamp at or below which the repair may read
     */
    public long getScanHighTsInclusive() {
        return highBoundTag == HighBoundTag.EOF ? Long.MAX_VALUE : highTsExclusive - 1;
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

    /**
     * @return true when {@code H} is pinned to end-of-frame, so the repair's
     * influence reaches the runtime head and the scratch runtime must be promoted.
     */
    public boolean isHighBoundEof() {
        return highBoundTag == HighBoundTag.EOF;
    }

    /**
     * @return true when a finite dependency raised the boundary rebuild's floors
     * above the view's {@code START FROM} boundary, so the rebuild reads no row
     * below {@code L} and re-emits no row below {@code R}. False leaves both floors
     * at {@code S}, which is the whole-history rebuild.
     */
    public boolean isLocalized() {
        return localized;
    }

    public boolean isResumeFromAnchor() {
        return disposition == DISPOSITION_RESUME_FROM_ANCHOR;
    }

    /**
     * @return true when the repair must put the pre-repair runtime window state
     * back after replaying, instead of promoting what the replay leaves behind.
     * <p>
     * A finite {@code H} is derived only when the runtime frontier sits at or above
     * it, which proves no changed row lies inside the frame the runtime currently
     * holds - so the state the repair found was already correct, and the state the
     * replay ends on (describing {@code H - 1}, not the frontier) is not. The two
     * are therefore the same predicate: whenever this repair stops short of the end
     * of the base table, it owes the runtime a restore.
     */
    public boolean isRuntimeStatePreserved() {
        return highBoundTag == HighBoundTag.FINITE;
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
     * A repair with no usable anchor rebuilds from the boundary. That rebuild is
     * localized to {@code [L, ...)} when the view carries a finite RANGE dependency
     * and the trigger carries a timestamp; otherwise it reads the whole view
     * history, which needs no such guarantee either.
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
     * @param rangeFrameWidth          the widest finite RANGE look-behind {@code W} among
     *                                 the view's RANGE window functions, in
     *                                 designated-timestamp units, or
     *                                 {@link Numbers#LONG_NULL} when the view has none
     * @param rowsBoundSource          the view's finite ROWS discovery, or null when the
     *                                 view has no ROWS window function
     * @param anchorPlan               the view's fixed anchor segment, or null when the view
     *                                 is unanchored, the anchor has no closed-form segment
     *                                 boundary, or an anchored window function is not reset
     *                                 by that anchor
     *                                 <p>
     *                                 The three describe disjoint sets of window functions
     *                                 and a factory may carry several at once, in which case
     *                                 the bounds are their union. The caller must hand over a
     *                                 set that covers every window function in the view -
     *                                 {@code LiveViewCheckpointFunctionCompiler.isDependencyComplete}
     *                                 is what proves it - because a function outside the
     *                                 union is one the replay cannot reconstruct
     * @param insertOnlyChangeSet      whether every change this repair incorporates only
     *                                 added rows. Gates the ROWS discovery, whose affected
     *                                 key domain is read off the post-change snapshot and
     *                                 would miss a key a deletion emptied out of the change
     *                                 interval. Ignored by the RANGE path, whose bounds a
     *                                 deletion cannot escape
     * @param durableOutputMaxTs       the highest designated timestamp the live-view
     *                                 table durably holds, or
     *                                 {@link Numbers#LONG_NULL} when it holds no row
     *                                 at all; a lower bound on {@code D}, the
     *                                 earliest output the runtime incorporated but
     *                                 has not made durable
     * @param changeMaxTs              the highest designated timestamp any change
     *                                 this repair incorporates touched - the
     *                                 triggering commit, everything the drain rolled
     *                                 back with it, and the apply-ahead range - or
     *                                 {@link Numbers#LONG_NULL} when the caller
     *                                 cannot bound it (a non-DATA or structural entry
     *                                 in the incorporated range, an unclassified
     *                                 apply-ahead range, or a caller that does not
     *                                 track it)
     * @param runtimeFrontierTs        the highest designated timestamp the runtime
     *                                 window state has incorporated, or
     *                                 {@link Numbers#LONG_NULL} when the repair
     *                                 cannot put that state back afterwards (no
     *                                 checkpoint-state support, or an anchored view
     *                                 whose anchor state this phase does not carry)
     */
    public void of(
            @NotNull AnchorSource anchors,
            long lateRowTs,
            long viewLowerBoundTimestamp,
            long triggerSeqTxn,
            long pinnedSeqTxn,
            long headLvSeqTxn,
            long headMaxTs,
            long applyAheadMinTs,
            long rangeFrameWidth,
            @Nullable RowsBoundSource rowsBoundSource,
            @Nullable LiveViewCheckpointAnchorPlan anchorPlan,
            boolean insertOnlyChangeSet,
            long durableOutputMaxTs,
            long changeMaxTs,
            long runtimeFrontierTs
    ) throws SqlException {
        assert pinnedSeqTxn >= triggerSeqTxn : "pinned base snapshot is below the trigger";
        this.triggerSeqTxn = triggerSeqTxn;
        this.pinnedSeqTxn = pinnedSeqTxn;
        this.changeMaxTs = changeMaxTs;
        // H starts pinned to end-of-frame and is lowered only by deriveHighBound
        // below, which runs after the floors and only for a localized rebuild. A
        // resume, an unlocalized rebuild and every repair whose change set has no
        // proven upper bound keep this value: reading and replacing through positive
        // infinity is the one disposition that cannot drop a row the repair still
        // owes.
        highBoundTag = HighBoundTag.EOF;
        highTsExclusive = Numbers.LONG_NULL;
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
            // The restored anchor state IS the warm-up, so the resume emits every
            // row it reads: L and R coincide.
            outputLowTs = replayLowTs;
            localized = false;
        } else {
            disposition = DISPOSITION_BOUNDARY_REBUILD;
            anchorLvSeqTxn = Numbers.LONG_NULL;
            anchorMaxTs = Numbers.LONG_NULL;
            deriveRebuildBounds(
                    viewLowerBoundTimestamp,
                    rangeFrameWidth,
                    rowsBoundSource,
                    anchorPlan,
                    insertOnlyChangeSet,
                    durableOutputMaxTs,
                    runtimeFrontierTs
            );
        }
    }

    /**
     * Saturating {@code value - width} for a non-negative {@code width}: a
     * subtraction that would wrap below {@link Long#MIN_VALUE} clamps there instead.
     * A view whose {@code W} exceeds the distance from {@code R} to the bottom of
     * the timestamp range simply depends on everything below it.
     */
    private static long saturatingSubtract(long value, long width) {
        final long result = value - width;
        // width >= 0, so a result ABOVE value can only be wrap-around.
        return result > value ? Long.MIN_VALUE : result;
    }

    /**
     * Derives the boundary rebuild's floors and its tagged high bound: the output floor
     * {@code R} every dependency shape shares, then the union of the {@code L}/{@code H}
     * pairs the shapes this view carries prove.
     * <p>
     * The change floor is {@link #getRetireLowTs() retireLowTs}, not {@code C}. The
     * two differ only under apply-ahead, and there the difference is load-bearing:
     * the rebuild materialises the whole pinned snapshot and then advances the
     * watermark past it, so a back-dated row in the range {@code ApplyWal2TableJob}
     * raced past the trigger would be dropped for good if the floors stopped at
     * {@code C}. {@code retireLowTs} already carries {@code min(C, applyAheadMinTs)}
     * for exactly this reason, and is {@link Numbers#LONG_NULL} when that range
     * cannot be classified - which refuses localization outright, since nothing then
     * bounds what changed.
     * <p>
     * {@code R = max(S, min(changeFloor, D))}. The change floor alone would be wrong:
     * output the runtime has already incorporated but not made durable - a discarded
     * in-RAM lead, a rolled-back current-turn draft - exists nowhere else, so a floor
     * above it would drop those rows permanently. {@code durableOutputMaxTs} is the
     * live-view table's own frontier and therefore a lower bound on {@code D}: every
     * such row was produced after the last flush, so it sits at or above that
     * frontier. Clamping {@code R} down to it re-emits at worst the topmost durable
     * timestamp group, which the replay reproduces identically. A floor that lands back
     * on {@code S} localizes nothing - the rebuild reads and re-emits the whole view
     * history either way - so the executor keeps its established
     * {@code replayMinTs}-clamped replacement boundary instead.
     * <p>
     * Each shape then contributes its own pair, and the plan keeps the lowest {@code L}
     * and the highest {@code H}:
     * <ul>
     *     <li><b>finite RANGE:</b> {@code L = max(S, R - W)} and
     *     {@code H = changeMaxTs + W + 1}. A {@code RANGE W PRECEDING ... CURRENT ROW}
     *     frame at {@code t} spans {@code [t - W, t]}, so feeding {@code [L, R)} leaves
     *     every such function holding exactly the state a whole-history replay would
     *     hold at {@code R} - the frame contents, not an accumulated prefix - and a row
     *     at {@code m} sits in the frame of every row in {@code [m, m + W]} and no
     *     other.</li>
     *     <li><b>fixed anchor segment:</b> {@code L = max(S, segmentStart(R))} and
     *     {@code H = segmentEndExclusive(changeMaxTs)}. The anchor is a wall in both
     *     directions: a row at {@code t} reads state only from rows at or above
     *     {@code segmentStart(t)}, because the reset there put every function on the
     *     anchor back to identity, and a row at {@code m} reaches output only within
     *     {@code m}'s own segment. Both are pure timestamp arithmetic - no key domain
     *     enters either - and {@code segmentStart} is monotone, so the floor taken at
     *     {@code R} bounds every row above it. The floor is deliberately not
     *     {@code segmentStart(changeMaxTs)}: the replacement re-emits from {@code R},
     *     and when a non-durable lead dropped {@code R} into an earlier segment the
     *     replay has to reconstruct that segment's state too.</li>
     *     <li><b>finite ROWS:</b> both bounds come back from the per-key discovery,
     *     because {@code Nmax} counts rows of one partition key and where those rows
     *     sit is a property of the data rather than of the frame.</li>
     * </ul>
     * Widening either end of the union is safe. A warm-up starting below a function's
     * own {@code L} feeds it rows that leave the frame again - or that a later anchor
     * reset discards - before {@code R}, and output above a function's own {@code H}
     * has converged, so re-emitting it reproduces what is already stored.
     * <p>
     * The guards, and which shapes they bind:
     * <ul>
     *     <li>the ROWS discovery reads the affected key domain {@code A} off the
     *     post-change snapshot, so a deletion that emptied a key's rows out of the
     *     change interval leaves it invisible there while its later rows still pull
     *     older history into their frames. A change set that is not provably
     *     insert-only therefore declines a view holding a ROWS function outright.
     *     RANGE and the anchor need no such proof - their bounds are key-independent
     *     arithmetic over an interval a deletion cannot escape.</li>
     *     <li>a finite {@code H} needs a known {@code changeMaxTs} (a non-DATA or
     *     structural entry in the incorporated range can change rows anywhere), a known
     *     runtime frontier, and every output row the runtime produced already durable.
     *     The last closes the direction the frontier does not: output that exists only
     *     in an un-flushed lead or a rolled-back draft sits above the durable frontier,
     *     and a replacement stopping at {@code H} would neither re-emit it nor leave it
     *     on disk.</li>
     *     <li>the frontier must sit at or above the union's {@code H}, which proves the
     *     change is outside the frame the runtime currently holds - so the pre-repair
     *     state is correct and must be restored rather than replaced by the state the
     *     replay ends on. The frontier the caller supplies may be a lower bound on where
     *     the runtime's state actually stands - a drain that fed in-order commits through
     *     the window cursor and then rolled the hand-off back leaves the watermark behind
     *     the state. That direction is safe (a lower bound only makes the comparison
     *     stricter), and the case does not slip through in disguise either: those same
     *     commits are in the change set, so they raise {@code changeMaxTs}, {@code H}
     *     lands above them, and the comparison refuses. Which is why the caller's ceiling
     *     has to cover the whole incorporated range rather than the triggering commit
     *     alone.</li>
     *     <li>a ROWS or anchored function cannot be localized behind an {@code EOF}
     *     bound. Neither expires by time - a ROWS frame holds a key's last {@code Nmax}
     *     rows however old they are, and an anchored function holds its segment - so a
     *     key with no row at or above {@code R} keeps state a replay from {@code L}
     *     never sees. Only a finite {@code H} puts the pre-repair runtime state back
     *     over the replay's (see {@link #isRuntimeStatePreserved()}); an {@code EOF} one
     *     would promote the replay's state and lose exactly those keys. A RANGE-only
     *     view localizes its floor either way, because its frame at any row at or above
     *     {@code R} reaches no further back than {@code L}.</li>
     * </ul>
     * Everything collapses to the whole-history rebuild - both floors at {@code S} and
     * {@code H} left at end-of-frame - when there is no change floor, when the live-view
     * table holds no durable row at all, or when the view carries no finite dependency
     * of any shape.
     */
    private void deriveRebuildBounds(
            long viewLowerBoundTimestamp,
            long rangeFrameWidth,
            RowsBoundSource rowsBoundSource,
            LiveViewCheckpointAnchorPlan anchorPlan,
            boolean insertOnlyChangeSet,
            long durableOutputMaxTs,
            long runtimeFrontierTs
    ) throws SqlException {
        outputLowTs = viewLowerBoundTimestamp;
        replayLowTs = viewLowerBoundTimestamp;
        localized = false;
        final boolean hasRange = rangeFrameWidth != Numbers.LONG_NULL;
        final boolean hasRows = rowsBoundSource != null;
        final boolean hasAnchor = anchorPlan != null;
        if ((!hasRange && !hasRows && !hasAnchor)
                || retireLowTs == Numbers.LONG_NULL
                || durableOutputMaxTs == Numbers.LONG_NULL) {
            return;
        }
        final long outputFloor = Math.max(viewLowerBoundTimestamp, Math.min(retireLowTs, durableOutputMaxTs));
        // Every guard below runs before the ROWS discovery, because each one describes a
        // repair that could not localize whatever the data proved - and the discovery is
        // the only part of planning that reads base rows.
        if (outputFloor <= viewLowerBoundTimestamp || (hasRows && !insertOnlyChangeSet)) {
            return;
        }
        final boolean isHighBoundDerivable = changeMaxTs != Numbers.LONG_NULL
                && runtimeFrontierTs != Numbers.LONG_NULL
                && durableOutputMaxTs >= runtimeFrontierTs;
        final boolean isFiniteHighRequired = hasRows || hasAnchor;
        if (isFiniteHighRequired && !isHighBoundDerivable) {
            return;
        }
        long lowTs = Long.MAX_VALUE;
        long highTs = Long.MIN_VALUE;
        boolean isHighEof = false;
        if (hasRange) {
            lowTs = Math.min(lowTs, Math.max(viewLowerBoundTimestamp, saturatingSubtract(outputFloor, rangeFrameWidth)));
            final long armHighTs = isHighBoundDerivable
                    ? rangeHighTs(rangeFrameWidth, outputFloor)
                    : Numbers.LONG_NULL;
            if (armHighTs == Numbers.LONG_NULL) {
                isHighEof = true;
            } else {
                highTs = Math.max(highTs, armHighTs);
            }
        }
        if (hasAnchor) {
            final long armHighTs = anchorPlan.getSegmentEndExclusive(changeMaxTs);
            // No representable segment end - a sub-resolution anchor period, or the
            // topmost segment - is H = EOF, which an anchored function cannot survive.
            // Neither can an end at or below R, which happens only when every changed row
            // sits below the view's own boundary and leaves the replacement range empty.
            if (armHighTs == Numbers.LONG_NULL || armHighTs <= outputFloor) {
                return;
            }
            highTs = Math.max(highTs, armHighTs);
            // getSegmentStart reports Long.MIN_VALUE for a segment that is open below -
            // every row under a non-zero alignment origin shares one - and the clamp
            // resolves it to S, which is as far down as the rebuild would read anyway.
            lowTs = Math.min(lowTs, Math.max(viewLowerBoundTimestamp, anchorPlan.getSegmentStart(outputFloor)));
        }
        if (hasRows) {
            rowsBoundSource.discoverRowsBounds(
                    viewLowerBoundTimestamp,
                    outputFloor,
                    // The change interval's floor in the view's own coordinate space. The
                    // retire floor already carries min(C, applyAheadMinTs), and the clamp
                    // drops an apply-ahead row below the view's boundary - not the view's
                    // row, and so a row that marks no key affected.
                    Math.max(viewLowerBoundTimestamp, retireLowTs),
                    changeMaxTs
            );
            if (rowsBoundSource.getRowsHighBoundTag() != HighBoundTag.FINITE) {
                return;
            }
            final long armHighTs = rowsBoundSource.getRowsHighTsExclusive();
            // H > R holds by construction - R is at or below the change floor, which is at
            // or below changeMaxTs, which the discovery's bound is strictly above - and the
            // guard states it, because a replacement whose exclusive high bound does not
            // clear its low bound is not a range at all.
            if (armHighTs <= outputFloor) {
                return;
            }
            highTs = Math.max(highTs, armHighTs);
            // The discovery reports R itself when no key in Q needs warm-up, and S when a
            // key ran out of history; the clamps state that L sits in [S, R] either way.
            lowTs = Math.min(
                    lowTs,
                    Math.min(outputFloor, Math.max(viewLowerBoundTimestamp, rowsBoundSource.getRowsDependencyLowTs()))
            );
        }
        if (isHighEof || runtimeFrontierTs < highTs) {
            if (isFiniteHighRequired) {
                return;
            }
        } else {
            highBoundTag = HighBoundTag.FINITE;
            highTsExclusive = highTs;
        }
        outputLowTs = outputFloor;
        replayLowTs = lowTs;
        localized = true;
    }

    /**
     * The finite RANGE arm's exclusive high bound {@code changeMaxTs + W + 1}, or
     * {@link Numbers#LONG_NULL} when the arithmetic names none.
     * <p>
     * A {@code RANGE W PRECEDING ... CURRENT ROW} frame at {@code t} spans
     * {@code [t - W, t]}, so a row at {@code m} belongs to the frame of every row in
     * {@code [m, m + W]} and to no frame above that. {@code changeMaxTs} is the highest
     * timestamp anything this repair incorporates touched - inserted, replaced or
     * deleted - so {@code changeMaxTs + W} is the last output row any of it can reach,
     * and the exclusive bound is one past it. The whole timestamp tie at
     * {@code changeMaxTs + W} is admitted because the bound is exclusive at the next
     * distinct value, not at a row position.
     * <p>
     * The arithmetic has to be representable. {@code changeMaxTs + W} may overflow, and
     * an exclusive bound one past {@link Long#MAX_VALUE} does not exist - which is
     * exactly why the bound is tagged rather than spelled as a timestamp. A change
     * reaching the top of the timestamp range therefore converges nowhere this can name.
     * <p>
     * The degenerate clamp is the case where every changed row sits so far below the
     * output floor that the bound lands at or under {@code R}. The interval must still be
     * non-empty, and re-emitting the single timestamp group at {@code R} is always sound -
     * the replay reproduces it identically.
     */
    private long rangeHighTs(long rangeFrameWidth, long outputFloor) {
        final long lastAffectedTs = changeMaxTs + rangeFrameWidth;
        // rangeFrameWidth >= 0, so a sum BELOW changeMaxTs can only be wrap-around.
        if (lastAffectedTs < changeMaxTs || lastAffectedTs == Long.MAX_VALUE) {
            return Numbers.LONG_NULL;
        }
        final long highTs = lastAffectedTs + 1;
        if (highTs > outputFloor) {
            return highTs;
        }
        return outputFloor == Long.MAX_VALUE ? Numbers.LONG_NULL : outputFloor + 1;
    }

    /**
     * The sealed checkpoints a resume may roll back to, ordered by ascending
     * {@code maxTs} with the newest (the head) last.
     * <p>
     * {@link LiveViewInstance} implements this over the retained-checkpoint
     * ring. The versioned timeline replaces that ring with a logarithmic
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

    /**
     * The bounded {@code ROWS N PRECEDING ... CURRENT ROW} dependency's {@code L} and
     * {@code H}, discovered against the same pinned snapshot the plan classifies from.
     * <p>
     * A RANGE width is a timestamp offset, so both of its bounds are arithmetic the plan
     * performs itself. {@code Nmax} is a per-key row count, and where a key's
     * {@code Nmax}-th predecessor sits - or how far above the change its output stops
     * moving - is a property of the data. The plan therefore calls back into the
     * discovery once it has computed the floor {@code R} both searches run from, and
     * reads the two bounds off the result.
     * <p>
     * {@link LiveViewRefreshJob} implements this over
     * {@link LiveViewCheckpointRowsBounds} and the pinned base reader.
     */
    public interface RowsBoundSource {
        /**
         * Runs the {@code H -> Q -> L} discovery for one repair. The caller has already
         * proven the change set insert-only and the floors worth discovering, so this
         * only ever runs for a repair that can use the answer.
         *
         * @param viewLowerBoundTs {@code S}, the view's {@code START FROM} boundary and
         *                         the lowest timestamp either search may reach
         * @param outputLowTs      {@code R}, the floor the replay emits and replaces
         *                         from: the forward search starts here and the backward
         *                         one walks down from it
         * @param changeLowTs      the lowest timestamp the incorporated change set can
         *                         have touched, clamped to {@code S}
         * @param changeMaxTs      the highest timestamp it can have touched
         */
        void discoverRowsBounds(long viewLowerBoundTs, long outputLowTs, long changeLowTs, long changeMaxTs) throws SqlException;

        /**
         * @return {@code L}: the inclusive timestamp from which replaying reconstructs
         * the state every key in {@code Q} holds at {@code R}. Meaningful only after
         * {@link #discoverRowsBounds}.
         */
        long getRowsDependencyLowTs();

        /**
         * @return whether the discovery proved a concrete exclusive {@code H}
         * ({@link HighBoundTag#FINITE}) or none at all ({@link HighBoundTag#EOF}).
         */
        HighBoundTag getRowsHighBoundTag();

        /**
         * @return {@code H}: the exclusive timestamp after which no output can have
         * changed. Meaningful only under {@link HighBoundTag#FINITE}.
         */
        long getRowsHighTsExclusive();
    }
}
