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
 * Which of the two dispositions runs is decided on price, not on availability. A
 * resume reads {@code [anchorMaxTs + 1, EOF)} - its high bound is end-of-frame, so
 * nothing stops it below the end of the base table - while a localized rebuild
 * reads {@code [L, H)}. Neither dominates: a change near the head leaves the resume
 * a short tail that no warm-up can beat, and a change deep in history leaves it the
 * whole view above the correction while the dependency interval stays the width of
 * one frame. The plan therefore derives the rebuild bounds even when an anchor is
 * available, prices both intervals through {@link ScanCostSource} against the same
 * pinned snapshot, and takes the cheaper; a tie, an unpriceable repair and a rebuild
 * that could not localize all keep the resume, which needs no warm-up and stages
 * nothing. This is what stops a dense checkpoint cadence from defeating the
 * localization: the anchor a cadence leaves just below an old correction is exactly
 * the anchor whose resume replays every row above it.
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
     * The base table deduplicates on commit, so a ROWS discovery cannot trust the
     * affected key domain it reads off the post-change snapshot. The caller withholds
     * every dependency input on these grounds; see
     * {@code LiveViewRefreshJob.hasDedupKeys}.
     */
    public static final int DENIAL_DEDUP = 1;
    /**
     * The runtime window state has not reached the convergence boundary the
     * dependencies proved, so the change sits inside the frame the runtime currently
     * holds and the pre-repair state cannot be put back over the replay's.
     */
    public static final int DENIAL_FRONTIER_BELOW_CONVERGENCE = 2;
    /**
     * At least one window function is covered by none of the RANGE, ROWS and anchor
     * plans, so no union of them describes the view. The caller withholds every
     * dependency input rather than bounding some functions and not others.
     */
    public static final int DENIAL_INCOMPLETE_DEPENDENCY = 3;
    /**
     * Nothing was denied: the repair reads {@code [L, ...)} rather than the whole view
     * history.
     */
    public static final int DENIAL_NONE = 0;
    /**
     * The trigger carries no timestamp - a restart restore, a corrupt-checkpoint
     * restore, a base-metadata drift or a mid-drain recovery - so there is no
     * correction floor to localize around.
     */
    public static final int DENIAL_NON_DATA_TRIGGER = 4;
    /**
     * A ROWS dependency over a change set that is not provably insert-only: a deletion
     * can empty a key out of the change interval, leaving it invisible to the discovery
     * while its later rows still pull older history into their frames.
     */
    public static final int DENIAL_NOT_INSERT_ONLY = 5;
    /**
     * The incorporated range holds no upper bound on what it touched - a non-DATA or
     * structural entry, an unclassified apply-ahead range, or a caller that tracks
     * none - so no shape can name a convergence boundary.
     */
    public static final int DENIAL_NO_CHANGE_CEILING = 6;
    /**
     * A shape that needs a finite convergence boundary could not name one: the RANGE
     * arithmetic overflowed the timestamp range, the anchor segment has no
     * representable end, or the discovered bound does not clear {@code R}.
     */
    public static final int DENIAL_NO_CONVERGENCE_BOUND = 7;
    /**
     * The view carries no finite RANGE, ROWS or anchor dependency at all, so nothing
     * bounds the rebuild from below.
     */
    public static final int DENIAL_NO_DEPENDENCY = 8;
    /**
     * The live-view table holds no durable row, so there is no output frontier to clamp
     * the replacement floor {@code R} down to.
     */
    public static final int DENIAL_NO_DURABLE_OUTPUT = 9;
    /**
     * The view cannot say where its runtime window state stands, because that state
     * travels through the checkpoint freeze/restore contract and this view's functions
     * do not support it.
     */
    public static final int DENIAL_NO_RUNTIME_FRONTIER = 10;
    /**
     * The rebuild localized and was priced anyway: a resume from the sealed anchor below
     * the change reads no more base rows than {@code [L, H)}, so it ran instead. The one
     * denial that reports a cheaper repair rather than a lost bound.
     */
    public static final int DENIAL_RESUME_CHEAPER = 11;
    /**
     * An anchor qualified but no {@link ScanCostSource} was supplied, so the two
     * dispositions were never compared and the resume was kept unpriced. Production
     * always prices; this is what a caller that passes no cost source gets.
     */
    public static final int DENIAL_RESUME_UNPRICED = 12;
    /**
     * A budget stopped the ROWS discovery, leaving the bound it was proving at the
     * conservative fallback an unlocalized repair uses. See
     * {@link LiveViewCheckpointRowsBounds.ScanBudgetStatus}.
     */
    public static final int DENIAL_SCAN_BUDGET = 13;
    /**
     * {@code ApplyWal2TableJob} raced the pinned reader past the trigger over a range
     * that cannot be classified, so nothing says how far down or up those un-examined
     * transactions reach.
     */
    public static final int DENIAL_UNCLASSIFIED_APPLY_AHEAD = 14;
    /**
     * The runtime has produced output that is not yet on disk, so a replacement stopping
     * at a finite {@code H} would neither re-emit it nor leave it stored.
     */
    public static final int DENIAL_UNFLUSHED_OUTPUT = 15;
    /**
     * The replacement floor {@code R} landed back on the view's {@code START FROM}
     * boundary, so the rebuild reads and re-emits the whole view history either way and
     * there is nothing left to localize.
     */
    public static final int DENIAL_VIEW_START_FLOOR = 16;
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
    /**
     * The anchor source a per-segment plan is derived against: none. A resume runs to
     * end-of-frame, so letting one win the price comparison would put back the union
     * range the decomposition exists to avoid.
     */
    private static final AnchorSource NO_ANCHORS = (ceilTs, out) -> false;
    private long anchorCheckpointId;
    // Scratch the anchor searches read into. Worker-owned, overwritten by every
    // lookup; only the two identity fields around it survive a plan.
    private final LiveViewCheckpointTimelineEntry anchorEntry = new LiveViewCheckpointTimelineEntry();
    private long anchorLogicalStateBytes;
    private long anchorMaxTs;
    private long applyAheadMinTs;
    private long changeMaxTs;
    // The base seqTxn this repair commits at and advances its watermarks to. The
    // pinned snapshot's own seqTxn for a repair that materialises the whole change
    // set, and the pre-repair watermark for a per-segment repair, which corrects one
    // closed segment and leaves the rest of the change set unconsumed.
    private long commitSeqTxn;
    private long correctionTs;
    // Why this repair reads the whole view history, or resumed instead of rebuilding:
    // one DENIAL_* code, DENIAL_NONE when nothing was denied. Localization is decided
    // by a chain of guards that each describe a different lost bound, and the code is
    // the one that fired - which the disposition alone does not say.
    private int denialReason;
    private int disposition;
    private boolean hasOutputKeyDomain;
    private HighBoundTag highBoundTag = HighBoundTag.EOF;
    private long highTsExclusive;
    // True when the state the replay stands on anywhere in [L, H) describes every
    // live key rather than only the keys the bounds were derived for. A
    // time-expiring dependency reconstructs all of them: nothing a RANGE frame or
    // an anchor segment holds at a row above R sits below L, so a key the replay
    // never saw holds nothing at that row either. A ROWS frame does not expire, and
    // its L only covers the warm-up of the output key domain Q, so a key outside Q
    // comes back holding the rows the replay happened to carry instead of its own
    // last Nmax. False for any localization a ROWS arm took part in.
    private boolean isReplayStateKeyComplete;
    private boolean localized;
    // Q, when the replay's own state is not key-complete but the discovery proved which
    // keys it does describe. Owned rather than referenced: the discovery's map is
    // overwritten by the next repair this worker plans, while a parked repair still owes
    // its publication.
    private final LiveViewCheckpointOutputKeyDomain outputKeyDomain = new LiveViewCheckpointOutputKeyDomain();
    private long outputLowTs;
    private long pinnedSeqTxn;
    private long rebuildScanRows;
    private long replayLowTs;
    private long resumeScanRows;
    private long retireLowTs;
    private long triggerSeqTxn;

    /**
     * Renders a {@code DENIAL_*} code the way the log and {@code live_views()} spell it.
     * {@link #DENIAL_NONE} renders as null rather than as a word: a repair that read
     * exactly {@code [L, H)} was denied nothing, and naming that would read as a reason.
     */
    public static @Nullable String denialReasonName(int denialReason) {
        return switch (denialReason) {
            case DENIAL_NONE -> null;
            case DENIAL_DEDUP -> "dedup";
            case DENIAL_FRONTIER_BELOW_CONVERGENCE -> "frontier below convergence";
            case DENIAL_INCOMPLETE_DEPENDENCY -> "incomplete dependency";
            case DENIAL_NON_DATA_TRIGGER -> "non-data trigger";
            case DENIAL_NOT_INSERT_ONLY -> "not insert only";
            case DENIAL_NO_CHANGE_CEILING -> "no change ceiling";
            case DENIAL_NO_CONVERGENCE_BOUND -> "no convergence bound";
            case DENIAL_NO_DEPENDENCY -> "no dependency";
            case DENIAL_NO_DURABLE_OUTPUT -> "no durable output";
            case DENIAL_NO_RUNTIME_FRONTIER -> "no runtime frontier";
            case DENIAL_RESUME_CHEAPER -> "resume cheaper";
            case DENIAL_RESUME_UNPRICED -> "resume unpriced";
            case DENIAL_SCAN_BUDGET -> "scan budget";
            case DENIAL_UNCLASSIFIED_APPLY_AHEAD -> "unclassified apply ahead";
            case DENIAL_UNFLUSHED_OUTPUT -> "unflushed output";
            case DENIAL_VIEW_START_FLOOR -> "view start floor";
            default -> null;
        };
    }

    /**
     * Renders what one repair actually did: which executor ran, and for the rebuild,
     * whether a dependency bounded it. The two rebuilds share a {@code DISPOSITION_*}
     * code because they run the same executor, and what separates them is exactly
     * whether anything was denied - a rebuild that read {@code [L, H)} denied nothing,
     * while a whole-history one carries the code that cost it the bound.
     *
     * @return {@code localized rebuild}, {@code boundary rebuild} or
     * {@code resume from anchor}, and null for a view that has run no repair
     */
    public static @Nullable String dispositionName(int disposition, int denialReason) {
        return switch (disposition) {
            case DISPOSITION_BOUNDARY_REBUILD -> denialReason == DENIAL_NONE ? "localized rebuild" : "boundary rebuild";
            case DISPOSITION_RESUME_FROM_ANCHOR -> "resume from anchor";
            default -> null;
        };
    }

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
        this.anchorCheckpointId = other.anchorCheckpointId;
        this.anchorLogicalStateBytes = other.anchorLogicalStateBytes;
        this.anchorMaxTs = other.anchorMaxTs;
        this.applyAheadMinTs = other.applyAheadMinTs;
        this.changeMaxTs = other.changeMaxTs;
        this.commitSeqTxn = other.commitSeqTxn;
        this.correctionTs = other.correctionTs;
        this.denialReason = other.denialReason;
        this.disposition = other.disposition;
        this.hasOutputKeyDomain = other.hasOutputKeyDomain;
        this.highBoundTag = other.highBoundTag;
        this.highTsExclusive = other.highTsExclusive;
        this.isReplayStateKeyComplete = other.isReplayStateKeyComplete;
        this.localized = other.localized;
        this.outputKeyDomain.copyFrom(other.outputKeyDomain);
        this.outputLowTs = other.outputLowTs;
        this.pinnedSeqTxn = other.pinnedSeqTxn;
        this.rebuildScanRows = other.rebuildScanRows;
        this.replayLowTs = other.replayLowTs;
        this.resumeScanRows = other.resumeScanRows;
        this.retireLowTs = other.retireLowTs;
        this.triggerSeqTxn = other.triggerSeqTxn;
    }

    /**
     * @return the {@code checkpointId} of the logical timeline boundary the resume
     * restores from. Together with {@link #getAnchorMaxTs()} it forms the
     * timeline's composite key. Meaningful only for
     * {@link #DISPOSITION_RESUME_FROM_ANCHOR}.
     */
    public long getAnchorCheckpointId() {
        return anchorCheckpointId;
    }

    /**
     * @return decoded state bytes attributed to the selected anchor root, or zero when
     * this plan does not resume from an anchor
     */
    public long getAnchorLogicalStateBytes() {
        return anchorLogicalStateBytes;
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
     * watermarks to. The pinned snapshot's {@code seqTxn} for a repair that
     * materialises the whole change set, because the replay reads everything that
     * snapshot holds, including any transaction apply raced past the trigger.
     * <p>
     * A per-segment repair ({@link #ofSegment}) commits at the pre-repair watermark
     * instead. It corrects one closed segment and leaves every other part of the
     * change set unconsumed, so advancing over the snapshot would declare base
     * transactions whose output the view does not hold. Leaving the watermark where it
     * was makes the segment repair idempotent: a crash before the residual repair
     * finishes replays the same change set, and re-running a whole-segment recompute
     * over the same base produces the same rows.
     */
    public long getCommitSeqTxn() {
        return commitSeqTxn;
    }

    /**
     * @return {@code C}: the earliest timestamp whose existing output may have
     * changed, clamped up to the view's {@code START FROM} boundary, or
     * {@link Numbers#LONG_NULL} for a non-DATA / recovery trigger.
     */
    public long getCorrectionTs() {
        return correctionTs;
    }

    /**
     * @return the {@code DENIAL_*} code naming why this repair could not localize, or
     * why a localized rebuild lost to a resume, and {@link #DENIAL_NONE} when the
     * rebuild localized and ran. The caller may replace a {@link #DENIAL_NO_DEPENDENCY}
     * verdict with the more specific reason it withheld the dependency inputs on -
     * {@link #of} sees only their absence, not the gate that caused it.
     */
    public int getDenialReason() {
        return denialReason;
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
     * @return {@code Q}, the keys the replay's state describes, or null when it
     * describes every live key ({@link #isReplayStateKeyComplete()}) or when nothing
     * proved which keys it describes. A publication handed this set takes the replay's
     * entry for a key inside it and leaves every key outside it exactly as the old root
     * wrote it - see {@link LiveViewCheckpointOutputKeyDomain} for why that is the whole
     * of the rule.
     */
    public @Nullable LiveViewCheckpointOutputKeyDomain getOutputKeyDomain() {
        return hasOutputKeyDomain ? outputKeyDomain : null;
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
     * @return the estimated base rows a localized rebuild over {@code [L, H)} would
     * pull, or {@link Numbers#LONG_NULL} when nothing priced it - no anchor competes
     * for the repair, no {@link ScanCostSource} was supplied, or the rebuild could
     * not localize and so has no interval to price. Diagnostic once the plan is
     * built: the disposition below already reflects the comparison.
     */
    public long getRebuildScanRows() {
        return rebuildScanRows;
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
     * @return the estimated base rows a resume from the selected anchor would pull -
     * every row at or above its floor, because its high bound is end-of-frame - or
     * {@link Numbers#LONG_NULL} when no anchor qualified or no {@link ScanCostSource}
     * was supplied. Diagnostic; see {@link #getRebuildScanRows()}.
     */
    public long getResumeScanRows() {
        return resumeScanRows;
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

    /**
     * @return true when the window state the replay stands on at any timestamp in
     * {@code [L, H)} is the state a whole-history replay would stand on there, for
     * <b>every</b> live key rather than only for the keys the bounds were derived
     * for.
     * <p>
     * A RANGE frame and an anchor segment both expire by time, so what a function
     * holds at a row at or above {@code R} came from rows at or above {@code L} and
     * the replay reconstructs it whatever the key: one the replay never saw holds
     * nothing there either, which is exactly what an absent key restores as. A ROWS
     * frame holds a key's last {@code Nmax} rows however old they are, and the
     * discovery only walks back far enough to warm up the output key domain
     * {@code Q} - the keys with a row in {@code [R, H)} - so a key outside {@code Q}
     * ends the replay holding the rows that happened to fall inside {@code [L, H)}
     * instead of the state it really has.
     * <p>
     * That is survivable for the runtime, which a finite {@code H} puts back from
     * the scratch overlay (see {@link #isRuntimeStatePreserved()}), and not for a
     * root a repair freezes: nothing puts those back. So a repair that re-versions
     * logical boundaries from its replay either needs this to hold, or needs
     * {@link #getOutputKeyDomain()} to say which keys the replay does describe - and a
     * ROWS repair with neither truncates the timeline at {@code R} instead.
     */
    public boolean isReplayStateKeyComplete() {
        return isReplayStateKeyComplete;
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
     *     <li>The anchor must sit strictly below the late row: a boundary at or
     *     above it already incorporates rows the change invalidates. The search
     *     runs on the raw trigger timestamp - the {@code START FROM} clamp governs
     *     deletion authority, not which state a resume may trust - and returns the
     *     newest qualifying boundary, which on the common path is the newest
     *     boundary the timeline holds.</li>
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
     * <p>
     * A qualifying anchor does not settle the question. The rebuild bounds are derived
     * either way and both intervals priced through {@code scanCostSource}, because an
     * anchor sitting just below an old correction buys a resume that still replays
     * every row above it - the very cost the dependency interval bounds. The resume
     * takes a tie and every repair the comparison cannot reach: a rebuild that did not
     * localize reads the whole view history and can never be the cheaper of the two.
     *
     * @param anchors                 the versioned timeline's predecessor lookup
     *                                over the logical boundaries a resume may roll
     *                                back to
     * @param lateRowTs               lowest timestamp the triggering DATA commit
     *                                touched, or {@link Numbers#LONG_NULL} for a
     *                                non-DATA / recovery trigger
     * @param viewLowerBoundTimestamp the view's {@code START FROM} boundary
     *                                {@code S}
     * @param triggerSeqTxn           base {@code seqTxn} that triggered the repair
     * @param pinnedSeqTxn            {@code seqTxn} of the pinned base reader
     *                                ({@code E}); never below
     *                                {@code triggerSeqTxn}
     * @param applyAheadMinTs         minimum in-view timestamp of the apply-ahead
     *                                range, or {@link Numbers#LONG_NULL} when that
     *                                range is unclassifiable; ignored unless
     *                                {@link #isApplyAheadClassificationRequired}
     *                                holds
     * @param rangeFrameWidth         the widest finite RANGE look-behind {@code W} among
     *                                the view's RANGE window functions, in
     *                                designated-timestamp units, or
     *                                {@link Numbers#LONG_NULL} when the view has none
     * @param rowsBoundSource         the view's finite ROWS discovery, or null when the
     *                                view has no ROWS window function
     * @param anchorPlan              the view's fixed anchor segment, or null when the view
     *                                is unanchored, the anchor has no closed-form segment
     *                                boundary, or an anchored window function is not reset
     *                                by that anchor
     *                                <p>
     *                                The three describe disjoint sets of window functions
     *                                and a factory may carry several at once, in which case
     *                                the bounds are their union. The caller must hand over a
     *                                set that covers every window function in the view -
     *                                {@code LiveViewCheckpointFunctionCompiler.isDependencyComplete}
     *                                is what proves it - because a function outside the
     *                                union is one the replay cannot reconstruct
     * @param insertOnlyChangeSet     whether every change this repair incorporates only
     *                                added rows. Gates the ROWS discovery, whose affected
     *                                key domain is read off the post-change snapshot and
     *                                would miss a key a deletion emptied out of the change
     *                                interval. Ignored by the RANGE path, whose bounds a
     *                                deletion cannot escape
     * @param durableOutputMaxTs      the highest designated timestamp the live-view
     *                                table durably holds, or
     *                                {@link Numbers#LONG_NULL} when it holds no row
     *                                at all; a lower bound on {@code D}, the
     *                                earliest output the runtime incorporated but
     *                                has not made durable
     * @param changeMaxTs             the highest designated timestamp any change
     *                                this repair incorporates touched - the
     *                                triggering commit, everything the drain rolled
     *                                back with it, and the apply-ahead range - or
     *                                {@link Numbers#LONG_NULL} when the caller
     *                                cannot bound it (a non-DATA or structural entry
     *                                in the incorporated range, an unclassified
     *                                apply-ahead range, or a caller that does not
     *                                track it)
     * @param runtimeFrontierTs       the highest designated timestamp the runtime
     *                                window state has incorporated, or
     *                                {@link Numbers#LONG_NULL} when the repair
     *                                cannot put that state back afterwards (no
     *                                checkpoint-state support, or an anchored view
     *                                whose anchor state this phase does not carry)
     * @param scanCostSource          prices a candidate scan interval against the pinned
     *                                snapshot, or null to leave the choice between a
     *                                qualifying anchor and a localized rebuild
     *                                unpriced - which keeps the anchor
     */
    public void of(
            @NotNull AnchorSource anchors,
            long lateRowTs,
            long viewLowerBoundTimestamp,
            long triggerSeqTxn,
            long pinnedSeqTxn,
            long applyAheadMinTs,
            long rangeFrameWidth,
            @Nullable RowsBoundSource rowsBoundSource,
            @Nullable LiveViewCheckpointAnchorPlan anchorPlan,
            boolean insertOnlyChangeSet,
            long durableOutputMaxTs,
            long changeMaxTs,
            long runtimeFrontierTs,
            @Nullable ScanCostSource scanCostSource
    ) throws SqlException {
        assert pinnedSeqTxn >= triggerSeqTxn : "pinned base snapshot is below the trigger";
        denialReason = DENIAL_NONE;
        this.triggerSeqTxn = triggerSeqTxn;
        this.pinnedSeqTxn = pinnedSeqTxn;
        // The whole change set is materialised from the pinned snapshot, so the
        // watermark advances over it. ofSegment overwrites this afterwards.
        this.commitSeqTxn = pinnedSeqTxn;
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
        // whole timeline and denies every anchor.
        if (classified) {
            retireLowTs = this.applyAheadMinTs == Numbers.LONG_NULL
                    ? Numbers.LONG_NULL
                    : Math.min(correctionTs, this.applyAheadMinTs);
        } else {
            retireLowTs = correctionTs;
        }

        // Search the timeline for the newest boundary strictly below the late row.
        // The strict comparison is load-bearing: a boundary covers rows up to AND
        // INCLUDING its maxTimestamp while the resume starts above it, so an anchor
        // at exactly the late row's timestamp would leave that row neither covered
        // nor re-read. A non-DATA / recovery trigger carries no timestamp to search
        // with and anchors nothing.
        anchorCheckpointId = Numbers.LONG_NULL;
        anchorLogicalStateBytes = 0;
        anchorMaxTs = Numbers.LONG_NULL;
        boolean hasAnchor = lateRowTs != Numbers.LONG_NULL && anchors.findAnchorBelow(lateRowTs, anchorEntry);
        if (hasAnchor && applyAhead) {
            // Re-anchor below the ahead range's floor, which is at or below C, so
            // this can only move the anchor down or reject it outright.
            hasAnchor = retireLowTs != Numbers.LONG_NULL && anchors.findAnchorBelow(retireLowTs, anchorEntry);
        }
        if (hasAnchor) {
            anchorCheckpointId = anchorEntry.checkpointId;
            anchorLogicalStateBytes = Math.max(0, anchorEntry.logicalStateBytes);
            anchorMaxTs = anchorEntry.maxTimestamp;
        }
        // The anchor's state already covers rows up to and including its maxTs, so a
        // resume starts strictly above it. Floored at the view's boundary so it applies
        // the same row predicate as the seed, the forward drain and the boundary
        // rebuild.
        final long resumeLowTs = hasAnchor ? Math.max(anchorMaxTs + 1, viewLowerBoundTimestamp) : Numbers.LONG_NULL;
        rebuildScanRows = Numbers.LONG_NULL;
        // What the resume would read: every base row at or above its floor, because its
        // high bound is end-of-frame and no dependency stops it below the end of the
        // base table.
        resumeScanRows = hasAnchor && scanCostSource != null
                ? scanCostSource.estimateScanRows(resumeLowTs, Long.MAX_VALUE)
                : Numbers.LONG_NULL;
        // The whole-history rebuild, which every path below either keeps or narrows.
        outputLowTs = viewLowerBoundTimestamp;
        replayLowTs = viewLowerBoundTimestamp;
        localized = false;
        isReplayStateKeyComplete = false;
        hasOutputKeyDomain = false;
        outputKeyDomain.clear();
        // Derive the rebuild bounds even with an anchor in hand: the two dispositions
        // are compared on price below, and an anchor the cadence left just under an old
        // correction buys a resume that replays the whole view above it. An unpriced
        // repair skips the derivation outright - the anchor wins by default, so the
        // bounds would be discarded, and for a ROWS view deriving them reads base rows.
        if (!hasAnchor || resumeScanRows != Numbers.LONG_NULL) {
            deriveRebuildBounds(
                    viewLowerBoundTimestamp,
                    rangeFrameWidth,
                    rowsBoundSource,
                    anchorPlan,
                    insertOnlyChangeSet,
                    durableOutputMaxTs,
                    runtimeFrontierTs
            );
        } else {
            denialReason = DENIAL_RESUME_UNPRICED;
        }
        if (localized && resumeScanRows != Numbers.LONG_NULL) {
            // Only a localized rebuild is worth pricing. An unlocalized one reads the
            // whole view history from S, which is every row the resume reads and then
            // the ones below the anchor as well.
            rebuildScanRows = scanCostSource.estimateScanRows(replayLowTs, getScanHighTsInclusive());
        }
        if (hasAnchor && (rebuildScanRows == Numbers.LONG_NULL || rebuildScanRows >= resumeScanRows)) {
            disposition = DISPOSITION_RESUME_FROM_ANCHOR;
            if (localized) {
                // The rebuild had a bounded interval and the resume still read no more
                // rows. Read before the flag is cleared below: a rebuild that never
                // localized keeps the reason its own guard recorded, which is the one
                // worth reporting.
                denialReason = DENIAL_RESUME_CHEAPER;
            }
            replayLowTs = resumeLowTs;
            // The restored anchor state IS the warm-up, so the resume emits every
            // row it reads: L and R coincide.
            outputLowTs = replayLowTs;
            localized = false;
            // A resume converges nowhere below the end of the base table, so whatever
            // bound the rebuild derived above is discarded with the rest of its plan.
            highBoundTag = HighBoundTag.EOF;
            highTsExclusive = Numbers.LONG_NULL;
        } else {
            disposition = DISPOSITION_BOUNDARY_REBUILD;
            anchorCheckpointId = Numbers.LONG_NULL;
            anchorLogicalStateBytes = 0;
            anchorMaxTs = Numbers.LONG_NULL;
        }
    }

    /**
     * Classifies one <b>closed anchor segment</b> of an already-decomposed change set,
     * rather than the change set as a whole.
     * <p>
     * The union range a whole-change-set plan derives is what makes a deep correction
     * expensive: {@code changeMaxTs} is the highest timestamp anything in the change set
     * touched, so a commit carrying rows at the head and rows a month back puts {@code H}
     * at the end of <i>today's</i> segment - above the runtime frontier, which denies the
     * localization outright - and the resume that replaces it replays and rewrites the
     * whole month. Neither is a property of the correction: under a pure fixed-anchor plan
     * the anchor resets every stateful function at the segment boundary, so the rows in
     * one old segment reach that segment's output and nothing else.
     * <p>
     * So the bounds come out of the same derivation with the segment's own extremes
     * standing in for the change set's: {@code L} at the segment's start, {@code H} at its
     * end, and a replacement covering that range alone. Two inputs are deliberately
     * withheld:
     * <ul>
     *     <li><b>the anchor source.</b> A resume reads {@code [anchorMaxTs + 1, EOF)} -
     *     its high bound is end-of-frame - so winning the price comparison would put the
     *     union range back. A segment repair is the localized rebuild or it is nothing;
     *     the caller falls back to the whole-change-set plan when this returns false.</li>
     *     <li><b>the apply-ahead range.</b> The caller classified the whole range
     *     {@code (fromSeqTxn, E]} row by row to produce the decomposition, so the ahead
     *     range's rows are already in the segment they belong to. Re-deriving its scalar
     *     minimum here would drop the retire floor back to the deepest row in the whole
     *     change set and widen every segment's bounds into the union again.</li>
     * </ul>
     * {@code viewLowerBoundTimestamp} still clamps the correction floor, but the
     * {@code DENIAL_VIEW_START_FLOOR} guard behind it cannot fire the way it does for a
     * RANGE or ROWS shape: a floor landing on {@code S} leaves those reading the whole view
     * history, while a segment repair still reads one segment. It is left in place because
     * a floor at {@code S} also means the segment arithmetic and the view's boundary
     * disagree about where this repair starts, and the caller's fallback is cheap.
     *
     * @param segmentMinTs            the lowest in-view timestamp the change set touched
     *                                inside this segment, already clamped above the view's
     *                                {@code START FROM} boundary by the decomposition
     * @param segmentMaxTs            the highest in-view timestamp the change set touched
     *                                inside this segment
     * @param viewLowerBoundTimestamp the view's {@code START FROM} boundary {@code S}
     * @param pinnedSeqTxn            {@code seqTxn} of the pinned base reader ({@code E}),
     *                                the snapshot the replay reads from
     * @param commitSeqTxn            the watermark this repair commits at - the pre-repair
     *                                value, since the rest of the change set stays
     *                                unconsumed. See {@link #getCommitSeqTxn()}
     * @param anchorPlan              the view's fixed anchor segment; never null, because a
     *                                change set only decomposes for a view that has one
     * @param durableOutputMaxTs      the highest designated timestamp the live-view table
     *                                durably holds
     * @param runtimeFrontierTs       the highest designated timestamp the runtime window
     *                                state has incorporated
     * @return true when the segment came back localized behind a finite convergence
     * boundary, which is the only shape a per-segment repair may run in
     */
    public boolean ofSegment(
            long segmentMinTs,
            long segmentMaxTs,
            long viewLowerBoundTimestamp,
            long pinnedSeqTxn,
            long commitSeqTxn,
            @NotNull LiveViewCheckpointAnchorPlan anchorPlan,
            long durableOutputMaxTs,
            long runtimeFrontierTs
    ) throws SqlException {
        of(
                NO_ANCHORS,
                segmentMinTs,
                viewLowerBoundTimestamp,
                // Trigger and pin quoted as the same value so no apply-ahead
                // classification is required: the caller already did it row by row.
                pinnedSeqTxn,
                pinnedSeqTxn,
                Numbers.LONG_NULL,
                Numbers.LONG_NULL,
                null,
                anchorPlan,
                true,
                durableOutputMaxTs,
                segmentMaxTs,
                runtimeFrontierTs,
                null
        );
        this.commitSeqTxn = commitSeqTxn;
        return localized && highBoundTag == HighBoundTag.FINITE;
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
     * of any shape. The caller has already written that rebuild into the floors, so
     * every guard below returns rather than restating it.
     * <p>
     * No cost guard stands in front of the ROWS discovery, and none can. The rebuild
     * beats the resume exactly when the rows it adds below the anchor's floor are fewer
     * than the rows the resume adds above {@code H}, and both quantities need the very
     * bounds the discovery returns: the interval every shape is known to read without
     * asking - {@code [R, changeMaxTs]} - sits inside the resume's tail whenever the
     * anchor is below {@code R}, which the anchor search guarantees on the common path.
     * So an anchored repair over a view holding a bounded ROWS function pays the
     * discovery even when the resume goes on to win. The discovery's own scan budget is
     * what bounds that, and the alternative it buys is an unbounded tail replay.
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
        final boolean hasRange = rangeFrameWidth != Numbers.LONG_NULL;
        final boolean hasRows = rowsBoundSource != null;
        final boolean hasAnchor = anchorPlan != null;
        if (!hasRange && !hasRows && !hasAnchor) {
            denialReason = DENIAL_NO_DEPENDENCY;
            return;
        }
        if (retireLowTs == Numbers.LONG_NULL) {
            denialReason = DENIAL_UNCLASSIFIED_APPLY_AHEAD;
            return;
        }
        if (durableOutputMaxTs == Numbers.LONG_NULL) {
            denialReason = DENIAL_NO_DURABLE_OUTPUT;
            return;
        }
        final long outputFloor = Math.max(viewLowerBoundTimestamp, Math.min(retireLowTs, durableOutputMaxTs));
        // Every guard below runs before the ROWS discovery, because each one describes a
        // repair that could not localize whatever the data proved - and the discovery is
        // the only part of planning that reads base rows.
        if (outputFloor <= viewLowerBoundTimestamp) {
            denialReason = DENIAL_VIEW_START_FLOOR;
            return;
        }
        if (hasRows && !insertOnlyChangeSet) {
            denialReason = DENIAL_NOT_INSERT_ONLY;
            return;
        }
        final boolean isHighBoundDerivable = changeMaxTs != Numbers.LONG_NULL
                && runtimeFrontierTs != Numbers.LONG_NULL
                && durableOutputMaxTs >= runtimeFrontierTs;
        final boolean isFiniteHighRequired = hasRows || hasAnchor;
        if (isFiniteHighRequired && !isHighBoundDerivable) {
            // The three inputs fail for three different reasons, and an operator can act
            // on each: a change set nothing bounds from above, a view whose functions
            // carry no checkpoint state, and output the runtime holds but has not
            // flushed.
            denialReason = changeMaxTs == Numbers.LONG_NULL
                    ? DENIAL_NO_CHANGE_CEILING
                    : runtimeFrontierTs == Numbers.LONG_NULL
                      ? DENIAL_NO_RUNTIME_FRONTIER
                      : DENIAL_UNFLUSHED_OUTPUT;
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
                denialReason = DENIAL_NO_CONVERGENCE_BOUND;
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
                // A budget that stopped the search and a search that ran to completion
                // without finding a bound leave the same EOF tag but call for different
                // actions - raise the budget, or accept that the data proves nothing.
                denialReason = rowsBoundSource.isRowsScanBudgetExceeded()
                        ? DENIAL_SCAN_BUDGET
                        : DENIAL_NO_CONVERGENCE_BOUND;
                return;
            }
            final long armHighTs = rowsBoundSource.getRowsHighTsExclusive();
            // H > R holds by construction - R is at or below the change floor, which is at
            // or below changeMaxTs, which the discovery's bound is strictly above - and the
            // guard states it, because a replacement whose exclusive high bound does not
            // clear its low bound is not a range at all.
            if (armHighTs <= outputFloor) {
                denialReason = DENIAL_NO_CONVERGENCE_BOUND;
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
                denialReason = isHighEof
                        ? DENIAL_NO_CONVERGENCE_BOUND
                        : DENIAL_FRONTIER_BELOW_CONVERGENCE;
                return;
            }
        } else {
            highBoundTag = HighBoundTag.FINITE;
            highTsExclusive = highTs;
        }
        outputLowTs = outputFloor;
        replayLowTs = lowTs;
        localized = true;
        // Only the ROWS arm leaves the replay's per-key state incomplete, and it does
        // so whether or not another arm pushed L lower: a wider warm-up feeds more
        // rows to the keys it covers, and says nothing about a key the discovery
        // never counted predecessors for. See isReplayStateKeyComplete().
        isReplayStateKeyComplete = !hasRows;
        // What the ROWS arm can say instead: exactly which keys the replay does
        // describe. A publication holding Q keeps every key outside it as the old root
        // wrote it, which is what lets a ROWS repair splice the timeline rather than
        // truncate it. The forward pass has to have collected the whole domain - a
        // fragment would leave the publication silently dropping the keys it lost.
        if (hasRows && rowsBoundSource.isRowsOutputKeyDomainComplete()) {
            rowsBoundSource.collectRowsOutputKeys(outputKeyDomain);
            hasOutputKeyDomain = true;
        }
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
     * The logical checkpoint boundaries a resume may roll back to.
     * <p>
     * {@link LiveViewRefreshJob} implements this over the versioned checkpoint
     * timeline's logarithmic predecessor lookup. The timeline retains every
     * boundary it ever created, so however old a correction is, the search still
     * answers with the newest one below it.
     */
    public interface AnchorSource {
        /**
         * Finds the newest logical checkpoint boundary whose {@code maxTimestamp}
         * is strictly below {@code ceilTs} and copies it into {@code out}. The
         * strict inequality preserves a complete timestamp tie: a boundary at
         * exactly {@code ceilTs} covers only part of the rows at that timestamp.
         *
         * @return false when the timeline holds no such boundary, or holds no
         * readable generation at all
         */
        boolean findAnchorBelow(long ceilTs, @NotNull LiveViewCheckpointTimelineEntry out);
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
         * Copies {@code Q} into {@code out}, in the encoding a checkpoint partition map
         * keys an entry by. Called only when {@link #isRowsOutputKeyDomainComplete()}
         * holds.
         */
        void collectRowsOutputKeys(@NotNull LiveViewCheckpointOutputKeyDomain out);

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

        /**
         * @return whether the discovery collected the whole of {@code Q} rather than the
         * fragment a budget stopped its forward pass on, and so whether
         * {@link #collectRowsOutputKeys} may be read. Meaningful only after
         * {@link #discoverRowsBounds}.
         */
        boolean isRowsOutputKeyDomainComplete();

        /**
         * @return whether a budget stopped the discovery, leaving the bound it was
         * proving at the conservative fallback. Reported rather than acted on: the plan
         * refuses an {@link HighBoundTag#EOF} bound whatever produced it, and this only
         * separates "the budget ran out" from "the data proves no bound" in
         * {@link #getDenialReason()}. Meaningful only after
         * {@link #discoverRowsBounds}.
         */
        boolean isRowsScanBudgetExceeded();
    }

    /**
     * Prices one candidate scan interval so the plan can pick the cheaper of the two
     * dispositions instead of taking whichever is available.
     * <p>
     * The two are not ordered by construction. A resume restores state that is already
     * correct below its anchor but reads every base row above it; a localized rebuild
     * warms its state up from the dependency floor but stops at the convergence
     * boundary. Which reads fewer rows depends entirely on where the correction sits
     * relative to the base table's head, which is a property of the data.
     * <p>
     * Estimates need only be comparable with one another, and only over one pinned
     * snapshot: the plan never compares one against a row budget or reports it as a
     * count. {@link LiveViewCheckpointScanCost} implements this off the pinned reader's
     * partition metadata.
     */
    public interface ScanCostSource {
        /**
         * @param lowTs           inclusive low bound of the candidate scan
         * @param highTsInclusive inclusive high bound, {@link Long#MAX_VALUE} for a
         *                        scan that runs to the end of the base table
         * @return an estimate of the base rows the interval holds, or 0 when it holds
         * none. Never {@link Numbers#LONG_NULL}: an interval the source cannot price is
         * not a thing the plan can act on
         */
        long estimateScanRows(long lowTs, long highTsInclusive);
    }
}
