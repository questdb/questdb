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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.lv.LiveViewCheckpointContracts.RepairPublicationStage;
import io.questdb.std.Mutable;
import io.questdb.std.Numbers;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * The publication ordering {@link RepairPublicationStage} declares, made
 * executable. One out-of-order repair walks the stages in ordinal order and this
 * object refuses any move that is not forward. A stage with nothing to do is
 * skipped rather than recorded: a repair that emits no replacement never commits
 * one, and a repair whose plan localized nothing never publishes a generation.
 * <p>
 * Two of the orderings are load-bearing rather than descriptive, and both are
 * enforced here instead of by comment:
 * <ul>
 *     <li><b>Nothing outlives an unreconciled replacement.</b> Once the live-view
 *     WAL replacement commits, the repair may not publish a timeline generation
 *     or advance the consumed watermark until it knows the replacement reached
 *     the live-view table. A repaired root's {@code lvRowPosition} and the suffix
 *     range-add are both derived from the materialised table, and a watermark
 *     that walks past output the table does not hold declares base transactions
 *     consumed that nothing materialised.</li>
 *     <li><b>The runtime is exchanged exactly once.</b>
 *     {@link #getRuntimeDisposition()} is decided while the candidate roots are
 *     built and acted on once, after the generation is published, so no failure
 *     path can leave the compiled factory holding half the replay's state and
 *     half the pre-repair state.</li>
 * </ul>
 * One instance per refresh worker, reused across repairs. Repairs never nest, so
 * the single instance cannot be observed mid-walk.
 */
public final class LiveViewCheckpointRepairPublication implements Mutable {
    // LV-WRITER space, not base space: the live-view writer's own seqTxn minted by
    // this repair's REPLACE_RANGE commit. Reconciliation compares it against the
    // live view's applied writer txn. LONG_NULL when the repair committed no
    // replacement.
    private long committedLvSeqTxn = Numbers.LONG_NULL;
    private RuntimeDisposition runtimeDisposition;
    private boolean runtimeSettled;
    // The last completed stage; null before plan().
    private RepairPublicationStage stage;

    /**
     * Records that the complete candidate roots and the scratch runtime exist and
     * fixes what the repair will do with the runtime once it publishes. The
     * replacement commits only after this point, so every commit site must pass
     * through here first.
     */
    public void candidateReady(@NotNull RuntimeDisposition disposition) {
        advanceTo(RepairPublicationStage.CANDIDATE_ROOTS_AND_RUNTIME_READY);
        runtimeDisposition = disposition;
    }

    @Override
    public void clear() {
        committedLvSeqTxn = Numbers.LONG_NULL;
        runtimeDisposition = null;
        runtimeSettled = false;
        stage = null;
    }

    public long getCommittedLvSeqTxn() {
        return committedLvSeqTxn;
    }

    public @Nullable RuntimeDisposition getRuntimeDisposition() {
        return runtimeDisposition;
    }

    public @Nullable RepairPublicationStage getStage() {
        return stage;
    }

    /** True when this repair committed a live-view WAL replacement. */
    public boolean hasCommittedReplacement() {
        return committedLvSeqTxn != Numbers.LONG_NULL;
    }

    public boolean isAtOrAfter(@NotNull RepairPublicationStage other) {
        return stage != null && stage.ordinal() >= other.ordinal();
    }

    /**
     * True when the repair's runtime disposition is to keep the state the primary
     * entered with, which is what a convergence boundary at or below the runtime
     * frontier proves correct.
     */
    public boolean isKeepPrimaryRuntime() {
        return runtimeDisposition == RuntimeDisposition.KEEP_PRIMARY;
    }

    /**
     * True when nothing was committed, or when what was committed is known to have
     * reached the live-view table. Everything downstream of the commit - the
     * timeline generation, the watermarks, the purge floor, the head seal - is
     * gated on this.
     */
    public boolean isReplacementReconciled() {
        return !hasCommittedReplacement() || isAtOrAfter(RepairPublicationStage.LV_REPLACEMENT_APPLIED);
    }

    public boolean isRuntimeSettled() {
        return runtimeSettled;
    }

    /** Opens a repair: the pinned snapshot is classified and the bounds derived. */
    public void plan() {
        advanceTo(RepairPublicationStage.PLAN);
    }

    public void replacementApplied() {
        require(RepairPublicationStage.LV_WAL_REPLACEMENT_COMMITTED, "replacement was not committed");
        advanceTo(RepairPublicationStage.LV_REPLACEMENT_APPLIED);
    }

    public void replacementCommitted(long committedLvSeqTxn) {
        require(RepairPublicationStage.CANDIDATE_ROOTS_AND_RUNTIME_READY, "candidate roots are not ready");
        advanceTo(RepairPublicationStage.LV_WAL_REPLACEMENT_COMMITTED);
        this.committedLvSeqTxn = committedLvSeqTxn;
    }

    /**
     * Records the one runtime exchange. Callers invoke this before the exchange
     * runs, not after: a failed exchange must not be retried by an unwinding
     * caller, which would restore twice or restore into a half-rebuilt runtime.
     * <p>
     * A committed-but-unapplied replacement settles the runtime too - the
     * disposition was fixed before the commit and the compiled factory must not be
     * left mixed - but does not claim the stage, so nothing downstream of it can
     * run.
     */
    public void runtimePromoted() {
        if (isReplacementReconciled()) {
            advanceTo(RepairPublicationStage.RUNTIME_TIER_PROMOTED_IF_NEEDED);
        }
        runtimeSettled = true;
    }

    public void timelinePublished() {
        if (!isReplacementReconciled()) {
            throw CairoException.critical(0)
                    .put("live view repair published a timeline generation over an unapplied replacement [lvSeqTxn=")
                    .put(committedLvSeqTxn)
                    .put(']');
        }
        advanceTo(RepairPublicationStage.TIMELINE_GENERATION_PUBLISHED);
    }

    public void watermarkAdvanced() {
        if (!isReplacementReconciled()) {
            throw CairoException.critical(0)
                    .put("live view repair advanced a watermark past unmaterialized output [lvSeqTxn=")
                    .put(committedLvSeqTxn)
                    .put(']');
        }
        if (!runtimeSettled) {
            throw CairoException.critical(0)
                    .put("live view repair advanced a watermark before settling its runtime");
        }
        advanceTo(RepairPublicationStage.CONSUMED_WATERMARK_AND_PURGE_FLOOR_ADVANCED);
    }

    private void advanceTo(@NotNull RepairPublicationStage next) {
        if (stage == null) {
            if (next != RepairPublicationStage.PLAN) {
                throw CairoException.critical(0)
                        .put("live view repair publication started at [stage=").put(next.name())
                        .put("] instead of PLAN");
            }
        } else if (next.ordinal() <= stage.ordinal()) {
            throw CairoException.critical(0)
                    .put("live view repair publication is not moving forward [from=").put(stage.name())
                    .put(", to=").put(next.name())
                    .put(']');
        }
        stage = next;
    }

    private void require(@NotNull RepairPublicationStage expected, @NotNull CharSequence why) {
        if (!isAtOrAfter(expected)) {
            throw CairoException.critical(0)
                    .put("live view repair publication is out of order: ").put(why)
                    .put(" [stage=").put(stage == null ? "NONE" : stage.name())
                    .put(", expected=").put(expected.name())
                    .put(']');
        }
    }

    /**
     * What the repair does with the compiled factory's window state once its
     * generation is published.
     */
    public enum RuntimeDisposition {
        /**
         * The convergence boundary {@code H} landed at or below the runtime
         * frontier, so the eligible state the primary entered with is still
         * correct and the scratch overlay is handed back. The replay's own end
         * state describes {@code H - 1} and is discarded.
         */
        KEEP_PRIMARY,
        /**
         * {@code H} reached pinned end-of-frame or crossed the runtime frontier,
         * so the state the replay produced <i>is</i> the runtime and stands as it
         * is.
         */
        PROMOTE_REPLAY
    }
}
