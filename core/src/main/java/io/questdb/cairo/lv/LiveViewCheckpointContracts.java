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

/**
 * Frozen contracts for the versioned checkpoint timeline and localized
 * out-of-order repair.
 * <p>
 * This class carries no runtime behavior beyond the floating-point tolerance
 * helper. It exists to <b>pin</b>, at compile time and under test, the design
 * decisions the file schemas and planning logic are built on, so an accidental
 * drift in the supported-window matrix, the repair publication ordering, or the
 * floating tolerance breaks a test rather than silently changing the contract.
 * The matching test is {@code LiveViewCheckpointContractsTest}.
 *
 * <h2>Coordinate terminology</h2>
 * These coordinates live in three distinct spaces that must never be conflated:
 * base-table {@code seqTxn} progress, live-view-writer {@code seqTxn} progress,
 * and per-root designated timestamp plus effective {@code lvRowPosition}.
 * <ul>
 *     <li>{@code S} - resolved {@code START FROM} boundary.</li>
 *     <li>{@code E} - applied base-table {@code seqTxn} pinned for one localized
 *     repair publication.</li>
 *     <li>{@code normalizedBaseSeqTxn} - per-generation base-{@code seqTxn}
 *     through which every current root is validated; the authoritative
 *     base-transaction-inclusion boundary for recovery replay.</li>
 *     <li>{@code F} - inclusive designated-timestamp frontier through which
 *     durable output is reconciled. A root-compatibility and output-
 *     reconciliation coordinate, <b>not</b> a base-transaction-inclusion
 *     bound.</li>
 *     <li>{@code C} - earliest timestamp whose existing result may have
 *     changed (the correction floor; state roots are versioned from here).</li>
 *     <li>{@code D} - earliest qualifying output already incorporated in
 *     runtime state but not yet durable; absent when all incorporated output is
 *     durable.</li>
 *     <li>{@code R} - inclusive non-durable-output / materialization floor:
 *     {@code max(S, min(C, D))} when {@code D} exists, otherwise
 *     {@code max(S, C)}. Durable output is re-emitted from {@code R}; logical
 *     state roots are versioned only from {@code C}.</li>
 *     <li>{@code L} - earliest timestamp/row position required to reconstruct
 *     state immediately before the replay floor {@code R}.</li>
 *     <li>{@code H} - tagged exclusive high influence bound
 *     ({@link HighBoundTag#FINITE} or {@link HighBoundTag#EOF}).</li>
 *     <li>{@code A} - partition keys actually affected by incorporated base
 *     changes; {@code Q} - partition keys whose output must be materialized in
 *     the timestamp-global replacement range {@code [R, H)}.</li>
 * </ul>
 *
 * <h2>Timeline invariants</h2>
 * <ol>
 *     <li>A boundary {@code B} represents function state after all qualifying
 *     rows with designated timestamp {@code <= B} in canonical cursor order.</li>
 *     <li>Every current root in generation {@code G} is correct for the same
 *     pinned base snapshot {@code G.normalizedBaseSeqTxn}.</li>
 *     <li>A generation is visible only after every file it references has
 *     reached its final versioned name.</li>
 *     <li>A reader pins one generation before resolving any root/page
 *     reference.</li>
 *     <li>Files referenced by a published superblock slot or a pinned reader are
 *     not deleted.</li>
 *     <li>Insert-only advancement with
 *     {@code minNewTimestamp > oldHead.maxTimestamp} cannot affect an older
 *     root.</li>
 *     <li>An O3 publication replaces every root with {@code C <= B < H} and
 *     reuses every root outside that interval.</li>
 *     <li>No current root points to a temporary, mutable, or retired physical
 *     object.</li>
 *     <li>Generation normalization records snapshot validity, not a per-root
 *     recovery position: restoring a root at {@code B} still rebuilds
 *     {@code (B, F]}, bounded by the base-{@code seqTxn} boundary, not by
 *     {@code F}.</li>
 *     <li>The effective {@code lvRowPosition} of every logical checkpoint is
 *     correct in every generation, including the reused suffix after an O3
 *     replacement changes the cumulative position.</li>
 * </ol>
 *
 * <h2>Three contracts the file schemas depend on</h2>
 * <ol>
 *     <li><b>Dual recovery model.</b> Recovery reconciles into two coordinates:
 *     the authoritative base-{@code seqTxn} inclusion boundary (which base
 *     transaction <i>versions</i> may be incorporated) and the durable
 *     designated-timestamp frontier {@code F} plus the selected root's
 *     effective {@code lvRowPosition} (which output is already durable). The
 *     {@code (B, F]} runtime rebuild is bounded by the base-{@code seqTxn}
 *     boundary, never by {@code F}, so an apply-ahead O3 correction below
 *     {@code F} is deferred to post-restore classification rather than double
 *     counted.</li>
 *     <li><b>Output-floor split.</b> Durable output is replaced over
 *     {@code [R, H)}; logical state roots are versioned only over
 *     {@code [C, H)}. When {@code R < C}, roots in {@code [R, C)} are
 *     re-emitted by the timestamp-global replacement but keep their existing
 *     state version - re-emitted, not re-versioned.</li>
 *     <li><b>Bounded startup validation / lazy corruption.</b> Startup selects
 *     the highest superblock slot that passes <i>bounded</i> validation (its
 *     own checksum, its root metadata pages, and the checksummed
 *     segment/completeness catalogue it references) without walking the whole
 *     timeline. Deep tree paths and state pages are validated lazily on first
 *     access; a structurally invalid page invalidates only that one root
 *     version, never the pinned generation.</li>
 * </ol>
 *
 * <h2>Failure ordering</h2>
 * A localized repair advances through {@link RepairPublicationStage} in strict
 * ordinal order. The inactive superblock slot is the sole commit point: recovery
 * selects either the complete old generation {@code G} or the complete new
 * generation {@code G + 1}, and never exposes a partial root splice. Before the
 * live-view WAL replacement commits, failure discards the candidate; after it
 * commits, refresh blocks behind reconciliation until the replacement is known
 * applied or not applied.
 */
public final class LiveViewCheckpointContracts {

    /**
     * Absolute floor for {@link #isWithinFloatingTolerance(double, double)}.
     * Guards comparisons where the expected value is at or near zero, for which
     * the relative term collapses.
     */
    public static final double FLOATING_ABSOLUTE_TOLERANCE = 1e-9;

    /**
     * Documented relative tolerance ceiling for <b>approved floating aggregate
     * fields and outputs only</b> after a localized replay. Restored checkpoint
     * bits are exact; frame contents, counts, deque structure, and all
     * non-floating state converge exactly and must be compared with exact
     * equality. Integer, decimal, and otherwise exact aggregates remain
     * bit-exact. Later work may record a tighter per-function tolerance
     * alongside its approved-field list, but must not exceed this ceiling.
     */
    public static final double FLOATING_RELATIVE_TOLERANCE = 1e-9;

    private LiveViewCheckpointContracts() {
    }

    /**
     * Returns whether {@code actual} matches {@code expected} within the
     * documented floating tolerance for an approved floating aggregate field.
     * <p>
     * Non-finite values (NaN, infinities) round-trip by raw bits in the generic
     * codec and are therefore restored exactly, so they must match bit-for-bit;
     * the tolerance applies only to finite accumulator drift.
     *
     * @param expected the value from a fresh recomputation
     * @param actual   the value produced by localized replay
     * @return true when the two are within the documented tolerance
     */
    public static boolean isWithinFloatingTolerance(double expected, double actual) {
        if (!Double.isFinite(expected) || !Double.isFinite(actual)) {
            return Double.doubleToRawLongBits(expected) == Double.doubleToRawLongBits(actual);
        }
        final double diff = Math.abs(expected - actual);
        if (diff <= FLOATING_ABSOLUTE_TOLERANCE) {
            return true;
        }
        final double scale = Math.max(Math.abs(expected), Math.abs(actual));
        return diff <= FLOATING_RELATIVE_TOLERANCE * scale;
    }

    /**
     * The supported-window dependency matrix. Each kind declares its low-bound
     * ({@code L}) and high-bound ({@code H}) derivation strategy and its
     * eligibility {@link Disposition}. Eligibility is a property of the proven
     * forward-influence contract, not of snapshot capability alone: a function
     * may support a snapshot codec and still be rejected here for lacking a
     * finite {@code H}.
     * <p>
     * An eligible kind is necessary but not sufficient for a localized repair.
     * The kind describes the frame; the repair reconstructs state by replaying
     * that frame's own extent, so the function must additionally hold
     * {@link LiveViewCheckpointDependency#hasFrameLocalState() frame-local
     * state}. One that reaches outside the frame it declares keeps its eligible
     * kind and declines the repair plan.
     */
    public enum DependencyKind {
        /**
         * {@code ROWS N PRECEDING ... CURRENT ROW}. Finite look-behind and a
         * finite following-row count give both bounds.
         */
        ROWS_N_PRECEDING_CURRENT_ROW(
                "at most Nmax qualifying predecessors for every key in Q",
                "Nmax qualifying following rows for every key in A, through the final timestamp tie",
                Disposition.ELIGIBLE
        ),
        /**
         * {@code RANGE W PRECEDING ... CURRENT ROW} with a constant finite
         * {@code W}. Timestamp arithmetic derives both bounds directly.
         */
        RANGE_W_PRECEDING_CURRENT_ROW(
                "saturating R - W, clamped to S",
                "after maxChangedTimestamp + W, including the complete upper tie",
                Disposition.ELIGIBLE
        ),
        /**
         * Fixed compiler-derived anchor segment, including anchored
         * {@code row_number}/{@code rank}/{@code dense_rank} with per-segment
         * reset. The segment boundaries give an exact {@code [L, H)}.
         */
        FIXED_ANCHOR_SEGMENT(
                "segment start, clamped to S",
                "segment end, exclusive",
                Disposition.ELIGIBLE
        ),
        /**
         * Unbounded cumulative aggregate without a fixed reset. An out-of-order
         * row joins the frame of every following row, so there is no finite
         * influence boundary. {@code SqlParser.validateLiveViewFiniteInfluence}
         * rejects it at CREATE, naming the aggregate. The anchored form resets
         * at every segment start and remains eligible via
         * {@link #FIXED_ANCHOR_SEGMENT}.
         */
        UNBOUNDED_CUMULATIVE_NO_RESET(
                "no finite bound",
                "no finite H",
                Disposition.REJECT
        ),
        /**
         * Unanchored {@code row_number}, {@code rank}, {@code dense_rank} with
         * no bounding anchor. A historical prefix is required and there is no
         * finite {@code H}; rejected at CREATE. This is the deliberate,
         * product-visible scope cut: the anchored, segment-reset forms remain
         * eligible via {@link #FIXED_ANCHOR_SEGMENT} and return with it.
         */
        UNANCHORED_RANK(
                "historical prefix required",
                "no finite H",
                Disposition.REJECT
        ),
        /**
         * {@code FOLLOWING} frames, arbitrary anchors, and data-dependent frames.
         * Bounds are function-specific or unknown; rejected in the first
         * implementation and revisited later.
         */
        FOLLOWING_OR_DATA_DEPENDENT(
                "function-specific or unknown",
                "unknown",
                Disposition.REJECT_INITIALLY
        );

        private final Disposition disposition;
        private final String highBoundStrategy;
        private final String lowBoundStrategy;

        DependencyKind(String lowBoundStrategy, String highBoundStrategy, Disposition disposition) {
            this.lowBoundStrategy = lowBoundStrategy;
            this.highBoundStrategy = highBoundStrategy;
            this.disposition = disposition;
        }

        public Disposition getDisposition() {
            return disposition;
        }

        public String getHighBoundStrategy() {
            return highBoundStrategy;
        }

        public String getLowBoundStrategy() {
            return lowBoundStrategy;
        }

        /**
         * Whether {@code CREATE LIVE VIEW} may accept this shape today.
         */
        public boolean isEligibleNow() {
            return disposition == Disposition.ELIGIBLE;
        }

        /**
         * Whether this shape has no finite {@code H} and is rejected at CREATE
         * permanently (as opposed to {@link Disposition#REJECT_INITIALLY}, which
         * is rejected only in the first implementation).
         */
        public boolean isRejectedPermanently() {
            return disposition == Disposition.REJECT;
        }
    }

    /**
     * Eligibility disposition of a {@link DependencyKind}.
     */
    public enum Disposition {
        /**
         * Finite {@code L} and {@code H} proven now; accepted at CREATE.
         */
        ELIGIBLE,
        /**
         * No finite {@code H}; rejected at CREATE permanently.
         */
        REJECT,
        /**
         * Finite bounds unknown in the first implementation; rejected initially,
         * may become eligible later.
         */
        REJECT_INITIALLY
    }

    /**
     * Tag for the exclusive high influence boundary {@code H}. It is a tagged
     * bound, not a bare {@code long}, because {@code Long.MAX_VALUE} is a valid
     * designated timestamp and cannot also mean infinity. A
     * {@code REPLACE_RANGE} that reaches end-of-frame carries {@link #EOF}
     * through planning and WAL application rather than encoding it as
     * {@code Long.MAX_VALUE} or {@code hi = maxTimestamp + 1}.
     */
    public enum HighBoundTag {
        /**
         * {@code H} carries a concrete exclusive designated timestamp.
         */
        FINITE,
        /**
         * {@code H} is pinned to end-of-frame; runtime head state is affected and
         * must be promoted.
         */
        EOF
    }

    /**
     * The strictly ordered stages of a localized O3 repair publication. Ordinal
     * order is the required happens-before order: a stage may begin only after
     * every earlier stage has completed. No watermark may advance past
     * unmaterialized output, which is why
     * {@link #CONSUMED_WATERMARK_AND_PURGE_FLOOR_ADVANCED} is last.
     */
    public enum RepairPublicationStage {
        /**
         * Pin the applied base reader, classify changes, and derive
         * {@code C/R/L/H}, {@code A/Q}.
         */
        PLAN,
        /**
         * The complete candidate roots and the scratch runtime exist; nothing is
         * published yet.
         */
        CANDIDATE_ROOTS_AND_RUNTIME_READY,
        /**
         * The live-view WAL replacement over {@code [R, H)} is committed.
         */
        LV_WAL_REPLACEMENT_COMMITTED,
        /**
         * The replacement has been applied to the live-view table.
         */
        LV_REPLACEMENT_APPLIED,
        /**
         * The new timeline generation is published via the inactive superblock
         * slot - the sole commit point.
         */
        TIMELINE_GENERATION_PUBLISHED,
        /**
         * If {@code H} reached EOF or crossed the runtime frontier, the scratch
         * runtime/tier is promoted.
         */
        RUNTIME_TIER_PROMOTED_IF_NEEDED,
        /**
         * The consumed watermark and the WAL-purge floor advance last.
         */
        CONSUMED_WATERMARK_AND_PURGE_FLOOR_ADVANCED
    }
}
