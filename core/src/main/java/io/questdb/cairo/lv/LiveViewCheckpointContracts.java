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

import io.questdb.std.Numbers;

/**
 * Contracts for checkpoint timelines and localized out-of-order repair.
 *
 * <h2>Coordinates</h2>
 * Base-table seqTxn, live-view-writer seqTxn, and designated timestamp/row position
 * are distinct coordinate spaces.
 * <ul>
 *     <li>{@code S}: resolved START FROM boundary.</li>
 *     <li>{@code E}: applied base seqTxn pinned for the repair.</li>
 *     <li>{@code normalizedBaseSeqTxn}: authoritative base-transaction inclusion
 *     boundary shared by every root in the generation.</li>
 *     <li>{@code F}: inclusive durable-output timestamp frontier, not a base-transaction bound.</li>
 *     <li>{@code C}: earliest timestamp whose result may have changed.</li>
 *     <li>{@code D}: earliest incorporated output not yet durable, if any.</li>
 *     <li>{@code R}: inclusive materialization floor, {@code max(S, min(C, D))}
 *     when D exists, otherwise {@code max(S, C)}.</li>
 *     <li>{@code L}: earliest timestamp/row position needed to reconstruct state before R.</li>
 *     <li>{@code H}: exclusive influence bound, tagged FINITE or EOF.</li>
 *     <li>{@code A}: affected keys; {@code Q}: keys materialized in {@code [R, H)}.</li>
 * </ul>
 * <h2>Timeline invariants</h2>
 * A boundary describes state after qualifying rows through its timestamp in canonical
 * cursor order. Every root in a generation is valid at its normalized base seqTxn.
 * Files reach their final names before publication; readers pin a generation before
 * resolving references. Published or pinned files cannot be deleted, and current roots
 * cannot reference temporary, mutable or retired objects.
 * <p>
 * Strictly forward inserts cannot affect older roots. O3 repair replaces state roots
 * in {@code [C, H)} and preserves roots outside it, but re-emits durable output over
 * {@code [R, H)}. Reused suffix roots must keep correct effective row positions.
 * Recovery rebuilds {@code (B, F]} from a selected root B, bounded by base seqTxn;
 * F alone cannot exclude apply-ahead corrections.
 * <p>
 * Startup validates the superblock, root metadata and segment catalogue without a
 * full timeline walk. Deep pages validate lazily; their corruption invalidates the
 * individual root version, not the pinned generation.
 *
 * <h2>Publication ordering</h2>
 * {@link RepairPublicationStage} advances in ordinal order. The inactive superblock
 * slot is the commit point: recovery sees the complete old or new generation.
 * Before WAL replacement commits, failure discards the candidate. After commit,
 * refresh blocks on reconciliation until application of the replacement is known.
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

    /**
     * Widest fixed-width state one accumulator component may carry in a
     * partition-map leaf's scalar slot instead of a data page it names with a
     * 40-byte reference. A component declaring more than this keeps the
     * page-backed shape.
     * <p>
     * This is a <b>format constant</b>, deliberately not configuration. A
     * configurable threshold would let two nodes compile the same live view into
     * different durable layouts, so the same checkpoint would be inline on one
     * and page-backed on the other with nothing in the entry to say which.
     * <p>
     * 64 is what a RANGE ring entry already inlines at its widest
     * ({@code LiveViewCheckpointRangeRingStateReader.scalarStateBytes(4)}), so a
     * component admitted here is no wider than one the leaf format has carried in
     * production already. It clears every fixed width the accumulator families
     * declare today with room to spare - the widest is a DECIMAL sum's 33 bytes,
     * a {@code Decimal256} accumulator beside its null-state flag.
     *
     * @see io.questdb.griffin.engine.window.WindowFunction#checkpointStateFixedLength()
     */
    public static final int MAX_INLINE_COMPONENT_STATE_BYTES = 64;

    /**
     * Widest complete scalar payload one leaf entry may inline, anchor value
     * included. A component group whose whole layout does not fit inlines the
     * longest prefix of its canonical component order that does, and every
     * component past the budget keeps its legacy page-backed root.
     * <p>
     * The budget exists because the B-tree splits on entry count rather than
     * encoded byte size, so an unbounded "fixed width means inline" rule would
     * build very large 64-entry leaves and make every CRC and decode along the
     * path more expensive. Seal cost, metadata written per seal and restore cost
     * all grow linearly in the entry width, with no knee, so the value is set
     * well clear of any shape a view plausibly compiles - 256 bytes admits
     * fifteen 16-byte components beside the anchor - and its job is to bound a
     * pathological leaf rather than to arbitrate the ordinary case. A full
     * 64-entry leaf then holds at most 16 KB of scalar payload, four times what a
     * leaf of RANGE ring entries already holds.
     */
    public static final int MAX_INLINE_LEAF_STATE_BYTES = 256;

    /**
     * The generation stamp a runtime carries while an out-of-order repair is freezing
     * a chain of boundaries out of it, in place of the real generation an ordinary
     * cadence baseline names.
     * <p>
     * A repair's replay restores a root that is not the timeline head - the anchor it
     * resumes from - and then freezes a boundary at every logical position it crosses,
     * each against the one below it. Every one of those freezes wants the incremental
     * path, and the incremental path is gated on the runtime's baseline naming exactly
     * the generation being sealed on top of. There is no such generation yet: nothing
     * is published until the whole repair splices, and the roots being built on are the
     * capture's own unpublished ones.
     * <p>
     * So the repair stamps this instead. It is not a generation any superblock can hold
     * - generations start at 1 and only ascend - which is precisely what makes the
     * stamp fail-safe: a cadence seal that slips in against a real generation finds no
     * match and full-scans, and a repair abandoned anywhere leaves the stamp behind
     * with the same effect. Only
     * {@link io.questdb.griffin.engine.window.WindowFunction#onCheckpointRepairBaselinePublished(long)}
     * turns it into a real generation, and only once the splice that published those
     * roots has committed.
     *
     * @see io.questdb.griffin.engine.window.WindowFunction#getCheckpointBaselineGeneration()
     */
    public static final long REPAIR_BASELINE_GENERATION = Numbers.LONG_NULL + 1;

    private LiveViewCheckpointContracts() {
    }

    /**
     * Returns whether a window function's declared
     * {@link io.questdb.griffin.engine.window.WindowFunction#checkpointStateFixedLength()
     * fixed state width} may be inlined into a partition-map leaf.
     * <p>
     * A declining function reports {@code -1} and lands here as false, as does a
     * fixed width past {@link #MAX_INLINE_COMPONENT_STATE_BYTES}. Zero is
     * excluded on purpose: a function holding no state at all declares that
     * through {@code isCheckpointStateless()}, and an empty scalar beside no page
     * reference is the one leaf shape that cannot be told from a corrupt entry.
     */
    public static boolean isInlineableStateLength(int fixedStateLength) {
        return fixedStateLength > 0 && fixedStateLength <= MAX_INLINE_COMPONENT_STATE_BYTES;
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
     * the {@link LiveViewCheckpointDependency#getStateExtentLo() state extent}
     * the descriptor declares, so the function must additionally hold
     * {@link LiveViewCheckpointDependency#hasFrameLocalState() frame-local
     * state}. One that reaches further back than the extent it declares keeps
     * its eligible kind and declines the repair plan.
     */
    public enum DependencyKind {
        /**
         * {@code ROWS N PRECEDING} ending at or below the current row - at
         * {@code CURRENT ROW}, at {@code M PRECEDING}, or at the {@code -1} an
         * {@code EXCLUDE CURRENT ROW} frame evaluates. Finite look-behind and a
         * finite following-row count give both bounds.
         * <p>
         * A lagging high bound rides on the same look-behind {@code Nmax}. The
         * frame at a key's {@code i}-th row above the change spans
         * {@code [f_i - Nmax, f_i - M]}, so it holds the change exactly while
         * {@code M <= i <= Nmax}: the forward scan converges on the upper end,
         * which the lag does not move, and the lower end only removes rows from
         * the affected set. Both strategies below therefore stay valid, and both
         * are looser than a lagging frame needs.
         * <p>
         * {@code Nmax} is the
         * {@link LiveViewCheckpointDependency#getStateExtentLo() state extent},
         * which is the frame's look-behind for an accumulator and the high
         * bound's lag for {@code last_value}. The frame start it names may
         * therefore be {@code UNBOUNDED PRECEDING}: {@code last_value} emits the
         * row {@code M} back rather than accumulating, so both strategies read
         * {@code Nmax = M} and bound it on the same two scans.
         */
        ROWS_N_PRECEDING_BOUNDED_HI(
                "at most Nmax qualifying predecessors for every key in Q",
                "Nmax qualifying following rows for every key in A, through the final timestamp tie",
                Disposition.ELIGIBLE
        ),
        /**
         * {@code RANGE W PRECEDING} with a constant finite {@code W}, ending at
         * or below the current row - at {@code CURRENT ROW}, at {@code V
         * PRECEDING}, or at the one tick below an {@code EXCLUDE CURRENT ROW}
         * frame evaluates. Timestamp arithmetic derives both bounds directly.
         * <p>
         * A lagging high bound rides on the same width {@code W}. Output at
         * {@code t} reads {@code [t - W, t - V]}, a subset of the
         * {@code [t - W, t]} the same-width frame ending at the current row
         * reads, so a replay from {@code R - W} still feeds every row the frame
         * admits; and a base row at {@code m} joins the frame of output in
         * {@code [m + V, m + W]} only, so nothing at or above
         * {@code maxChangedTimestamp + W} can have moved.
         */
        RANGE_W_PRECEDING_BOUNDED_HI(
                "saturating R - W, clamped to S",
                "after maxChangedTimestamp + W, including the complete upper tie",
                Disposition.ELIGIBLE
        ),
        /**
         * A window function whose value at a row is that row's alone, so it
         * carries no state a checkpoint has to hold and no influence beyond the
         * row that changed. {@code last_value} over a frame ending at the
         * current row is the family: the whole of its {@code computeNext} reads
         * the argument off the record it was handed, however far back its frame
         * nominally starts.
         * <p>
         * The kind is assigned off the compiled function's
         * {@code isCheckpointStateless()} rather than off the frame, because the
         * frame is what the shape does not depend on - an unbounded frame start
         * and a bounded one compile to the same class and read the same single
         * row. Its
         * {@link LiveViewCheckpointDependency#getStateExtentLo() state extent} is
         * zero, which makes both strategies below the degenerate case of the
         * RANGE ones: {@code R - 0} and {@code maxChangedTimestamp + 0}.
         * <p>
         * The zero forward influence rests on QuestDB's RANGE peer handling,
         * which stops {@code last_value} at the current row instead of taking
         * the last row of its tie group. Correcting that to the standard
         * semantics would give this shape a one-tie-group forward reach, and the
         * high bound below would have to grow with it.
         */
        STATELESS_CURRENT_ROW(
                "R itself; the replay warms nothing up",
                "after maxChangedTimestamp, including the complete tie",
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
         * rejects it at CREATE, naming the aggregate; it applies the same test
         * to every other window function over an unbounded frame start, since
         * the frame - not the function - is what leaves an accumulator's
         * boundary open. The two exceptions it carves out are both
         * {@code last_value}: over {@code ROWS ... AND K PRECEDING} it
         * accumulates nothing and the lag bounds its influence, and over a frame
         * ending at {@code CURRENT ROW} it reads the row it is handed and lands
         * on {@link #STATELESS_CURRENT_ROW}. The anchored form resets at every
         * segment start and remains eligible via
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
         * <p>
         * A {@code FOLLOWING} high bound is what keeps this kind rejected, and it
         * is why the two eligible kinds test the high bound's sign rather than
         * ignoring it: a base row at {@code m} then joins the frame of output
         * below {@code m}, so neither {@code R - W} nor
         * {@code maxChangedTimestamp + W} bounds the repair. A high bound that
         * merely lags the current row does not land here - the two eligible kinds
         * admit it on the look-behind they already carry.
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
