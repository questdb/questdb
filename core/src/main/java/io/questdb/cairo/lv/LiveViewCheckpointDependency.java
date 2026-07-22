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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.lv.LiveViewCheckpointContracts.DependencyKind;
import org.jetbrains.annotations.NotNull;

/**
 * Immutable compiler-visible dependency contract for one live-view function.
 * It carries the information later repair phases need without retaining parser
 * model objects in the runtime function graph.
 */
public final class LiveViewCheckpointDependency {
    private final long frameHi;
    private final long frameLo;
    private final boolean hasFrameLocalState;
    private final String highBoundStrategy;
    private final DependencyKind kind;
    private final String lowBoundStrategy;
    private final NumericConvergence numericConvergence;
    private final String orderSignature;
    private final String partitionSignature;
    private final StructuralConvergence structuralConvergence;
    private final boolean supportsKeyReset;
    private final boolean supportsKeyRestore;
    private final int timestampType;

    public LiveViewCheckpointDependency(
            @NotNull DependencyKind kind,
            @NotNull CharSequence partitionSignature,
            @NotNull CharSequence orderSignature,
            long frameLo,
            long frameHi,
            int timestampType,
            boolean hasFrameLocalState,
            boolean supportsKeyRestore,
            boolean supportsKeyReset,
            @NotNull StructuralConvergence structuralConvergence,
            @NotNull NumericConvergence numericConvergence
    ) {
        this.kind = kind;
        this.partitionSignature = partitionSignature.toString();
        this.orderSignature = orderSignature.toString();
        this.frameLo = frameLo;
        this.frameHi = frameHi;
        this.timestampType = timestampType;
        this.hasFrameLocalState = hasFrameLocalState;
        this.lowBoundStrategy = kind.getLowBoundStrategy();
        this.highBoundStrategy = kind.getHighBoundStrategy();
        this.supportsKeyRestore = supportsKeyRestore;
        this.supportsKeyReset = supportsKeyReset;
        this.structuralConvergence = structuralConvergence;
        this.numericConvergence = numericConvergence;
    }

    /**
     * Returns the negated lag the frame ends at: 0 at the current row, negative below it. A
     * RANGE descriptor reports it in the designated timestamp column's native units and a ROWS
     * descriptor as a row count, which is the same split {@link #getFrameLo()} carries, so the
     * two bounds of one descriptor are always commensurable with each other.
     * <p>
     * The compiler folds {@code EXCLUDE CURRENT ROW} in before recording it, so this describes
     * the frame the window factories evaluate rather than the one the parser model holds.
     */
    public long getFrameHi() {
        return frameHi;
    }

    /**
     * Returns the negated look-behind the frame starts at, in the units
     * {@link #getFrameHi()} describes. {@link #getRangeFrameWidth()} and
     * {@link #getRowsPrecedingCount()} are the readers a repair uses; they answer the same
     * magnitude with the finite-frame check the bound needs in front of it.
     */
    public long getFrameLo() {
        return frameLo;
    }

    public String getHighBoundStrategy() {
        return highBoundStrategy;
    }

    public DependencyKind getKind() {
        return kind;
    }

    public String getLowBoundStrategy() {
        return lowBoundStrategy;
    }

    public NumericConvergence getNumericConvergence() {
        return numericConvergence;
    }

    public String getOrderSignature() {
        return orderSignature;
    }

    public String getPartitionSignature() {
        return partitionSignature;
    }

    /**
     * Returns the finite preceding width {@code W}, in the designated timestamp
     * column's native units, for a bounded RANGE descriptor. The compiler has
     * already normalized any time unit the user wrote into those units, so a
     * caller can subtract this from a timestamp directly.
     * <p>
     * The width runs from the current row, not from where the frame ends, so a frame ending
     * {@code V} below its own row reports the same {@code W} as the one ending at it -
     * {@link #isFiniteRange()} states why that stays a bound.
     */
    public long getRangeFrameWidth() {
        if (!isFiniteRange()) {
            throw new IllegalStateException("not a finite RANGE dependency");
        }
        return -frameLo;
    }

    /**
     * Returns the finite look-behind row count {@code Nmax} for a bounded ROWS
     * descriptor. Unlike the RANGE width this needs no unit normalization - the
     * parser already carries a row count - but it is equally a per-key quantity:
     * {@code Nmax} rows of the <b>same partition key</b>, not {@code Nmax} rows of
     * the cursor. That is why neither bound follows from timestamp arithmetic and
     * both have to be discovered by scanning.
     * <p>
     * The count runs from the current row, not from where the frame ends, so a frame ending
     * {@code M} rows below its own row reports the same {@code Nmax} as the one ending at it -
     * {@link #isFiniteRows()} states why that stays a bound.
     */
    public long getRowsPrecedingCount() {
        if (!isFiniteRows()) {
            throw new IllegalStateException("not a finite ROWS dependency");
        }
        return -frameLo;
    }

    public StructuralConvergence getStructuralConvergence() {
        return structuralConvergence;
    }

    /**
     * Returns the base table's designated timestamp type the frame bounds are expressed in.
     */
    public int getTimestampType() {
        return timestampType;
    }

    /**
     * Returns whether the function's state is fully determined by the rows its frame
     * admits, so a replay warmed up over the frame's own extent reproduces every value it
     * emits from the output floor onward. This is what licenses a localized repair to read
     * nothing below the dependency floor {@code L}; a function that reaches outside its
     * declared frame - {@code lag()} counts rows by its own offset, not by the frame's -
     * declines the repair plan instead of being replayed against a warm-up that never fed
     * the rows it needs.
     *
     * @see io.questdb.griffin.engine.window.WindowFunction#hasFrameLocalCheckpointState()
     */
    public boolean hasFrameLocalState() {
        return hasFrameLocalState;
    }

    /**
     * Returns true when this descriptor is a {@code RANGE W PRECEDING} frame of finite width
     * over a timestamp column, ending at or below the current row - the RANGE shapes whose
     * forward influence boundary {@code H} is derivable by timestamp arithmetic.
     * <p>
     * A frame ending {@code V} below its own row is admitted on the same width. Output at
     * {@code t} then reads base rows in {@code [t - W, t - V]}, a subset of the
     * {@code [t - W, t]} the same-width frame ending at the current row reads, so the
     * look-behind alone keeps bounding the repair on both sides: a replay from
     * {@code R - W} feeds every row the frame admits, and a base row at {@code m} joins the
     * frame of output in {@code [m + V, m + W]} only, so nothing at or above
     * {@code changeMaxTs + W + 1} can have moved. Both bounds are looser than a lagging
     * frame needs, which widens the repair interval and never narrows it.
     * <p>
     * Only the high bound's sign is read here. The magnitude the two bounds are functions
     * of is the look-behind, which {@link #getRangeFrameWidth()} answers. The one high bound
     * this rejects on its magnitude is {@code Long.MIN_VALUE}, the encoding an unbounded
     * look-behind uses - a bound that reaches it names no finite lag, and negating a literal
     * {@code Long.MAX_VALUE PRECEDING} is what gets there.
     */
    public boolean isFiniteRange() {
        return kind == DependencyKind.RANGE_W_PRECEDING_CURRENT_ROW
                && frameLo != Long.MIN_VALUE
                && frameLo <= 0
                && frameHi != Long.MIN_VALUE
                && frameHi <= 0
                && ColumnType.isTimestamp(timestampType);
    }

    /**
     * Returns true when this descriptor is a {@code ROWS N PRECEDING} frame with a finite
     * {@code N}, ending at or below the current row - the ROWS shapes whose bounds a repair
     * can discover at all. Both bounds are data-dependent: a change of {@code N} rows
     * spans however much time the key's own rows happen to span, so the planner
     * counts rows per key instead of adding a width to a timestamp.
     * <p>
     * A frame ending {@code M} rows below its own row is admitted on the same count,
     * because it leaves that discovery where it stands. Let {@code c} be a key's last row at
     * or below the change and {@code f_i} its {@code i}-th row above: the frame at
     * {@code f_i} spans {@code [f_i - N, f_i - M]}, so it holds {@code c} exactly while
     * {@code M <= i <= N}. The forward scan converges on the upper end of that interval,
     * which does not move; the lower end only removes rows from the affected set. The
     * backward walk follows the same subset argument - computing a row needs its {@code N}-th
     * through {@code M}-th predecessors, which the {@code N} predecessors it counts contain.
     * <p>
     * Only the high bound's sign is read here. The magnitude both scans count against is the
     * look-behind, which {@link #getRowsPrecedingCount()} answers, and {@code Long.MIN_VALUE}
     * is turned away for the reason {@link #isFiniteRange()} gives.
     */
    public boolean isFiniteRows() {
        return kind == DependencyKind.ROWS_N_PRECEDING_CURRENT_ROW
                && frameLo != Long.MIN_VALUE
                && frameLo <= 0
                && frameHi != Long.MIN_VALUE
                && frameHi <= 0
                && ColumnType.isTimestamp(timestampType);
    }

    public boolean supportsKeyReset() {
        return supportsKeyReset;
    }

    public boolean supportsKeyRestore() {
        return supportsKeyRestore;
    }

    public enum NumericConvergence {
        EXACT,
        FLOATING_TOLERANCE
    }

    public enum StructuralConvergence {
        EXACT
    }
}
