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

package io.questdb.griffin.engine.window;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.engine.functions.columns.ColumnFunction;
import io.questdb.std.Decimals;
import io.questdb.std.Numbers;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Immutable identity and runtime layout of one <b>accumulator component</b>: the unit a
 * fused window state is made of.
 * <p>
 * The unit is deliberately not a SELECT-list function call. {@code sum(x)},
 * {@code avg(x)} and {@code count(x)} over one window are three readings of one running
 * {@code (sum, nonNullCount)} pair, so the component is the pair and each output is a
 * {@link WindowAccumulatorProjection projection} onto it. That is what lets one map value
 * slice serve all three, and what keeps the sharing decision free of an "owner output
 * position" a recompile could invalidate.
 * <p>
 * Everything here is a runtime fact - which rows contribute, how many map value slots the
 * state occupies, which slot holds which field, whether one family's state contains
 * another's. The durable side of the same component - a codec version, a byte image, a
 * persisted manifest offset - lives in {@code LiveViewAccumulatorDescriptor}, which wraps
 * one of these. There is one family, contribution and containment table and it is this
 * one: two would drift.
 *
 * <h2>Two components are the same only when everything about their state is</h2>
 * The identity below is the whole of the sharing proof, and it deliberately carries more
 * than the family:
 * <ul>
 *     <li>the <b>argument</b>, as a {@code (base column index, column type)} pair.
 *     Only a direct compiled column reference gets a key at all - see
 *     {@link #directColumnIndex} - because that pair is the whole of the room this
 *     identity has for one. {@link WindowKeyExpressionIdentity} names an expression a
 *     PARTITION BY term may be, and admitting one here is a wider identity rather than a
 *     use of that name: it is also what {@code nth_value(x, n)} needs, whose state is keyed
 *     by a compiled constant as well as by a column. A family
 *     {@link #familyTakesArgument} says takes none
 *     carries the fixed {@code (NO_ARGUMENT_COLUMN_INDEX, UNDEFINED)} pair instead, so its
 *     identity is one exact value rather than an absence;</li>
 *     <li>the <b>contribution predicate</b>, because two counters over the same window can
 *     still diverge on which rows they count. {@code sum(a)} counts finite {@code a}
 *     values and {@code count(b)} counts non-null {@code b} values: those disagree on every
 *     row where exactly one is null, so they are never one component however identical the
 *     rest of the window is.</li>
 * </ul>
 * The window, frame and partition identity are <b>not</b> repeated here: a component lives
 * under exactly one window group, and the group carries them, so a component identity is
 * only ever compared against another under the same group.
 *
 * <h2>One component may contain another</h2>
 * Two identities that differ can still describe one state slice, when one family's state
 * <i>contains</i> the other's verbatim: the counter a {@code count(x)} keeps on its own is
 * the same counter a {@code sum(x)} over the same argument already keeps beside its sum,
 * and equally the one Welford's accumulator keeps behind its running mean.
 * {@link #derivedSlotOffset} is where that containment is stated, one proved pair at a
 * time, and it is what lets a fused group hold {@code sum(x) + avg(x) + count(x)} in two
 * slots rather than three. Containment is strictly narrower than identity and is never
 * assumed from the arithmetic alone - the argument and the contribution predicate still
 * have to match, for the same reason two identities do.
 * <p>
 * {@code count(*)} is a {@link #FAMILY_ROW_COUNT row-count} component rather than a
 * non-null count, and the two are never interchangeable: a row count has no argument, so
 * nothing about it could make it agree with a counter that skips the rows where some
 * column is null. What it does share is {@code row_number()}, which keeps the very same
 * counter over the very same rows.
 *
 * <h2>A running extremum is its own family, one per direction</h2>
 * {@code max} and {@code min} keep one slot each - the largest, or smallest, contributing
 * value seen so far - and they are {@link #FAMILY_DOUBLE_MAX six} separate families rather
 * than one with a direction beside it. A component is a state, and a running maximum is
 * simply not the same state as a running minimum: neither can be computed from the other,
 * so two calls over one column merge only when they point the same way. The state's own
 * type splits them again, for the reason the sum families are split from the dispersion
 * ones - a DOUBLE extremum, a 64-bit one and a DECIMAL one are read and reset differently.
 * <p>
 * They also mark the point at which the identity value of a slice stops being zero.
 * {@code sum}, {@code count} and Welford's accumulator all start at zero and mean it; an
 * extremum has to start at "nothing has contributed yet", which is
 * {@link #getSlotIdentityBits NaN} for a DOUBLE state, {@code LONG_NULL} for a 64-bit one
 * and the argument width's own NULL for a DECIMAL one. All of them are values the
 * contribution predicate refuses, so none can be confused with a real one, and all are what
 * the unbounded frame's own implementation emits for a partition no row has contributed to.
 *
 * <h2>A captured value is a state too, and the flag beside it is what makes it one</h2>
 * {@code first_value} and {@code last_value} keep no total and no counter: they keep one of the
 * argument's own values, chosen by the order the traversal visits the partition's rows in. That
 * is a state and not a projection of one - which row's value it is depends on every row absorbed
 * so far - so they are families here like anything else, {@link #FAMILY_DOUBLE_FIRST_VALUE} and
 * its five siblings.
 * <p>
 * What they add to the model is the flag. An accumulating family reads its own emptiness off its
 * state - a zero total, a NaN extremum - and the two respect-nulls families cannot, because the
 * value they capture may be the argument's own NULL and every later row has to leave it alone.
 * The unfused implementations answer that with {@code MapValue.isNew()}, which a group cannot:
 * the entry is created and put to identity before any contributor runs, and a live view's entry
 * may have been created by its anchor. So {@link #FIELD_CAPTURED} carries it in the slice, the
 * way {@link #RING_STATE_UNALLOCATED} carries "this partition has no ring yet" for the bounded
 * families. The two IGNORE NULLS {@code first_value} families need no such flag and do not have
 * one: they only ever write a value their own predicate admits, so their empty state is the
 * argument type's own NULL and is unambiguous.
 *
 * <h2>A slot is not always a 64-bit word</h2>
 * {@link #FAMILY_DECIMAL_MAX} is the first family whose layout is a function of its
 * <b>argument</b> as well as of its family: a {@code max} over a DECIMAL column accumulates
 * at that column's own width, so the component keeps one LONG for the four narrow widths -
 * which is what those implementations store - one {@code DECIMAL128} for a DECIMAL128
 * argument and one {@code DECIMAL256} for a DECIMAL256 one. The argument type is part of
 * every identity already, so nothing about the sharing proof changes; what changes is that
 * {@link #getSlotColumnType} and {@link #getSlotIdentityBits} are the descriptor's answers
 * rather than the family's, and that a slice's identity no longer always fits one word -
 * see {@link #resetState}.
 *
 * <h2>A component's state is not always all in the map value</h2>
 * {@link #FAMILY_DOUBLE_ROWS_SUM_COUNT} and {@link #FAMILY_ROWS_NON_NULL_COUNT} are the first
 * families whose state continues outside the slice - see {@link #isRingBacked}. A bounded ROWS
 * frame gives rows back as well as taking them, so the accumulator has to keep the frame's own
 * values beside its total: the map value carries the total, the counter and the ring's address,
 * and the ring lives in the arena the contributing function already owns. The sharing proof does
 * not change, because the frame is part of the group's {@link WindowMapSpec} and a component only
 * ever merges inside one group; what changes is that the group is no longer the only owner of a
 * fused query's state, and that such a state cannot be moved between maps or reset into
 * existence.
 * <p>
 * {@link #FAMILY_DOUBLE_RANGE_SUM_COUNT} and {@link #FAMILY_RANGE_NON_NULL_COUNT} are the bounded
 * RANGE counterparts, and they are ring-backed in the same way and wider: a RANGE frame's length
 * is the data's rather than the query's, so its ring is <b>resizable</b> and the slice has to
 * carry its length and its capacity as well as its address. That is the whole of the difference -
 * the ring is still the contributor's, still written by nothing else, and still invisible to
 * every projection.
 *
 * <h2>Two sums over one column can still be two components</h2>
 * {@link #FAMILY_DOUBLE_KAHAN_SUM_COUNT} and {@link #FAMILY_DOUBLE_SUM_COUNT} agree on which
 * rows contribute and both start their first slot at zero, and they are still separate
 * states in both directions, because a compensated total and a plain one are different
 * numbers over the same rows. That is the case that says a component's identity is the
 * arithmetic and not the layout: the two would be indistinguishable to a rule that compared
 * widths, slot types or contribution predicates. Their counters do agree, and that pair is
 * declared in {@link #derivedSlotOffset} like every other.
 */
public final class WindowAccumulatorDescriptor {
    /**
     * Every row contributes. The predicate of a row-count component, and the reason
     * such a component is never interchangeable with a counter over an argument: a
     * {@code count(x)} skips the rows where {@code x} is absent and this one does not.
     */
    public static final int CONTRIBUTION_EVERY_ROW = 3;
    /**
     * {@code Numbers.isFinite(arg.getDouble(record))} - the predicate a DOUBLE
     * {@code sum}/{@code avg} contributes under and, deliberately, the one
     * {@code count(double)} uses too, so the three agree on infinities as well as on
     * NULL. Distinct from {@link #CONTRIBUTION_TYPED_NOT_NULL} precisely because a
     * plain null test would admit an infinity a finite-sum accumulator never counted.
     * <p>
     * It is not confined to a DOUBLE argument. Every type
     * {@link #isWidenedToDouble} admits reaches the same factories through the same
     * {@code getDouble}, so the predicate is the argument type's only through the
     * widening - which is exactly why the type stays part of the identity.
     */
    public static final int CONTRIBUTION_FINITE_DOUBLE = 1;
    /**
     * No predicate this class can name for the requested {@code (family, argument
     * type)} pair. The caller declines the component rather than guessing.
     */
    public static final int CONTRIBUTION_NONE = 0;
    /**
     * The argument type's own null test. The type is part of the identity, so two
     * components carrying this kind over different types stay distinct even though
     * they name the same predicate family.
     */
    public static final int CONTRIBUTION_TYPED_NOT_NULL = 2;
    /**
     * State {@code [max: <the argument's own DECIMAL payload>]}, contributed by a
     * {@code max} over a DECIMAL argument on an unbounded partitioned frame. The identity is
     * that width's NULL - {@code Decimals.DECIMAL8_NULL} through {@code DECIMAL64_NULL} for
     * the four narrow widths, and the raw NULL of a {@code Decimal128} or {@code Decimal256}
     * for the two wide ones - which {@link #CONTRIBUTION_TYPED_NOT_NULL} refuses over every
     * one of them.
     * <p>
     * One family for six widths, and the width is carried where it already was: in the
     * argument type, which is part of every identity. What it decides here beyond the sharing
     * proof is the slot's own type, because the six implementations store at three different
     * widths - the narrow four put the raw payload in a LONG, and the wide two keep a
     * {@code DECIMAL128} or a {@code DECIMAL256}. Two extrema over columns of different
     * widths therefore never share a slice, exactly as a DATE and a TIMESTAMP one do not.
     * <p>
     * Separate from {@link #FAMILY_LONG_MAX} even where both keep a LONG, because the two
     * store different things in it: a 64-bit extremum keeps the argument's payload word and
     * starts at {@code LONG_NULL}, while a narrow DECIMAL extremum keeps a scaled payload
     * whose absent value is its own width's sentinel - {@code Byte.MIN_VALUE} for a DECIMAL8,
     * which is an ordinary value of every other type.
     */
    public static final int FAMILY_DECIMAL_MAX = 10;
    /**
     * State {@code [min: <the argument's own DECIMAL payload>]}, the {@link #FAMILY_DECIMAL_MAX}
     * state pointing the other way, and separate for the reason {@link #FAMILY_DOUBLE_MIN} is.
     */
    public static final int FAMILY_DECIMAL_MIN = 11;
    /**
     * State {@code [value: DOUBLE]}, contributed by a DOUBLE {@code first_value(x) ignore nulls}
     * over an unbounded partitioned frame: the first value the partition offered that
     * {@link #CONTRIBUTION_FINITE_DOUBLE} admits.
     * <p>
     * One slot where {@link #FAMILY_DOUBLE_FIRST_VALUE} keeps two, and the difference is the whole
     * of what IGNORE NULLS changes about the state. This family writes only values its own
     * predicate admits, and NaN is not one of them, so NaN is an empty slice - which is also
     * exactly what the same window emits for a partition no finite row has reached. Its
     * respect-nulls sibling captures whatever the first row held, NULL included, and so cannot
     * read its own emptiness off the value at all.
     */
    public static final int FAMILY_DOUBLE_FIRST_NOT_NULL_VALUE = 17;
    /**
     * State {@code [value: DOUBLE, captured: LONG]}, contributed by a DOUBLE
     * {@code first_value(x)} over an unbounded partitioned frame: the value the first row of the
     * partition held, whatever it was.
     * <p>
     * {@link #CONTRIBUTION_EVERY_ROW} is the predicate, and it is the honest one - every row is a
     * candidate, and which one wins is the traversal's answer rather than the value's. That also
     * makes the family reusable by a call that is not a {@code first_value} at all: a
     * {@code last_value(x)} over a whole partition is the same state read under a backward
     * pass-1 scan, since the first row such a traversal visits is the partition's last. The two
     * never meet in one group - a group's {@link WindowMapSpec} carries the pass count and the
     * scan direction - so one family serving both is a shared rule and not a shared slice.
     * <p>
     * The flag is what a group needs and a private map does not: see the class javadoc.
     */
    public static final int FAMILY_DOUBLE_FIRST_VALUE = 16;
    /**
     * State {@code [sum: DOUBLE, compensation: DOUBLE, nonNullCount: LONG]}, contributed by
     * a {@code ksum} over an unbounded partitioned frame: a compensated (Kahan) running
     * total, the compensation term that makes it one, and the counter every DOUBLE family
     * keeps.
     * <p>
     * Separate from {@link #FAMILY_DOUBLE_SUM_COUNT} in both directions, and not because the
     * widths differ. A compensated total and a plain one are different numbers over the same
     * rows - that is the whole point of the compensation - so a {@code sum} projection must
     * never read this component's first slot, nor a {@code ksum} projection a plain sum's.
     * What the two genuinely share is the counter: it counts the same rows under the same
     * predicate, so a {@code count(x)} folds onto either.
     */
    public static final int FAMILY_DOUBLE_KAHAN_SUM_COUNT = 9;
    /**
     * State {@code [value: DOUBLE, captured: LONG]}, contributed by a DOUBLE
     * {@code last_value(x) ignore nulls} over an unbounded partitioned frame: the most recent
     * value {@link #CONTRIBUTION_FINITE_DOUBLE} admits, and the partition's first row's value
     * while none has arrived.
     * <p>
     * The second clause is the implementation's and is why the flag is here. A
     * {@code last_value ignore nulls} takes the first row of a partition unconditionally and
     * only then starts skipping the rows its predicate refuses, so a partition beginning with
     * an infinity emits that infinity until a finite value replaces it. An empty-is-NULL
     * reading would emit NULL there instead, which is a different answer on real data rather
     * than a tidier one, so the flag says "this partition has written its slot" and the
     * contributor writes iff it has not or the row contributes.
     */
    public static final int FAMILY_DOUBLE_LAST_NOT_NULL_VALUE = 18;
    /**
     * State {@code [max: DOUBLE]}, contributed by a DOUBLE {@code max} over an unbounded
     * partitioned frame. The identity is NaN, which is a value
     * {@link #CONTRIBUTION_FINITE_DOUBLE} refuses, so an empty state and a state holding a
     * real value are never confused - and NaN is what the same window emits for a partition
     * no finite row has reached.
     * <p>
     * "DOUBLE" names the state and not the argument, as it does for
     * {@link #FAMILY_DOUBLE_SUM_COUNT}: a column that reaches {@code max(D)} by widening
     * accumulates into the same slot, and the argument type stays in the identity because
     * the widening is what the contribution predicate is proved through.
     */
    public static final int FAMILY_DOUBLE_MAX = 5;
    /**
     * State {@code [min: DOUBLE]}, contributed by a DOUBLE {@code min} over an unbounded
     * partitioned frame. Separate from {@link #FAMILY_DOUBLE_MAX} because a running minimum
     * cannot be read out of a running maximum, however identical the rest of the window is.
     */
    public static final int FAMILY_DOUBLE_MIN = 6;
    /**
     * State {@code [sum: DOUBLE, nonNullCount: LONG, ringIndex: LONG, ringOffset: LONG,
     * ringSize: LONG, ringCapacity: LONG]} plus the contributor's own resizable ring of
     * {@code (timestamp, value)} pairs, contributed by a DOUBLE {@code sum} or {@code avg} over a
     * <b>bounded</b> RANGE frame.
     * <p>
     * {@link #FAMILY_DOUBLE_ROWS_SUM_COUNT}'s state with the ring's geometry added, and
     * {@link #isRingBacked ring-backed} for the same reason: a bounded frame gives rows back. What
     * a RANGE frame adds is that how many rows it spans is the timestamps' answer rather than the
     * query's, so the ring grows on demand - and the three slots that describe it are the address,
     * the number of pairs it holds and the number it can hold. The pairs carry their own timestamp
     * because that, and not a position, is what decides when one leaves.
     * <p>
     * Separate from every other family here, the bounded ROWS one included: two frames of different
     * kinds never meet in one group - the framing mode is part of a group's {@link WindowMapSpec} -
     * and the states are not the same shape anyway.
     * <p>
     * The slice holds the <b>current frame's</b> total and count, exactly as the ROWS family's
     * does, so {@link WindowAccumulatorProjection#PROJECTION_SUM} and
     * {@link WindowAccumulatorProjection#PROJECTION_AVG} read the same two fields here. Unlike the
     * ROWS family it needs no schedule shift to get there: a RANGE row drops what the frame has
     * left before it absorbs what the frame has gained, so the unfused implementation already ends
     * every row with the answer in its slots.
     */
    public static final int FAMILY_DOUBLE_RANGE_SUM_COUNT = 14;
    /**
     * State {@code [sum: DOUBLE, nonNullCount: LONG, ringIndex: LONG, ringOffset: LONG]} plus
     * the contributor's own ring of {@code ringSize} doubles, contributed by a DOUBLE
     * {@code sum} or {@code avg} over a <b>bounded</b> ROWS frame.
     * <p>
     * The first family whose state is not wholly in the map value - see {@link #isRingBacked} -
     * and separate from {@link #FAMILY_DOUBLE_SUM_COUNT} for that reason and not only because
     * the frames differ. A cumulative sum absorbs a row and keeps it; a bounded one has to give
     * it back when the frame passes it, which is what the ring of the frame's own values is for
     * and what the two index slots address. The two also never meet: a component only ever
     * merges inside one group, and the frame is part of a group's {@link WindowMapSpec}.
     * <p>
     * The state the map value carries is the <b>current frame's</b> total and count, so a
     * {@link WindowAccumulatorProjection#PROJECTION_SUM} or
     * {@link WindowAccumulatorProjection#PROJECTION_AVG} output reads the same two fields here
     * that it reads off a cumulative component. That is a change of schedule rather than of
     * arithmetic: the unfused implementation subtracts the value leaving the frame at the end of
     * the row that last needed it, leaving a total that is nobody's answer, and a fused
     * contributor defers that subtraction to the row the value actually leaves on. The
     * operations and their order are identical, so the two paths agree bit for bit; what it
     * costs is one ring cell, since the value has to survive one row longer.
     */
    public static final int FAMILY_DOUBLE_ROWS_SUM_COUNT = 12;
    /**
     * State {@code [sum: DOUBLE, nonNullCount: LONG]}, contributed by a DOUBLE
     * {@code sum} or {@code avg} over an unbounded partitioned frame.
     * <p>
     * "DOUBLE" names the <b>state</b> rather than the argument. There is one
     * {@code sum(D)} window factory and no integral one, so a BYTE, SHORT, INT, LONG or
     * FLOAT column reaches it by widening and accumulates into the same two fields; the
     * argument type still separates the identities, because the widening is what the
     * contribution predicate is proved through.
     */
    public static final int FAMILY_DOUBLE_SUM_COUNT = 1;
    /**
     * State {@code [mean: DOUBLE, m2: DOUBLE, nonNullCount: LONG]} - Welford's online
     * accumulator - contributed by any of {@code stddev_samp}, {@code stddev_pop},
     * {@code var_samp} and {@code var_pop} over an unbounded partitioned frame. The four
     * differ only in the arithmetic they read off {@code (m2, nonNullCount)}, so a query
     * naming several of them over one column keeps one three-slot component rather than
     * one per call. Like {@link #FAMILY_DOUBLE_SUM_COUNT} it names its state's type and
     * not its argument's.
     */
    public static final int FAMILY_DOUBLE_WELFORD = 4;
    /**
     * State {@code [value: LONG]} - one raw 64-bit payload - contributed by a
     * {@code first_value(x) ignore nulls} over a LONG, DATE or TIMESTAMP argument on an
     * unbounded partitioned frame. {@link #FAMILY_DOUBLE_FIRST_NOT_NULL_VALUE} at the other
     * state width, empty at {@code Numbers.LONG_NULL}, which is what
     * {@link #CONTRIBUTION_TYPED_NOT_NULL} refuses over every one of those types.
     */
    public static final int FAMILY_LONG_FIRST_NOT_NULL_VALUE = 20;
    /**
     * State {@code [value: LONG, captured: LONG]} - one raw 64-bit payload and the flag -
     * contributed by a {@code first_value(x)} over a LONG, DATE or TIMESTAMP argument on an
     * unbounded partitioned frame. {@link #FAMILY_DOUBLE_FIRST_VALUE} at the other state width,
     * and separate from it for the reason {@link #FAMILY_LONG_MAX} is separate from
     * {@link #FAMILY_DOUBLE_MAX}: the two implementations read and store different words.
     */
    public static final int FAMILY_LONG_FIRST_VALUE = 19;
    /**
     * State {@code [value: LONG, captured: LONG]}, contributed by a
     * {@code last_value(x) ignore nulls} over a LONG, DATE or TIMESTAMP argument on an unbounded
     * partitioned frame. {@link #FAMILY_DOUBLE_LAST_NOT_NULL_VALUE} at the other state width,
     * including its second clause: the partition's first row is taken whatever it held.
     */
    public static final int FAMILY_LONG_LAST_NOT_NULL_VALUE = 21;
    /**
     * State {@code [max: LONG]} - one raw 64-bit payload - contributed by a {@code max} over
     * a LONG, DATE or TIMESTAMP argument on an unbounded partitioned frame. The identity is
     * {@code Numbers.LONG_NULL}, which {@link #CONTRIBUTION_TYPED_NOT_NULL} refuses over
     * every one of those types, so it doubles as the "nothing has contributed" marker and as
     * the value the same window emits for such a partition.
     * <p>
     * One family rather than three, because the state is the argument's payload word and
     * nothing about the argument's type: {@code max(M)} and {@code max(N)} reach one
     * implementation that stores what {@code max(L)} stores. The three stay separate
     * <b>components</b> anyway - the argument type is part of the identity - so a DATE and a
     * TIMESTAMP extremum never share a slice.
     */
    public static final int FAMILY_LONG_MAX = 7;
    /**
     * State {@code [min: LONG]}, the {@link #FAMILY_LONG_MAX} state pointing the other way,
     * and separate for the reason {@link #FAMILY_DOUBLE_MIN} is.
     */
    public static final int FAMILY_LONG_MIN = 8;
    /**
     * Not an accumulator component. The default a window function reports when it
     * does not participate in a fused window state.
     */
    public static final int FAMILY_NONE = 0;
    /**
     * State {@code [nonNullCount: LONG]}, contributed by a {@code count(x)} over an
     * unbounded partitioned frame.
     */
    public static final int FAMILY_NON_NULL_COUNT = 2;
    /**
     * State {@code [nonNullCount: LONG, ringIndex: LONG, ringOffset: LONG, ringSize: LONG,
     * ringCapacity: LONG]} plus the contributor's own resizable ring of timestamps, contributed by
     * a {@code count(x)} over a <b>bounded</b> RANGE frame.
     * <p>
     * {@link #FAMILY_DOUBLE_RANGE_SUM_COUNT}'s state without a total. The ring is a timestamp per
     * contributing row where the sum family's is a {@code (timestamp, value)} pair, which is the
     * contributor's own business - the map value carries the same five slots either way.
     */
    public static final int FAMILY_RANGE_NON_NULL_COUNT = 15;
    /**
     * State {@code [nonNullCount: LONG, ringIndex: LONG, ringOffset: LONG]} plus the
     * contributor's own ring of {@code ringSize} flags, contributed by a {@code count(x)} over a
     * <b>bounded</b> ROWS frame.
     * <p>
     * {@link #FAMILY_DOUBLE_ROWS_SUM_COUNT}'s counter without a total, and
     * {@link #isRingBacked ring-backed} for the same reason: a bounded counter has to know which
     * of the frame's rows contributed so it can give one back when the frame passes it, and one
     * flag per row of the frame is what that takes. The ring is a byte per cell where the sum
     * family's is a double, which is the contributor's own business - the map value carries the
     * same three slots either way.
     */
    public static final int FAMILY_ROWS_NON_NULL_COUNT = 13;
    /**
     * State {@code [rowCount: LONG]}, contributed by {@code count(*)} or by a
     * partitioned {@code row_number()} over an unbounded partitioned frame. Both keep
     * the same counter of rows, and after {@code n} rows both read {@code n} off it.
     * <p>
     * It takes no argument at all, which is what keeps it apart from
     * {@link #FAMILY_NON_NULL_COUNT}: a row count and a non-null count agree only on
     * data where the counted column is never null, and the identity must not depend on
     * the data.
     */
    public static final int FAMILY_ROW_COUNT = 3;
    /**
     * Whether this partition has written {@link #FIELD_CAPTURED_VALUE} yet - zero at identity
     * and one afterwards. Present only in the three respect-the-first-row families
     * ({@link #FAMILY_DOUBLE_FIRST_VALUE}, {@link #FAMILY_LONG_FIRST_VALUE} and the two
     * {@code last_value} ones), and read and written by their contributor alone: it says what
     * a private map answers with {@code MapValue.isNew()} and a group's value cannot.
     * <p>
     * The two IGNORE NULLS {@code first_value} families deliberately do not carry it. Their
     * value slot is only ever written with a value their predicate admits, so its NULL is the
     * same statement in one slot fewer.
     */
    public static final int FIELD_CAPTURED = 11;
    /**
     * The value one row of the partition held - the first row's, the first contributing row's or
     * the most recent contributing row's, depending on the family. Present only in the six
     * {@code first_value}/{@code last_value} families, which carry it and at most
     * {@link #FIELD_CAPTURED} beside it.
     */
    public static final int FIELD_CAPTURED_VALUE = 10;
    /**
     * The running largest or smallest contributing value. Present only in the four
     * {@code max}/{@code min} families, which carry it and nothing else.
     */
    public static final int FIELD_EXTREMUM = 4;
    /**
     * The compensation term a Kahan summation carries beside its running total. Present
     * only in {@link #FAMILY_DOUBLE_KAHAN_SUM_COUNT}, and read and written by its
     * contributor alone - no output projects it.
     */
    public static final int FIELD_KAHAN_COMPENSATION = 5;
    /**
     * The running sum of squared deviations from the running mean. Present only in
     * {@link #FAMILY_DOUBLE_WELFORD}.
     */
    public static final int FIELD_M2 = 3;
    /**
     * The running mean. Present only in {@link #FAMILY_DOUBLE_WELFORD}.
     */
    public static final int FIELD_MEAN = 2;
    /**
     * The count of contributing rows. Present in every accumulating family, and in none of
     * the four extremum ones - a running max keeps its answer and nothing else, which is
     * why a bound function's "am I fused" test is its component's slot base rather than
     * this field's.
     */
    public static final int FIELD_NON_NULL_COUNT = 1;
    /**
     * How many cells this partition's ring can hold. Present only in the two bounded-RANGE
     * families, whose ring grows on demand because a RANGE frame's length is the data's answer,
     * and the contributor's alone for the reason {@link #FIELD_RING_INDEX} is.
     */
    public static final int FIELD_RING_CAPACITY = 9;
    /**
     * The ring cell the oldest of the frame's values sits in - the one the next row drops.
     * Present only in the {@link #isRingBacked ring-backed} families, and read and written
     * by their contributor alone: it addresses the contributor's own ring, so no output has any
     * use for it.
     */
    public static final int FIELD_RING_INDEX = 6;
    /**
     * Where this partition's ring starts in the contributor's arena, or
     * {@link #RING_STATE_UNALLOCATED} while it has none. Present only in the
     * {@link #isRingBacked ring-backed} families, and the contributor's alone for the reason
     * {@link #FIELD_RING_INDEX} is.
     */
    public static final int FIELD_RING_OFFSET = 7;
    /**
     * How many cells of this partition's ring are in use - which is not the frame's own count,
     * because a ring also buffers the rows between the frame's high bound and the current one.
     * Present only in the two bounded-RANGE families, and the contributor's alone for the reason
     * {@link #FIELD_RING_INDEX} is.
     */
    public static final int FIELD_RING_SIZE = 8;
    /**
     * The running sum. Present only in {@link #FAMILY_DOUBLE_SUM_COUNT}.
     */
    public static final int FIELD_SUM = 0;
    /**
     * The argument key of a family that takes no argument. Paired with
     * {@link ColumnType#UNDEFINED} as the type, so an argumentless identity is one exact
     * pair rather than a range of them.
     */
    public static final int NO_ARGUMENT_COLUMN_INDEX = -1;
    /**
     * The {@link #FIELD_RING_OFFSET} a partition no row has reached yet carries: "this entry has
     * no ring". It is what {@link #resetState} writes, because only a contributor can allocate
     * one - the descriptor has no arena and the group that resets the slice has no ring of its
     * own - and it is negative rather than zero because zero is the first partition's perfectly
     * ordinary address.
     */
    public static final long RING_STATE_UNALLOCATED = -1L;
    private final int argumentColumnIndex;
    private final int argumentColumnType;
    private final int contributionKind;
    private final int family;

    private WindowAccumulatorDescriptor(
            int family,
            int contributionKind,
            int argumentColumnIndex,
            int argumentColumnType
    ) {
        this.family = family;
        this.contributionKind = contributionKind;
        this.argumentColumnIndex = argumentColumnIndex;
        this.argumentColumnType = argumentColumnType;
    }

    /**
     * Returns the predicate under which a row of {@code argumentColumnType} joins a
     * component of {@code family}, or {@link #CONTRIBUTION_NONE} when this build can
     * prove none.
     * <p>
     * The mapping is a table rather than a guess because it has to agree, case for
     * case, with the predicate the compiled function actually evaluates: the
     * {@code count} factory is selected by the argument's signature type, so the type
     * is what decides whether the runtime counts with {@code Numbers.isFinite} or with
     * a null test. A type not listed here declines, which costs the caller only the
     * fused component and leaves the function on the private map it owns anyway.
     */
    public static int contributionKindFor(int family, int argumentColumnType) {
        return switch (family) {
            // No argument to predicate on, and the caller must not have supplied one:
            // a row count that quietly accepted an argument type would be a second
            // identity for the same state, and the two would not merge.
            case FAMILY_ROW_COUNT -> argumentColumnType == ColumnType.UNDEFINED
                    ? CONTRIBUTION_EVERY_ROW
                    : CONTRIBUTION_NONE;
            // Every row is a candidate: which one the state keeps is the traversal's answer
            // and not the value's, so there is no predicate to name beyond "this argument
            // reaches the implementation that declares the family". Each of the two is
            // admitted over the types its own implementation is selected for, exactly as the
            // extremum pair is - a DOUBLE-stated capture through getDouble, a 64-bit one
            // through the argument's payload word.
            case FAMILY_DOUBLE_FIRST_VALUE, FAMILY_LONG_FIRST_VALUE -> (family == FAMILY_DOUBLE_FIRST_VALUE
                    ? isWidenedToDouble(argumentColumnType)
                    : isLongPayload(argumentColumnType))
                    ? CONTRIBUTION_EVERY_ROW
                    : CONTRIBUTION_NONE;
            // The 64-bit capture's own null test, which is the predicate the IGNORE NULLS
            // implementations apply: they skip a row whose payload word is LONG_NULL. Same
            // list and same reasoning as the 64-bit extremum's.
            case FAMILY_LONG_FIRST_NOT_NULL_VALUE, FAMILY_LONG_LAST_NOT_NULL_VALUE -> isLongPayload(argumentColumnType)
                    ? CONTRIBUTION_TYPED_NOT_NULL
                    : CONTRIBUTION_NONE;
            // Those families have one factory each and it takes a DOUBLE, so every
            // other argument they accept arrives by widening into that DOUBLE. The
            // max/min pair reads its argument through the same getDouble and skips the
            // row on the same isFinite test, so it contributes under the same predicate
            // - which is also why an extremum over a DOUBLE argument never sees an
            // infinity, and its empty state can be NaN. The bounded-ROWS sum applies that
            // same test twice, once as a value enters the frame and once as it leaves, so
            // the rows it holds are exactly the rows this predicate names. The bounded-RANGE
            // one applies it once and keeps only the values that passed, which names the same
            // rows a different way. The two DOUBLE IGNORE NULLS capture families apply it
            // once per row to decide whether the row may replace what the slot holds, which
            // is the same reading of the same rows again.
            case FAMILY_DOUBLE_FIRST_NOT_NULL_VALUE,
                 FAMILY_DOUBLE_KAHAN_SUM_COUNT,
                 FAMILY_DOUBLE_LAST_NOT_NULL_VALUE,
                 FAMILY_DOUBLE_MAX,
                 FAMILY_DOUBLE_MIN,
                 FAMILY_DOUBLE_RANGE_SUM_COUNT,
                 FAMILY_DOUBLE_ROWS_SUM_COUNT,
                 FAMILY_DOUBLE_SUM_COUNT,
                 FAMILY_DOUBLE_WELFORD -> isWidenedToDouble(argumentColumnType)
                    ? CONTRIBUTION_FINITE_DOUBLE
                    : CONTRIBUTION_NONE;
            // The 64-bit extremum's own null test: the implementation reads the
            // argument's payload word and skips it when it equals LONG_NULL. Every type
            // below reaches that reading as its own column function - LONG, DATE and
            // TIMESTAMP have a factory each, and the narrower integrals widen into
            // max(L) - and the type stays in the identity, so a DATE extremum and a
            // TIMESTAMP one over the same word are still two components.
            case FAMILY_LONG_MAX, FAMILY_LONG_MIN -> isLongPayload(argumentColumnType)
                    ? CONTRIBUTION_TYPED_NOT_NULL
                    : CONTRIBUTION_NONE;
            // The DECIMAL extremum's own null test, which is the width's null sentinel and
            // nothing else - the same predicate a count over the same column applies, since
            // max(D) has an implementation per width and each of them skips exactly the rows
            // that width calls absent. A non-DECIMAL argument declines: it reaches a
            // different max() factory storing a different thing.
            case FAMILY_DECIMAL_MAX, FAMILY_DECIMAL_MIN -> isDecimalPayload(argumentColumnType)
                    ? CONTRIBUTION_TYPED_NOT_NULL
                    : CONTRIBUTION_NONE;
            // count() has a factory per argument shape, so this arm is the one that
            // can name a predicate other than the DOUBLE one - and the type is what
            // selects between them. All three counting families share the arm because they
            // share those predicates exactly: one class serves every bounded-ROWS count and
            // one every bounded-RANGE count, and each applies the very lambda the cumulative
            // one does - to the row entering the frame and to the row leaving it alike.
            case FAMILY_NON_NULL_COUNT, FAMILY_RANGE_NON_NULL_COUNT, FAMILY_ROWS_NON_NULL_COUNT -> {
                if (isWidenedToDouble(argumentColumnType)) {
                    yield CONTRIBUTION_FINITE_DOUBLE;
                }
                yield switch (ColumnType.tagOf(argumentColumnType)) {
                    case ColumnType.SYMBOL, ColumnType.VARCHAR -> CONTRIBUTION_TYPED_NOT_NULL;
                    // A DECIMAL count is the argument type's own null test and nothing
                    // else: CountDecimalWindowFunctionFactory selects a predicate per
                    // width, and every one of the six compares against that width's null
                    // sentinel. The type stays in the identity, so a count over a
                    // DECIMAL64 column and one over a DECIMAL128 column remain two
                    // components even though both name this kind.
                    case ColumnType.DECIMAL8,
                         ColumnType.DECIMAL16,
                         ColumnType.DECIMAL32,
                         ColumnType.DECIMAL64,
                         ColumnType.DECIMAL128,
                         ColumnType.DECIMAL256 -> CONTRIBUTION_TYPED_NOT_NULL;
                    default -> CONTRIBUTION_NONE;
                };
            }
            default -> CONTRIBUTION_NONE;
        };
    }

    /**
     * Resolves {@code argument} to the base column it reads, or {@code -1} when it is
     * not a direct compiled column reference of that column's own type.
     * <p>
     * The type check is not redundant with the {@code instanceof}. It is what keeps the
     * argument key's type the one the runtime really evaluates the predicate against, so
     * a column function carrying a type its base column does not have cannot key a
     * component under the wrong contribution semantics.
     * <p>
     * A signature match handing a narrower column straight to a wider factory - a LONG
     * column reaching {@code sum(D)} and {@code count(D)} - passes both halves, because
     * no cast wrapper is inserted and the column function is still the column's own
     * type. Whether that widening is fusible is not this method's answer but
     * {@link #contributionKindFor}'s, which names a predicate only for the types it has
     * been proved over.
     * <p>
     * The same predicate serves a PARTITION BY term, which is why it is here rather than
     * on either caller: a key term and an accumulator argument are the same question -
     * is this expression a column of the record - and answering it twice would be two
     * chances to answer it differently.
     *
     * @param recordTypes the types of the record {@code argument} reads, by index. It is
     *                    the metadata the argument was compiled against for a streaming
     *                    compile; a cached compile passes the record chain's own type list,
     *                    because the chain metadata leaves a hole where every window output
     *                    sits and so cannot be asked how many indexes it spans
     */
    public static int directColumnIndex(@Nullable Function argument, ColumnTypes recordTypes) {
        if (!(argument instanceof ColumnFunction columnFunction)) {
            return -1;
        }
        final int index = columnFunction.getColumnIndex();
        if (index < 0 || index >= recordTypes.getColumnCount()) {
            return -1;
        }
        return argument.getType() == recordTypes.getColumnType(index) ? index : -1;
    }

    /**
     * Returns how many map value slots a component of {@code family} occupies, or
     * {@code 0} for a family this build does not know.
     */
    public static int familySlotCount(int family) {
        return switch (family) {
            case FAMILY_DOUBLE_RANGE_SUM_COUNT -> 6;
            case FAMILY_RANGE_NON_NULL_COUNT -> 5;
            case FAMILY_DOUBLE_ROWS_SUM_COUNT -> 4;
            case FAMILY_DOUBLE_FIRST_VALUE,
                 FAMILY_DOUBLE_LAST_NOT_NULL_VALUE,
                 FAMILY_DOUBLE_SUM_COUNT,
                 FAMILY_LONG_FIRST_VALUE,
                 FAMILY_LONG_LAST_NOT_NULL_VALUE -> 2;
            case FAMILY_DOUBLE_KAHAN_SUM_COUNT,
                 FAMILY_DOUBLE_WELFORD,
                 FAMILY_ROWS_NON_NULL_COUNT -> 3;
            case FAMILY_DECIMAL_MAX,
                 FAMILY_DECIMAL_MIN,
                 FAMILY_DOUBLE_FIRST_NOT_NULL_VALUE,
                 FAMILY_DOUBLE_MAX,
                 FAMILY_DOUBLE_MIN,
                 FAMILY_LONG_FIRST_NOT_NULL_VALUE,
                 FAMILY_LONG_MAX,
                 FAMILY_LONG_MIN,
                 FAMILY_NON_NULL_COUNT,
                 FAMILY_ROW_COUNT -> 1;
            default -> 0;
        };
    }

    /**
     * Whether {@code family}'s identity includes an argument. A family that takes none
     * is identified by its family alone, and its descriptor carries
     * {@link #NO_ARGUMENT_COLUMN_INDEX} with {@link ColumnType#UNDEFINED}.
     * <p>
     * The compiler reads this before it looks for an argument key: a {@code count(*)}
     * has no argument to resolve and would otherwise be declined for the absence, while
     * a function that declares an argumentless family and then hands over an argument is
     * declining itself.
     */
    public static boolean familyTakesArgument(int family) {
        return family != FAMILY_ROW_COUNT;
    }

    /**
     * Builds the component a function over {@code argumentColumnIndex} contributes to,
     * or null when this build cannot name every part of its identity - an unknown
     * family, or an argument type whose contribution predicate
     * {@link #contributionKindFor} declines.
     *
     * @param family              one of the {@code FAMILY_*} constants
     * @param argumentColumnIndex the argument's index in the base metadata the window
     *                            functions were compiled against, or
     *                            {@link #NO_ARGUMENT_COLUMN_INDEX} for an argumentless
     *                            family
     * @param argumentColumnType  the argument's compiled column type, or
     *                            {@link ColumnType#UNDEFINED} for an argumentless family
     */
    public static @Nullable WindowAccumulatorDescriptor of(int family, int argumentColumnIndex, int argumentColumnType) {
        if (familyTakesArgument(family)
                ? argumentColumnIndex < 0
                : argumentColumnIndex != NO_ARGUMENT_COLUMN_INDEX) {
            return null;
        }
        final int contributionKind = contributionKindFor(family, argumentColumnType);
        if (contributionKind == CONTRIBUTION_NONE || familySlotCount(family) <= 0) {
            return null;
        }
        return new WindowAccumulatorDescriptor(family, contributionKind, argumentColumnIndex, argumentColumnType);
    }

    /**
     * Orders two identities canonically. A fused layout is assigned in this order and
     * never in SELECT-list order, so reordering the outputs of one query cannot move a
     * component's slot base.
     * <p>
     * The field order and the unsigned comparison are the ones the live-view durable
     * encoding sorts by, so a plan ordered here and a manifest ordered by encoded bytes
     * agree. The durable side additionally carries a codec version, which is a fact
     * about the persisted image rather than about the state and is
     * {@code LiveViewAccumulatorDescriptor}'s to compare.
     */
    public int compareIdentity(@NotNull WindowAccumulatorDescriptor other) {
        if (family != other.family) {
            return Integer.compare(family, other.family);
        }
        if (contributionKind != other.contributionKind) {
            return Integer.compare(contributionKind, other.contributionKind);
        }
        if (argumentColumnIndex != other.argumentColumnIndex) {
            // Unsigned, because NO_ARGUMENT_COLUMN_INDEX rides in the durable encoding as
            // 0xffffffff and sorts last there. Nothing today compares an argumentless
            // identity against an argument-bearing one under the same family - only the
            // row count takes no argument - but the two orders must not be able to
            // disagree if one ever does.
            return Integer.compareUnsigned(argumentColumnIndex, other.argumentColumnIndex);
        }
        return Integer.compareUnsigned(argumentColumnType, other.argumentColumnType);
    }

    /**
     * Copies this component's slots from one map value to another, so a runtime whose
     * ownership is moving - a window adopting a plan, or handing the state back -
     * carries the accumulator across without going through any durable encoding.
     * <p>
     * Only a live view moves a component this way, and only a component it can persist. A
     * family with no codec is never in one of its plans, so neither a slot wider than a word nor
     * a state that continues outside the map value can reach this - and the throw says so rather
     * than copying the first word of one and leaving the rest of the state behind.
     */
    public void copyState(@NotNull MapValue src, int srcSlotBase, @NotNull MapValue dst, int dstSlotBase) {
        if (isRingBacked()) {
            // The slots would copy cleanly and the copy would be wrong: the ring offset they
            // carry addresses the arena of the contributor the state came from, so the
            // destination would read another map's frame. Unreachable - no ring-backed family
            // has a codec - and stated because "this cannot happen" and "this silently shared
            // one ring between two states" are not the same bug.
            throw new UnsupportedOperationException("a ring-backed component's state does not move between maps");
        }
        for (int i = 0, n = getSlotCount(); i < n; i++) {
            final int slotType = getSlotColumnType(i);
            if (isWideDecimalSlot(slotType)) {
                throw new UnsupportedOperationException("a wide DECIMAL component's state does not move between maps");
            }
            if (slotType == ColumnType.DOUBLE) {
                dst.putDouble(dstSlotBase + i, src.getDouble(srcSlotBase + i));
            } else {
                dst.putLong(dstSlotBase + i, src.getLong(srcSlotBase + i));
            }
        }
    }

    /**
     * Returns the relative slot inside this component's state at which {@code other}'s
     * whole state begins, or {@code -1} when it does not appear at all.
     * <p>
     * A non-negative answer is the licence for one component to serve a projection whose
     * own function would keep {@code other}: the host maintains the state and the guest
     * reads its own fields out of the host's slice at this offset. That is how
     * {@code count(x)} stops costing a component of its own beside {@code sum(x)}.
     * <p>
     * The table below is deliberately a list of proved pairs rather than a rule derived
     * from the families' fields. A containment claim says the guest's whole state is a
     * run inside the host's, which is a fact about the two implementations and not
     * something the field offsets alone establish.
     * <p>
     * Everything the identity comparison requires still applies to a derivation - the
     * same argument, the same contribution predicate - because a counter that counts
     * different rows is a different counter however it is stored. {@code count(b)}
     * beside {@code sum(a)} matches on family containment and on nothing else, so it
     * gets {@code -1} and keeps its own component.
     */
    public int derivedSlotOffset(@NotNull WindowAccumulatorDescriptor other) {
        if (isSameIdentity(other)) {
            return 0;
        }
        if (contributionKind != other.contributionKind
                || argumentColumnIndex != other.argumentColumnIndex
                || argumentColumnType != other.argumentColumnType) {
            return -1;
        }
        // A DOUBLE sum/avg keeps (sum, nonNullCount) and a count keeps that same counter
        // alone, so the count's whole state is the host's second slot - and the two count
        // the same rows, since contributionKind has already been required to match.
        if (family == FAMILY_DOUBLE_SUM_COUNT && other.family == FAMILY_NON_NULL_COUNT) {
            return getFieldSlot(FIELD_NON_NULL_COUNT);
        }
        // Welford's accumulator ends with the same counter, and increments it under the
        // same isFinite test the DOUBLE families use, so a count(x) beside a stddev(x)
        // costs nothing either. It is stated as its own pair rather than derived from
        // "the families both end in a counter": containment is a claim about the two
        // implementations, and Welford's happens to keep (mean, m2, count) in an order
        // that puts the counter last.
        if (family == FAMILY_DOUBLE_WELFORD && other.family == FAMILY_NON_NULL_COUNT) {
            return getFieldSlot(FIELD_NON_NULL_COUNT);
        }
        // A Kahan sum keeps (sum, compensation, count) and increments that counter on the
        // same isFinite test, so a count(x) folds onto it exactly as it does onto a plain
        // sum's. The pair stops there: the two sums are different numbers over the same
        // rows, which is what the compensation is for, so neither total is readable out of
        // the other and no wider host holds this one as a run.
        if (family == FAMILY_DOUBLE_KAHAN_SUM_COUNT && other.family == FAMILY_NON_NULL_COUNT) {
            return getFieldSlot(FIELD_NON_NULL_COUNT);
        }
        // The six max/min families appear in no pair, in either role, and the reason is not
        // that nobody has looked. A running extremum keeps no counter, so nothing narrower
        // sits inside it; and it is a single slot whose value is the arithmetic's whole
        // answer, so it is not a run inside anything wider either - a sum's first slot is a
        // running total and not the largest thing ever added to it.
        //
        // No ring-backed family appears either, and there the arithmetic would allow what the
        // relation does not. A bounded count(x)'s answer really is the counter a bounded sum(x)
        // over the same frame keeps beside its total, so a projection reading that slot would emit
        // the right number. What this method licenses is wider than that: a non-negative answer
        // says the guest's whole state is a run inside the host's, and the guest's state here
        // continues outside the map value in a ring of its own shape - a flag or a timestamp where
        // the host keeps a double or a (timestamp, value) pair. Admitting it would make containment
        // a claim about two arenas, which is a different proof from the one every pair above rests
        // on. It is also not a run in the slice either way: a RANGE counter's five slots and a
        // RANGE (sum, count)'s six agree on the ring's geometry and disagree about where the
        // counter sits, since the host keeps a total in front of it.
        //
        // None of the six capture families appears either, in either role, and the near miss is
        // worth naming: a first_value(x) ignore nulls keeps one slot that a first_value(x) beside
        // it appears to contain, both of them at slot 0 of a slice of the same width. They are
        // not the same word. The respect-nulls slice holds whatever the first row carried and the
        // IGNORE NULLS one the first row the predicate admitted, and those differ on every
        // partition whose first row is absent - which is why the identity comparison has already
        // refused the pair on its contribution kind before this method could be asked. Nothing
        // wider holds a capture as a run either: what a total or a counter keeps is not one row's
        // value.
        return -1;
    }

    public int getArgumentColumnIndex() {
        return argumentColumnIndex;
    }

    public int getArgumentColumnType() {
        return argumentColumnType;
    }

    public int getContributionKind() {
        return contributionKind;
    }

    public int getFamily() {
        return family;
    }

    /**
     * Returns {@code field}'s slot inside this component's own state, or {@code -1} when
     * the family does not carry it.
     */
    public int getFieldSlot(int field) {
        return switch (family) {
            case FAMILY_DECIMAL_MAX,
                 FAMILY_DECIMAL_MIN,
                 FAMILY_DOUBLE_MAX,
                 FAMILY_DOUBLE_MIN,
                 FAMILY_LONG_MAX,
                 FAMILY_LONG_MIN -> field == FIELD_EXTREMUM ? 0 : -1;
            // No flag: an IGNORE NULLS first value writes only what its predicate admits,
            // so its empty state is the value slot's own NULL.
            case FAMILY_DOUBLE_FIRST_NOT_NULL_VALUE, FAMILY_LONG_FIRST_NOT_NULL_VALUE ->
                    field == FIELD_CAPTURED_VALUE ? 0 : -1;
            case FAMILY_DOUBLE_FIRST_VALUE,
                 FAMILY_DOUBLE_LAST_NOT_NULL_VALUE,
                 FAMILY_LONG_FIRST_VALUE,
                 FAMILY_LONG_LAST_NOT_NULL_VALUE -> switch (field) {
                case FIELD_CAPTURED_VALUE -> 0;
                case FIELD_CAPTURED -> 1;
                default -> -1;
            };
            case FAMILY_DOUBLE_KAHAN_SUM_COUNT -> switch (field) {
                case FIELD_SUM -> 0;
                case FIELD_KAHAN_COMPENSATION -> 1;
                case FIELD_NON_NULL_COUNT -> 2;
                default -> -1;
            };
            case FAMILY_DOUBLE_RANGE_SUM_COUNT -> switch (field) {
                case FIELD_SUM -> 0;
                case FIELD_NON_NULL_COUNT -> 1;
                case FIELD_RING_INDEX -> 2;
                case FIELD_RING_OFFSET -> 3;
                case FIELD_RING_SIZE -> 4;
                case FIELD_RING_CAPACITY -> 5;
                default -> -1;
            };
            case FAMILY_RANGE_NON_NULL_COUNT -> switch (field) {
                case FIELD_NON_NULL_COUNT -> 0;
                case FIELD_RING_INDEX -> 1;
                case FIELD_RING_OFFSET -> 2;
                case FIELD_RING_SIZE -> 3;
                case FIELD_RING_CAPACITY -> 4;
                default -> -1;
            };
            case FAMILY_DOUBLE_ROWS_SUM_COUNT -> switch (field) {
                case FIELD_SUM -> 0;
                case FIELD_NON_NULL_COUNT -> 1;
                case FIELD_RING_INDEX -> 2;
                case FIELD_RING_OFFSET -> 3;
                default -> -1;
            };
            case FAMILY_DOUBLE_SUM_COUNT -> switch (field) {
                case FIELD_SUM -> 0;
                case FIELD_NON_NULL_COUNT -> 1;
                default -> -1;
            };
            case FAMILY_ROWS_NON_NULL_COUNT -> switch (field) {
                case FIELD_NON_NULL_COUNT -> 0;
                case FIELD_RING_INDEX -> 1;
                case FIELD_RING_OFFSET -> 2;
                default -> -1;
            };
            case FAMILY_DOUBLE_WELFORD -> switch (field) {
                case FIELD_MEAN -> 0;
                case FIELD_M2 -> 1;
                case FIELD_NON_NULL_COUNT -> 2;
                default -> -1;
            };
            case FAMILY_NON_NULL_COUNT, FAMILY_ROW_COUNT -> field == FIELD_NON_NULL_COUNT ? 0 : -1;
            default -> -1;
        };
    }

    /**
     * Returns how many {@link MapValue} slots this component occupies in the group's
     * fused map value.
     */
    public int getSlotCount() {
        return familySlotCount(family);
    }

    /**
     * Returns the column type of one of this component's slots.
     * <p>
     * A function of the descriptor and not only of its family: a DECIMAL extremum keeps its
     * argument's own payload, so the slot is a LONG for the four narrow widths and a
     * {@code DECIMAL128} or {@code DECIMAL256} for the two wide ones.
     */
    public int getSlotColumnType(int slot) {
        switch (family) {
            case FAMILY_DECIMAL_MAX, FAMILY_DECIMAL_MIN -> {
                if (slot == 0) {
                    return decimalStateColumnType(argumentColumnType);
                }
            }
            case FAMILY_DOUBLE_FIRST_NOT_NULL_VALUE, FAMILY_DOUBLE_MAX, FAMILY_DOUBLE_MIN -> {
                if (slot == 0) {
                    return ColumnType.DOUBLE;
                }
            }
            case FAMILY_DOUBLE_FIRST_VALUE, FAMILY_DOUBLE_LAST_NOT_NULL_VALUE -> {
                if (slot == 0) {
                    return ColumnType.DOUBLE;
                }
                if (slot == 1) {
                    return ColumnType.LONG;
                }
            }
            case FAMILY_LONG_FIRST_NOT_NULL_VALUE, FAMILY_LONG_MAX, FAMILY_LONG_MIN -> {
                if (slot == 0) {
                    return ColumnType.LONG;
                }
            }
            case FAMILY_LONG_FIRST_VALUE, FAMILY_LONG_LAST_NOT_NULL_VALUE -> {
                if (slot == 0 || slot == 1) {
                    return ColumnType.LONG;
                }
            }
            case FAMILY_DOUBLE_RANGE_SUM_COUNT -> {
                if (slot == 0) {
                    return ColumnType.DOUBLE;
                }
                if (slot >= 1 && slot <= 5) {
                    return ColumnType.LONG;
                }
            }
            case FAMILY_RANGE_NON_NULL_COUNT -> {
                if (slot >= 0 && slot <= 4) {
                    return ColumnType.LONG;
                }
            }
            case FAMILY_DOUBLE_ROWS_SUM_COUNT -> {
                if (slot == 0) {
                    return ColumnType.DOUBLE;
                }
                if (slot == 1 || slot == 2 || slot == 3) {
                    return ColumnType.LONG;
                }
            }
            case FAMILY_DOUBLE_SUM_COUNT -> {
                if (slot == 0) {
                    return ColumnType.DOUBLE;
                }
                if (slot == 1) {
                    return ColumnType.LONG;
                }
            }
            case FAMILY_ROWS_NON_NULL_COUNT -> {
                if (slot == 0 || slot == 1 || slot == 2) {
                    return ColumnType.LONG;
                }
            }
            case FAMILY_DOUBLE_KAHAN_SUM_COUNT, FAMILY_DOUBLE_WELFORD -> {
                if (slot == 0 || slot == 1) {
                    return ColumnType.DOUBLE;
                }
                if (slot == 2) {
                    return ColumnType.LONG;
                }
            }
            case FAMILY_NON_NULL_COUNT, FAMILY_ROW_COUNT -> {
                if (slot == 0) {
                    return ColumnType.LONG;
                }
            }
            default -> {
                // An unknown family has no slot layout, so every slot is out of range.
            }
        }
        throw new IndexOutOfBoundsException();
    }

    /**
     * Returns the raw 64-bit image of the value {@link #resetState} puts in {@code slot}:
     * the state this component starts a partition at.
     * <p>
     * Zero for every accumulating family, which is what a sum, a counter and Welford's
     * {@code (mean, m2)} all begin at and mean. It is <b>not</b> zero for a running
     * extremum, whose starting state has to say "nothing has contributed yet" rather than
     * "the largest value so far is zero" - so a DOUBLE extremum starts at NaN, a 64-bit one
     * at {@code Numbers.LONG_NULL} and a narrow DECIMAL one at its own width's null
     * sentinel, all of them values the family's contribution predicate refuses and so never
     * confusable with a real one.
     * <p>
     * Stated in bits because that is the currency the durable image already uses - one
     * little-endian 64-bit field per slot - so a family whose identity is not zero is
     * describable here without a second accessor per slot type. A slot wider than a word has
     * no answer here and throws: the two wide DECIMAL slot types start at the SQL NULL of
     * their own type, which {@link #resetState} writes through the map's own
     * {@code putDecimal*Null}, and a caller asking for that in one word is describing a
     * layout this class does not have.
     *
     * @param slot a slot of this component's own state, which is bounds-checked: an
     *             out-of-range slot is a layout bug and must not quietly answer zero
     */
    public long getSlotIdentityBits(int slot) {
        // Asked for its throw rather than its answer: an out-of-range slot is a layout bug and
        // must not quietly come back as an identity.
        final int slotType = getSlotColumnType(slot);
        if (isWideDecimalSlot(slotType)) {
            throw new UnsupportedOperationException("a wide DECIMAL slot's identity is not one word");
        }
        return switch (family) {
            // Sign-extended into the word the narrow implementations store it in, which is
            // how they compare it back - (byte) mv.getLong(0) against DECIMAL8_NULL.
            case FAMILY_DECIMAL_MAX, FAMILY_DECIMAL_MIN -> decimalNullPayload(argumentColumnType);
            case FAMILY_DOUBLE_MAX, FAMILY_DOUBLE_MIN -> Double.doubleToRawLongBits(Double.NaN);
            case FAMILY_LONG_MAX, FAMILY_LONG_MIN -> Numbers.LONG_NULL;
            // The captured value starts at its own state type's NULL, which is what the same
            // window emits for a partition nothing has been captured from - so a projection
            // reads the slot straight and needs no empty test. The flag beside it, where the
            // family has one, starts at zero and means "nothing written yet"; it is the
            // contributor's alone.
            case FAMILY_DOUBLE_FIRST_NOT_NULL_VALUE,
                 FAMILY_DOUBLE_FIRST_VALUE,
                 FAMILY_DOUBLE_LAST_NOT_NULL_VALUE,
                 FAMILY_LONG_FIRST_NOT_NULL_VALUE,
                 FAMILY_LONG_FIRST_VALUE,
                 FAMILY_LONG_LAST_NOT_NULL_VALUE -> {
                if (slot != getFieldSlot(FIELD_CAPTURED_VALUE)) {
                    yield 0L;
                }
                yield family == FAMILY_DOUBLE_FIRST_NOT_NULL_VALUE
                        || family == FAMILY_DOUBLE_FIRST_VALUE
                        || family == FAMILY_DOUBLE_LAST_NOT_NULL_VALUE
                        ? Double.doubleToRawLongBits(Double.NaN)
                        : Numbers.LONG_NULL;
            }
            // Zero for the accumulating slots, which a bounded total and a bounded counter
            // both start at and mean, and RING_STATE_UNALLOCATED for the ring's address,
            // which is the one slot in this build whose identity is not a value the
            // arithmetic could produce - it says "no ring yet" and is what makes the
            // contributor's first row on a partition allocate one. The RANGE families'
            // length and capacity start at zero too and mean it: an unallocated ring holds
            // nothing and can hold nothing, and the contributor writes both when it
            // allocates.
            case FAMILY_DOUBLE_RANGE_SUM_COUNT,
                 FAMILY_DOUBLE_ROWS_SUM_COUNT,
                 FAMILY_RANGE_NON_NULL_COUNT,
                 FAMILY_ROWS_NON_NULL_COUNT -> slot == getFieldSlot(FIELD_RING_OFFSET) ? RING_STATE_UNALLOCATED : 0L;
            // Zero, whichever way the slot is read back: a DOUBLE zero and a LONG zero are
            // the same word.
            default -> 0L;
        };
    }

    /**
     * Whether this component's state continues outside the group's map value, in a ring of the
     * frame's own values that its <b>contributor</b> owns and the ring slots address.
     * <p>
     * True for the four bounded-frame families and false for every other, and it is the model
     * change they bring: until them a component's whole state was the slice, so the group's map
     * was the only thing a fused query allocated. A ring-backed component keeps the slice - the
     * total, the counter and the ring's geometry - in the shared value like anything else, and the
     * ring itself in the arena the contributing function already owned and already frees. That
     * division is deliberate: the group owns the key domain and nothing per-function, so the
     * arena's lifecycle stays exactly where {@code close}, {@code reset} and {@code toTop} left
     * it, and a projection that reads the slice needs no arena at all.
     * <p>
     * Two things follow for anything that moves such a state. Its slots are meaningless in
     * another map - {@link #copyState} refuses them - and its identity cannot be produced by a
     * reset alone, which is why {@link #RING_STATE_UNALLOCATED} exists.
     * <p>
     * How much geometry the slice carries is the frame's: a ROWS ring is as long as the query says
     * and needs an address and a read cursor, while a RANGE ring grows with the data and needs its
     * length and its capacity beside them. That is a layout difference and not a lifecycle one, so
     * everything above holds for all four alike.
     */
    public boolean isRingBacked() {
        return family == FAMILY_DOUBLE_RANGE_SUM_COUNT
                || family == FAMILY_DOUBLE_ROWS_SUM_COUNT
                || family == FAMILY_RANGE_NON_NULL_COUNT
                || family == FAMILY_ROWS_NON_NULL_COUNT;
    }

    /**
     * Whether the two descriptors name the same component, and so may share one state
     * slice.
     */
    public boolean isSameIdentity(@NotNull WindowAccumulatorDescriptor other) {
        return family == other.family
                && contributionKind == other.contributionKind
                && argumentColumnIndex == other.argumentColumnIndex
                && argumentColumnType == other.argumentColumnType;
    }

    /**
     * Puts this component's slots back to the identity a new partition needs, and that a
     * live view's anchor crossing also leaves behind: a map value's slots are not
     * zero-filled by {@code createValue()} on any implementation.
     * <p>
     * The identity is the family's rather than the slot type's - see
     * {@link #getSlotIdentityBits} - because an extremum's empty state is not zero.
     * <p>
     * The two wide DECIMAL slot types are the exception, and deliberately state their
     * identity here rather than in bits: every wide-DECIMAL slot this build admits starts at
     * the SQL NULL of its own type, which is what the map writes through
     * {@code putDecimal128Null}. A family that wanted a wide slot to start at zero - a
     * DECIMAL accumulator would - has to say so here rather than reuse this arm.
     */
    public void resetState(@NotNull MapValue value, int slotBase) {
        for (int i = 0, n = getSlotCount(); i < n; i++) {
            final int slotType = getSlotColumnType(i);
            switch (ColumnType.tagOf(slotType)) {
                case ColumnType.DECIMAL128 -> value.putDecimal128Null(slotBase + i);
                case ColumnType.DECIMAL256 -> value.putDecimal256Null(slotBase + i);
                case ColumnType.DOUBLE ->
                        value.putDouble(slotBase + i, Double.longBitsToDouble(getSlotIdentityBits(i)));
                default -> value.putLong(slotBase + i, getSlotIdentityBits(i));
            }
        }
    }

    /**
     * Returns the raw payload a narrow DECIMAL extremum stores for an absent value, in the
     * 64-bit slot the four narrow implementations keep it in.
     * <p>
     * One sentinel per width rather than one for all four, because that is what the
     * implementations compare against: a DECIMAL8 column's absent value is
     * {@code Byte.MIN_VALUE}, which is an ordinary payload for every wider one. The wide
     * widths have no answer here - their identity is the type's own NULL, written by
     * {@link #resetState}.
     */
    private static long decimalNullPayload(int columnType) {
        return switch (ColumnType.tagOf(columnType)) {
            case ColumnType.DECIMAL8 -> Decimals.DECIMAL8_NULL;
            case ColumnType.DECIMAL16 -> Decimals.DECIMAL16_NULL;
            case ColumnType.DECIMAL32 -> Decimals.DECIMAL32_NULL;
            case ColumnType.DECIMAL64 -> Decimals.DECIMAL64_NULL;
            default -> throw new IndexOutOfBoundsException();
        };
    }

    /**
     * Returns the map value type a DECIMAL extremum over {@code columnType} keeps its state
     * in: the raw payload in a LONG for the four narrow widths, which is what those
     * implementations store, and the argument's own type for the two wide ones.
     */
    private static int decimalStateColumnType(int columnType) {
        return switch (ColumnType.tagOf(columnType)) {
            case ColumnType.DECIMAL8, ColumnType.DECIMAL16, ColumnType.DECIMAL32, ColumnType.DECIMAL64 ->
                    ColumnType.LONG;
            case ColumnType.DECIMAL128 -> ColumnType.DECIMAL128;
            case ColumnType.DECIMAL256 -> ColumnType.DECIMAL256;
            default -> throw new IndexOutOfBoundsException();
        };
    }

    /**
     * Whether a column of {@code columnType} reaches a DECIMAL extremum as a direct column
     * reference, contributing under {@link #CONTRIBUTION_TYPED_NOT_NULL} - the width's own
     * null test - once it gets there.
     * <p>
     * All six widths, each through a factory arm of its own, and nothing else: a DECIMAL
     * {@code max} is selected by the argument's width and stores at that width, so a type
     * that is not one of these reaches a different implementation keeping a different state.
     * The list is exactly {@link #contributionKindFor}'s DECIMAL arm for a {@code count},
     * which is the same predicate over the same rows - what differs between the two families
     * is what they keep, not which rows they keep it from.
     */
    private static boolean isDecimalPayload(int columnType) {
        return switch (ColumnType.tagOf(columnType)) {
            case ColumnType.DECIMAL8,
                 ColumnType.DECIMAL16,
                 ColumnType.DECIMAL32,
                 ColumnType.DECIMAL64,
                 ColumnType.DECIMAL128,
                 ColumnType.DECIMAL256 -> true;
            default -> false;
        };
    }

    /**
     * Whether {@code slotType} is one of the two map value types this build keeps wider than
     * a 64-bit word. Such a slot has no one-word identity and does not move between maps -
     * see {@link #getSlotIdentityBits} and {@link #copyState}.
     */
    private static boolean isWideDecimalSlot(int slotType) {
        final int tag = ColumnType.tagOf(slotType);
        return tag == ColumnType.DECIMAL128 || tag == ColumnType.DECIMAL256;
    }

    /**
     * Whether a column of {@code columnType} reaches a 64-bit extremum as a direct column
     * reference whose absent value is {@code Numbers.LONG_NULL}, contributing under
     * {@link #CONTRIBUTION_TYPED_NOT_NULL} once it gets there.
     * <p>
     * The three that have a factory of their own are LONG, DATE and TIMESTAMP, and the
     * implementation behind all three stores the argument's payload word. The narrower
     * integrals are here because they match {@code max(L)} by widening with no cast
     * wrapper inserted - {@code IntFunction.getLong} answers {@code LONG_NULL} for
     * {@code Numbers.INT_NULL}, so the null carries across, while BYTE and SHORT have no
     * null representation at all and so contribute on every row.
     * <p>
     * Everything else declines, one type at a time and for the reason
     * {@link #isWidenedToDouble} declines its own list: a type that reaches the extremum
     * through some other reading contributes under some other predicate, and the identity
     * would then name a predicate the runtime does not apply. That includes DOUBLE and
     * FLOAT, which have a DOUBLE-stated extremum of their own, and DECIMAL, which has
     * {@link #FAMILY_DECIMAL_MAX} - it keeps its argument's own payload under that width's
     * null test, and a narrow one lands in a LONG slot like this family's without being the
     * same state, since {@code Byte.MIN_VALUE} is an absent DECIMAL8 and an ordinary LONG.
     */
    private static boolean isLongPayload(int columnType) {
        return switch (ColumnType.tagOf(columnType)) {
            case ColumnType.BYTE,
                 ColumnType.SHORT,
                 ColumnType.INT,
                 ColumnType.LONG,
                 ColumnType.DATE,
                 ColumnType.TIMESTAMP -> true;
            default -> false;
        };
    }

    /**
     * Whether a column of {@code columnType} reaches the DOUBLE-stated window factories
     * as a direct column reference, contributing under
     * {@link #CONTRIBUTION_FINITE_DOUBLE} once it gets there.
     * <p>
     * There is no {@code sum(L)} window factory and no {@code count(L)} either. An
     * integral or FLOAT column matches {@code sum(D)}, {@code avg(D)}, {@code count(D)}
     * and the four dispersion signatures by numeric widening, and {@code FunctionParser}
     * wraps none of those in a cast function - it inserts one only where a physical
     * representation has to change, which widening into a double does not. The argument
     * therefore arrives as a {@code ColumnFunction} of the column's own type, which is
     * what {@link #directColumnIndex} requires and what the component identity keys by.
     * <p>
     * One predicate then serves all of them because every one of those factories reads
     * its argument through {@code getDouble} and contributes on
     * {@code Numbers.isFinite} of the result. The widening carries the null across:
     * {@code LongFunction.getDouble} answers NaN for {@code Numbers.LONG_NULL} and
     * {@code IntFunction.getDouble} answers NaN for {@code Numbers.INT_NULL}, while BYTE
     * and SHORT have no null representation at all and so contribute on every row - to
     * the sum and to the count alike, which is the only agreement that matters.
     * <p>
     * The list is deliberately shorter than the set of types that widen into a DOUBLE,
     * because widening is necessary and not sufficient. CHAR reaches {@code sum(D)} but
     * its {@code count} resolves to the VARCHAR factory and a null test, so the two
     * would count different rows; DATE and TIMESTAMP widen as well, but a timestamp
     * carries its precision in its column type and neither family has been checked
     * against them; STRING and VARCHAR reach {@code sum(D)} by parsing the text, which
     * is a third predicate again beside the null test their {@code count} uses. Each of
     * those declines here rather than being assumed, one argument type at a time.
     * <p>
     * DECIMAL is absent because it has factories of its own and so never widens into a
     * double at all. Its {@code count} is nevertheless a fused component - see the
     * {@link #CONTRIBUTION_TYPED_NOT_NULL} arm of {@link #contributionKindFor} - because
     * a {@code count} over a DECIMAL column is the shared counting implementation under
     * that width's null test, not a decimal accumulator; and its {@code max} and
     * {@code min} are {@link #FAMILY_DECIMAL_MAX}, which keeps the argument's own payload.
     * What has no family here is {@code sum} and {@code avg} over a DECIMAL: those
     * accumulate into a {@code Decimal128} or {@code Decimal256} beside a flag or a counter,
     * and the two implementations disagree about which of those it is, so one shared
     * component would have to re-decide arithmetic rather than describe a state.
     */
    private static boolean isWidenedToDouble(int columnType) {
        return switch (ColumnType.tagOf(columnType)) {
            case ColumnType.BYTE,
                 ColumnType.SHORT,
                 ColumnType.INT,
                 ColumnType.LONG,
                 ColumnType.FLOAT,
                 ColumnType.DOUBLE -> true;
            default -> false;
        };
    }
}
