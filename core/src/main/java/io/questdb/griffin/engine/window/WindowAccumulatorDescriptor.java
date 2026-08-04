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
 *     {@link #directColumnIndex} - because a canonical, type-resolved fingerprint for
 *     arbitrary expressions does not exist yet, and rendering the SQL text is not a proof
 *     of expression equivalence. A family {@link #familyTakesArgument} says takes none
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
     * The running sum of squared deviations from the running mean. Present only in
     * {@link #FAMILY_DOUBLE_WELFORD}.
     */
    public static final int FIELD_M2 = 3;
    /**
     * The running mean. Present only in {@link #FAMILY_DOUBLE_WELFORD}.
     */
    public static final int FIELD_MEAN = 2;
    /**
     * The count of contributing rows. Present in every family.
     */
    public static final int FIELD_NON_NULL_COUNT = 1;
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
        switch (family) {
            case FAMILY_ROW_COUNT:
                // No argument to predicate on, and the caller must not have supplied one:
                // a row count that quietly accepted an argument type would be a second
                // identity for the same state, and the two would not merge.
                return argumentColumnType == ColumnType.UNDEFINED
                        ? CONTRIBUTION_EVERY_ROW
                        : CONTRIBUTION_NONE;
            case FAMILY_DOUBLE_SUM_COUNT:
            case FAMILY_DOUBLE_WELFORD:
                // Those families have one factory each and it takes a DOUBLE, so every
                // other argument they accept arrives by widening into that DOUBLE.
                return isWidenedToDouble(argumentColumnType)
                        ? CONTRIBUTION_FINITE_DOUBLE
                        : CONTRIBUTION_NONE;
            case FAMILY_NON_NULL_COUNT:
                // count() has a factory per argument shape, so this arm is the one that
                // can name a predicate other than the DOUBLE one - and the type is what
                // selects between them.
                if (isWidenedToDouble(argumentColumnType)) {
                    return CONTRIBUTION_FINITE_DOUBLE;
                }
                switch (ColumnType.tagOf(argumentColumnType)) {
                    case ColumnType.SYMBOL:
                    case ColumnType.VARCHAR:
                        return CONTRIBUTION_TYPED_NOT_NULL;
                    // A DECIMAL count is the argument type's own null test and nothing
                    // else: CountDecimalWindowFunctionFactory selects a predicate per
                    // width, and every one of the six compares against that width's null
                    // sentinel. The type stays in the identity, so a count over a
                    // DECIMAL64 column and one over a DECIMAL128 column remain two
                    // components even though both name this kind.
                    case ColumnType.DECIMAL8:
                    case ColumnType.DECIMAL16:
                    case ColumnType.DECIMAL32:
                    case ColumnType.DECIMAL64:
                    case ColumnType.DECIMAL128:
                    case ColumnType.DECIMAL256:
                        return CONTRIBUTION_TYPED_NOT_NULL;
                    default:
                        return CONTRIBUTION_NONE;
                }
            default:
                return CONTRIBUTION_NONE;
        }
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
        switch (family) {
            case FAMILY_DOUBLE_SUM_COUNT:
                return 2;
            case FAMILY_DOUBLE_WELFORD:
                return 3;
            case FAMILY_NON_NULL_COUNT:
            case FAMILY_ROW_COUNT:
                return 1;
            default:
                return 0;
        }
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
     */
    public void copyState(@NotNull MapValue src, int srcSlotBase, @NotNull MapValue dst, int dstSlotBase) {
        for (int i = 0, n = getSlotCount(); i < n; i++) {
            if (getSlotColumnType(i) == ColumnType.DOUBLE) {
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
        switch (family) {
            case FAMILY_DOUBLE_SUM_COUNT:
                if (field == FIELD_SUM) {
                    return 0;
                }
                return field == FIELD_NON_NULL_COUNT ? 1 : -1;
            case FAMILY_DOUBLE_WELFORD:
                if (field == FIELD_MEAN) {
                    return 0;
                }
                if (field == FIELD_M2) {
                    return 1;
                }
                return field == FIELD_NON_NULL_COUNT ? 2 : -1;
            case FAMILY_NON_NULL_COUNT:
            case FAMILY_ROW_COUNT:
                return field == FIELD_NON_NULL_COUNT ? 0 : -1;
            default:
                return -1;
        }
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
     */
    public int getSlotColumnType(int slot) {
        switch (family) {
            case FAMILY_DOUBLE_SUM_COUNT:
                if (slot == 0) {
                    return ColumnType.DOUBLE;
                }
                if (slot == 1) {
                    return ColumnType.LONG;
                }
                break;
            case FAMILY_DOUBLE_WELFORD:
                if (slot == 0 || slot == 1) {
                    return ColumnType.DOUBLE;
                }
                if (slot == 2) {
                    return ColumnType.LONG;
                }
                break;
            case FAMILY_NON_NULL_COUNT:
            case FAMILY_ROW_COUNT:
                if (slot == 0) {
                    return ColumnType.LONG;
                }
                break;
            default:
                break;
        }
        throw new IndexOutOfBoundsException();
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
     */
    public void resetState(@NotNull MapValue value, int slotBase) {
        for (int i = 0, n = getSlotCount(); i < n; i++) {
            if (getSlotColumnType(i) == ColumnType.DOUBLE) {
                value.putDouble(slotBase + i, 0.0);
            } else {
                value.putLong(slotBase + i, 0L);
            }
        }
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
     * that width's null test, not a decimal accumulator. What has no family here is
     * {@code sum} and {@code avg} over a DECIMAL: those accumulate into a
     * {@code Decimal128} or {@code Decimal256} beside a flag or a counter, which is a
     * state shape this class's slot model does not describe.
     */
    private static boolean isWidenedToDouble(int columnType) {
        switch (ColumnType.tagOf(columnType)) {
            case ColumnType.BYTE:
            case ColumnType.SHORT:
            case ColumnType.INT:
            case ColumnType.LONG:
            case ColumnType.FLOAT:
            case ColumnType.DOUBLE:
                return true;
            default:
                return false;
        }
    }
}
