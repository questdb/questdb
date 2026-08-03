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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.map.MapValue;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Immutable identity and layout of one <b>accumulator component</b>: the durable
 * unit a fused live-view window state is made of.
 * <p>
 * The durable unit is deliberately not a SELECT-list function call. A view's
 * projections may be added, removed or reordered when it is recreated, while the
 * mathematics behind them does not change: {@code sum(x)}, {@code avg(x)} and
 * {@code count(x)} over one window are three readings of one running
 * {@code (sum, nonNullCount)} pair. Persisting the component and binding each output
 * to it as a {@link LiveViewAccumulatorProjection projection} is what lets one image
 * serve all three, and what keeps the persisted manifest free of an "owner output
 * position" that a recompile could invalidate.
 *
 * <h2>Two components are the same only when everything about their state is</h2>
 * The encoded identity below is the whole of the sharing proof, and it deliberately
 * carries more than the family:
 * <ul>
 *     <li>the <b>argument</b>, as a {@code (base column index, column type)} pair.
 *     Only a direct compiled column reference gets a key at all - a canonical,
 *     type-resolved fingerprint for arbitrary expressions does not exist yet, and
 *     rendering the SQL text is not a proof of expression equivalence. A family
 *     {@link #familyTakesArgument} says takes none carries the fixed
 *     {@code (NO_ARGUMENT_COLUMN_INDEX, UNDEFINED)} pair instead, so its identity is
 *     one exact value rather than an absence;</li>
 *     <li>the <b>contribution predicate</b>, because two counters over the same
 *     window can still diverge on which rows they count. {@code sum(amt)} counts
 *     finite {@code amt} values and {@code count(acct)} counts non-null {@code acct}
 *     values: those disagree on every row where exactly one is null, so they are
 *     never one component however identical the rest of the window is;</li>
 *     <li>the component <b>codec version</b>, so a layout change across a binary
 *     upgrade produces a different identity rather than a silent reinterpretation of
 *     bytes an older build wrote.</li>
 * </ul>
 * The window, frame and anchor identity are <b>not</b> repeated here: a component
 * lives under exactly one window-state root, and the root carries them, so a
 * component identity is only ever compared against another under the same root.
 *
 * <h2>One component may contain another</h2>
 * Two identities that differ can still describe one durable image, when one family's
 * state <i>contains</i> the other's verbatim: the counter a {@code count(x)} persists
 * on its own is the same counter a {@code sum(x)} over the same argument already keeps
 * beside its sum, and equally the one Welford's accumulator keeps behind its running
 * mean. {@link #derivedStateOffset} is where that containment is stated, one proved
 * pair at a time, and it is what lets a fused group persist
 * {@code sum(x) + avg(x) + count(x)} as one 16-byte component rather than 24 bytes in
 * two. Containment is strictly narrower than identity and is never assumed from the
 * arithmetic alone - the argument and the contribution predicate still have to match,
 * for the same reason two identities do.
 * <p>
 * {@code count(*)} is a {@link #FAMILY_ROW_COUNT row-count} component rather than a
 * non-null count, and the two are never interchangeable: a row count has no argument,
 * so nothing about it could make it agree with a counter that skips the rows where some
 * column is null. What it does share is {@code row_number()}, which keeps the very same
 * counter over the very same rows.
 */
public final class LiveViewAccumulatorDescriptor {
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
     * differ only in the arithmetic they read off {@code (m2, nonNullCount)}, so a view
     * naming several of them over one column persists one 24-byte component rather than
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
     * the same counter of rows since the partition's last anchor crossing, and after
     * {@code n} rows both read {@code n} off it.
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
    private static final int FORMAT_VERSION = 1;
    private static final int MAGIC = 0x4c564143; // LVAC
    private final int argumentColumnIndex;
    private final int argumentColumnType;
    private final int codecVersion;
    private final int contributionKind;
    private final byte[] encoded;
    private final int family;
    private final int stateLength;

    private LiveViewAccumulatorDescriptor(
            int family,
            int contributionKind,
            int argumentColumnIndex,
            int argumentColumnType,
            int codecVersion,
            int stateLength
    ) {
        this.family = family;
        this.contributionKind = contributionKind;
        this.argumentColumnIndex = argumentColumnIndex;
        this.argumentColumnType = argumentColumnType;
        this.codecVersion = codecVersion;
        this.stateLength = stateLength;
        this.encoded = encode();
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
     * a null test. A type not listed here declines, which costs a view only the fused
     * component and leaves it on its own legacy root.
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
                    default:
                        return CONTRIBUTION_NONE;
                }
            default:
                return CONTRIBUTION_NONE;
        }
    }

    /**
     * Returns the state layout version this build writes for {@code family}, or
     * {@code -1} for a family it does not know. Folded into the encoded identity, so a
     * bump makes an older component unresolvable rather than reinterpretable.
     */
    public static int familyCodecVersion(int family) {
        switch (family) {
            case FAMILY_DOUBLE_SUM_COUNT:
            case FAMILY_DOUBLE_WELFORD:
            case FAMILY_NON_NULL_COUNT:
            case FAMILY_ROW_COUNT:
                return 1;
            default:
                return -1;
        }
    }

    /**
     * Whether {@code family}'s identity includes an argument. A family that takes none
     * is identified by its family and codec alone, and its descriptor carries
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
    public static @Nullable LiveViewAccumulatorDescriptor of(int family, int argumentColumnIndex, int argumentColumnType) {
        if (familyTakesArgument(family)
                ? argumentColumnIndex < 0
                : argumentColumnIndex != NO_ARGUMENT_COLUMN_INDEX) {
            return null;
        }
        final int contributionKind = contributionKindFor(family, argumentColumnType);
        if (contributionKind == CONTRIBUTION_NONE) {
            return null;
        }
        final int codecVersion = familyCodecVersion(family);
        final int stateLength = familyStateLength(family);
        if (codecVersion < 0 || stateLength <= 0) {
            return null;
        }
        return new LiveViewAccumulatorDescriptor(
                family,
                contributionKind,
                argumentColumnIndex,
                argumentColumnType,
                codecVersion,
                stateLength
        );
    }

    /**
     * Returns the whole-state width of {@code family}, which must equal the width the
     * contributing implementation declares through
     * {@code WindowFunction.checkpointStateFixedLength()}. The plan checks the two
     * against each other rather than trusting either alone.
     */
    public static int familyStateLength(int family) {
        switch (family) {
            case FAMILY_DOUBLE_SUM_COUNT:
                return Double.BYTES + Long.BYTES;
            case FAMILY_DOUBLE_WELFORD:
                return 2 * Double.BYTES + Long.BYTES;
            case FAMILY_NON_NULL_COUNT:
            case FAMILY_ROW_COUNT:
                return Long.BYTES;
            default:
                return -1;
        }
    }

    /**
     * Orders two identities by their encoded bytes, unsigned. The fused layout is
     * assigned in this order and never in SELECT-list order, so reordering the
     * projections of one view cannot move a component's offset.
     */
    public int compareIdentity(@NotNull LiveViewAccumulatorDescriptor other) {
        final byte[] a = encoded;
        final byte[] b = other.encoded;
        final int n = Math.min(a.length, b.length);
        for (int i = 0; i < n; i++) {
            final int diff = (a[i] & 0xff) - (b[i] & 0xff);
            if (diff != 0) {
                return diff;
            }
        }
        return a.length - b.length;
    }

    /**
     * Returns the offset inside this component's state at which {@code other}'s whole
     * state image appears verbatim, or {@code -1} when it does not appear at all.
     * <p>
     * A non-negative answer is the licence for one durable component to serve a
     * projection whose own function persists {@code other}: the host writes the image,
     * and the guest's decoder - unchanged, the one it already had - reads its own bytes
     * out of the host's slice at this offset. That is how {@code count(x)} stops costing
     * a component of its own beside {@code sum(x)}.
     * <p>
     * The table below is deliberately a list of proved pairs rather than a rule derived
     * from the families' fields. A containment claim is a claim about two <b>codecs</b>:
     * it says the guest's {@code freezeCheckpointState} image is byte-for-byte a run
     * inside the host's, which is a fact about the two implementations at their current
     * versions and not something the field offsets alone establish. Both codec versions
     * are therefore pinned, so a bump on either side withdraws the claim instead of
     * silently carrying it onto a layout nobody checked.
     * <p>
     * Everything the identity comparison requires still applies to a derivation - the
     * same argument, the same contribution predicate - because a counter that counts
     * different rows is a different counter however it is stored. The target shape is the
     * negative control: {@code count(cod_acct_no)} beside {@code sum(amt_txn)} matches
     * on family containment and on nothing else, so it gets {@code -1} and keeps its own
     * component.
     */
    public int derivedStateOffset(@NotNull LiveViewAccumulatorDescriptor other) {
        if (isSameIdentity(other)) {
            return 0;
        }
        if (contributionKind != other.contributionKind
                || argumentColumnIndex != other.argumentColumnIndex
                || argumentColumnType != other.argumentColumnType) {
            return -1;
        }
        // A DOUBLE sum/avg freezes (sum, nonNullCount) and a count freezes that same
        // counter alone, so the count's whole image is the host's second field - and the
        // two count the same rows, since contributionKind has already been required to
        // match.
        if (family == FAMILY_DOUBLE_SUM_COUNT && codecVersion == 1
                && other.family == FAMILY_NON_NULL_COUNT && other.codecVersion == 1) {
            return getFieldOffset(FIELD_NON_NULL_COUNT);
        }
        // Welford's accumulator ends with the same counter, and increments it under the
        // same isFinite test the DOUBLE families use, so a count(x) beside a stddev(x)
        // costs nothing either. It is stated as its own pair rather than derived from
        // "the families both end in a counter": containment is a claim about the two
        // freeze implementations, and Welford's happens to write (mean, m2, count) in an
        // order that puts the counter last.
        if (family == FAMILY_DOUBLE_WELFORD && codecVersion == 1
                && other.family == FAMILY_NON_NULL_COUNT && other.codecVersion == 1) {
            return getFieldOffset(FIELD_NON_NULL_COUNT);
        }
        return -1;
    }

    /**
     * Returns the relative slot inside this component's own runtime state at which
     * {@code other}'s whole state begins, or {@code -1} when it does not appear at all.
     * The slot counterpart of {@link #derivedStateOffset}, and it answers for the same
     * pairs: the two express one containment, once in the durable image's bytes and once
     * in the fused map value's slots.
     */
    public int derivedSlotOffset(@NotNull LiveViewAccumulatorDescriptor other) {
        final int byteOffset = derivedStateOffset(other);
        if (byteOffset < 0) {
            return -1;
        }
        int slot = 0;
        int offset = 0;
        final int slotCount = getSlotCount();
        while (offset < byteOffset && slot < slotCount) {
            offset += ColumnType.sizeOf(getSlotColumnType(slot));
            slot++;
        }
        return offset == byteOffset ? slot : -1;
    }

    public int getArgumentColumnIndex() {
        return argumentColumnIndex;
    }

    public int getArgumentColumnType() {
        return argumentColumnType;
    }

    public int getCodecVersion() {
        return codecVersion;
    }

    public int getContributionKind() {
        return contributionKind;
    }

    /**
     * Returns an owned copy of the encoded identity, which is what the persisted
     * manifest carries.
     */
    public byte[] getEncoded() {
        return Arrays.copyOf(encoded, encoded.length);
    }

    public int getFamily() {
        return family;
    }

    /**
     * Returns {@code field}'s offset inside this component's own state, or {@code -1}
     * when the family does not carry it.
     */
    public int getFieldOffset(int field) {
        switch (family) {
            case FAMILY_DOUBLE_SUM_COUNT:
                if (field == FIELD_SUM) {
                    return 0;
                }
                return field == FIELD_NON_NULL_COUNT ? Double.BYTES : -1;
            case FAMILY_DOUBLE_WELFORD:
                if (field == FIELD_MEAN) {
                    return 0;
                }
                if (field == FIELD_M2) {
                    return Double.BYTES;
                }
                return field == FIELD_NON_NULL_COUNT ? 2 * Double.BYTES : -1;
            case FAMILY_NON_NULL_COUNT:
            case FAMILY_ROW_COUNT:
                return field == FIELD_NON_NULL_COUNT ? 0 : -1;
            default:
                return -1;
        }
    }

    /**
     * Returns {@code field}'s slot inside this component's own runtime state, or
     * {@code -1} when the family does not carry it. The slot counterpart of
     * {@link #getFieldOffset(int)}: the durable image and the fused map value lay the
     * same fields out in the same order, one in bytes and one in value slots.
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
     * Returns how many {@link MapValue} slots this component occupies in the window's
     * fused runtime map value.
     */
    public int getSlotCount() {
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
     * Returns the column type of one of this component's runtime slots. The widths must
     * add up to {@link #getStateLength()}, because the durable image is those same
     * fields in that same order.
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

    public int getStateLength() {
        return stateLength;
    }

    /**
     * Copies this component's slots from one map value to another, so a runtime whose
     * ownership is moving - the window adopting the plan, or handing the state back -
     * carries the accumulator across without going through the durable encoding.
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
     * Writes this component's whole-state image into {@code payload} at {@code offset},
     * reading the fields out of the fused map value's slots.
     * <p>
     * The bytes are the contributing function's own {@code freezeCheckpointState} image:
     * the same fields in the same order, little-endian, which is what
     * {@link LiveViewStatePageWriter} produces through {@code MemoryA}. That equality is
     * the whole of the component codec's contract with the implementations that declare
     * the family, and it is held to it directly by test rather than inferred - a leaf
     * carries no length for an inlined slice, so a divergence would be decoded at the
     * right width out of the wrong bytes.
     */
    public void freezeStateInto(@NotNull MapValue value, int slotBase, byte @NotNull [] payload, int offset) {
        checkPayloadBounds(payload, offset);
        int at = offset;
        for (int i = 0, n = getSlotCount(); i < n; i++) {
            final int slotType = getSlotColumnType(i);
            final long bits = slotType == ColumnType.DOUBLE
                    ? Double.doubleToRawLongBits(value.getDouble(slotBase + i))
                    : value.getLong(slotBase + i);
            putLongLE(payload, at, bits);
            at += Long.BYTES;
        }
    }

    /**
     * Fills the fused map value's slots for this component from a whole-state image, the
     * exact inverse of {@link #freezeStateInto}.
     */
    public void restoreStateFrom(byte @NotNull [] payload, int offset, @NotNull MapValue value, int slotBase) {
        checkPayloadBounds(payload, offset);
        int at = offset;
        for (int i = 0, n = getSlotCount(); i < n; i++) {
            final long bits = getLongLE(payload, at);
            if (getSlotColumnType(i) == ColumnType.DOUBLE) {
                value.putDouble(slotBase + i, Double.longBitsToDouble(bits));
            } else {
                value.putLong(slotBase + i, bits);
            }
            at += Long.BYTES;
        }
    }

    /**
     * Puts this component's slots back to the identity an anchor crossing leaves behind,
     * which is also what a brand-new partition needs: a map value's slots are not
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
     * Whether the two descriptors name the same durable component, and so may share
     * one state slice.
     */
    public boolean isSameIdentity(@NotNull LiveViewAccumulatorDescriptor other) {
        return Arrays.equals(encoded, other.encoded);
    }

    private static long getLongLE(byte[] payload, int offset) {
        long value = 0;
        for (int i = Long.BYTES - 1; i >= 0; i--) {
            value = (value << 8) | (payload[offset + i] & 0xffL);
        }
        return value;
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
     * what {@code directColumnIndex} requires and what the component identity keys by.
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
     * DECIMAL is absent for a different reason: it has factories of its own, so it never
     * widens, and its sum accumulates into a {@code Decimal256} whose whole image is
     * past {@link LiveViewCheckpointContracts#MAX_INLINE_COMPONENT_STATE_BYTES}. It
     * needs the format's combined overflow page before it could join a fused group at
     * all.
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

    private static void putLongLE(byte[] payload, int offset, long value) {
        for (int i = 0; i < Long.BYTES; i++) {
            payload[offset + i] = (byte) (value >>> (i * Byte.SIZE));
        }
    }

    /**
     * Proves the slice this component is about to read or write is inside
     * {@code payload}. The fused leaf carries no per-component length, so an offset the
     * manifest and the payload disagree about would otherwise be a silent read of a
     * neighbouring component's bytes.
     */
    private void checkPayloadBounds(byte[] payload, int offset) {
        if (offset < 0 || offset + stateLength > payload.length) {
            throw CairoException.critical(0)
                    .put("live view accumulator component slice is outside its payload [offset=")
                    .put(offset).put(", length=").put(stateLength)
                    .put(", payload=").put(payload.length).put(']');
        }
    }

    /**
     * The codec version rides inside the identity as well as beside it in the
     * manifest. The manifest field is what the doc's byte-equal predecessor test
     * reads; carrying it here too makes {@link #isSameIdentity} complete on its own,
     * so the plan's merge cannot fold two components a codec bump has separated.
     */
    private byte[] encode() {
        final ByteBuffer buffer = ByteBuffer.allocate(7 * Integer.BYTES);
        buffer.putInt(MAGIC);
        buffer.putInt(FORMAT_VERSION);
        buffer.putInt(family);
        buffer.putInt(codecVersion);
        buffer.putInt(contributionKind);
        buffer.putInt(argumentColumnIndex);
        buffer.putInt(argumentColumnType);
        return buffer.array();
    }
}
