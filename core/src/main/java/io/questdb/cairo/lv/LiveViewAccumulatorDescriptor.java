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
 *     rendering the SQL text is not a proof of expression equivalence;</li>
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
 * <p>
 * {@code count(*)} is a row-count component rather than a non-null count and gets no
 * descriptor from this class at all: it has no argument, so it can never be
 * interchangeable with a {@code count(x)}, and admitting it would need the row-count
 * family the later work adds.
 */
public final class LiveViewAccumulatorDescriptor {
    /**
     * {@code Numbers.isFinite(arg.getDouble(record))} - the predicate a DOUBLE
     * {@code sum}/{@code avg} contributes under and, deliberately, the one
     * {@code count(double)} uses too, so the three agree on infinities as well as on
     * NULL. Distinct from {@link #CONTRIBUTION_TYPED_NOT_NULL} precisely because a
     * plain null test would admit an infinity a finite-sum accumulator never counted.
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
     */
    public static final int FAMILY_DOUBLE_SUM_COUNT = 1;
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
     * The count of contributing rows. Present in every family.
     */
    public static final int FIELD_NON_NULL_COUNT = 1;
    /**
     * The running sum. Present only in {@link #FAMILY_DOUBLE_SUM_COUNT}.
     */
    public static final int FIELD_SUM = 0;
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
            case FAMILY_DOUBLE_SUM_COUNT:
                // sum(D)/avg(D) exist only over DOUBLE. Anything else reaches the same
                // factory through an implicit cast, which is not a direct column
                // reference and has already been turned away by the caller.
                return ColumnType.tagOf(argumentColumnType) == ColumnType.DOUBLE
                        ? CONTRIBUTION_FINITE_DOUBLE
                        : CONTRIBUTION_NONE;
            case FAMILY_NON_NULL_COUNT:
                switch (ColumnType.tagOf(argumentColumnType)) {
                    case ColumnType.DOUBLE:
                        return CONTRIBUTION_FINITE_DOUBLE;
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
            case FAMILY_NON_NULL_COUNT:
                return 1;
            default:
                return -1;
        }
    }

    /**
     * Builds the component a function over {@code argumentColumnIndex} contributes to,
     * or null when this build cannot name every part of its identity - an unknown
     * family, or an argument type whose contribution predicate
     * {@link #contributionKindFor} declines.
     *
     * @param family              one of the {@code FAMILY_*} constants
     * @param argumentColumnIndex the argument's index in the base metadata the window
     *                            functions were compiled against
     * @param argumentColumnType  the argument's compiled column type
     */
    public static @Nullable LiveViewAccumulatorDescriptor of(int family, int argumentColumnIndex, int argumentColumnType) {
        if (argumentColumnIndex < 0) {
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
            case FAMILY_NON_NULL_COUNT:
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
            case FAMILY_NON_NULL_COUNT:
                return field == FIELD_NON_NULL_COUNT ? 0 : -1;
            default:
                return -1;
        }
    }

    public int getStateLength() {
        return stateLength;
    }

    /**
     * Whether the two descriptors name the same durable component, and so may share
     * one state slice.
     */
    public boolean isSameIdentity(@NotNull LiveViewAccumulatorDescriptor other) {
        return Arrays.equals(encoded, other.encoded);
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
