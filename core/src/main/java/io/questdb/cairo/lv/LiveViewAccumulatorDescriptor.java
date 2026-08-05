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
import io.questdb.griffin.engine.window.WindowAccumulatorDescriptor;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * The <b>durable</b> half of one accumulator component: what a fused live-view window
 * state writes to disk about the {@link WindowAccumulatorDescriptor runtime component} it
 * wraps.
 * <p>
 * The split is deliberate and one-directional. Which rows contribute, how many map value
 * slots the state occupies, which slot holds which field and whether one family's state
 * contains another's are runtime facts, and this class asks the runtime descriptor for all
 * of them rather than restating any: two family tables would drift, and a drift here is a
 * persisted layout that no longer describes the state it was written from. What this class
 * owns is everything that only exists because the state is persisted - the component codec
 * version, the encoded identity a manifest carries, the byte offsets inside a leaf's
 * scalar payload, and the freeze/restore codec itself.
 *
 * <h2>What the encoded identity is for</h2>
 * A component's identity is the whole of the sharing proof, and the persisted form of it
 * is what a predecessor root is compared against. It carries the runtime identity - family,
 * contribution predicate, and the argument as a {@code (base column index, column type)}
 * pair - plus the component <b>codec version</b>, so a layout change across a binary
 * upgrade produces a different identity rather than a silent reinterpretation of bytes an
 * older build wrote.
 * <p>
 * The window, frame and anchor identity are <b>not</b> repeated here: a component lives
 * under exactly one window-state root, and the root carries them, so a component identity
 * is only ever compared against another under the same root.
 *
 * <h2>The image is one 64-bit field per runtime slot</h2>
 * {@link #freezeStateInto} writes each of the component's slots as one little-endian
 * 64-bit field, in slot order, which is exactly what the contributing function's own
 * {@code freezeCheckpointState} produces through {@code MemoryA}. The durable width and
 * every field offset therefore follow the runtime slot model rather than being tabulated a
 * second time. A family whose image is not that shape - a DECIMAL accumulator, say - needs
 * a codec of its own and must not be described by this one.
 * <p>
 * {@link #familyCodecVersion} is the list of families this codec covers, and a family joins
 * it only once its contributing implementation writes that exact image: the accumulating
 * families a live view has fused since the first slice, the compensated total, and the four
 * fixed scalar extrema, whose implementations keep the running extremum in one slot and read
 * the family's own identity - {@code LONG_NULL} or NaN - as "no row has contributed".
 * Getting the extrema there took their own value layout down to that one slot: the redundant
 * "initialized" byte they used to keep beside it was a field the group's value has no room
 * for and a byte the component image would have had to carry.
 *
 * <h2>Containment is a claim about two codecs</h2>
 * {@link WindowAccumulatorDescriptor#derivedSlotOffset} names the family pairs whose state
 * one contains the other's; {@link #derivedStateOffset} is the same claim in bytes, and it
 * additionally requires both sides to be at the codec version the claim was proved at. A
 * bump on either side withdraws the fold instead of silently carrying it onto a layout
 * nobody checked.
 */
public final class LiveViewAccumulatorDescriptor {
    /**
     * The component codec version at which every containment relation in
     * {@link WindowAccumulatorDescriptor#derivedSlotOffset}'s table was proved byte for
     * byte against the contributing implementations' own freeze images. A component at any
     * other version keeps its own slice rather than being folded onto a host whose bytes
     * were never checked against it.
     */
    private static final int CONTAINMENT_PROOF_CODEC_VERSION = 1;
    private static final int FORMAT_VERSION = 1;
    private static final int MAGIC = 0x4c564143; // LVAC
    private final int codecVersion;
    private final byte[] encoded;
    private final WindowAccumulatorDescriptor runtime;
    private final int stateLength;

    private LiveViewAccumulatorDescriptor(@NotNull WindowAccumulatorDescriptor runtime, int codecVersion) {
        this.runtime = runtime;
        this.codecVersion = codecVersion;
        this.stateLength = runtime.getSlotCount() * Long.BYTES;
        this.encoded = encode();
    }

    /**
     * Returns the state layout version this build writes for {@code family}, or
     * {@code -1} for a family it does not know. Folded into the encoded identity, so a
     * bump makes an older component unresolvable rather than reinterpretable.
     */
    public static int familyCodecVersion(int family) {
        switch (family) {
            case WindowAccumulatorDescriptor.FAMILY_DOUBLE_KAHAN_SUM_COUNT:
            case WindowAccumulatorDescriptor.FAMILY_DOUBLE_MAX:
            case WindowAccumulatorDescriptor.FAMILY_DOUBLE_MIN:
            case WindowAccumulatorDescriptor.FAMILY_DOUBLE_SUM_COUNT:
            case WindowAccumulatorDescriptor.FAMILY_DOUBLE_WELFORD:
            case WindowAccumulatorDescriptor.FAMILY_LONG_MAX:
            case WindowAccumulatorDescriptor.FAMILY_LONG_MIN:
            case WindowAccumulatorDescriptor.FAMILY_NON_NULL_COUNT:
            case WindowAccumulatorDescriptor.FAMILY_ROW_COUNT:
                return 1;
            default:
                return -1;
        }
    }

    /**
     * Returns the whole-state width of {@code family}, which must equal the width the
     * contributing implementation declares through
     * {@code WindowFunction.checkpointStateFixedLength()}. The plan checks the two
     * against each other rather than trusting either alone.
     * <p>
     * Derived from the family's slot count rather than tabulated, because the codec
     * writes one 64-bit field per slot and a second table could only ever disagree with
     * it.
     */
    public static int familyStateLength(int family) {
        final int slots = WindowAccumulatorDescriptor.familySlotCount(family);
        return slots <= 0 ? -1 : slots * Long.BYTES;
    }

    /**
     * Whether {@code family}'s component codec is at the version every containment relation
     * in {@link WindowAccumulatorDescriptor#derivedSlotOffset}'s table was proved byte for
     * byte at.
     * <p>
     * The family form of {@link #derivedSlotOffset}'s pinning, and it exists because the
     * fold itself is decided one layer down, in the runtime plan builder, which holds
     * families and slots rather than built durable descriptors. {@code LiveViewWindowStatePlan}
     * hands it in as that builder's fold policy: which pairs contain which is the runtime
     * table's answer, and whether a persisted layout will carry that answer is this one's.
     */
    public static boolean isContainmentProofCodec(int family) {
        return familyCodecVersion(family) == CONTAINMENT_PROOF_CODEC_VERSION;
    }

    /**
     * Builds the durable component a function over {@code argumentColumnIndex}
     * contributes to, or null when this build cannot name every part of its identity -
     * an unknown family, an argument type whose contribution predicate the runtime
     * descriptor declines, or a family with no component codec.
     *
     * @param family              one of the {@link WindowAccumulatorDescriptor}
     *                            {@code FAMILY_*} constants
     * @param argumentColumnIndex the argument's index in the base metadata the window
     *                            functions were compiled against, or
     *                            {@link WindowAccumulatorDescriptor#NO_ARGUMENT_COLUMN_INDEX}
     *                            for an argumentless family
     * @param argumentColumnType  the argument's compiled column type, or
     *                            {@link ColumnType#UNDEFINED} for an argumentless family
     */
    public static @Nullable LiveViewAccumulatorDescriptor of(int family, int argumentColumnIndex, int argumentColumnType) {
        return of(WindowAccumulatorDescriptor.of(family, argumentColumnIndex, argumentColumnType));
    }

    /**
     * Wraps a compiled runtime component in the durable facts a live view persists it
     * with, or returns null when the component has no codec in this build.
     */
    public static @Nullable LiveViewAccumulatorDescriptor of(@Nullable WindowAccumulatorDescriptor runtime) {
        if (runtime == null) {
            return null;
        }
        final int codecVersion = familyCodecVersion(runtime.getFamily());
        if (codecVersion < 0 || runtime.getSlotCount() <= 0) {
            return null;
        }
        return new LiveViewAccumulatorDescriptor(runtime, codecVersion);
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
     * Copies this component's slots from one map value to another, so a runtime whose
     * ownership is moving - the window adopting the plan, or handing the state back -
     * carries the accumulator across without going through the durable encoding.
     */
    public void copyState(@NotNull MapValue src, int srcSlotBase, @NotNull MapValue dst, int dstSlotBase) {
        runtime.copyState(src, srcSlotBase, dst, dstSlotBase);
    }

    /**
     * Returns the relative slot inside this component's own runtime state at which
     * {@code other}'s whole state begins, or {@code -1} when the fold does not hold. The
     * slot counterpart of {@link #derivedStateOffset}: the two express one containment,
     * once in the durable image's bytes and once in the fused map value's slots, and both
     * are withheld when either side's codec version has moved off the one the claim was
     * proved at.
     */
    public int derivedSlotOffset(@NotNull LiveViewAccumulatorDescriptor other) {
        // Identical components need no codec pinning: a family fixes its own codec
        // version, so two components that are the same runtime component are at the same
        // version by construction and the guest's image is the whole of the host's.
        if (runtime.isSameIdentity(other.runtime)) {
            return 0;
        }
        final int slot = runtime.derivedSlotOffset(other.runtime);
        if (slot < 0) {
            return -1;
        }
        // One pinning rule, read here off two built descriptors and in isContainmentProofCodec
        // off the two families a plan's fold is deciding between. A component's codec version
        // is its family's, so the two spellings cannot disagree.
        return isContainmentProofCodec(runtime.getFamily())
                && isContainmentProofCodec(other.runtime.getFamily())
                ? slot
                : -1;
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
     * Which pairs contain which is {@link WindowAccumulatorDescriptor#derivedSlotOffset}'s
     * answer. What this adds is the codec pinning: a containment claim is a claim about
     * two <b>codecs</b> - that the guest's {@code freezeCheckpointState} image is
     * byte-for-byte a run inside the host's - which is a fact about the two
     * implementations at their current versions and not something the field offsets alone
     * establish.
     */
    public int derivedStateOffset(@NotNull LiveViewAccumulatorDescriptor other) {
        final int slot = derivedSlotOffset(other);
        return slot < 0 ? -1 : slot * Long.BYTES;
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

    public int getArgumentColumnIndex() {
        return runtime.getArgumentColumnIndex();
    }

    public int getArgumentColumnType() {
        return runtime.getArgumentColumnType();
    }

    public int getCodecVersion() {
        return codecVersion;
    }

    public int getContributionKind() {
        return runtime.getContributionKind();
    }

    /**
     * Returns an owned copy of the encoded identity, which is what the persisted
     * manifest carries.
     */
    public byte[] getEncoded() {
        return Arrays.copyOf(encoded, encoded.length);
    }

    public int getFamily() {
        return runtime.getFamily();
    }

    /**
     * Returns {@code field}'s offset inside this component's own state, or {@code -1}
     * when the family does not carry it. The byte counterpart of
     * {@link #getFieldSlot(int)}, and derived from it: the image is one 64-bit field per
     * slot.
     */
    public int getFieldOffset(int field) {
        final int slot = runtime.getFieldSlot(field);
        return slot < 0 ? -1 : slot * Long.BYTES;
    }

    /**
     * Returns {@code field}'s slot inside this component's own runtime state, or
     * {@code -1} when the family does not carry it.
     */
    public int getFieldSlot(int field) {
        return runtime.getFieldSlot(field);
    }

    /**
     * The runtime component this one persists. Everything about the state itself - the
     * family, the contribution predicate, the argument key, the slot layout - is read off
     * this rather than off the durable wrapper.
     */
    public @NotNull WindowAccumulatorDescriptor getRuntime() {
        return runtime;
    }

    /**
     * Returns how many {@link MapValue} slots this component occupies in the window's
     * fused runtime map value.
     */
    public int getSlotCount() {
        return runtime.getSlotCount();
    }

    /**
     * Returns the column type of one of this component's runtime slots. The widths add up
     * to {@link #getStateLength()}, because the durable image is those same fields in
     * that same order.
     */
    public int getSlotColumnType(int slot) {
        return runtime.getSlotColumnType(slot);
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
     * Puts this component's slots back to the identity an anchor crossing leaves behind,
     * which is also what a brand-new partition needs: a map value's slots are not
     * zero-filled by {@code createValue()} on any implementation.
     */
    public void resetState(@NotNull MapValue value, int slotBase) {
        runtime.resetState(value, slotBase);
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

    private static long getLongLE(byte[] payload, int offset) {
        long value = 0;
        for (int i = Long.BYTES - 1; i >= 0; i--) {
            value = (value << 8) | (payload[offset + i] & 0xffL);
        }
        return value;
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
        buffer.putInt(runtime.getFamily());
        buffer.putInt(codecVersion);
        buffer.putInt(runtime.getContributionKind());
        buffer.putInt(runtime.getArgumentColumnIndex());
        buffer.putInt(runtime.getArgumentColumnType());
        return buffer.array();
    }
}
