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
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.util.Arrays;

/**
 * Durable codec and identity for a {@link WindowAccumulatorDescriptor}. Runtime
 * slot widths and contribution rules come from that descriptor; this class adds
 * the codec version, encoded identity, byte offsets and freeze/restore operations.
 * <p>
 * The encoded identity includes family, contribution predicate, argument column and
 * type, and codec version. Window, frame and anchor identity belong to the enclosing
 * root. Codec changes must change the identity before old bytes can be reused.
 * <p>
 * Each supported runtime slot encodes as one little-endian 64-bit field in slot order.
 * {@link #familyCodecVersion} admits only families with that layout. Containment via
 * {@link #derivedStateOffset} additionally requires both codecs at their proven
 * versions; changing either version withdraws the fold.
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
        return switch (family) {
            case WindowAccumulatorDescriptor.FAMILY_DOUBLE_KAHAN_SUM_COUNT,
                 WindowAccumulatorDescriptor.FAMILY_DOUBLE_MAX,
                 WindowAccumulatorDescriptor.FAMILY_DOUBLE_MIN,
                 WindowAccumulatorDescriptor.FAMILY_DOUBLE_SUM_COUNT,
                 WindowAccumulatorDescriptor.FAMILY_DOUBLE_WELFORD,
                 WindowAccumulatorDescriptor.FAMILY_LONG_MAX,
                 WindowAccumulatorDescriptor.FAMILY_LONG_MIN,
                 WindowAccumulatorDescriptor.FAMILY_NON_NULL_COUNT,
                 WindowAccumulatorDescriptor.FAMILY_ROW_COUNT -> 1;
            default -> -1;
        };
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
     * Writes this component's whole-state image into the {@code payloadLength}-byte native
     * payload at {@code payloadAddress}, at {@code offset}, reading the fields out of the
     * fused map value's slots.
     * <p>
     * The bytes are the contributing function's own {@code freezeCheckpointState} image:
     * the same fields in the same order, little-endian, which is what
     * {@link LiveViewStatePageWriter} produces through {@code MemoryA}. That equality is
     * the whole of the component codec's contract with the implementations that declare
     * the family, and it is held to it directly by test rather than inferred - a leaf
     * carries no length for an inlined slice, so a divergence would be decoded at the
     * right width out of the wrong bytes.
     * <p>
     * Each field goes in native byte order. Every platform QuestDB supports is
     * little-endian, so that is the little-endian image.
     */
    public void freezeStateInto(@NotNull MapValue value, int slotBase, long payloadAddress, int payloadLength, int offset) {
        checkPayloadBounds(payloadLength, offset);
        long at = payloadAddress + offset;
        for (int i = 0, n = getSlotCount(); i < n; i++) {
            final int slotType = getSlotColumnType(i);
            final long bits = slotType == ColumnType.DOUBLE
                    ? Double.doubleToRawLongBits(value.getDouble(slotBase + i))
                    : value.getLong(slotBase + i);
            Unsafe.putLong(at, bits);
            at += Long.BYTES;
        }
    }

    public int getCodecVersion() {
        return codecVersion;
    }

    /**
     * Returns an owned copy of the encoded identity, which is what the persisted
     * manifest carries.
     */
    @TestOnly
    public byte[] getEncoded() {
        return Arrays.copyOf(encoded, encoded.length);
    }

    /**
     * Borrows this immutable descriptor's encoded identity while a compiled window-state
     * plan owns the descriptor. Package-internal callers must not mutate the array.
     */
    byte[] borrowEncoded() {
        return encoded;
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
     * Writes this component's identity image straight into the {@code payloadLength}-byte
     * native payload at {@code payloadAddress}, at {@code offset}, which is what
     * {@link #freezeStateInto} would produce from a value {@link #resetState} had just
     * put to identity, with the same bounds check and in the same native byte order.
     * <p>
     * It exists for the one key a private-map freeze finds no entry for. Outside a fused
     * group a function creates its map entry on the row that first contributes, so an
     * anchor key the contributor's map does not hold is a key whose accumulator is empty
     * rather than one whose state went missing - and an empty accumulator's image is not
     * zero for either extremum family.
     * <p>
     * The two encoders are held to the same slot walk deliberately: identity is the
     * runtime descriptor's answer, read here through
     * {@link WindowAccumulatorDescriptor#getSlotIdentityBits} rather than restated, so a
     * family whose empty state changes cannot leave the two disagreeing.
     */
    public void resetStateInto(long payloadAddress, int payloadLength, int offset) {
        checkPayloadBounds(payloadLength, offset);
        long at = payloadAddress + offset;
        for (int i = 0, n = getSlotCount(); i < n; i++) {
            Unsafe.putLong(at, runtime.getSlotIdentityBits(i));
            at += Long.BYTES;
        }
    }

    /**
     * Fills the fused map value's slots for this component from a whole-state image framed
     * by {@code payload}, at {@code offset}: the exact inverse of {@link #freezeStateInto}.
     * The slice is bounds-checked against the frame before the reader is touched, with the
     * same message and errno as the freeze.
     */
    public void restoreStateFrom(@NotNull LiveViewStatePageReader payload, long offset, @NotNull MapValue value, int slotBase) {
        checkPayloadBounds(payload.size(), offset);
        long at = offset;
        for (int i = 0, n = getSlotCount(); i < n; i++) {
            final long bits = payload.getLong(at);
            if (getSlotColumnType(i) == ColumnType.DOUBLE) {
                value.putDouble(slotBase + i, Double.longBitsToDouble(bits));
            } else {
                value.putLong(slotBase + i, bits);
            }
            at += Long.BYTES;
        }
    }

    /**
     * Proves the slice this component is about to read or write is inside a
     * {@code payloadLength}-byte payload. The fused leaf carries no per-component length,
     * so an offset the manifest and the payload disagree about would otherwise be a silent
     * read of a neighbouring component's bytes.
     */
    private void checkPayloadBounds(long payloadLength, long offset) {
        if (offset < 0 || offset > payloadLength - stateLength) {
            throw CairoException.critical(0)
                    .put("live view accumulator component slice is outside its payload [offset=")
                    .put(offset).put(", length=").put(stateLength)
                    .put(", payload=").put(payloadLength).put(']');
        }
    }

    /**
     * The codec version rides inside the identity as well as beside it in the
     * manifest. The byte-for-byte manifest comparison reads the manifest field;
     * carrying it here too makes {@link #isSameIdentity} complete on its own,
     * so the plan's merge cannot fold two components a codec bump has separated.
     */
    private byte[] encode() {
        final byte[] encoded = new byte[7 * Integer.BYTES];
        int offset = LiveViewCheckpointMetadata.putInt(encoded, 0, MAGIC);
        offset = LiveViewCheckpointMetadata.putInt(encoded, offset, FORMAT_VERSION);
        offset = LiveViewCheckpointMetadata.putInt(encoded, offset, runtime.getFamily());
        offset = LiveViewCheckpointMetadata.putInt(encoded, offset, codecVersion);
        offset = LiveViewCheckpointMetadata.putInt(encoded, offset, runtime.getContributionKind());
        offset = LiveViewCheckpointMetadata.putInt(encoded, offset, runtime.getArgumentColumnIndex());
        LiveViewCheckpointMetadata.putInt(encoded, offset, runtime.getArgumentColumnType());
        return encoded;
    }
}
