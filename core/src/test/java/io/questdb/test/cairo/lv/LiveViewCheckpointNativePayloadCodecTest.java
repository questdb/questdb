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

package io.questdb.test.cairo.lv;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.lv.LiveViewAccumulatorDescriptor;
import io.questdb.cairo.lv.LiveViewCheckpointRangeRingStateReader;
import io.questdb.cairo.lv.LiveViewCheckpointWindowRoot;
import io.questdb.cairo.lv.LiveViewStatePageReader;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.engine.window.WindowAccumulatorDescriptor;
import io.questdb.std.IntHashSet;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * The native payload codecs must write and read exactly the bytes the heap codecs they
 * replaced did, because those bytes are persisted into checkpoint roots and no on-disk format
 * change is allowed. Three encoders are held to that here: the accumulator component codec
 * (freeze, reset and restore), the fused payload's anchor value, and the RANGE ring's scalar
 * continuation state.
 * <p>
 * Each native form answers to a test-side oracle: a literal copy of the heap encoders as
 * they stood when the native forms were introduced. The production heap forms are gone, so
 * the oracle is what pins the byte format to something that is not the code under test.
 * <p>
 * Every encoder writes into a buffer pre-filled with random bytes, so the comparison also
 * proves each writes exactly its own slice and nothing beside it. Bounds violations must
 * fail with the format's own exception, errno and message, and the native form must write
 * nothing when it fails.
 */
public class LiveViewCheckpointNativePayloadCodecTest extends AbstractCairoTest {
    private static final int[] ARGUMENT_TYPES = {
            ColumnType.UNDEFINED,
            ColumnType.BYTE,
            ColumnType.SHORT,
            ColumnType.INT,
            ColumnType.LONG,
            ColumnType.FLOAT,
            ColumnType.DOUBLE,
            ColumnType.DATE,
            ColumnType.TIMESTAMP,
            ColumnType.SYMBOL,
            ColumnType.STRING,
            ColumnType.VARCHAR
    };
    private static final Method ENCODE_ANCHOR_NATIVE;
    private static final Method ENCODE_RING_SCALAR_NATIVE;
    private static final int MAX_FAMILY = 64;
    private static final int PAYLOAD_SLACK = 24;
    private static final int ROUNDS = 200;

    static {
        // The anchor and ring encoders are package-private to io.questdb.cairo.lv.
        try {
            ENCODE_ANCHOR_NATIVE = accessible(LiveViewCheckpointWindowRoot.class.getDeclaredMethod(
                    "encodeAnchorValue", long.class, long.class));
            ENCODE_RING_SCALAR_NATIVE = accessible(LiveViewCheckpointRangeRingStateReader.class.getDeclaredMethod(
                    "encodeScalar", long.class, int.class, int.class, int.class, long.class, long.class, long.class,
                    long.class, long.class, long.class, long.class));
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    @Test
    public void testAnchorCodecMatchesTheOracle() throws Exception {
        assertMemoryLeak(() -> {
            final Rnd rnd = new Rnd(23, 29);
            final LiveViewStatePageReader reader = new LiveViewStatePageReader();
            try (MemoryCARW buffer = newBuffer()) {
                for (int round = 0; round < ROUNDS; round++) {
                    // The bare anchor, and the anchor leading a fused payload whose components
                    // follow it and must be left alone.
                    final int payloadLength = rnd.nextBoolean() ? Long.BYTES : Long.BYTES + 8 * (1 + rnd.nextInt(8));
                    final long anchor = randomLongBits(rnd);
                    final byte[] garbage = randomBytes(rnd, payloadLength);

                    final long address = buffer.addressOf(0);
                    copyIn(garbage, address);
                    invokeStatic(ENCODE_ANCHOR_NATIVE, anchor, address);
                    final byte[] oracle = garbage.clone();
                    Oracle.encodeAnchorValue(anchor, oracle);

                    Assert.assertArrayEquals("native anchor encoder against the oracle", oracle, copyOut(address, payloadLength));
                    Assert.assertEquals(anchor, Oracle.readAnchorValue(oracle));
                    // The oracle's image, decoded by the production reader.
                    copyIn(oracle, address);
                    Assert.assertEquals(anchor, LiveViewCheckpointWindowRoot.readAnchorValue(reader.of(buffer, 0, payloadLength)));
                }

                // A payload too short for its anchor is recoverable corruption, so a restore
                // skips the root.
                for (int length = 0; length < Long.BYTES; length++) {
                    final LiveViewStatePageReader shortPage = reader.of(buffer, 0, length);
                    final CairoException e = Assert.assertThrows(CairoException.class,
                            () -> LiveViewCheckpointWindowRoot.readAnchorValue(shortPage)
                    );
                    Assert.assertEquals(CairoException.LV_CHECKPOINT_TIMELINE_INVALID, e.getErrno());
                    TestUtils.assertContains(
                            e.getFlyweightMessage(),
                            "window state entry is too short for its anchor value, length=" + length
                    );
                }
            }
        });
    }

    @Test
    public void testComponentCodecMatchesTheOracle() throws Exception {
        assertMemoryLeak(() -> {
            final ObjList<LiveViewAccumulatorDescriptor> components = everyComponent();
            final Rnd rnd = new Rnd(31, 37);
            final LiveViewStatePageReader reader = new LiveViewStatePageReader();
            try (MemoryCARW buffer = newBuffer()) {
                for (int c = 0, n = components.size(); c < n; c++) {
                    final LiveViewAccumulatorDescriptor component = components.getQuick(c);
                    final int stateLength = component.getStateLength();
                    try (
                            SlotValue source = new SlotValue(component);
                            SlotValue target = new SlotValue(component)
                    ) {
                        for (int round = 0; round < ROUNDS; round++) {
                            final int offset = rnd.nextInt(PAYLOAD_SLACK);
                            final int payloadLength = offset + stateLength + rnd.nextInt(PAYLOAD_SLACK);
                            final long[] bits = source.fillRandom(rnd);
                            final byte[] garbage = randomBytes(rnd, payloadLength);
                            final long address = buffer.addressOf(0);
                            final String what = "family " + component.getFamily() + " round " + round;

                            // freezeStateInto
                            copyIn(garbage, address);
                            component.freezeStateInto(source.value, SlotValue.SLOT_BASE, address, payloadLength, offset);
                            final byte[] oracle = garbage.clone();
                            Oracle.freezeStateInto(component, bits, oracle, offset);
                            Assert.assertArrayEquals(what + ": native freeze against the oracle", oracle, copyOut(address, payloadLength));

                            // restoreStateFrom: the oracle's image, read back through the
                            // reader, lands the bits the oracle encoded.
                            target.fillRandom(rnd);
                            copyIn(oracle, address);
                            component.restoreStateFrom(reader.of(buffer, 0, payloadLength), offset, target.value, SlotValue.SLOT_BASE);
                            final long[] decoded = Oracle.restoreStateFrom(component, oracle, offset);
                            Assert.assertArrayEquals(what + ": the oracle must decode what it encoded", bits, decoded);
                            Assert.assertArrayEquals(what + ": native restore", bits, target.readBits());
                            Assert.assertEquals(what + ": native restore must leave the slot before the slice", SlotValue.PREFIX, target.readPrefix());

                            // resetStateInto
                            copyIn(garbage, address);
                            component.resetStateInto(address, payloadLength, offset);
                            final byte[] oracleReset = garbage.clone();
                            Oracle.resetStateInto(component, oracleReset, offset);
                            Assert.assertArrayEquals(what + ": native reset against the oracle", oracleReset, copyOut(address, payloadLength));
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testComponentCodecRefusesASliceOutsideItsPayloadAlike() throws Exception {
        // The fused leaf carries no per-component length, so a slice outside the payload is
        // rejected before a byte moves - the same exception, errno and message on freeze,
        // reset and restore, and on the restore it is the component's own check that fires
        // rather than the reader's, which would classify the same fault as recoverable
        // corruption.
        assertMemoryLeak(() -> {
            final ObjList<LiveViewAccumulatorDescriptor> components = everyComponent();
            final Rnd rnd = new Rnd(41, 43);
            final LiveViewStatePageReader reader = new LiveViewStatePageReader();
            try (MemoryCARW buffer = newBuffer()) {
                for (int c = 0, n = components.size(); c < n; c++) {
                    final LiveViewAccumulatorDescriptor component = components.getQuick(c);
                    final int stateLength = component.getStateLength();
                    final int payloadLength = stateLength + Long.BYTES;
                    final int[] offsets = {
                            -1,
                            Integer.MIN_VALUE,
                            payloadLength - stateLength + 1,
                            payloadLength,
                            Integer.MAX_VALUE - stateLength + 1,
                            Integer.MAX_VALUE
                    };
                    try (
                            SlotValue source = new SlotValue(component);
                            SlotValue target = new SlotValue(component)
                    ) {
                        source.fillRandom(rnd);
                        for (int offset : offsets) {
                            final byte[] garbage = randomBytes(rnd, payloadLength);
                            final long address = buffer.addressOf(0);

                            copyIn(garbage, address);
                            final CairoException freeze = Assert.assertThrows(CairoException.class,
                                    () -> component.freezeStateInto(source.value, SlotValue.SLOT_BASE, address, payloadLength, offset));
                            Assert.assertEquals(0, freeze.getErrno());
                            Assert.assertTrue(freeze.isCritical());
                            TestUtils.assertContains(
                                    freeze.getFlyweightMessage(),
                                    "live view accumulator component slice is outside its payload [offset=" + offset
                                            + ", length=" + stateLength + ", payload=" + payloadLength + ']'
                            );
                            Assert.assertArrayEquals("a refused freeze must write nothing", garbage, copyOut(address, payloadLength));

                            final CairoException reset = Assert.assertThrows(CairoException.class,
                                    () -> component.resetStateInto(address, payloadLength, offset));
                            assertSameFailure(freeze, reset);
                            Assert.assertArrayEquals("a refused reset must write nothing", garbage, copyOut(address, payloadLength));

                            final long[] before = target.fillRandom(rnd);
                            final CairoException restore = Assert.assertThrows(CairoException.class,
                                    () -> component.restoreStateFrom(reader.of(buffer, 0, payloadLength), offset, target.value, SlotValue.SLOT_BASE));
                            assertSameFailure(freeze, restore);
                            Assert.assertArrayEquals("a refused restore must leave the value", before, target.readBits());
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testRingScalarCodecMatchesTheOracle() throws Exception {
        assertMemoryLeak(() -> {
            final Rnd rnd = new Rnd(47, 53);
            final int[] valueKinds = {
                    LiveViewCheckpointRangeRingStateReader.VALUE_KIND_DOUBLE,
                    LiveViewCheckpointRangeRingStateReader.VALUE_KIND_LONG,
                    LiveViewCheckpointRangeRingStateReader.VALUE_KIND_DEQUE_DOUBLE,
                    LiveViewCheckpointRangeRingStateReader.VALUE_KIND_DEQUE_LONG,
                    LiveViewCheckpointRangeRingStateReader.VALUE_KIND_DECIMAL128,
                    LiveViewCheckpointRangeRingStateReader.VALUE_KIND_DECIMAL256,
                    LiveViewCheckpointRangeRingStateReader.VALUE_KIND_DEQUE_DECIMAL128,
                    LiveViewCheckpointRangeRingStateReader.VALUE_KIND_DEQUE_DECIMAL256,
                    LiveViewCheckpointRangeRingStateReader.VALUE_KIND_NONE
            };
            final int[] scalarWordCounts = {1, 2, 4};
            try (MemoryCARW buffer = newBuffer()) {
                for (int scalarWords : scalarWordCounts) {
                    final int scalarBytes = LiveViewCheckpointRangeRingStateReader.scalarStateBytes(scalarWords);
                    for (int round = 0; round < ROUNDS; round++) {
                        final int valueKind = valueKinds[rnd.nextInt(valueKinds.length)];
                        final int headOffset = rnd.nextBoolean() ? rnd.nextPositiveInt() : rnd.nextInt();
                        final long rowCount = randomLongBits(rnd);
                        final long w0 = randomLongBits(rnd);
                        final long w1 = randomLongBits(rnd);
                        final long w2 = randomLongBits(rnd);
                        final long w3 = randomLongBits(rnd);
                        final long frameSize = randomLongBits(rnd);
                        final long lastTimestamp = randomLongBits(rnd);

                        final byte[] oracle = Oracle.encodeScalar(valueKind, scalarWords, headOffset,
                                rowCount, w0, w1, w2, w3, frameSize, lastTimestamp);
                        Assert.assertEquals(scalarBytes, oracle.length);

                        // Guard bytes on both sides prove the native form writes exactly the
                        // scalar's width and every byte of it.
                        final byte[] garbage = randomBytes(rnd, scalarBytes + 2 * PAYLOAD_SLACK);
                        final long address = buffer.addressOf(0);
                        copyIn(garbage, address);
                        invokeStatic(ENCODE_RING_SCALAR_NATIVE, address + PAYLOAD_SLACK, valueKind, scalarWords, headOffset,
                                rowCount, w0, w1, w2, w3, frameSize, lastTimestamp);
                        final byte[] expected = garbage.clone();
                        System.arraycopy(oracle, 0, expected, PAYLOAD_SLACK, scalarBytes);
                        Assert.assertArrayEquals("native ring scalar against the oracle", expected, copyOut(address, garbage.length));
                    }
                }

                // A width the format has no layout for fails as the format's own width check
                // does, and writes nothing.
                for (int scalarWords : new int[]{0, 3, 5, -1}) {
                    final byte[] garbage = randomBytes(rnd, 128);
                    final long address = buffer.addressOf(0);
                    copyIn(garbage, address);
                    final CairoException expected = Assert.assertThrows(CairoException.class,
                            () -> LiveViewCheckpointRangeRingStateReader.scalarStateBytes(scalarWords));
                    TestUtils.assertContains(expected.getFlyweightMessage(), "RANGE ring scalar width invalid");
                    final CairoException nativeException = Assert.assertThrows(CairoException.class, () -> invokeStatic(ENCODE_RING_SCALAR_NATIVE,
                            address, LiveViewCheckpointRangeRingStateReader.VALUE_KIND_DOUBLE, scalarWords, 0, 1L, 2L, 3L, 4L, 5L, 6L, 7L));
                    assertSameFailure(expected, nativeException);
                    Assert.assertArrayEquals(garbage, copyOut(address, garbage.length));
                }
            }
        });
    }

    private static <T extends java.lang.reflect.AccessibleObject> T accessible(T member) {
        member.setAccessible(true);
        return member;
    }

    private static void assertSameFailure(CairoException expected, CairoException actual) {
        Assert.assertEquals(expected.getErrno(), actual.getErrno());
        Assert.assertEquals(expected.isCritical(), actual.isCritical());
        Assert.assertEquals(expected.getFlyweightMessage().toString(), actual.getFlyweightMessage().toString());
    }

    private static void copyIn(byte[] bytes, long address) {
        for (int i = 0; i < bytes.length; i++) {
            Unsafe.putByte(address + i, bytes[i]);
        }
    }

    private static byte[] copyOut(long address, int length) {
        final byte[] bytes = new byte[length];
        for (int i = 0; i < length; i++) {
            bytes[i] = Unsafe.getByte(address + i);
        }
        return bytes;
    }

    /**
     * Every durable component this build can persist: each family the component codec
     * covers, over every argument type its contribution predicate admits. Built from the
     * runtime's own acceptance rather than listed, so a family that joins the codec joins
     * this case too.
     */
    private static ObjList<LiveViewAccumulatorDescriptor> everyComponent() {
        final ObjList<LiveViewAccumulatorDescriptor> components = new ObjList<>();
        final IntHashSet families = new IntHashSet();
        final IntHashSet slotTypes = new IntHashSet();
        for (int family = 0; family < MAX_FAMILY; family++) {
            for (int argumentType : ARGUMENT_TYPES) {
                final int argumentColumnIndex = argumentType == ColumnType.UNDEFINED
                        ? WindowAccumulatorDescriptor.NO_ARGUMENT_COLUMN_INDEX
                        : 2;
                final LiveViewAccumulatorDescriptor component = LiveViewAccumulatorDescriptor.of(
                        family,
                        argumentColumnIndex,
                        argumentType
                );
                if (component != null) {
                    components.add(component);
                    families.add(family);
                    for (int i = 0, n = component.getSlotCount(); i < n; i++) {
                        slotTypes.add(component.getSlotColumnType(i));
                    }
                }
            }
        }
        for (int family = 0; family < MAX_FAMILY; family++) {
            if (LiveViewAccumulatorDescriptor.familyCodecVersion(family) > 0) {
                Assert.assertTrue("codec family " + family + " must reach the case", families.contains(family));
            }
        }
        Assert.assertTrue(slotTypes.contains(ColumnType.DOUBLE));
        Assert.assertTrue(slotTypes.contains(ColumnType.LONG));
        return components;
    }

    private static Object invokeStatic(Method method, Object... args) {
        try {
            return method.invoke(null, args);
        } catch (InvocationTargetException e) {
            final Throwable cause = e.getCause();
            if (cause instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            if (cause instanceof Error error) {
                throw error;
            }
            throw new AssertionError(cause);
        } catch (IllegalAccessException e) {
            throw new AssertionError(e);
        }
    }

    private static MemoryCARW newBuffer() {
        final MemoryCARW buffer = Vm.getCARWInstance(4096, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
        buffer.jumpTo(1024);
        return buffer;
    }

    private static byte[] randomBytes(Rnd rnd, int length) {
        final byte[] bytes = new byte[length];
        for (int i = 0; i < length; i++) {
            bytes[i] = rnd.nextByte();
        }
        return bytes;
    }

    /**
     * A 64-bit field that is often one of the values a codec is most likely to get wrong:
     * the NULL sentinels, both zeros, NaN, the infinities, the extremes and a value with
     * every byte distinct.
     */
    private static long randomLongBits(Rnd rnd) {
        return switch (rnd.nextInt(12)) {
            case 0 -> Numbers.LONG_NULL;
            case 1 -> Long.MIN_VALUE;
            case 2 -> Long.MAX_VALUE;
            case 3 -> 0L;
            case 4 -> -1L;
            case 5 -> Double.doubleToRawLongBits(Double.NaN);
            case 6 -> Double.doubleToRawLongBits(-0.0);
            case 7 -> Double.doubleToRawLongBits(Double.NEGATIVE_INFINITY);
            case 8 -> Double.doubleToRawLongBits(Double.MIN_VALUE);
            case 9 -> 0x0102030405060708L;
            default -> rnd.nextLong();
        };
    }

    /**
     * The heap encoders as they stood when the native forms were introduced, copied rather
     * than called: explicit little-endian loops over a byte array. They are the byte format's
     * reference once the production heap forms are gone.
     */
    private static final class Oracle {
        private static final int ANCHOR_STATE_BYTES = Long.BYTES;
        private static final int ANCHOR_STATE_OFFSET = 0;
        private static final int RING_FORMAT_VERSION = 1;
        private static final int RING_SCALAR_FIXED_WORDS = 4;

        static void encodeAnchorValue(long anchorValue, byte[] scalarState) {
            for (int i = 0; i < ANCHOR_STATE_BYTES; i++) {
                scalarState[ANCHOR_STATE_OFFSET + i] = (byte) (anchorValue >>> (i * Byte.SIZE));
            }
        }

        static byte[] encodeScalar(
                int valueKind,
                int scalarWords,
                int headOffset,
                long rowCount,
                long scalarWord0,
                long scalarWord1,
                long scalarWord2,
                long scalarWord3,
                long frameSize,
                long lastTimestamp
        ) {
            final byte[] scalar = new byte[(RING_SCALAR_FIXED_WORDS + scalarWords) * Long.BYTES];
            putLong(scalar, 0, ((long) headOffset << 32)
                    | ((long) (scalarWords & 0xff) << 24)
                    | ((long) (valueKind & 0xff) << 16)
                    | (RING_FORMAT_VERSION & 0xffffL));
            putLong(scalar, Long.BYTES, rowCount);
            putLong(scalar, 2 * Long.BYTES, scalarWord0);
            if (scalarWords > 1) {
                putLong(scalar, 3 * Long.BYTES, scalarWord1);
            }
            if (scalarWords > 2) {
                putLong(scalar, 4 * Long.BYTES, scalarWord2);
                putLong(scalar, 5 * Long.BYTES, scalarWord3);
            }
            putLong(scalar, (2 + scalarWords) * Long.BYTES, frameSize);
            putLong(scalar, (3 + scalarWords) * Long.BYTES, lastTimestamp);
            return scalar;
        }

        /**
         * The component freeze: each slot's 64 bits, a DOUBLE slot's as its raw IEEE-754
         * bits, little-endian in slot order. {@code bits} are the slots as the value holds
         * them.
         */
        static void freezeStateInto(LiveViewAccumulatorDescriptor component, long[] bits, byte[] payload, int offset) {
            int at = offset;
            for (int i = 0, n = component.getSlotCount(); i < n; i++) {
                putLongLE(payload, at, bits[i]);
                at += Long.BYTES;
            }
        }

        static long readAnchorValue(byte[] scalarState) {
            long value = 0;
            for (int i = ANCHOR_STATE_BYTES - 1; i >= 0; i--) {
                value = (value << 8) | (scalarState[ANCHOR_STATE_OFFSET + i] & 0xffL);
            }
            return value;
        }

        static void resetStateInto(LiveViewAccumulatorDescriptor component, byte[] payload, int offset) {
            int at = offset;
            for (int i = 0, n = component.getSlotCount(); i < n; i++) {
                putLongLE(payload, at, component.getRuntime().getSlotIdentityBits(i));
                at += Long.BYTES;
            }
        }

        static long[] restoreStateFrom(LiveViewAccumulatorDescriptor component, byte[] payload, int offset) {
            final long[] bits = new long[component.getSlotCount()];
            int at = offset;
            for (int i = 0; i < bits.length; i++) {
                bits[i] = getLongLE(payload, at);
                at += Long.BYTES;
            }
            return bits;
        }

        private static long getLongLE(byte[] payload, int offset) {
            long value = 0;
            for (int i = Long.BYTES - 1; i >= 0; i--) {
                value = (value << 8) | (payload[offset + i] & 0xffL);
            }
            return value;
        }

        private static void putLong(byte[] bytes, int offset, long value) {
            for (int i = 0; i < Long.BYTES; i++) {
                bytes[offset + i] = (byte) (value >>> (i * 8));
            }
        }

        private static void putLongLE(byte[] payload, int offset, long value) {
            for (int i = 0; i < Long.BYTES; i++) {
                payload[offset + i] = (byte) (value >>> (i * Byte.SIZE));
            }
        }
    }

    /**
     * One map value laid out for {@code component}: a sentinel LONG slot ahead of the
     * component's own slots, so a codec that ignored its slot base, or wrote before it,
     * shows up as a changed sentinel.
     */
    private static final class SlotValue implements AutoCloseable {
        static final long PREFIX = 0x5a5a5a5a_a5a5a5a5L;
        static final int SLOT_BASE = 1;
        private final LiveViewAccumulatorDescriptor component;
        private final Map map;
        private final MapValue value;

        SlotValue(LiveViewAccumulatorDescriptor component) {
            this.component = component;
            final ArrayColumnTypes valueTypes = new ArrayColumnTypes();
            valueTypes.add(ColumnType.LONG);
            for (int i = 0, n = component.getSlotCount(); i < n; i++) {
                valueTypes.add(component.getSlotColumnType(i));
            }
            final ArrayColumnTypes keyTypes = new ArrayColumnTypes();
            keyTypes.add(ColumnType.LONG);
            map = MapFactory.createUnorderedMap(configuration, keyTypes, valueTypes);
            final MapKey key = map.withKey();
            key.putLong(1);
            value = key.createValue();
            value.putLong(0, PREFIX);
        }

        @Override
        public void close() {
            map.close();
        }

        long[] fillRandom(Rnd rnd) {
            final long[] bits = new long[component.getSlotCount()];
            for (int i = 0; i < bits.length; i++) {
                bits[i] = randomLongBits(rnd);
            }
            writeBits(bits);
            return bits;
        }

        long[] readBits() {
            final long[] bits = new long[component.getSlotCount()];
            for (int i = 0; i < bits.length; i++) {
                bits[i] = component.getSlotColumnType(i) == ColumnType.DOUBLE
                        ? Double.doubleToRawLongBits(value.getDouble(SLOT_BASE + i))
                        : value.getLong(SLOT_BASE + i);
            }
            return bits;
        }

        long readPrefix() {
            return value.getLong(0);
        }

        void writeBits(long[] bits) {
            for (int i = 0; i < bits.length; i++) {
                if (component.getSlotColumnType(i) == ColumnType.DOUBLE) {
                    value.putDouble(SLOT_BASE + i, Double.longBitsToDouble(bits[i]));
                } else {
                    value.putLong(SLOT_BASE + i, bits[i]);
                }
            }
        }
    }
}
