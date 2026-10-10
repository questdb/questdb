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

import io.questdb.std.Hash;
import io.questdb.std.MemoryTag;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.Arrays;

/**
 * The native key helpers must agree with the heap-array semantics they replace, bit for bit:
 * the order a persisted partition map is sorted in, the hash a key table's slot order depends
 * on, and equality. Keys of every length from 0 to 300 cross the eight-byte chunk boundaries
 * the helpers read at, carry high-bit bytes a signed compare would misorder, and share long
 * prefixes so the first difference falls anywhere in a chunk.
 */
public class LiveViewCheckpointKeysTest {
    private static final Method COMPARE_BYTES;
    private static final Method COMPARE_NATIVE;
    private static final Method EQUALS_NATIVE;
    private static final Method HASH;
    private static final Method HASH_CODE;
    private static final int MAX_KEY_LENGTH = 300;

    static {
        // The helpers are package-private to io.questdb.cairo.lv.
        try {
            final Class<?> keys = Class.forName("io.questdb.cairo.lv.LiveViewCheckpointKeys");
            final Class<?> metadata = Class.forName("io.questdb.cairo.lv.LiveViewCheckpointMetadata");
            COMPARE_BYTES = accessible(metadata.getDeclaredMethod("compareBytes", byte[].class, byte[].class));
            COMPARE_NATIVE = accessible(keys.getDeclaredMethod("compare", long.class, int.class, long.class, int.class));
            EQUALS_NATIVE = accessible(keys.getDeclaredMethod("equals", long.class, int.class, long.class, int.class));
            HASH = accessible(keys.getDeclaredMethod("hash", long.class, int.class));
            HASH_CODE = accessible(keys.getDeclaredMethod("hashCode", long.class, int.class));
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    @Test
    public void testCompareOrdersAsThePersistedMapDoes() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final Rnd rnd = new Rnd(42, 7);
            final long left = Unsafe.malloc(MAX_KEY_LENGTH, MemoryTag.NATIVE_DEFAULT);
            final long right = Unsafe.malloc(MAX_KEY_LENGTH, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int round = 0; round < 20_000; round++) {
                    final byte[] a = randomKey(rnd);
                    final byte[] b = relatedKey(rnd, a);
                    copy(a, left);
                    copy(b, right);
                    final int expected = (int) COMPARE_BYTES.invoke(null, a, b);
                    Assert.assertEquals(describe(a, b), expected, (int) COMPARE_NATIVE.invoke(null, left, a.length, right, b.length));
                    Assert.assertEquals(describe(b, a), -expected, (int) COMPARE_NATIVE.invoke(null, right, b.length, left, a.length));
                }
            } finally {
                Unsafe.free(left, MAX_KEY_LENGTH, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(right, MAX_KEY_LENGTH, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testEqualsAtEveryChunkBoundary() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final long left = Unsafe.malloc(MAX_KEY_LENGTH, MemoryTag.NATIVE_DEFAULT);
            final long right = Unsafe.malloc(MAX_KEY_LENGTH, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int length = 0; length <= 40; length++) {
                    final byte[] key = new byte[length];
                    for (int i = 0; i < length; i++) {
                        key[i] = (byte) (0x80 | i);
                    }
                    copy(key, left);
                    copy(key, right);
                    Assert.assertTrue((boolean) EQUALS_NATIVE.invoke(null, left, length, right, length));
                    if (length > 0) {
                        Assert.assertFalse("a shorter prefix is not equal", (boolean) EQUALS_NATIVE.invoke(null, left, length, right, length - 1));
                    }
                    // A difference at any position, in a full chunk or in the tail, is seen.
                    for (int p = 0; p < length; p++) {
                        final byte[] other = key.clone();
                        other[p] ^= 1;
                        copy(other, right);
                        Assert.assertFalse("[length=" + length + ", at=" + p + ']', (boolean) EQUALS_NATIVE.invoke(null, left, length, right, length));
                    }
                }
            } finally {
                Unsafe.free(left, MAX_KEY_LENGTH, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(right, MAX_KEY_LENGTH, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testHashIsTheHeapArrayHash() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final Rnd rnd = new Rnd(42, 7);
            final long address = Unsafe.malloc(MAX_KEY_LENGTH, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int round = 0; round < 5_000; round++) {
                    final byte[] key = randomKey(rnd);
                    copy(key, address);
                    Assert.assertEquals(Arrays.hashCode(key), (int) HASH_CODE.invoke(null, address, key.length));
                    Assert.assertEquals(Hash.spread(Arrays.hashCode(key)), (int) HASH.invoke(null, address, key.length));
                }
            } finally {
                Unsafe.free(address, MAX_KEY_LENGTH, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    private static Method accessible(Method method) {
        method.setAccessible(true);
        return method;
    }

    private static void copy(byte[] key, long address) {
        for (int i = 0; i < key.length; i++) {
            Unsafe.putByte(address + i, key[i]);
        }
    }

    private static String describe(byte[] left, byte[] right) {
        return Arrays.toString(left) + " vs " + Arrays.toString(right);
    }

    // Lengths 0 to 300, with a bias toward the few bytes either side of each eight-byte chunk
    // boundary, and bytes that are half high-bit and half near zero.
    private static byte[] randomKey(Rnd rnd) {
        final int length = rnd.nextBoolean()
                ? rnd.nextInt(MAX_KEY_LENGTH + 1)
                : Math.max(0, Math.min(MAX_KEY_LENGTH, 8 * rnd.nextInt(MAX_KEY_LENGTH / 8 + 1) + rnd.nextInt(3) - 1));
        final byte[] key = new byte[length];
        for (int i = 0; i < length; i++) {
            key[i] = randomByte(rnd);
        }
        return key;
    }

    private static byte randomByte(Rnd rnd) {
        return (byte) (rnd.nextBoolean() ? 0x80 | rnd.nextInt(128) : rnd.nextInt(3));
    }

    // A key that shares a prefix of any length with key: equal, a prefix of it, an extension of
    // it, or one byte different at a random position; otherwise an unrelated key.
    private static byte[] relatedKey(Rnd rnd, byte[] key) {
        return switch (rnd.nextInt(5)) {
            case 0 -> key.clone();
            case 1 -> Arrays.copyOf(key, key.length == 0 ? 0 : rnd.nextInt(key.length));
            case 2 -> {
                final byte[] longer = Arrays.copyOf(key, Math.min(MAX_KEY_LENGTH, key.length + 1 + rnd.nextInt(9)));
                for (int i = key.length; i < longer.length; i++) {
                    longer[i] = randomByte(rnd);
                }
                yield longer;
            }
            case 3 -> {
                if (key.length == 0) {
                    yield key.clone();
                }
                final byte[] other = key.clone();
                final int at = rnd.nextInt(key.length);
                other[at] = (byte) (other[at] ^ (rnd.nextBoolean() ? 0x80 : 1 + rnd.nextInt(127)));
                yield other;
            }
            default -> randomKey(rnd);
        };
    }
}
