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

import io.questdb.std.DirectLongList;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Method;

/**
 * Pins the order the seal's predecessor lookups walk in: {@code sortKeyOrdinals} must
 * produce the unsigned byte order a partition map lays its leaves out in, for every key
 * width, whichever arm - the native radix sort over packed longs, or the comparison sort
 * a wider key falls back to - computes it.
 */
public class LiveViewCheckpointKeyOrdinalSortTest {

    @Test
    public void testEightByteKeysWithEitherSignBitSortInTreeOrder() throws Exception {
        final Rnd rnd = new Rnd();
        final ObjList<byte[]> keys = new ObjList<>();
        for (int i = 0; i < 20_000; i++) {
            final byte[] key = new byte[8];
            // The first component carries either sign bit, including the NULL id, so the
            // packed long spans both halves of the signed range.
            final int first = (i & 3) == 0 ? Integer.MIN_VALUE + rnd.nextInt(1000) : rnd.nextInt(1_000_000);
            putInt(key, 0, first);
            putInt(key, 4, rnd.nextInt(16));
            keys.add(key);
        }
        assertTreeOrder(keys);
    }

    @Test
    public void testEmptyAndSingleKey() throws Exception {
        assertTreeOrder(new ObjList<>());
        final ObjList<byte[]> one = new ObjList<>();
        final byte[] key = new byte[4];
        putInt(key, 0, 42);
        one.add(key);
        assertTreeOrder(one);
    }

    @Test
    public void testFourByteKeysIncludingNullSortInTreeOrder() throws Exception {
        final Rnd rnd = new Rnd();
        final ObjList<byte[]> keys = new ObjList<>();
        for (int i = 0; i < 100_000; i++) {
            final byte[] key = new byte[4];
            // Dense ids in hash-slot order, plus the NULL key, which leads with the sign
            // byte and so sorts after every id however early it arrived.
            putInt(key, 0, i == 500 ? Integer.MIN_VALUE : rnd.nextInt(3_000_000));
            keys.add(key);
        }
        assertTreeOrder(keys);
    }

    @Test
    public void testWideKeysFallBackToComparisonSort() throws Exception {
        final Rnd rnd = new Rnd();
        final ObjList<byte[]> keys = new ObjList<>();
        for (int i = 0; i < 5_000; i++) {
            final byte[] key = new byte[12];
            putInt(key, 0, rnd.nextInt(100));
            putInt(key, 4, rnd.nextInt());
            putInt(key, 8, rnd.nextInt());
            keys.add(key);
        }
        assertTreeOrder(keys);
    }

    private static void assertTreeOrder(ObjList<byte[]> keys) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final IntList ordinals = new IntList();
            try (DirectLongList scratch = new DirectLongList(16, MemoryTag.NATIVE_DEFAULT, true)) {
                sortKeyOrdinals(keys, scratch, ordinals);
            }
            final int n = keys.size();
            Assert.assertEquals(n, ordinals.size());
            final boolean[] seen = new boolean[n];
            for (int i = 0; i < n; i++) {
                final int ordinal = ordinals.getQuick(i);
                Assert.assertFalse("ordinal repeated: " + ordinal, seen[ordinal]);
                seen[ordinal] = true;
                if (i > 0) {
                    final int cmp = compareBytes(keys.getQuick(ordinals.getQuick(i - 1)), keys.getQuick(ordinal));
                    Assert.assertTrue("keys out of tree order at position " + i, cmp <= 0);
                }
            }
        });
    }

    private static int compareBytes(byte[] left, byte[] right) {
        final int n = Math.min(left.length, right.length);
        for (int i = 0; i < n; i++) {
            final int a = left[i] & 0xff;
            final int b = right[i] & 0xff;
            if (a != b) {
                return a < b ? -1 : 1;
            }
        }
        return Integer.compare(left.length, right.length);
    }

    private static void putInt(byte[] key, int offset, int value) {
        key[offset] = (byte) (value >>> 24);
        key[offset + 1] = (byte) (value >>> 16);
        key[offset + 2] = (byte) (value >>> 8);
        key[offset + 3] = (byte) value;
    }

    private static void sortKeyOrdinals(ObjList<byte[]> keys, DirectLongList scratch, IntList ordinals) throws Exception {
        final Class<?> metadata = Class.forName("io.questdb.cairo.lv.LiveViewCheckpointMetadata");
        final Method method = metadata.getDeclaredMethod("sortKeyOrdinals", ObjList.class, DirectLongList.class, IntList.class);
        method.setAccessible(true);
        method.invoke(null, keys, scratch, ordinals);
    }
}
