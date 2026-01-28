/*******************************************************************************
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

package io.questdb.test.std;

import io.questdb.std.DirectLongLongHashMap;
import io.questdb.std.Hash;
import io.questdb.std.MemoryTag;
import io.questdb.std.Rnd;
import org.junit.Assert;
import org.junit.Test;

public class DirectLongLongHashMapTest {
    private static final long NO_KEY = Long.MIN_VALUE;

    @Test
    public void testAll() {
        Rnd rnd = new Rnd();
        // populate map
        try (DirectLongLongHashMap map = new DirectLongLongHashMap(4, 0.5, Long.MIN_VALUE, Long.MIN_VALUE, MemoryTag.NATIVE_DEFAULT)) {
            map.put(Integer.MAX_VALUE, Long.MAX_VALUE);
            Assert.assertEquals(1, map.size());
            Assert.assertEquals(Long.MAX_VALUE, map.get(Integer.MAX_VALUE));
            map.clear();

            final int N = 1000;
            for (int i = 0; i < N; i++) {
                int value = i + 1;
                map.put(i, value);
            }
            Assert.assertEquals(N, map.size());

            rnd.reset();

            // assert that map contains the values we just added
            for (int i = 0; i < N; i++) {
                Assert.assertFalse(map.excludes(i));
                Assert.assertEquals(i + 1, map.get(i));
            }

            Rnd rnd2 = new Rnd();

            rnd.reset();

            rnd2.reset();
            rnd.reset();

            Rnd rnd3 = new Rnd();

            // assert that keys we didn't remove are still there and
            // keys we removed are not
            for (int i = 0; i < N; i++) {
                int value = rnd.nextInt();
                Assert.assertFalse(map.excludes(i));

                long index = map.keyIndex(i);
                Assert.assertEquals(i + 1, map.valueAt(index));

                // update value
                map.putAt(index, value, rnd3.nextInt());
            }

            // assert that update is visible correctly
            rnd3.reset();
            rnd2.reset();
            rnd.reset();

            // assert that keys we didn't remove are still there and
            // keys we removed are not
            for (int i = 0; i < N; i++) {
                Assert.assertFalse(map.excludes(i));
                Assert.assertEquals(rnd3.nextInt(), map.get(i));
            }

            map.restoreInitialCapacity();
            Assert.assertEquals(0, map.size());
            Assert.assertEquals(8, map.capacity());

            // test putIfAbsent
            for (int i = 0; i < N; i++) {
                int value = i + 1;
                Assert.assertTrue(map.putIfAbsent(i, value));
                Assert.assertFalse(map.putIfAbsent(i, value));
            }
            Assert.assertEquals(N, map.size());

            // assert that map contains the values we just added
            for (int i = 0; i < N; i++) {
                Assert.assertFalse(map.excludes(i));
                Assert.assertEquals(i + 1, map.get(i));
            }
        }
    }

    @Test
    public void testRemoveAt() {
        try (DirectLongLongHashMap map = new DirectLongLongHashMap(4, 0.5, -1, -1, MemoryTag.NATIVE_DEFAULT)) {
            final int N = 100;
            for (int i = 0; i < N; i++) {
                map.put(i, i + 1);
            }
            Assert.assertEquals(N, map.size());

            // Remove every other entry.
            for (int i = 0; i < N; i += 2) {
                long index = map.keyIndex(i);
                Assert.assertTrue(index < 0);
                map.removeAt(index);
            }
            Assert.assertEquals(N / 2, map.size());

            // Verify removed entries are gone and remaining entries are still there.
            for (int i = 0; i < N; i++) {
                if (i % 2 == 0) {
                    Assert.assertTrue(map.excludes(i));
                    Assert.assertEquals(-1, map.get(i));
                } else {
                    Assert.assertFalse(map.excludes(i));
                    Assert.assertEquals(i + 1, map.get(i));
                }
            }

            // Remove all remaining entries.
            for (int i = 1; i < N; i += 2) {
                long index = map.keyIndex(i);
                Assert.assertTrue(index < 0);
                map.removeAt(index);
            }
            Assert.assertEquals(0, map.size());

            // Verify all entries are gone.
            for (int i = 0; i < N; i++) {
                Assert.assertTrue(map.excludes(i));
            }
        }
    }

    @Test
    public void removeCompactsProbeChainWithCircularWraparound() {
        try (DirectLongLongHashMap map = new DirectLongLongHashMap(8, .999, NO_KEY, -1, MemoryTag.NATIVE_DEFAULT)) {
            long a0 = keyHashedToPos(map, 5, 0); // hashes to 5, fills [5] (ideal hit)
            long a1 = keyHashedToPos(map, 5, 1); // hashes to 5, fills [6] (displaced)
            long a2 = keyHashedToPos(map, 5, 2); // hashes to 5, fills [7] (displaced)
            long a3 = keyHashedToPos(map, 5, 3); // hashes to 5, fills [0] (displaced, wrapped)

            putAll(map, a0, a1, a2, a3);
            assertKeysOrder(map, new long[]{a3, NO_KEY, NO_KEY, NO_KEY, NO_KEY, a0, a1, a2});

            map.removeAt(map.keyIndex(a0));
            assertKeysOrder(map, new long[]{NO_KEY, NO_KEY, NO_KEY, NO_KEY, NO_KEY, a1, a2, a3});
        }
    }

    @Test
    public void removePreservesNonDisplacedElements() {
        try (DirectLongLongHashMap map = new DirectLongLongHashMap(8, .999, NO_KEY, -1, MemoryTag.NATIVE_DEFAULT)) {
            long a0 = keyHashedToPos(map, 4, 0); // hashes to 4, fills [4] (ideal hit)
            long b0 = keyHashedToPos(map, 5, 0); // hashes to 5, fills [5] (ideal hit)
            long c0 = keyHashedToPos(map, 6, 0); // hashes to 6, fills [6] (ideal hit)
            long b1 = keyHashedToPos(map, 5, 1); // hashes to 5, fills [7] (displaced)
            long b2 = keyHashedToPos(map, 5, 2); // hashes to 5, fills [0] (displaced, wrapped)

            putAll(map, a0, b0, c0, b1, b2);
            assertKeysOrder(map, new long[]{b2, NO_KEY, NO_KEY, NO_KEY, a0, b0, c0, b1});

            map.removeAt(map.keyIndex(b0));
            assertKeysOrder(map, new long[]{NO_KEY, NO_KEY, NO_KEY, NO_KEY, a0, b1, c0, b2});
            map.removeAt(map.keyIndex(a0));
            assertKeysOrder(map, new long[]{NO_KEY, NO_KEY, NO_KEY, NO_KEY, NO_KEY, b1, c0, b2});
        }
    }

    @Test
    public void removeHandlesWrappedTailAndMiddleDeletionsCorrectly() {
        try (DirectLongLongHashMap map = new DirectLongLongHashMap(8, .999, NO_KEY, -1, MemoryTag.NATIVE_DEFAULT)) {
            long a0 = keyHashedToPos(map, 5, 0); // hashes to 5, fills [5] (ideal hit)
            long b0 = keyHashedToPos(map, 6, 0); // hashes to 6, fills [6] (ideal hit)
            long c0 = keyHashedToPos(map, 7, 0); // hashes to 7, fills [7] (ideal hit)
            long c1 = keyHashedToPos(map, 7, 1); // hashes to 7, fills [0] (displaced, wrapped)
            long c2 = keyHashedToPos(map, 7, 2); // hashes to 7, fills [1] (displaced, wrapped)

            putAll(map, a0, b0, c0, c1, c2);
            assertKeysOrder(map, new long[]{c1, c2, NO_KEY, NO_KEY, NO_KEY, a0, b0, c0});

            map.removeAt(map.keyIndex(c1));
            assertKeysOrder(map, new long[]{c2, NO_KEY, NO_KEY, NO_KEY, NO_KEY, a0, b0, c0});

            map.removeAt(map.keyIndex(c0));
            assertKeysOrder(map, new long[]{NO_KEY, NO_KEY, NO_KEY, NO_KEY, NO_KEY, a0, b0, c2});
        }
    }

    @Test
    public void removeMovesElementOnlyWhenNewPositionIsCloserToIdeal() {
        try (DirectLongLongHashMap map = new DirectLongLongHashMap(8, .999, NO_KEY, -1, MemoryTag.NATIVE_DEFAULT)) {
            long a = keyHashedToPos(map, 0, 0); // hashes to 0, fills [0] (ideal hit)
            long b = keyHashedToPos(map, 1, 0); // hashes to 1, fills [1] (ideal hit)
            long c = keyHashedToPos(map, 1, 1); // hashes to 1, fills [2] (displaced)

            putAll(map, a, b, c);
            assertKeysOrder(map, new long[]{a, b, c, NO_KEY, NO_KEY, NO_KEY, NO_KEY, NO_KEY});

            map.removeAt(map.keyIndex(a));
            // Element C is displaced from its ideal position (1),
            // and after deleting A it shouldn’t move to the new gap,
            // because it is farther from the ideal position.
            assertKeysOrder(map, new long[]{NO_KEY, b, c, NO_KEY, NO_KEY, NO_KEY, NO_KEY, NO_KEY});
        }
    }

    @Test
    public void removeMovesElementOnlyWhenNewPositionIsCloserToIdealWithWrapAround() {
        try (DirectLongLongHashMap map = new DirectLongLongHashMap(8, .999, NO_KEY, -1, MemoryTag.NATIVE_DEFAULT)) {
            long a = keyHashedToPos(map, 0, 0); // hashes to 0, fills [0] (ideal hit)
            long b = keyHashedToPos(map, 1, 0); // hashes to 1, fills [1] (ideal hit)
            long c = keyHashedToPos(map, 0, 1); // hashes to 0, fills [2] (displaced)
            long d = keyHashedToPos(map, 6, 0); // hashes to 6, fills [6] (ideal hit)
            long e = keyHashedToPos(map, 7, 0); // hashes to 7, fills [7] (ideal hit)

            putAll(map, a, b, c, d, e);
            assertKeysOrder(map, new long[]{a, b, c, NO_KEY, NO_KEY, NO_KEY, d, e});

            map.removeAt(map.keyIndex(d));
            // Element C is displaced from its ideal position (0),
            // and after deleting D it shouldn’t move to the new gap,
            // because it is farther from the ideal position.
            assertKeysOrder(map, new long[]{a, b, c, NO_KEY, NO_KEY, NO_KEY, NO_KEY, e});
        }
    }

    private static void putAll(DirectLongLongHashMap map, long... keys) {
        for (long key : keys)
            map.put(key, key);
    }

    /**
     * @param overlaps - we can create different objects that hash to the same position in the map.
     *                   e.g., 0 - is the first object that hashes to the position, 1 - the second, etc.
     * @return a key that hashes to the specified position in the map.
     */
    private static long keyHashedToPos(DirectLongLongHashMap map, long pos, int overlaps) {
        int mapCapacity = map.capacity();
        long k = Hash.fastHashLong64(1) % mapCapacity;
        return (k * pos + (long) mapCapacity * overlaps);
    }

    private static void assertKeysOrder(DirectLongLongHashMap map, long[] expected) {
        long[] actual = new long[map.capacity()];
        for (int i = 0; i < map.capacity(); i++)
            actual[i] = map.keyAtRaw(i);
        Assert.assertArrayEquals(expected, actual);
    }
}
