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

package io.questdb.test.std;

import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.ReadOnlyObjList;
import io.questdb.std.Rnd;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CharSequenceObjHashMapTest {
    private static final String[] KEYS = {"a", "b", "c", "d", "e"};

    @Test
    public void testContains() {
        CharSequenceObjHashMap<CharSequence> map = new CharSequenceObjHashMap<>();
        map.put("EGG", "CHICKEN");
        Assert.assertTrue(map.contains("EGG"));
        Assert.assertFalse(map.contains("CHICKEN"));
    }

    @Test
    public void testRemove() {
        // these are specific keys that have the same hash code value when map capacity is 8
        String[] collisionKeys = {
                "SHRUE",
                "JCTIZ",
                "FLSVI",
                "CJBEV",
                "XFSUW",
                "JIGFI",
                "RZUPV",
                "BHLNE",
                "WRSLB",
                "NVZHC",
                "KUNRD",
                "QELQD",
                "CVBNE",
                "ZMFYL",
                "XVBHB"
        };

        Rnd rnd = new Rnd();
        for (int i = 0; i < 10_000; i++) {
            final CharSequenceObjHashMap<Object> map = new CharSequenceObjHashMap<>(8);
            final ReadOnlyObjList<CharSequence> keys = map.keys();
            for (int k = 0; k < collisionKeys.length; k++) {
                // all four of these keys collide give the size of the hash map
                map.put(collisionKeys[k], new Object());
            }

            CharSequence v = collisionKeys[rnd.nextInt(collisionKeys.length)];
            map.remove(v);
            assertKeys(map, keys);
            map.put(v, new Object());
            assertKeys(map, keys);
        }
    }

    @Test
    public void testRemoveAtQuick() {
        final CharSequenceObjHashMap<Integer> map = new CharSequenceObjHashMap<>();
        final String first = "first";
        final String middle = "middle";
        final String last = "last";
        map.put(first, 1);
        map.put(middle, 2);
        map.put(last, 3);

        map.removeAtQuick(map.keyIndex(middle), 1);

        Assert.assertEquals(2, map.size());
        Assert.assertEquals(-1, map.remove(middle));
        Assert.assertSame(first, map.keys().getQuick(0));
        Assert.assertSame(last, map.keys().getQuick(1));
        Assert.assertEquals(Integer.valueOf(1), map.get(first));
        Assert.assertEquals(Integer.valueOf(3), map.get(last));
        Assert.assertTrue(map.keys().hasOnlyNullsBeyondSizeForTesting());
    }

    @Test
    public void testRemoveAtQuickMaintainsMapAndKeyListConsistency() {
        CharSequenceObjHashMap<Integer> map = newMap();
        removeAtQuick(map, 0);
        assertMap(map, "e", "b", "c", "d");

        map = newMap();
        removeAtQuick(map, 2);
        assertMap(map, "a", "b", "e", "d");

        // next-to-last index: the swap-in key is the one right behind it
        map = newMap();
        removeAtQuick(map, 3);
        assertMap(map, "a", "b", "c", "e");

        // last index: popLast() returns the key being removed, so setQuick() must not run
        map = newMap();
        removeAtQuick(map, 4);
        assertMap(map, "a", "b", "c", "d");

        map = newMap();
        removeAtQuick(map, 1);
        assertMap(map, "a", "e", "c", "d");
        removeAtQuick(map, 1);
        assertMap(map, "a", "d", "c");

        // drain from the head, down to the single-element map and to empty
        map = newMap();
        removeAtQuick(map, 0);
        assertMap(map, "e", "b", "c", "d");
        removeAtQuick(map, 0);
        assertMap(map, "d", "b", "c");
        removeAtQuick(map, 0);
        assertMap(map, "c", "b");
        removeAtQuick(map, 0);
        assertMap(map, "b");
        removeAtQuick(map, 0);
        assertMap(map);

        // drain from the tail, so every removal takes the last-index branch
        map = newMap();
        removeAtQuick(map, 4);
        assertMap(map, "a", "b", "c", "d");
        removeAtQuick(map, 3);
        assertMap(map, "a", "b", "c");
        removeAtQuick(map, 2);
        assertMap(map, "a", "b");
        removeAtQuick(map, 1);
        assertMap(map, "a");
        removeAtQuick(map, 0);
        assertMap(map);
    }

    @Test
    public void testRemoveAtQuickRelocatesCollidingKeys() {
        final List<String> collidingKeys = findCollidingKeys(3);
        final String head = collidingKeys.get(0);
        final String middle = collidingKeys.get(1);
        final String tail = collidingKeys.get(2);
        final int sharedSlot = new CharSequenceObjHashMap<Integer>().keyIndex(head);

        final CharSequenceObjHashMap<Integer> map = new CharSequenceObjHashMap<>();
        map.put(head, 1);
        map.put(middle, 2);
        map.put(tail, 3);

        // the chain: only the first key sits in the slot all three hash to
        Assert.assertEquals(sharedSlot, slotOf(map, head));
        Assert.assertNotEquals(sharedSlot, slotOf(map, middle));
        Assert.assertNotEquals(sharedSlot, slotOf(map, tail));

        map.removeAtQuick(map.keyIndex(head), 0);

        // freeing the slot makes removeAt()'s relocation loop pull the chain back
        Assert.assertEquals(sharedSlot, slotOf(map, middle));
        Assert.assertEquals(2, map.size());
        Assert.assertNull(map.get(head));
        Assert.assertEquals(Integer.valueOf(2), map.get(middle));
        Assert.assertEquals(Integer.valueOf(3), map.get(tail));
        Assert.assertSame(tail, map.keys().getQuick(0));
        Assert.assertSame(middle, map.keys().getQuick(1));
        Assert.assertTrue(map.keys().hasOnlyNullsBeyondSizeForTesting());
    }

    private static void assertMap(CharSequenceObjHashMap<Integer> map, String... expectedKeys) {
        Assert.assertEquals(expectedKeys.length, map.size());
        Assert.assertEquals(expectedKeys.length, map.keys().size());
        for (int i = 0; i < expectedKeys.length; i++) {
            int ordinal = expectedKeys[i].charAt(0) - 'a';
            // the map keeps the key instance it was given, at the expected list position
            Assert.assertSame(KEYS[ordinal], map.keys().getQuick(i));
            Assert.assertEquals(ordinal + 1, (int) map.valueQuick(i));
            Assert.assertEquals(ordinal + 1, (int) map.get(KEYS[ordinal]));
        }
        for (int ordinal = 0; ordinal < KEYS.length; ordinal++) {
            boolean isExpected = false;
            for (String expectedKey : expectedKeys) {
                if (expectedKey.charAt(0) - 'a' == ordinal) {
                    isExpected = true;
                    break;
                }
            }
            if (!isExpected) {
                Assert.assertNull(map.get(KEYS[ordinal]));
                Assert.assertEquals(-1, map.remove(KEYS[ordinal]));
            }
        }
        Assert.assertTrue(map.keys().hasOnlyNullsBeyondSizeForTesting());
    }

    private static List<String> findCollidingKeys(int keyCount) {
        // keyIndex() on an empty map returns the key's ideal slot, so keys sharing
        // that index are the ones that chain in the open-addressed table
        final CharSequenceObjHashMap<Integer> emptyMap = new CharSequenceObjHashMap<>();
        final Map<Integer, List<String>> keysBySlot = new HashMap<>();
        for (int i = 0; i < 10_000; i++) {
            String key = "key" + i;
            List<String> keys = keysBySlot.computeIfAbsent(emptyMap.keyIndex(key), slot -> new ArrayList<>());
            keys.add(key);
            if (keys.size() == keyCount) {
                return keys;
            }
        }
        throw new AssertionError("could not find " + keyCount + " colliding keys");
    }

    private static CharSequenceObjHashMap<Integer> newMap() {
        CharSequenceObjHashMap<Integer> map = new CharSequenceObjHashMap<>();
        for (int i = 0; i < KEYS.length; i++) {
            map.put(KEYS[i], i + 1);
        }
        return map;
    }

    private static void removeAtQuick(CharSequenceObjHashMap<Integer> map, int listIndex) {
        CharSequence key = map.keys().getQuick(listIndex);
        map.removeAtQuick(map.keyIndex(key), listIndex);
        Assert.assertNull(map.get(key));
    }

    private static int slotOf(CharSequenceObjHashMap<Integer> map, CharSequence key) {
        int keyIndex = map.keyIndex(key);
        Assert.assertTrue(keyIndex < 0);
        return -keyIndex - 1;
    }

    private void assertKeys(CharSequenceObjHashMap<Object> map, ReadOnlyObjList<CharSequence> keys) {
        for (int j = 0, n = keys.size(); j < n; j++) {
            Assert.assertNotNull(map.get(keys.getQuick(j)));
        }
    }
}
