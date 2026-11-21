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


import io.questdb.cairo.TableToken;
import io.questdb.std.Chars;
import io.questdb.std.LowerCaseCharSequenceObjHashMap;
import io.questdb.std.Rnd;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LowerCaseCharSequenceObjHashMapTest {
    private static final String[] KEYS = {"a", "B", "c", "D", "e"};

    @Test
    public void testKeyIndexWithLowerCaseHashCode() {
        final LowerCaseCharSequenceObjHashMap<Integer> map = new LowerCaseCharSequenceObjHashMap<>();
        for (int i = 0; i < 100; i++) {
            map.put("Key_" + i, i);
        }
        final TableToken token = new TableToken("KEY_42", "key_42~1", null, 1, false, false, false);
        Assert.assertEquals(Chars.lowerCaseHashCode("KEY_42"), token.getTableNameLowerCaseHashCode());
        Assert.assertEquals(Integer.valueOf(42), map.valueAt(map.keyIndex(token.getTableName(), token.getTableNameLowerCaseHashCode())));
        // present and absent keys, in any case, land where keyIndex(key) lands
        for (int i = 0; i < 200; i++) {
            for (String key : new String[]{"key_" + i, "KEY_" + i, "Key_" + i}) {
                Assert.assertEquals(key, map.keyIndex(key), map.keyIndex(key, Chars.lowerCaseHashCode(key)));
            }
        }
    }

    @Test
    public void testPutIfAbsent() {
        final LowerCaseCharSequenceObjHashMap<Integer> lowerCaseMap = new LowerCaseCharSequenceObjHashMap<>();
        lowerCaseMap.put("a", 1);

        lowerCaseMap.putIfAbsent("a", 1);
        Assert.assertEquals(1, lowerCaseMap.keys().size());

        lowerCaseMap.putIfAbsent("b", 2);
        Assert.assertEquals(2, lowerCaseMap.keys().size());
    }

    @Test
    public void testRemoveAtQuick() {
        final LowerCaseCharSequenceObjHashMap<Integer> map = new LowerCaseCharSequenceObjHashMap<>();
        final String first = "first";
        final String middle = "Middle";
        final String last = "last";
        map.put(first, 1);
        map.put(middle, 2);
        map.put(last, 3);

        // the key index of a key spelled in another case
        map.removeAtQuick(map.keyIndex("mIDDLE"), 1);

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
        LowerCaseCharSequenceObjHashMap<Integer> map = newMap();
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
        final int sharedSlot = new LowerCaseCharSequenceObjHashMap<Integer>().keyIndex(head);

        final LowerCaseCharSequenceObjHashMap<Integer> map = new LowerCaseCharSequenceObjHashMap<>();
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

    @Test
    public void testSaturation() {

        final int N = 10_000;
        final Rnd rnd = new Rnd();
        final LowerCaseCharSequenceObjHashMap<Integer> lowerCaseMap = new LowerCaseCharSequenceObjHashMap<>();
        final HashMap<String, Integer> referenceMap = new HashMap<>();
        for (int i = 0; i < N; i++) {
            String str = rnd.nextString(4);
            int value = rnd.nextInt();

            int keyIndex = lowerCaseMap.keyIndex(str, 0, str.length());
            if (lowerCaseMap.put(str, value)) {
                Assert.assertNull("at " + i, referenceMap.put(str, value));
                Assert.assertTrue(keyIndex > -1);
            } else {
                Assert.assertTrue(keyIndex < 0);
                // this should fail to put
                lowerCaseMap.putIfAbsent(str, value);
                lowerCaseMap.putAt(keyIndex, str, referenceMap.get(str));
            }
        }

        // verify
        for (Map.Entry<String, Integer> e : referenceMap.entrySet()) {
            Assert.assertTrue(lowerCaseMap.contains(e.getKey()));
            Assert.assertFalse(lowerCaseMap.excludes(e.getKey()));
            Assert.assertFalse(lowerCaseMap.excludes(e.getKey(), 0, e.getKey().length()));

            int keyIndex = lowerCaseMap.keyIndex(e.getKey());
            Assert.assertTrue(keyIndex < 0);
            Assert.assertEquals(e.getKey(), e.getValue(), lowerCaseMap.valueAt(keyIndex));

            keyIndex = lowerCaseMap.keyIndex(e.getKey(), 0, e.getKey().length());
            Assert.assertEquals(e.getValue(), lowerCaseMap.valueAt(keyIndex));
        }

        for (int i = 0, n = lowerCaseMap.getKeyCount(); i < n; i++) {
            CharSequence v = lowerCaseMap.getKey(i);
            if (v != null) {
                Assert.assertTrue(referenceMap.containsKey(v.toString()));
            }
        }

        // remove every forth key
        // make sure rnd generates the same keys again
        rnd.reset();

        for (int i = 0; i < N; i++) {
            String s = rnd.nextString(4);
            if (i % 4 == 0) {
                lowerCaseMap.remove(s);
                referenceMap.remove(s);
            }
        }

        // verify
        for (Map.Entry<String, Integer> e : referenceMap.entrySet()) {
            Assert.assertTrue(e.getKey(), lowerCaseMap.contains(e.getKey()));
        }

        for (int i = 0, n = lowerCaseMap.getKeyCount(); i < n; i++) {
            CharSequence v = lowerCaseMap.getKey(i);
            if (v != null) {
                Assert.assertTrue(referenceMap.containsKey(v.toString()));
            }
        }
    }

    // expectedKeys are in lower case, KEYS holds some of them in upper case
    private static void assertMap(LowerCaseCharSequenceObjHashMap<Integer> map, String... expectedKeys) {
        Assert.assertEquals(expectedKeys.length, map.size());
        Assert.assertEquals(expectedKeys.length, map.keys().size());
        for (int i = 0; i < expectedKeys.length; i++) {
            int ordinal = expectedKeys[i].charAt(0) - 'a';
            // the map keeps the key instance it was given, at the expected list position
            Assert.assertSame(KEYS[ordinal], map.keys().getQuick(i));
            Assert.assertEquals(ordinal + 1, (int) map.get(map.keys().getQuick(i)));
            Assert.assertEquals(ordinal + 1, (int) map.get(expectedKeys[i]));
            Assert.assertEquals(ordinal + 1, (int) map.get(expectedKeys[i].toUpperCase()));
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
        final LowerCaseCharSequenceObjHashMap<Integer> emptyMap = new LowerCaseCharSequenceObjHashMap<>();
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

    private static LowerCaseCharSequenceObjHashMap<Integer> newMap() {
        LowerCaseCharSequenceObjHashMap<Integer> map = new LowerCaseCharSequenceObjHashMap<>();
        for (int i = 0; i < KEYS.length; i++) {
            map.put(KEYS[i], i + 1);
        }
        return map;
    }

    private static void removeAtQuick(LowerCaseCharSequenceObjHashMap<Integer> map, int listIndex) {
        CharSequence key = map.keys().getQuick(listIndex);
        map.removeAtQuick(map.keyIndex(key), listIndex);
        Assert.assertNull(map.get(key));
    }

    private static int slotOf(LowerCaseCharSequenceObjHashMap<Integer> map, CharSequence key) {
        int keyIndex = map.keyIndex(key);
        Assert.assertTrue(keyIndex < 0);
        return -keyIndex - 1;
    }
}