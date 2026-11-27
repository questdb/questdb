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
import io.questdb.test.tools.TestUtils;
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
        // a name with letters outside ASCII that change case hashes as the name spelled in lower case, which a hash
        // that folds only ASCII letters gets wrong; U+0130 and U+212A, the dotted capital I and the Kelvin sign, have
        // the ASCII 'i' and 'k' as their lower case
        final TableToken nonAsciiToken = new TableToken("ÉŁΩЖ_\u0130\u212A", "éłωж_ik~2", null, 2, false, false, false);
        Assert.assertEquals(Chars.lowerCaseHashCode("éłωж_ik"), nonAsciiToken.getTableNameLowerCaseHashCode());
        map.put("éłωж_ik", 100);
        Assert.assertEquals(Integer.valueOf(100), map.valueAt(map.keyIndex(nonAsciiToken.getTableName(), nonAsciiToken.getTableNameLowerCaseHashCode())));
        // present and absent keys, in any case, land where keyIndex(key) lands
        for (int i = 0; i < 200; i++) {
            for (String key : new String[]{"key_" + i, "KEY_" + i, "Key_" + i}) {
                Assert.assertEquals(key, map.keyIndex(key), map.keyIndex(key, Chars.lowerCaseHashCode(key)));
            }
        }
    }

    @Test
    public void testKeyIndexWithLowerCaseHashCodeMatchesCharacterToLowerCase() {
        final LowerCaseCharSequenceObjHashMap<Integer> map = new LowerCaseCharSequenceObjHashMap<>();
        TestUtils.forEachCaseVariantPair(
                new Rnd(42, 7),
                (mapKey, key, isEqual) -> assertKeyIndexWithLowerCaseHashCode(map, mapKey, key, isEqual)
        );
    }

    @Test
    public void testKeyIndexWithLowerCaseHashCodeSkipsHashing() {
        // the keys count the chars a lookup reads; hashing a key would read every one of them
        final String name = "Some_Table_Name";
        final int hashCode = Chars.lowerCaseHashCode(name);
        final LowerCaseCharSequenceObjHashMap<Integer> map = new LowerCaseCharSequenceObjHashMap<>();

        // absent from an empty map: the lookup reads nothing of the key
        CountingCharSequence key = new CountingCharSequence(name);
        Assert.assertTrue(map.keyIndex(key, hashCode) > -1);
        Assert.assertEquals(0, key.charAtCount);
        Assert.assertEquals(0, key.lengthCount);

        // absent from a map of shorter keys: comparing keys of different length reads no char
        for (int i = 0; i < 1_000; i++) {
            map.put("k" + i, i);
        }
        key = new CountingCharSequence(name);
        Assert.assertTrue(map.keyIndex(key, hashCode) > -1);
        Assert.assertEquals(0, key.charAtCount);

        // present, spelled in another case: only the comparison reads the chars, each once
        map.put("SOME_table_NAME", 42);
        key = new CountingCharSequence(name);
        Assert.assertEquals(Integer.valueOf(42), map.valueAt(map.keyIndex(key, hashCode)));
        Assert.assertEquals(name.length(), key.charAtCount);
    }

    @Test
    public void testKeyIndexWithLowerCaseHashCodeWrapsAround() {
        // keys whose slot is the last one: the second key goes to the first slot, past the end of the table
        final LowerCaseCharSequenceObjHashMap<Integer> map = new LowerCaseCharSequenceObjHashMap<>();
        final int lastSlot = map.getKeyCount() - 1;
        final List<String> keys = findKeysWithSlot(lastSlot, 3);
        map.put(keys.get(0), 0);
        map.put(keys.get(1), 1);
        Assert.assertEquals(lastSlot, slotOf(map, keys.get(0)));
        Assert.assertEquals(0, slotOf(map, keys.get(1)));

        for (String key : new String[]{keys.get(0), keys.get(0).toUpperCase()}) {
            Assert.assertEquals(-lastSlot - 1, map.keyIndex(key, Chars.lowerCaseHashCode(key)));
        }
        for (String key : new String[]{keys.get(1), keys.get(1).toUpperCase()}) {
            Assert.assertEquals(-1, map.keyIndex(key, Chars.lowerCaseHashCode(key)));
            Assert.assertEquals(Integer.valueOf(1), map.valueAt(map.keyIndex(key, Chars.lowerCaseHashCode(key))));
        }
        // absent, with the same slot: the lookup walks past both keys, to the first empty slot
        Assert.assertEquals(1, map.keyIndex(keys.get(2), Chars.lowerCaseHashCode(keys.get(2))));
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

    // Looks key up in a map that holds only mapKey, from the slot of mapKey, so that the lookup compares the two keys
    // whether they are equal or not, and checks the result against isEqual, which compares the chars through
    // Character.toLowerCase(). Two Strings take the loop of keyIndex(CharSequence, int), any other pair takes
    // Chars.equalsIgnoreCase().
    private static void assertKeyIndexWithLowerCaseHashCode(LowerCaseCharSequenceObjHashMap<Integer> map, String mapKey, String key, boolean isEqual) {
        final int hashCode = Chars.lowerCaseHashCode(mapKey);
        final int mask = map.getKeyCount() - 1;
        final int slot = hashCode & mask;
        // a key that is equal is found in the slot of mapKey, any other is absent, with the empty slot after it
        final int expectedKeyIndex = isEqual ? -slot - 1 : (slot + 1) & mask;
        final String message = "mapKey=" + mapKey + ", key=" + key;
        for (CharSequence storedKey : new CharSequence[]{mapKey, new StringBuilder(mapKey)}) {
            map.clear();
            map.put(storedKey, 1);
            Assert.assertEquals(message, expectedKeyIndex, map.keyIndex(key, hashCode));
            Assert.assertEquals(message, expectedKeyIndex, map.keyIndex(new StringBuilder(key), hashCode));
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

    private static List<String> findKeysWithSlot(int slot, int keyCount) {
        // keyIndex() on an empty map returns the key's ideal slot
        final LowerCaseCharSequenceObjHashMap<Integer> emptyMap = new LowerCaseCharSequenceObjHashMap<>();
        final List<String> keys = new ArrayList<>();
        for (int i = 0; i < 10_000; i++) {
            String key = "key" + i;
            if (emptyMap.keyIndex(key) == slot) {
                keys.add(key);
                if (keys.size() == keyCount) {
                    return keys;
                }
            }
        }
        throw new AssertionError("could not find " + keyCount + " keys with slot " + slot);
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

    // Not a String, so that a test can count the chars a lookup reads.
    private static class CountingCharSequence implements CharSequence {
        private final String value;
        private int charAtCount;
        private int lengthCount;

        private CountingCharSequence(String value) {
            this.value = value;
        }

        @Override
        public char charAt(int index) {
            charAtCount++;
            return value.charAt(index);
        }

        @Override
        public int length() {
            lengthCount++;
            return value.length();
        }

        @Override
        public CharSequence subSequence(int start, int end) {
            return value.subSequence(start, end);
        }
    }
}