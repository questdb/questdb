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

import io.questdb.std.CharSequenceObjSortedHashMap;
import io.questdb.std.ReadOnlyObjList;
import io.questdb.std.Rnd;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

public class CharSequenceObjSortedHashMapTest {

    @Test
    public void testContains() {
        CharSequenceObjSortedHashMap<CharSequence> map = new CharSequenceObjSortedHashMap<>();
        map.put("EGG", "CHICKEN");
        assertTrue(map.contains("EGG"));
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
            final CharSequenceObjSortedHashMap<Object> map = new CharSequenceObjSortedHashMap<>(8);
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

    private void assertKeys(CharSequenceObjSortedHashMap<Object> map, ReadOnlyObjList<CharSequence> keys) {
        for (int j = 0, n = keys.size(); j < n; j++) {
            Assert.assertNotNull(map.get(keys.get(j)));
        }
    }

    public CharSequenceObjSortedHashMap<String> sortedMapUnderTest() {
        CharSequenceObjSortedHashMap<String> map = new CharSequenceObjSortedHashMap<>(16);

        map.put("zebra", "zebraValue");
        map.put("Yardstick", "YardstickValue");
        map.put("theodora", "theodoraValue");
        map.put("sierra", "sierraValue");
        map.put("Osbourne", "OsbourneValue");
        map.put("nothingness", "nothingnessValue");
        map.put("Knightsbridge", "KnightsbridgeValue");
        map.put("hunter", "hunterValue");
        map.put("Finance", "FinanceValue");
        map.put("alpinist", "alpinistValue");
        map.put("whiskey", "whiskeyValue");
        map.put("Xylophone", "XylophoneValue");
        map.put("velocity", "velocityValue");
        map.put("Umbrella", "UmbrellaValue");
        map.put("quicksilver", "quicksilverValue");
        map.put("Prestige", "PrestigeValue");
        map.put("legionnaire", "legionnaireValue");

        return map;
    }

    @Test
    public void testRemovalEntriesAndOrderAndSize() {
        CharSequenceObjSortedHashMap<String> sortedMapUnderTest = sortedMapUnderTest();

        sortedMapUnderTest.remove("Osbourne");
        sortedMapUnderTest.remove("hunter");
        sortedMapUnderTest.remove("Finance");
        sortedMapUnderTest.remove("Knightsbridge");
        sortedMapUnderTest.remove("theodora");
        sortedMapUnderTest.remove("Z");

        CharSequenceObjSortedHashMap<String> mapToUpdate = new CharSequenceObjSortedHashMap<>(16);
        mapToUpdate.putAll(sortedMapUnderTest);

        verifyAllElementsPresentAndOrdered(sortedMapUnderTest.keys(), sortedMapUnderTest);
        verifyAllElementsPresentAndOrdered(mapToUpdate.keys(), mapToUpdate);
    }

    private static void verifyAllElementsPresentAndOrdered(ReadOnlyObjList<CharSequence> listUnderTest, CharSequenceObjSortedHashMap<String> sortedMapUnderTest) {
        assertEquals("Prestige", listUnderTest.get(0));
        assertEquals("Umbrella", listUnderTest.get(1));
        assertEquals("Xylophone", listUnderTest.get(2));
        assertEquals("Yardstick", listUnderTest.get(3));
        assertEquals("alpinist", listUnderTest.get(4));
        assertEquals("legionnaire", listUnderTest.get(5));
        assertEquals("nothingness", listUnderTest.get(6));
        assertEquals("quicksilver", listUnderTest.get(7));
        assertEquals("sierra", listUnderTest.get(8));
        assertEquals("velocity", listUnderTest.get(9));
        assertEquals("whiskey", listUnderTest.get(10));
        assertEquals("zebra", listUnderTest.get(11));
        assertEquals(12, listUnderTest.size());
        assertEquals("PrestigeValue", sortedMapUnderTest.get("Prestige"));
        assertEquals("PrestigeValue", sortedMapUnderTest.getAt(0));
        assertEquals("UmbrellaValue", sortedMapUnderTest.get("Umbrella"));
        assertEquals("UmbrellaValue", sortedMapUnderTest.getAt(1));
        assertEquals("XylophoneValue", sortedMapUnderTest.get("Xylophone"));
        assertEquals("XylophoneValue", sortedMapUnderTest.getAt(2));
        assertEquals("YardstickValue", sortedMapUnderTest.get("Yardstick"));
        assertEquals("YardstickValue", sortedMapUnderTest.getAt(3));
        assertEquals("alpinistValue", sortedMapUnderTest.get("alpinist"));
        assertEquals("alpinistValue", sortedMapUnderTest.getAt(4));
        assertEquals("legionnaireValue", sortedMapUnderTest.get("legionnaire"));
        assertEquals("legionnaireValue", sortedMapUnderTest.getAt(5));
        assertEquals("nothingnessValue", sortedMapUnderTest.get("nothingness"));
        assertEquals("nothingnessValue", sortedMapUnderTest.getAt(6));
        assertEquals("quicksilverValue", sortedMapUnderTest.get("quicksilver"));
        assertEquals("quicksilverValue", sortedMapUnderTest.getAt(7));
        assertEquals("sierraValue", sortedMapUnderTest.get("sierra"));
        assertEquals("sierraValue", sortedMapUnderTest.getAt(8));
        assertEquals("velocityValue", sortedMapUnderTest.get("velocity"));
        assertEquals("velocityValue", sortedMapUnderTest.getAt(9));
        assertEquals("whiskeyValue", sortedMapUnderTest.get("whiskey"));
        assertEquals("whiskeyValue", sortedMapUnderTest.getAt(10));
        assertEquals("zebraValue", sortedMapUnderTest.get("zebra"));
        assertEquals("zebraValue", sortedMapUnderTest.getAt(11));
        assertEquals(12, sortedMapUnderTest.size());
    }

    @Test
    public void testPutAtWithNegativeIndexDoesNotInsert() {
        CharSequenceObjSortedHashMap<String> sortedMapUnderTest = sortedMapUnderTest();

        int hunterIndex = sortedMapUnderTest.keyIndex("hunter");
        boolean putResult = sortedMapUnderTest.put("hunter", "hunterValue2");

        assertFalse(putResult);
        assertTrue(hunterIndex < 0);
        assertEquals("hunterValue2", sortedMapUnderTest.get("hunter"));
    }

    @Test
    public void testValueQuickReturnsValueByIndex() {
        CharSequenceObjSortedHashMap<String> sortedMapUnderTest = sortedMapUnderTest();
        assertEquals("XylophoneValue", sortedMapUnderTest.valueQuick(5));

        var e = assertThrows(ArrayIndexOutOfBoundsException.class, () -> sortedMapUnderTest.valueQuick(500));
        assertEquals("Array index out of range: 500", e.getMessage());
    }

    @Test
    public void testClearWorks() {
        CharSequenceObjSortedHashMap<String> sortedMapUnderTest = sortedMapUnderTest();
        sortedMapUnderTest.clear();

        assertEquals(0, sortedMapUnderTest.size());
        assertNull(sortedMapUnderTest.get("a"));
    }

    @Test
    public void testValueAtOnlyWorksWithNegativeIndices() {
        CharSequenceObjSortedHashMap<String> sortedMapUnderTest = sortedMapUnderTest();

        assertNull(sortedMapUnderTest.valueAt(0));
    }

    @Test
    public void testEmptyMapsMethodsWorkAsExpected() {
        CharSequenceObjSortedHashMap<String> sortedMapUnderTest = new CharSequenceObjSortedHashMap<>();

        assertEquals(0, sortedMapUnderTest.size());
        assertNull(sortedMapUnderTest.get("a"));
        assertFalse(sortedMapUnderTest.contains("a"));
    }

    @Test
    public void testGetAtThrowsExceptionWhenOutOfBounds() {
        CharSequenceObjSortedHashMap<String> sortedMapUnderTest = new CharSequenceObjSortedHashMap<>();

        var e = assertThrows(ArrayIndexOutOfBoundsException.class, () -> sortedMapUnderTest.getAt(1));
        assertEquals("Array index out of range: 1", e.getMessage());
    }
}
