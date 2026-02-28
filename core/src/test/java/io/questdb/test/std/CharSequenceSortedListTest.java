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

import io.questdb.std.CharSequenceSortedList;
import io.questdb.std.Misc;
import io.questdb.std.ReadOnlyObjList;
import io.questdb.std.str.Utf16Sink;
import org.junit.Test;

import static org.junit.Assert.*;

public class CharSequenceSortedListTest {

    @Test
    public void testOrderAndSize() {
        CharSequenceSortedList listUnderTest = listUnderTest();

        assertEquals("Finance", listUnderTest.get(0));
        assertEquals("Knightsbridge", listUnderTest.get(1));
        assertEquals("Osbourne", listUnderTest.get(2));
        assertEquals("Yardstick", listUnderTest.get(3));
        assertEquals("alpinist", listUnderTest.get(4));
        assertEquals("hunter", listUnderTest.get(5));
        assertEquals("nothingness", listUnderTest.get(6));
        assertEquals("sierra", listUnderTest.get(7));
        assertEquals("theodora", listUnderTest.get(8));
        assertEquals("zebra", listUnderTest.get(9));
        assertEquals(10, listUnderTest.size());
    }

    public CharSequenceSortedList listUnderTest() {
        CharSequenceSortedList list = new CharSequenceSortedList(4);

        list.add("zebra");
        list.add("Yardstick");
        list.add("theodora");
        list.add("sierra");
        list.add("Osbourne");
        list.add("nothingness");
        list.add("Knightsbridge");
        list.add("hunter");
        list.add("Finance");
        list.add("alpinist");

        return list;
    }

    @Test
    public void testRemovalForUnder66Entries() {
        CharSequenceSortedList listUnderTest = listUnderTest();

        listUnderTest.remove("Osbourne");
        listUnderTest.removeIndex(4);
        listUnderTest.removeIndex(0);
        listUnderTest.remove("Knightsbridge");
        listUnderTest.removeIndex(5);
        listUnderTest.remove("theodora");
        listUnderTest.remove("Z");

        assertEquals("Yardstick", listUnderTest.get(0));
        assertEquals("alpinist", listUnderTest.get(1));
        assertEquals("nothingness", listUnderTest.get(2));
        assertEquals("sierra", listUnderTest.get(3));
        assertEquals(4, listUnderTest.size());
    }

    @Test
    public void testRemovalForOver65Entries() {
        CharSequenceSortedList listUnderTest = new CharSequenceSortedList(677);

        for (int cu = 'A'; cu < 'Z' + 1; cu++) {
            for (int cl = 'a'; cl < 'z' + 1; cl++) {
                listUnderTest.add(new String(new char[]{(char) cu, (char) cl}));
            }
        }

        listUnderTest.remove("Aa");
        listUnderTest.removeIndex(0);
        listUnderTest.remove("Bb");
        listUnderTest.remove("Ccc");
        listUnderTest.removeIndex(100);
        listUnderTest.remove("Zz");
        listUnderTest.removeIndex(669);

        int index = 0;
        for (int cu = 'A'; cu < 'Z' + 1; cu++) {
            for (int cl = 'a'; cl < 'z' + 1; cl++) {
                if (index != 100 && (!(cu == 'A' && cl == 'a') && !(cu == 'A' && cl == 'b') && !(cu == 'B' && cl == 'b') && !(cu == 'Z' && cl == 'z'))) {
                    assertEquals(new String(new char[]{(char) cu, (char) cl}), listUnderTest.get(index++));
                }
            }
        }

        assertEquals(670, listUnderTest.size());
    }

    @Test
    public void testClearance() {
        CharSequenceSortedList listUnderTest = listUnderTest();

        listUnderTest.clear();

        assertEquals(0, listUnderTest.size());
    }

    @Test
    public void testEquals() {
        CharSequenceSortedList listUnderTest1 = listUnderTest();
        CharSequenceSortedList listUnderTest2 = listUnderTest();

        assertEquals(listUnderTest1, listUnderTest2);
        listUnderTest2.removeIndex(5);
        assertNotEquals(listUnderTest1, listUnderTest2);
        assertNotEquals(null, listUnderTest1);
        assertNotEquals(new CharSequenceSortedList(), listUnderTest1);
    }

    @Test
    public void testHashCode() {
        CharSequenceSortedList listUnderTest1 = listUnderTest();
        CharSequenceSortedList listUnderTest2 = listUnderTest();
        CharSequenceSortedList listUnderTest3 = listUnderTest();

        assertEquals(listUnderTest1.hashCode(), listUnderTest2.hashCode());
        assertEquals(listUnderTest2.hashCode(), listUnderTest1.hashCode());
        assertEquals(listUnderTest3.hashCode(), listUnderTest1.hashCode());
        assertEquals(listUnderTest3.hashCode(), listUnderTest2.hashCode());
        listUnderTest2.removeIndex(5);
        assertNotEquals(listUnderTest1.hashCode(), listUnderTest2.hashCode());
        assertNotEquals(new CharSequenceSortedList().hashCode(), listUnderTest1.hashCode());
    }

    @Test
    public void testToString() {
        CharSequenceSortedList listUnderTest = listUnderTest();

        assertEquals("[Finance,Knightsbridge,Osbourne,Yardstick,alpinist,hunter,nothingness,sierra,theodora,zebra]", listUnderTest.toString());
    }

    @Test
    public void testToSink() {
        CharSequenceSortedList listUnderTest = listUnderTest();
        Utf16Sink b = Misc.getThreadLocalSink();
        listUnderTest.toSink(b);

        assertEquals("[Finance,Knightsbridge,Osbourne,Yardstick,alpinist,hunter,nothingness,sierra,theodora,zebra]", b.toString());
    }

    @Test
    public void testNonEmpty() {
        CharSequenceSortedList listUnderTestNotEmpty = listUnderTest();
        CharSequenceSortedList listUnderTestEmpty = new CharSequenceSortedList();
        listUnderTestEmpty.add("value");
        listUnderTestEmpty.remove("value");
        listUnderTestEmpty.add("SecondValue");
        listUnderTestEmpty.removeIndex(0);

        assertTrue(listUnderTestNotEmpty.size() > 0);
        assertFalse(listUnderTestEmpty.size() > 0);
    }

    @Test
    public void testGetAtIndex0OnEmptyList() {
        CharSequenceSortedList list = new CharSequenceSortedList();

        var e = assertThrows(ArrayIndexOutOfBoundsException.class, () -> list.get(0));
        assertEquals("Array index out of range: 0", e.getMessage());
    }

    @Test
    public void testRemoveAtIndexEdgeCases() {
        CharSequenceSortedList list = new CharSequenceSortedList();
        list.removeIndex(0);
        CharSequenceSortedList secondList = new CharSequenceSortedList();
        secondList.removeIndex(-1);
        secondList.add("value");
        secondList.removeIndex(-1);
        secondList.removeIndex(1);

        assertEquals(0, list.size());
        assertEquals(1, secondList.size());
    }

    @Test
    public void testRemoveNonExistentElementOnEmptyList() {
        CharSequenceSortedList list = new CharSequenceSortedList();
        list.remove("sample");

        assertEquals(0, list.size());
    }

    @Test
    public void testDuplicatesNotAllowed() {
        CharSequenceSortedList list = new CharSequenceSortedList();

        list.add("aaa");
        list.add("bbb");
        list.add("ccc");
        list.add("aaa");

        assertEquals(3, list.size());
    }

    @Test
    public void testCopy() {
        CharSequenceSortedList listUnderTest = listUnderTest();

        ReadOnlyObjList listCopy = listUnderTest.copy();

        assertEquals(listCopy, listUnderTest);
    }

    @Test
    public void testGetLast() {
        CharSequenceSortedList listUnderTest = listUnderTest();

        assertEquals("zebra", listUnderTest.getLast());
        assertNull(new CharSequenceSortedList().getLast());
    }

    @Test
    public void testGetQuick() {
        CharSequenceSortedList list = new CharSequenceSortedList();

        var e = assertThrows(AssertionError.class, () -> list.getQuick(1));
        assertEquals("index out of bounds, 1 >= 0", e.getMessage());
        assertEquals("zebra", listUnderTest().get(9));
    }

    @Test
    public void testGetQuiet() {
        CharSequenceSortedList list = listUnderTest();

        assertEquals("zebra", list.getQuiet(9));
        assertNull(list.getQuiet(10));
    }

    @Test
    public void testIndexOf() {
        CharSequenceSortedList list = listUnderTest();

        assertEquals(0, list.indexOf("Finance"));
        assertEquals(9, list.indexOf("zebra"));
        assertEquals(-1, list.indexOf("test"));
    }
}
