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

import io.questdb.std.BoolList;
import io.questdb.std.Rnd;
import org.junit.Test;

import static org.junit.Assert.*;

public class BoolListTest {
    private final Rnd rnd = new Rnd();

    @Test
    public void testAddAllAndEquals() {
        final int length = 500;
        final BoolList list1 = new BoolList();
        final BoolList list2 = new BoolList(128);

        for (int i = 0; i < length; i++) {
            list1.add(rnd.nextBoolean());
        }
        list2.addAll(list1);

        for (int i = 0; i < length; i++) {
            assertEquals(list1.get(i), list2.get(i));
        }
        assertEquals(list1, list2);

        list2.add(true);
        assertNotEquals(list1, list2);
        list1.add(true);
        assertEquals(list1, list2);

        list2.add(true);
        assertNotEquals(list1, list2);
        list1.add(false);
        assertNotEquals(list1, list2);
    }

    @Test
    public void testAddAndGet() {
        final int length = 1000;
        final boolean[] values = new boolean[length];
        for (int i = 0; i < values.length; i++) {
            values[i] = rnd.nextBoolean();
        }

        final BoolList list = new BoolList();
        for (int i = 0; i < values.length; i++) {
            list.add(values[i]);
        }

        for (int i = 0; i < values.length; i++) {
            assertEquals(values[i], list.get(i));
        }
        assertEquals(length, list.size());

        assertThrows(AssertionError.class, () -> list.get(length + 100));

        final int pos = 106;
        list.set(pos, true);
        assertTrue(list.getQuiet(pos));
        assertFalse(list.getQuiet(length + 100));
    }

    @Test
    public void testArrayCopy() {
        final int length = 1000;
        final boolean[] values = new boolean[length];
        for (int i = 0; i < values.length; i++) {
            values[i] = rnd.nextBoolean();
        }

        final BoolList list = new BoolList();
        for (int i = 0; i < values.length; i++) {
            list.add(values[i]);
        }
        assertEquals(length, list.size());

        final int src = 259;
        final int dst = 706;
        final int len = 250;
        list.arrayCopy(src, dst, len);

        for (int i = 0; i < dst; i++) {
            assertEquals(values[i], list.get(i));
        }
        for (int i = 0; i < len; i++) {
            assertEquals(values[src + i], list.get(dst + i));
        }
        for (int i = 0; i < length - dst - len; i++) {
            assertEquals(values[dst + len + i], list.get(dst + len + i));
        }
        assertEquals(length, list.size());
    }

    @Test
    public void testClear() {
        final int length = 100;
        final BoolList list = new BoolList(32);
        assertEquals(0, list.size());

        list.setAll(length, true);
        assertEquals(length, list.size());

        list.clear();
        assertEquals(0, list.size());
        assertThrows(AssertionError.class, () -> list.get(10));

        list.setAll(length, true);
        assertEquals(length, list.size());

        list.clear(length);
        assertEquals(0, list.size());
        assertThrows(AssertionError.class, () -> list.get(10));
    }

    @Test
    public void testEnsureCapacityAndSize() {
        final int length = 100;
        final BoolList list = new BoolList(32);
        assertEquals(0, list.size());

        for (int i = 0; i < length; i++) {
            list.add(rnd.nextBoolean());
        }
        assertEquals(length, list.size());
    }

    @Test
    public void testExtendAndSet() {
        final int length = 1000;
        final BoolList list = new BoolList(length);

        final int pos = 100;
        list.extendAndSet(pos, true);
        for (int i = 0; i < pos + 1; i++) {
            assertEquals(i == pos, list.get(i));
        }
        assertEquals(pos + 1, list.size());

        list.set(pos, false);

        final int pos2 = 3000;
        list.extendAndSet(pos2, true);
        for (int i = 0; i < pos2 + 1; i++) {
            assertEquals(i == pos2, list.get(i));
        }
        assertEquals(pos2 + 1, list.size());

        list.setAll(pos, false);
        list.extendAndSet(pos2 + 1, true);
        assertFalse(list.get(pos2));
        assertEquals(pos2 + 2, list.size());
    }

    @Test
    public void testGetLast() {
        final int length = 1000;
        final BoolList list = new BoolList(length);
        list.setPos(length);

        assertFalse(list.getLast());

        list.set(length - 1, true);
        assertTrue(list.getLast());

        list.setPos(0);
        assertFalse(list.getLast());
    }

    @Test
    public void testInsert() {
        final int length = 1000;
        final BoolList list = new BoolList();
        list.setAll(length, false);
        assertEquals(length, list.size());

        final int pos = 259;
        list.insert(pos, true);

        for (int i = 0; i < pos; i++) {
            assertFalse(list.get(i));
        }
        assertTrue(list.get(pos));
        for (int i = pos + 1; i < length + 1; i++) {
            assertFalse(list.get(i));
        }
        assertEquals(length + 1, list.size());
    }

    @Test
    public void testRemoveIndex() {
        final int length = 1000;
        final boolean[] values = new boolean[length];
        for (int i = 0; i < values.length; i++) {
            values[i] = rnd.nextBoolean();
        }

        final BoolList list = new BoolList();
        for (int i = 0; i < values.length; i++) {
            list.add(values[i]);
        }
        assertEquals(length, list.size());

        final int pos = 259;
        list.removeIndex(pos);

        for (int i = 0; i < pos; i++) {
            assertEquals(values[i], list.get(i));
        }
        for (int i = pos; i < length - 1; i++) {
            assertEquals(values[i + 1], list.get(i));
        }
        assertEquals(length - 1, list.size());
    }

    @Test
    public void testReplace() {
        final int length = 1000;
        final BoolList list = new BoolList();
        list.setAll(length, true);

        final int pos = 15;
        assertTrue(list.replace(pos, false));
        assertFalse(list.replace(pos, true));
        assertTrue(list.replace(pos, true));
        assertThrows(AssertionError.class, () -> list.replace(1000, false));

        final int pos2 = 6003;
        assertFalse(list.extendAndReplace(length, true));
        assertFalse(list.extendAndReplace(pos2, true));
        assertTrue(list.replace(pos2, true));
        assertThrows(AssertionError.class, () -> list.replace(pos2 + 1, false));

        list.setAll(pos2 - 700, false);
        assertEquals(pos2 - 700, list.size());
        assertFalse(list.extendAndReplace(pos2, true));
        assertEquals(pos2 + 1, list.size());
    }

    @Test
    public void testSetAll() {
        final int length = 100;
        final BoolList list = new BoolList(length);

        list.setAll(length, true);
        for (int i = 0; i < length; i++) {
            assertTrue(list.get(i));
        }

        final int pos = length - 10;
        list.setPos(pos);
        list.setAll(length, false);
        for (int i = 0; i < length; i++) {
            assertFalse(list.get(i));
        }
    }

    @Test
    public void testSetAndSetQuick() {
        final int length = 1000;
        final BoolList list = new BoolList();
        list.setAll(length, false);
        for (int i = 0; i < length; i++) {
            assertFalse(list.get(i));
        }

        final int pos = 10;
        list.set(pos, true);
        for (int i = 0; i < length; i++) {
            assertEquals(i == pos, list.get(i));
        }
        assertEquals(length, list.size());

        final int posQuick = 100;
        list.set(pos, false);
        list.setQuick(posQuick, true);
        for (int i = 0; i < length; i++) {
            assertEquals(i == posQuick, list.get(i));
        }
        assertEquals(length, list.size());

        assertThrows(ArrayIndexOutOfBoundsException.class, () -> list.set(1000, true));
        assertThrows(ArrayIndexOutOfBoundsException.class, () -> list.set(1001, true));
        assertThrows(AssertionError.class, () -> list.setQuick(1000, true));
        assertThrows(AssertionError.class, () -> list.setQuick(1001, true));
    }

    @Test
    public void testToString() {
        final int length = 5;
        final boolean[] values = {true, false, true, true, false};
        final BoolList list = new BoolList();

        for (int i = 0; i < length; i++) {
            list.add(values[i]);
        }

        assertEquals("[true,false,true,true,false]", list.toString());
    }

    @Test
    public void testZeroAndPosition() {
        final int length = 100;
        final BoolList list = new BoolList();
        list.setAll(length, true);

        list.zero(false);
        for (int i = 0; i < length; i++) {
            assertFalse(list.get(i));
        }

        list.setPos(length);
        list.zero(true);
        for (int i = 0; i < length; i++) {
            assertTrue(list.get(i));
        }

        final int pos = length - 10;
        list.setPos(pos);
        list.zero(false);
        list.setPos(length);
        for (int i = 0; i < pos; i++) {
            assertFalse(list.get(i));
        }
        for (int i = pos; i < length; i++) {
            assertTrue(list.get(i));
        }
    }
}