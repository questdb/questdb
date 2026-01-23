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


import io.questdb.std.CharSequenceBoolHashMap;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import org.junit.Test;

import static org.junit.Assert.*;

public class CharSequenceBoolHashMapTest {

    @Test
    public void testAll() {

        Rnd rnd = new Rnd();
        // populate map
        CharSequenceBoolHashMap map = new CharSequenceBoolHashMap();
        final int N = 1000;
        for (int i = 0; i < N; i++) {
            CharSequence cs = rnd.nextChars(15);
            boolean b = map.put(cs, rnd.nextBoolean());
            assertTrue(b);
        }
        assertEquals(N, map.size());

        rnd.reset();

        // assert that map contains the values we just added
        for (int i = 0; i < N; i++) {
            CharSequence cs = rnd.nextChars(15);
            assertFalse(map.excludes(cs));
            assertEquals(rnd.nextBoolean(), map.get(cs));
        }

        Rnd rnd2 = new Rnd();

        rnd.reset();

        // remove some keys and assert that the size() complies
        int removed = 0;
        for (int i = 0; i < N; i++) {
            CharSequence cs = rnd.nextChars(15);
            rnd.nextBoolean();
            if (rnd2.nextPositiveInt() % 16 == 0) {
                assertTrue(map.remove(cs) > -1);
                removed++;
                assertEquals(N - removed, map.size());
            }
        }

        // if we didn't remove anything test has no value
        assertTrue(removed > 0);

        rnd2.reset();
        rnd.reset();

        Rnd rnd3 = new Rnd();

        // assert that keys we didn't remove are still there and
        // keys we removed are not
        for (int i = 0; i < N; i++) {
            CharSequence cs = rnd.nextChars(15);
            boolean value = rnd.nextBoolean();
            if (rnd2.nextPositiveInt() % 16 == 0) {
                assertTrue(map.excludes(cs));
            } else {
                assertFalse(map.excludes(cs));

                int index = map.keyIndex(cs);
                assertEquals(value, map.valueAt(index));

                // update value
                map.putAt(index, cs, rnd3.nextBoolean());
            }
        }
    }

    @Test
    public void testEquals() {
        CharSequenceBoolHashMap map1 = new CharSequenceBoolHashMap();
        map1.put("a", false);

        assertNotEquals(null, map1);
        assertNotEquals(map1, new Object());
        assertEquals(map1, map1);

        CharSequenceBoolHashMap map2 = new CharSequenceBoolHashMap();

        assertNotEquals(map1, map2);

        map2.put("a", true);
        assertNotEquals(map1, map2);
    }

    @Test
    public void testKeys() {
        CharSequenceBoolHashMap map = new CharSequenceBoolHashMap();
        map.put("a", false);
        map.put("b", true);

        assertEquals(map.keys(), new ObjList<>("a", "b"));
    }

    @Test
    public void testPutAll() {
        CharSequenceBoolHashMap map1 = new CharSequenceBoolHashMap();
        CharSequenceBoolHashMap map2 = new CharSequenceBoolHashMap();
        map2.put("a", false);
        map1.putAll(map2);

        assertEquals(map1, map2);
    }

    @Test
    public void testPutIfAbsent() {
        CharSequenceBoolHashMap map = new CharSequenceBoolHashMap();
        map.putIfAbsent("a", false);
        map.putIfAbsent("a", false);

        assertFalse(map.valueQuick(0));
    }

    @Test
    public void testRemoveAt() {
        CharSequenceBoolHashMap map = new CharSequenceBoolHashMap();
        map.put("a", false);
        map.put("b", true);
        map.put("c", true);

        map.removeAt(map.keyIndex("b"));

        CharSequenceBoolHashMap expected = new CharSequenceBoolHashMap();
        expected.put("a", false);
        expected.put("c", true);

        assertEquals(map, expected);
    }
}
