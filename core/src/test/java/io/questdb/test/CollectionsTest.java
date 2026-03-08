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

package io.questdb.test;

import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.IntObjHashMap;
import io.questdb.std.LongList;
import io.questdb.std.ObjIntHashMap;
import io.questdb.std.Rnd;
import io.questdb.std.Vect;
import org.junit.Assert;
import org.junit.Test;

public class CollectionsTest {

    @Test
    public void testIntHash() {
        IntHashSet set = new IntHashSet(10);

        for (int i = 0; i < 1000; i++) {
            Assert.assertTrue(set.add(i));
        }

        for (int i = 0; i < 1000; i++) {
            Assert.assertFalse(set.add(i));
        }
    }

    @Test
    public void testIntList() {
        IntList list = new IntList();
        final int N = 1000;
        for (int i = 0; i < N; i++) {
            list.add(N - i);
        }

        Assert.assertEquals(N, list.size());

        for (int i = 0; i < N; i++) {
            Assert.assertEquals(N - i, list.get(i));
        }

        // add small list that would not need resizing of target
        IntList list2 = new IntList();
        list2.add(1001);
        list2.add(2001);

        list.addAll(list2);


        Assert.assertEquals(N + 2, list.size());
        Assert.assertEquals(1001, list.get(N));
        Assert.assertEquals(2001, list.get(N + 1));


        IntList list3 = new IntList();
        for (int i = 0; i < N; i++) {
            list3.add(i + 5000);
        }

        list.addAll(list3);
        Assert.assertEquals(2 * N + 2, list.size());
    }

    @Test
    public void testIntObjHashMap() {
        IntObjHashMap<String> map = new IntObjHashMap<>();
        Rnd rnd = new Rnd();

        for (int i = 0; i < 1000; i++) {
            map.put(i, rnd.nextString(25));
        }

        Assert.assertEquals(1000, map.size());

        rnd.reset();
        for (int i = 0; i < 1000; i++) {
            Assert.assertEquals(rnd.nextString(25), map.get(i));
        }
    }

    @Test
    public void testLongBinaryBlockSearch() {
        LongList list = new LongList();

        for (int blockSizeHint = 0; blockSizeHint < 5; blockSizeHint++) {
            list.clear();
            Rnd rnd = new Rnd();

            for (int i = 7; i < 2000; i += 10) {
                list.add(i + (rnd.nextPositiveInt() & 9));
                int blockFullLen = 1 << blockSizeHint;

                for (int j = 1; j < blockFullLen; j++) {
                    list.add(0L);
                }
            }

            // Assert.assertEquals("Block hint " + blockSizeHint, 18 << blockSizeHint, list.binarySearchBlock(0, list.size(), blockSizeHint, 188));
            Assert.assertEquals("Block hint " + blockSizeHint, -1, list.binarySearchBlock(blockSizeHint, 6, Vect.BIN_SEARCH_SCAN_UP));
            Assert.assertEquals("Block hint " + blockSizeHint, -((24 << blockSizeHint) + 1), list.binarySearchBlock(blockSizeHint, 240, Vect.BIN_SEARCH_SCAN_UP));
            Assert.assertEquals("Block hint " + blockSizeHint, -((200 << blockSizeHint) + 1), list.binarySearchBlock(blockSizeHint, 2010, Vect.BIN_SEARCH_SCAN_UP));
        }
    }

    @Test
    public void testLongBinaryBlockSearch2() {
        LongList list = new LongList();
        list.add(10);
        list.add(0);
        list.add(0);
        list.add(0);
        list.add(30);
        list.add(0);
        list.add(0);
        list.add(0);
        Assert.assertEquals(-5, list.binarySearchBlock(2, 20, Vect.BIN_SEARCH_SCAN_DOWN));
    }

    @Test
    public void testLongSearch2() {
        LongList list = new LongList();
        Rnd rnd = new Rnd();

        for (int i = 7; i < 2000; i += 10) {
            list.add(i + (rnd.nextPositiveInt() & 9));
        }
        Assert.assertEquals(18, list.binarySearch(188, Vect.BIN_SEARCH_SCAN_UP));
        Assert.assertEquals(-1, list.binarySearch(6, Vect.BIN_SEARCH_SCAN_UP));
        Assert.assertEquals(-25, list.binarySearch(240, Vect.BIN_SEARCH_SCAN_UP));
        Assert.assertEquals(-201, list.binarySearch(2010, Vect.BIN_SEARCH_SCAN_UP));
    }

    @Test
    public void testLongSearch3() {
        final int N = 100;
        final LongList list = new LongList();

        for (int i = 0; i < N; i++) {
            list.add(2 * i);
        }

        for (int i = 0; i < N; i++) {
            Assert.assertEquals(i, list.binarySearch(2 * i, Vect.BIN_SEARCH_SCAN_UP));
            Assert.assertEquals(i, list.binarySearch(2 * i, Vect.BIN_SEARCH_SCAN_DOWN));
            Assert.assertEquals(-i - 2, list.binarySearch(2 * i + 1, Vect.BIN_SEARCH_SCAN_UP));
            Assert.assertEquals(-i - 2, list.binarySearch(2 * i + 1, Vect.BIN_SEARCH_SCAN_DOWN));
        }
    }

    @Test
    public void testObjIntHashMap() {
        ObjIntHashMap<String> map = new ObjIntHashMap<>();
        Rnd rnd = new Rnd();

        for (int i = 0; i < 1000; i++) {
            map.put(rnd.nextString(25), i);
        }

        rnd.reset();
        for (int i = 0; i < 1000; i++) {
            Assert.assertEquals(i, map.get(rnd.nextString(25)));
        }

        Assert.assertEquals(1000, map.size());

        Assert.assertTrue(map.putIfAbsent("ABC", 100));
        Assert.assertFalse(map.putIfAbsent("ABC", 100));
    }
}
