/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package io.questdb;

import io.questdb.std.*;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class CollectionsTest {

    @Rule
    public final TemporaryFolder temp = new TemporaryFolder();

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
    public void testLongSearch2() {
        LongList list = new LongList();
        Rnd rnd = new Rnd();

        for (int i = 7; i < 2000; i += 10) {
            list.add(i + (rnd.nextPositiveInt() & 9));
        }
        Assert.assertEquals(18, list.binarySearch(188));
        Assert.assertEquals(-1, list.binarySearch(6));
        Assert.assertEquals(-25, list.binarySearch(240));
        Assert.assertEquals(-201, list.binarySearch(2010));
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
