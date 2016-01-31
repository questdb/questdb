/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package com.nfsdb;

import com.nfsdb.misc.Rnd;
import com.nfsdb.std.*;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class CollectionsTest {

    @Rule
    public final TemporaryFolder temp = new TemporaryFolder();

    @Test
    public void testIntHash() throws Exception {
        IntHashSet set = new IntHashSet(10);

        for (int i = 0; i < 1000; i++) {
            Assert.assertTrue(set.add(i));
        }

        for (int i = 0; i < 1000; i++) {
            Assert.assertFalse(set.add(i));
        }
    }

    @Test
    public void testIntList() throws Exception {
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
    public void testIntObjHashMap() throws Exception {
        IntObjHashMap<String> map = new IntObjHashMap<>();
        Rnd rnd = new Rnd();

        for (int i = 0; i < 1000; i++) {
            map.put(i, rnd.nextString(25));
        }

        Assert.assertEquals(1000, map.size());

        Rnd rnd2 = new Rnd();
        for (int i = 0; i < 1000; i++) {
            Assert.assertEquals(rnd2.nextString(25), map.get(i));
        }
    }

    @Test
    public void testLongSearch2() throws Exception {
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
    public void testObjIntHashMap() throws Exception {
        ObjIntHashMap<String> map = new ObjIntHashMap<>();
        Rnd rnd = new Rnd();

        for (int i = 0; i < 1000; i++) {
            map.put(rnd.nextString(25), i);
        }

        Rnd rnd2 = new Rnd();
        for (int i = 0; i < 1000; i++) {
            Assert.assertEquals(i, map.get(rnd2.nextString(25)));
        }

        Assert.assertEquals(1000, map.size());

        Assert.assertTrue(map.putIfAbsent("ABC", 100));
        Assert.assertFalse(map.putIfAbsent("ABC", 100));
    }
}
