/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (C) 2014-2016 Appsicle
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
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 *
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
