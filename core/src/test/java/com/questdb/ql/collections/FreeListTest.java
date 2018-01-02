/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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

package com.questdb.ql.collections;

import com.questdb.ql.join.asof.FreeList;
import org.junit.Assert;
import org.junit.Test;

public class FreeListTest {

    @Test
    public void testFindAndRemove() {
        FreeList list = new FreeList();

        list.add(100000L, 11);
        list.add(200000L, 7);
        list.add(300000L, 13);
        list.add(400000L, 15);
        list.add(500000L, 10);

        Assert.assertEquals(-1L, list.findAndRemove(20));
        Assert.assertEquals("FreeList{sizes=[7,10,11,13,15], offsets=[200000,500000,100000,300000,400000], maxSize=15}", list.toString());

        Assert.assertEquals(500000L, list.findAndRemove(10));
        Assert.assertEquals("FreeList{sizes=[7,11,13,15], offsets=[200000,100000,300000,400000], maxSize=15}", list.toString());

        Assert.assertEquals(200000L, list.findAndRemove(6));
        Assert.assertEquals("FreeList{sizes=[11,13,15], offsets=[100000,300000,400000], maxSize=15}", list.toString());

        Assert.assertEquals(100000L, list.findAndRemove(10));
        Assert.assertEquals("FreeList{sizes=[13,15], offsets=[300000,400000], maxSize=15}", list.toString());

        Assert.assertEquals(400000L, list.findAndRemove(15));
        Assert.assertEquals(-1, list.findAndRemove(14));
        Assert.assertEquals(300000L, list.findAndRemove(13));

        Assert.assertEquals(-1, list.findAndRemove(15));
        Assert.assertEquals("FreeList{sizes=[], offsets=[], maxSize=13}", list.toString());

        Assert.assertEquals(-1, list.findAndRemove(10));
        Assert.assertEquals("FreeList{sizes=[], offsets=[], maxSize=-1}", list.toString());

        list.add(13000L, 20);

        Assert.assertEquals(13000L, list.findAndRemove(15));
        Assert.assertEquals("FreeList{sizes=[], offsets=[], maxSize=20}", list.toString());
    }

    @Test
    public void testSimple() {
        FreeList list = new FreeList();

        list.add(100000L, 11);
        list.add(200000L, 7);
        list.add(300000L, 14);
        list.add(400000L, 15);
        list.add(500000L, 10);

        Assert.assertEquals("FreeList{sizes=[7,10,11,14,15], offsets=[200000,500000,100000,300000,400000], maxSize=15}", list.toString());
    }
}
