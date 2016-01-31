/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2016. The NFSdb project and its contributors.
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

package com.nfsdb.ql.collections;

import com.nfsdb.ql.impl.join.asof.FreeList;
import org.junit.Assert;
import org.junit.Test;

public class FreeListTest {

    @Test
    public void testFindAndRemove() throws Exception {
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
    public void testSimple() throws Exception {
        FreeList list = new FreeList();

        list.add(100000L, 11);
        list.add(200000L, 7);
        list.add(300000L, 14);
        list.add(400000L, 15);
        list.add(500000L, 10);

        Assert.assertEquals("FreeList{sizes=[7,10,11,14,15], offsets=[200000,500000,100000,300000,400000], maxSize=15}", list.toString());
    }
}
