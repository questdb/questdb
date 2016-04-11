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
