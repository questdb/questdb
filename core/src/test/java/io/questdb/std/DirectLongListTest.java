/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.std;

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import org.junit.Assert;
import org.junit.Test;

public class DirectLongListTest {

    private static final Log LOG = LogFactory.getLog(DirectLongListTest.class);

    @Test
    public void testResizeMemLeak() {
        // use logger so that static memory allocation happens before our control measurement
        LOG.info().$("testResizeMemLeak").$();
        long expected = Unsafe.getMemUsed();
        try (DirectLongList list = new DirectLongList(1024)) {
            for (int i = 0; i < 1_000_000; i++) {
                list.add(i);
            }
        }
        Assert.assertEquals(expected, Unsafe.getMemUsed());
    }

    @Test
    public void testCapacityAndSize() {
        // use logger so that static memory allocation happens before our control measurement
        LOG.info().$("testCapacityAndSize").$();
        long expected = Unsafe.getMemUsed();
        DirectLongList list = new DirectLongList(1024);
        Assert.assertEquals(1024, list.getCapacity());
        list.extend(2048);
        Assert.assertEquals(2048, list.getCapacity());
        Assert.assertEquals(0, list.size());
        long addr = list.getAddress();
        Unsafe.getUnsafe().putLong(addr, 42);
        Assert.assertEquals(42, list.get(0));
        for (long i = 0; i < list.getCapacity(); ++i) {
            list.add(i);
        }
        for (long i = 0; i < list.size(); ++i) {
            Assert.assertEquals(i, list.get(i));
        }
        list.clear(0);
        Assert.assertEquals(0, list.size());
        for (long i = 0; i < list.getCapacity(); ++i) {
            Assert.assertEquals(0, list.get(i));
        }
        list.setPos(42);
        Assert.assertEquals(42, list.size());
        list.clear();
        Assert.assertEquals(0, list.size());
        list.close(); //release memory
        Assert.assertEquals(expected, Unsafe.getMemUsed());
    }

    @Test
    public void testAddList() {
        DirectLongList list = new DirectLongList(256);
        DirectLongList list2 = new DirectLongList(256);
        final int N = 100;
        for (int i = 0; i < N; ++i) {
            list.add(i);
            list2.add(N + i);
        }
        list.add(list2);
        Assert.assertEquals(256, list.getCapacity());
        Assert.assertEquals(2*N, list.size());
        for (long i = 0; i < list.size(); ++i) {
            Assert.assertEquals(i, list.get(i));
        }
        list.close();
        list2.close();
    }

    @Test
    public void testAddListExpand() {
        DirectLongList list = new DirectLongList(128);
        DirectLongList list2 = new DirectLongList(128);
        final int N = 100;
        for (int i = 0; i < N; ++i) {
            list.add(i);
            list2.add(N + i);
        }
        list.add(list2);
        Assert.assertEquals(200, list.getCapacity()); //128 + 100 - 28
        Assert.assertEquals(2*N, list.size());
        for (long i = 0; i < list.size(); ++i) {
            Assert.assertEquals(i, list.get(i));
        }
        list.close();
        list2.close();
    }
    @Test
    public void testSearch() {
        DirectLongList list = new DirectLongList(256);
        final int N = 100;
        for (int i = 0; i < N; ++i) {
            list.add(i);
        }
        Assert.assertEquals(N/2, list.scanSearch(N/2, 0, list.size()));
        Assert.assertEquals(N/2, list.binarySearch(N/2));
        list.close();
    }
}