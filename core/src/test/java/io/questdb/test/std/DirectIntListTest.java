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

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.DirectIntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

public class DirectIntListTest {

    private static final Log LOG = LogFactory.getLog(DirectIntListTest.class);

    @Test
    public void testAddAll() {
        try (
                DirectIntList list = new DirectIntList(256, MemoryTag.NATIVE_DEFAULT);
                DirectIntList list2 = new DirectIntList(256, MemoryTag.NATIVE_DEFAULT)
        ) {
            final int N = 100;
            for (int i = 0; i < N; i++) {
                list.add(i);
                list2.add(N + i);
            }
            list.addAll(list2);
            Assert.assertEquals(256, list.getCapacity());
            Assert.assertEquals(2 * N, list.size());
            for (long i = 0; i < list.size(); i++) {
                Assert.assertEquals(i, list.get(i));
            }
        }
    }

    @Test
    public void testAddAllExpand() {
        try (
                DirectIntList list = new DirectIntList(128, MemoryTag.NATIVE_DEFAULT);
                DirectIntList list2 = new DirectIntList(128, MemoryTag.NATIVE_DEFAULT)
        ) {
            final int N = 100;
            for (int i = 0; i < N; i++) {
                list.add(i);
                list2.add(N + i);
            }
            list.addAll(list2);
            Assert.assertEquals(200, list.getCapacity()); // 128 + 100 - 28
            Assert.assertEquals(2 * N, list.size());
            for (long i = 0; i < list.size(); i++) {
                Assert.assertEquals(i, list.get(i));
            }
        }
    }

    @Test
    public void testCapacityAndSize() {
        // use logger so that static memory allocation happens before our control measurement
        LOG.info().$("testCapacityAndSize").$();
        long expected = Unsafe.getMemUsed();
        try (DirectIntList list = new DirectIntList(1024, MemoryTag.NATIVE_DEFAULT)) {
            Assert.assertEquals(1024, list.getCapacity());

            list.setCapacity(2048);
            Assert.assertEquals(2048, list.getCapacity());
            // verify that extend also shrinks capacity
            list.setCapacity(1024);
            Assert.assertEquals(1024, list.getCapacity());

            Assert.assertEquals(0, list.size());
            long addr = list.getAddress();
            Unsafe.getUnsafe().putLong(addr, 42);
            Assert.assertEquals(42, list.get(0));
            for (int i = 0; i < list.getCapacity(); i++) {
                list.add(i);
            }
            for (long i = 0; i < list.size(); i++) {
                Assert.assertEquals(i, list.get(i));
            }
            list.clear(0);
            Assert.assertEquals(0, list.size());
            for (long i = 0; i < list.getCapacity(); i++) {
                Assert.assertEquals(0, list.get(i));
            }
            list.setPos(42);
            Assert.assertEquals(42, list.size());
            list.clear();
            Assert.assertEquals(0, list.size());
        }
        Assert.assertEquals(expected, Unsafe.getMemUsed());
    }

    @Test
    public void testRemoveLast() {
        try (DirectIntList list = new DirectIntList(32, MemoryTag.NATIVE_DEFAULT)) {
            final int N = 100;
            for (int i = 0; i < N; i++) {
                list.add(i);
            }
            Assert.assertEquals(128, list.getCapacity());
            Assert.assertEquals(N, list.size());
            for (long i = 0; i < list.size(); i++) {
                list.removeLast();
                Assert.assertEquals(128, list.getCapacity());
                Assert.assertEquals(N - i - 1, list.size());
            }
        }
    }

    @Test
    public void testResizeMemLeak() {
        // use logger so that static memory allocation happens before our control measurement
        LOG.info().$("testResizeMemLeak").$();
        long expected = Unsafe.getMemUsed();
        try (DirectIntList list = new DirectIntList(1024, MemoryTag.NATIVE_DEFAULT)) {
            for (int i = 0; i < 1_000_000; i++) {
                list.add(i);
            }
        }
        Assert.assertEquals(expected, Unsafe.getMemUsed());
    }

    @Test
    public void testSet() {
        try (DirectIntList list = new DirectIntList(32, MemoryTag.NATIVE_DEFAULT)) {
            final int N = 100;
            for (int i = 0; i < N; i++) {
                list.add(i);
            }
            Assert.assertEquals(128, list.getCapacity());
            Assert.assertEquals(N, list.size());
            for (long i = 0; i < list.size(); i++) {
                Assert.assertEquals(i, list.get(i));
            }

            for (int i = 0; i < N; i++) {
                list.set(i, N - i);
            }
            Assert.assertEquals(128, list.getCapacity());
            Assert.assertEquals(N, list.size());
            for (long i = 0; i < list.size(); i++) {
                Assert.assertEquals(N - i, list.get(i));
            }
        }
    }

    @Test
    public void testShrink() {
        try (DirectIntList list = new DirectIntList(32, MemoryTag.NATIVE_DEFAULT)) {
            final int N = 100;
            for (int i = 0; i < N; i++) {
                list.add(i);
            }
            Assert.assertEquals(128, list.getCapacity());
            Assert.assertEquals(N, list.size());
            for (long i = 0; i < list.size(); i++) {
                Assert.assertEquals(i, list.get(i));
            }

            list.shrink(N);
            Assert.assertEquals(N, list.getCapacity());
            Assert.assertEquals(N, list.size());

            list.shrink(16);
            Assert.assertEquals(16, list.getCapacity());
            Assert.assertEquals(16, list.size());
        }
    }

    @Test
    public void testToString() {
        try (DirectIntList list = new DirectIntList(1001, MemoryTag.NATIVE_DEFAULT)) {
            final int N = 1000;
            for (int i = 0; i < N; i++) {
                list.add(i);
            }
            String str1 = list.toString();
            list.add(1001);
            String str2 = list.toString();

            Assert.assertEquals(str1.substring(0, str1.length() - 1) + ", .. ]", str2);
        }
    }

    @Test
    public void testZeroCapacity() {
        try (DirectIntList list = new DirectIntList(0, MemoryTag.NATIVE_DEFAULT)) {
            list.add(42);
            Assert.assertEquals(42, list.get(0));
            Assert.assertEquals(2, list.getCapacity());  // allocating the first elem gave us cap for two.
            Assert.assertEquals(1, list.size());
            list.add(43);
            Assert.assertEquals(43, list.get(1));
            Assert.assertEquals(2, list.getCapacity());  // still fits.
            Assert.assertEquals(2, list.size());
            list.resetCapacity();
            Assert.assertEquals(0, list.getCapacity());
            list.add(1);
            list.add(2);
            list.add(3);
            list.add(4);
            Assert.assertEquals(4, list.getCapacity());
            Assert.assertEquals(4, list.size());
        }
    }
}
