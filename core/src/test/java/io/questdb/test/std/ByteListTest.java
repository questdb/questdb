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

import io.questdb.std.ByteList;
import org.junit.Assert;
import org.junit.Test;

public class ByteListTest {

    @Test
    public void testAddAll() {
        ByteList src = new ByteList();
        for (int i = 0; i < 100; i++) {
            src.add((byte) i);
        }

        ByteList dst = new ByteList();
        dst.clear();
        dst.addAll(src);

        Assert.assertEquals(dst, src);
    }

    @Test
    public void testEquals() {
        final ByteList list1 = new ByteList();
        list1.add((byte) 1);
        list1.add((byte) 2);
        list1.add((byte) 3);

        // different order
        final ByteList list2 = new ByteList();
        list2.add((byte) 1);
        list2.add((byte) 3);
        list2.add((byte) 2);
        Assert.assertNotEquals(list1, list2);

        // longer
        final ByteList list3 = new ByteList();
        list3.add((byte) 1);
        list3.add((byte) 2);
        list3.add((byte) 3);
        list3.add((byte) 4);
        Assert.assertNotEquals(list1, list3);

        // shorter
        final ByteList list4 = new ByteList();
        list4.add((byte) 1);
        list4.add((byte) 2);
        Assert.assertNotEquals(list1, list4);

        // empty
        final ByteList list5 = new ByteList();
        Assert.assertNotEquals(list1, list5);

        // null
        Assert.assertNotEquals(list1, null);

        // equals
        final ByteList list6 = new ByteList();
        list6.add((byte) 1);
        list6.add((byte) 2);
        list6.add((byte) 3);
        Assert.assertEquals(list1, list6);
    }

    @Test
    public void testRestoreInitialCapacity() {
        final int N = 100;
        ByteList list = new ByteList();
        int initialCapacity = list.capacity();

        for (int i = 0; i < N; i++) {
            list.add((byte) i);
        }

        Assert.assertEquals(N, list.size());
        Assert.assertTrue(list.capacity() >= N);

        list.restoreInitialCapacity();
        Assert.assertEquals(0, list.size());
        Assert.assertEquals(initialCapacity, list.capacity());
    }

    @Test
    public void testSmoke() {
        ByteList list = new ByteList();
        for (int i = 0; i < 100; i++) {
            list.add((byte) i);
        }
        Assert.assertEquals(100, list.size());
        Assert.assertTrue(list.capacity() >= 100);

        for (int i = 0; i < 100; i++) {
            Assert.assertEquals((byte) i, list.getQuick(i));
            Assert.assertTrue(list.contains((byte) i));
        }

        for (int i = 0; i < 100; i++) {
            list.remove((byte) i);
        }
        Assert.assertEquals(0, list.size());
    }
}
