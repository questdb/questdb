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

import io.questdb.std.DoubleList;
import io.questdb.std.Vect;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class DoubleListTest {

    @Test
    public void testAdd() {
        DoubleList a = new DoubleList();
        a.add(1d);
        a.add(2d);
        a.add(3d);

        a.add(0, 0);

        Assert.assertEquals(new DoubleList(new double[]{0d, 1d, 2d, 3d}), a);
    }

    @Test
    public void testBinarySearch1() {
        DoubleList a = new DoubleList();
        a.add(1d);
        a.add(2d);
        a.add(3d);

        Assert.assertEquals(0, a.binarySearch(1d, Vect.BIN_SEARCH_SCAN_UP));
        Assert.assertEquals(1, a.binarySearch(2d, Vect.BIN_SEARCH_SCAN_UP));
        Assert.assertEquals(2, a.binarySearch(3d, Vect.BIN_SEARCH_SCAN_UP));
        Assert.assertEquals(-1, a.binarySearch(-3d, Vect.BIN_SEARCH_SCAN_UP));
        Assert.assertEquals(-4, a.binarySearch(5d, Vect.BIN_SEARCH_SCAN_UP));
    }

    @Test
    public void testBinarySearch2() {
        DoubleList a = new DoubleList();
        for (int i = 0; i < 100; i++) {
            a.add(i);
        }

        for (int scan : new int[]{Vect.BIN_SEARCH_SCAN_UP, Vect.BIN_SEARCH_SCAN_DOWN}) {
            Assert.assertEquals(-1, a.binarySearch(-1d, scan));

            for (int i = 0; i < 100; i++) {
                Assert.assertEquals(i, a.binarySearch(i, scan));
            }

            Assert.assertEquals(-101, a.binarySearch(100d, scan));
        }
    }

    @Test
    public void testBinarySearch3() {
        DoubleList a = new DoubleList();
        for (int i = 0; i < 100; i++) {
            a.add((i / 10));
        }

        Assert.assertEquals(-1, a.binarySearch(-1d, Vect.BIN_SEARCH_SCAN_UP));

        for (int i = 0; i < 10; i++) {
            Assert.assertEquals(i * 10, a.binarySearch(i, Vect.BIN_SEARCH_SCAN_UP));
            Assert.assertEquals(i * 10 + 9, a.binarySearch(i, Vect.BIN_SEARCH_SCAN_DOWN));
        }

        Assert.assertEquals(-101, a.binarySearch(100d, Vect.BIN_SEARCH_SCAN_UP));
    }

    @Test
    public void testClear() {
        DoubleList a = new DoubleList();
        a.add(1d);
        a.clear();

        Assert.assertEquals(0, a.size());
    }

    @Test
    public void testConstruct1() {
        DoubleList a = new DoubleList();
        a.add(1d);
        a.add(2d);
        a.add(3d);

        DoubleList b = new DoubleList(a);
        Assert.assertEquals(a, b);
    }

    @Test
    public void testConstruct2() {
        DoubleList a = new DoubleList();
        a.add(1d);
        a.add(2d);
        a.add(3d);

        DoubleList b = new DoubleList(new double[]{1d, 2d, 3d});
        Assert.assertEquals(a, b);
    }

    @Test
    public void testEquals() {
        DoubleList a = new DoubleList();
        a.add(1d);

        DoubleList b = new DoubleList();
        b.add(1d);
        b.add(2d);
        Assert.assertNotEquals(a, b);

        DoubleList c = new DoubleList(1, -100);
        c.add(1d);
        Assert.assertNotEquals(a, c);

        DoubleList d = new DoubleList();
        d.add(2d);
        Assert.assertNotEquals(a, d);

        DoubleList e = new DoubleList(a);
        Assert.assertEquals(a, e);
    }

    @Test
    public void testErase() {
        DoubleList a = new DoubleList();
        a.add(1d);
        a.add(2d);
        a.erase();

        Assert.assertEquals(0, a.size());
    }

    @Test
    public void testExtendAndSet() {
        DoubleList a = new DoubleList(2);
        a.add(1d);
        a.extendAndSet(3, 100d);

        DoubleList b = new DoubleList(new double[]{1d, 0, 0, 100d});
        Assert.assertEquals(a, b);
    }

    @Test
    public void testFill() {
        DoubleList a = new DoubleList(5);
        a.setPos(5);
        a.fill(0, 5, 5);

        DoubleList b = new DoubleList(new double[]{5d, 5d, 5d, 5d, 5d});
        Assert.assertEquals(b, a);
    }

    @Test
    public void testGet() {
        DoubleList a = new DoubleList();
        a.add(3d);
        a.add(2d);
        a.add(1d);

        Assert.assertEquals(3d, a.get(0), 0.00000001d);
        Assert.assertEquals(2d, a.get(1), 0.00000001d);
        Assert.assertEquals(1d, a.getLast(), 0.00000001d);

        a.clear();
        Assert.assertEquals(DoubleList.DEFAULT_NO_ENTRY_VALUE, a.getLast(), 0.00000001d);
    }

    @Test
    public void testHashCodeEquals() {
        DoubleList a = new DoubleList(5);
        a.setPos(5);
        a.fill(0, 5, 5);

        DoubleList b = new DoubleList(new double[]{5d, 5d, 5d, 5d, 5d});
        Assert.assertEquals(b.hashCode(), a.hashCode());
    }

    @Test
    public void testIndexOf() {
        DoubleList a = new DoubleList();
        a.add(1d);
        a.add(2d);
        a.add(3d);

        Assert.assertEquals(0, a.indexOf(1d));
        Assert.assertEquals(1, a.indexOf(2d));
        Assert.assertEquals(2, a.indexOf(3d));
        Assert.assertEquals(-1, a.indexOf(-3d));
        Assert.assertEquals(-1, a.indexOf(5d));
    }

    @Test
    public void testRemove() {
        DoubleList a = new DoubleList();
        a.add(1d);
        a.add(2d);
        a.add(4d);
        a.add(2d);
        a.add(Double.NaN);

        a.remove(2d);

        Assert.assertEquals(new DoubleList(new double[]{1d, 4d, 2d, Double.NaN}), a);

        a.remove(Double.NaN);

        Assert.assertEquals(new DoubleList(new double[]{1d, 4d, 2d}), a);
    }

    @Test
    public void testReverse() {
        DoubleList a = new DoubleList();
        a.add(3d);
        a.add(2d);
        a.add(1d);

        a.reverse();

        Assert.assertEquals(new DoubleList(new double[]{1d, 2d, 3d}), a);
    }

    @Test
    public void testSet() {
        DoubleList a = new DoubleList();
        a.add(1d);
        a.add(2d);
        a.add(3d);

        a.set(0, 5);
        a.setQuick(1, 6);
        a.setLast(7);

        Assert.assertEquals(new DoubleList(new double[]{5d, 6d, 7d}), a);
    }

    @Test
    public void testSetAll() {
        DoubleList a = new DoubleList();
        a.add(1d);
        a.add(2d);
        a.add(4d);

        a.setAll(3, 7d);

        Assert.assertEquals(new DoubleList(new double[]{7d, 7d, 7d}), a);
    }

    @Test
    public void testSetFails() {
        DoubleList a = new DoubleList();
        a.add(1d);

        try {
            a.set(5, 7);
            Assert.fail();
        } catch (ArrayIndexOutOfBoundsException e) {
            Assert.assertEquals("Array index out of range: 5", e.getMessage());
        }
    }

    @Test
    public void testSort() {
        DoubleList a = new DoubleList();
        a.add(3d);
        a.add(2d);
        a.add(1d);

        a.sort();

        Assert.assertEquals(new DoubleList(new double[]{1d, 2d, 3d}), a);
    }

    @Test
    public void testToSink() {
        DoubleList a = new DoubleList();
        a.add(3d);
        a.add(2d);
        a.add(1d);

        Assert.assertEquals("[3.0,2.0,1.0]", a.toString());

        StringSink sink = new StringSink();
        a.toSink(sink);

        TestUtils.assertEquals("[3.0,2.0,1.0]", sink);

        sink.clear();
        a.toSink(sink, 1d);

        TestUtils.assertEquals("[3.0,2.0]", sink);
    }
}
