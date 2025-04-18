/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.std.ObjStack;
import io.questdb.std.Rnd;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class ObjStackTest {
    @Test
    public void clearTest() {
        ObjStack<Integer> s = new ObjStack<>();
        for (int i = 0; i < 10; i++) {
            s.push(i);
        }

        Assert.assertEquals(10, s.size());
        s.clear();
        Assert.assertEquals(0, s.size());

        for (int i = 0; i < 10; i++) {
            Assert.assertNull(s.peek(i));
        }
    }

    @Test
    public void constCapacityTest() {
        ObjStack<Integer> q = new ObjStack<>(3);
        for (int i = 0; i < 10; i++) {
            q.push(2 * i);
        }
        Assert.assertEquals(10, q.size());
    }

    @Test
    public void fifoConstCapacityTest() {
        ObjStack<Integer> s = new ObjStack<>(3);
        for (int i = 0; i < 10; i++) {
            s.push(2 * i);
        }
        Assert.assertEquals(10, s.size());
    }

    @Test
    public void notEmptyTest() {
        ObjStack<Integer> s = new ObjStack<>();
        Assert.assertFalse(s.notEmpty());
        s.push(1);
        Assert.assertTrue(s.notEmpty());
        s.pop();
        Assert.assertFalse(s.notEmpty());
    }

    @Test
    public void peekLifoTest() {
        ObjStack<Integer> s = new ObjStack<>();
        for (int i = 0; i < 10; i++) {
            s.push(i);
        }

        Assert.assertEquals(s.size() - 1, (int) s.peek());
        for (int i = 0; i < 10; i++) {
            Assert.assertEquals(s.size() - i - 1, (int) s.peek(i));
        }
    }

    @Test
    public void testLifo10ElementsInOut() {
        ObjStack<Integer> s = new ObjStack<>();
        for (int i = 0; i < 10; i++) {
            s.push(i);
        }

        for (int i = 9; i >= 0; i--) {
            Integer r = s.pop();
            Assert.assertEquals((Integer) i, r);
        }
        Assert.assertNull(s.pop());
    }

    @Test
    public void testLifo120ElementsInOut() {
        ObjStack<Integer> s = new ObjStack<>();
        for (int i = 0; i < 20; i++) {
            s.push(i);
        }

        for (int i = 19; i >= 0; i--) {
            Integer r = s.pop();
            Assert.assertEquals((Integer) i, r);
        }
        Assert.assertNull(s.pop());
    }

    @Test
    public void testLifo1ElementInOut() {
        ObjStack<Integer> s = new ObjStack<>();
        s.push(1);
        int i = s.pop();

        Assert.assertEquals(1, i);
        Assert.assertNull(s.pop());
    }

    @Test
    public void testLifo2In1Out() {
        ObjStack<Integer> s = new ObjStack<>();
        for (int i = 0; i < 1000; i++) {
            s.push(2 * i);
            s.push(2 * i + 1);

            Integer r = s.pop();
            Assert.assertEquals((Integer) (2 * i + 1), r);
            Assert.assertEquals(i + 1, s.size());
        }
    }

    @Test
    public void testLifo2In2Out() {
        ObjStack<Integer> s = new ObjStack<>();
        for (int i = 0; i < 1000; i++) {
            s.push(2 * i);
            s.push(2 * i + 1);

            Integer r = s.pop();
            Assert.assertEquals((Integer) (2 * i + 1), r);

            Integer r2 = s.pop();
            Assert.assertEquals((Integer) (2 * i), r2);

            Assert.assertEquals(0, s.size());
        }
    }

    @Test
    public void testResetCapacity() {
        ObjStack<Integer> s = new ObjStack<>();
        int n = ObjStack.DEFAULT_INITIAL_CAPACITY * 2;
        for (int i = 0; i < n; i++) {
            s.push(i);
        }

        Assert.assertEquals(n, s.size());
        s.resetCapacity();
        Assert.assertEquals(ObjStack.DEFAULT_INITIAL_CAPACITY - 1, s.size());

        // we must be able to pop the latest items out
        for (int i = n - 1; i >= n / 2 + 1; i--) {
            Assert.assertEquals(i, s.pop().intValue());
        }
        Assert.assertEquals(0, s.size());
    }

    @Test
    public void testResetCapacityEmpty() {
        ObjStack<Integer> s = new ObjStack<>();
        int n = ObjStack.DEFAULT_INITIAL_CAPACITY * 2;
        for (int i = 0; i < n; i++) {
            s.push(i);
        }

        for (int i = 0; i < n; i++) {
            s.pop();
        }

        s.resetCapacity();
        Assert.assertEquals(ObjStack.DEFAULT_INITIAL_CAPACITY, s.getCapacity());
        Assert.assertEquals(0, s.size());
    }

    @Test
    public void testResetCapacityFuzz() {
        ObjStack<Integer> s = new ObjStack<>();
        Rnd rnd = TestUtils.generateRandom(null);
        int n = 1 + rnd.nextInt(ObjStack.DEFAULT_INITIAL_CAPACITY * 4);
        int p = rnd.nextInt(n);
        pushPop(n, p, s);

        int n1 = 1 + rnd.nextInt(ObjStack.DEFAULT_INITIAL_CAPACITY * 3);
        int p1 = rnd.nextInt(n1);
        pushPop(n1, p1, s);
    }

    @Test
    public void updateTest() {
        ObjStack<Integer> s = new ObjStack<>();
        for (int i = 0; i < 10; i++) {
            s.push(i);
        }

        Assert.assertEquals(s.size() - 1, (int) s.peek());
        s.update(20);
        Assert.assertEquals(20, (int) s.peek());
        Assert.assertEquals(20, (int) s.pop());
    }

    private static void pushPop(int n, int p, ObjStack<Integer> s) {
        for (int i = 0; i < n; i++) {
            s.push(i);
        }

        for (int i = 0; i < p; i++) {
            Assert.assertEquals(n - i - 1, s.pop().intValue());
        }

        Assert.assertEquals(n - p, s.size());
        s.resetCapacity();
        Assert.assertEquals(Math.min(ObjStack.DEFAULT_INITIAL_CAPACITY - 1, n - p), s.size());

        // we must be able to pop the latest items out
        for (int i = 0, size = s.size(); i < size; i++) {
            Assert.assertEquals(n - p - i - 1, s.pop().intValue());
        }
        Assert.assertEquals(0, s.size());
    }
}
