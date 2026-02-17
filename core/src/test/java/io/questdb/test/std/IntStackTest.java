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

import io.questdb.std.IntStack;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class IntStackTest {

    @Test
    public void testBottomBeyondTop() {
        IntStack s = new IntStack();
        s.push(1);
        s.push(2);
        try {
            s.setBottom(3);
            fail();
        } catch (IllegalStateException e) {
            assertEquals("Tried to set bottom beyond the top of the stack", e.getMessage());
        }
    }

    @Test
    public void testBottomClear() {
        IntStack s = new IntStack();
        s.push(1);
        s.push(2);
        s.setBottom(2);
        assertEquals(-1, s.peek());
        s.clear();
        s.push(1);
        assertEquals(1, s.pop());
    }

    @Test
    public void testBottomPeek() {
        IntStack s = new IntStack();
        s.push(1);
        s.push(2);
        s.push(3);
        s.setBottom(1);
        assertEquals(3, s.peek(0));
        assertEquals(2, s.peek(1));
        assertEquals(-1, s.peek(2));
    }

    @Test
    public void testBottomPop() {
        IntStack s = new IntStack();
        s.push(1);
        s.push(2);
        s.setBottom(1);
        assertEquals(1, s.size());
        assertEquals(2, s.pop());
        assertEquals(-1, s.pop());
        s.setBottom(0);
        assertEquals(1, s.size());
        assertEquals(1, s.pop());
    }

    @Test
    public void testBottomPopAll() {
        IntStack s = new IntStack();
        s.push(1);
        s.push(2);
        s.setBottom(1);
        s.popAll();
        assertEquals(-1, s.pop());
        s.setBottom(0);
        assertEquals(1, s.pop());
    }

    @Test
    public void testBottomSize() {
        IntStack s = new IntStack();
        s.push(1);
        s.push(2);
        assertEquals(2, s.size());
        s.setBottom(1);
        assertEquals(1, s.size());
        s.setBottom(2);
        assertEquals(0, s.size());
    }

    @Test
    public void testPollLast() {
        int sz = 2050;
        int[] expected = new int[sz];
        int l = expected.length - 1;

        IntStack stack = new IntStack();
        for (int i = expected.length - 1; i > -1; i--) {
            stack.push(expected[l--] = i);
        }

        Assert.assertEquals(sz, stack.size());

        int count = expected.length - 1;
        while (stack.notEmpty()) {
            Assert.assertEquals("at " + count, expected[count--], stack.pollLast());
        }

        Assert.assertEquals(-1, stack.pollLast());
        Assert.assertFalse(stack.notEmpty());
    }

    @Test
    public void testPollLastNotAllowedWithBottom() {
        IntStack s = new IntStack();
        s.push(1);
        s.push(2);
        s.setBottom(1);
        try {
            s.pollLast();
            fail("Allowed pollLast() while bottom != 0");
        } catch (IllegalStateException e) {
            assertEquals("pollLast() called while bottom != 0", e.getMessage());
        }
    }

    @Test
    public void testStack() {

        int sz = 2050;
        int[] expected = new int[sz];
        int l = expected.length - 1;

        IntStack stack = new IntStack();
        for (int i = expected.length - 1; i > -1; i--) {
            stack.push(expected[l--] = i);
        }

        Assert.assertEquals(sz, stack.size());

        int count = 0;
        while (stack.notEmpty()) {
            Assert.assertEquals("at " + count, expected[count++], stack.pop());
        }
    }
}
