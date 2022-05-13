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

import org.junit.Assert;
import org.junit.Test;

public class IntStackTest {
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
}
