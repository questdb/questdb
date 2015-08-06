/*
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
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
 */

package com.nfsdb;

import com.nfsdb.collections.IntStack;
import org.junit.Assert;
import org.junit.Test;

public class IntStackTest {
    @Test
    public void testStack() throws Exception {

        int expected[] = new int[29];
        int l = expected.length - 1;

        IntStack stack = new IntStack();
        for (int i = expected.length - 1; i > -1; i--) {
            stack.push(expected[l--] = i);
        }

        Assert.assertEquals(29, stack.size());

        int count = 0;
        while (stack.notEmpty()) {
            Assert.assertEquals("at " + count, expected[count++], stack.pop());
        }
    }
}
