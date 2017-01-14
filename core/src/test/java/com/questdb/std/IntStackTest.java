/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2017 Appsicle
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
 ******************************************************************************/

package com.questdb.std;

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
