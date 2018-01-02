/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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

public class IntListTest {
    @Test
    public void testBinarySearch() {
        IntList list = new IntList();
        for (int i = 0; i < 65; i++) {
            list.add(i);
        }
        list.add(76);
        list.add(974);
        list.add(1115);
        list.add(2094);
        Assert.assertEquals(-66, list.binarySearch(70));
        Assert.assertEquals(65, list.binarySearch(76));
        Assert.assertEquals(-67, list.binarySearch(950));
    }
}