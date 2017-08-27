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

public class ObjHashSetTest {
    @Test
    public void testNull() throws Exception {
        ObjHashSet<String> set = new ObjHashSet<>();
        set.add("X");
        set.add(null);
        set.add("Y");
        Assert.assertEquals(3, set.size());

        Assert.assertEquals("X", set.get(0));
        Assert.assertNull(set.get(1));
        Assert.assertEquals("Y", set.get(2));

        int n = set.size();
        Assert.assertEquals(3, n);
        int nullCount = 0;
        for (int i = 0; i < n; i++) {
            if (set.get(i) == null) {
                nullCount++;
            }
        }

        Assert.assertEquals(1, nullCount);
    }
}
