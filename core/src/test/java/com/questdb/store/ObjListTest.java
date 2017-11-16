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

package com.questdb.store;

import com.questdb.std.ObjList;
import com.questdb.std.Rnd;
import org.junit.Assert;
import org.junit.Test;

public class ObjListTest {
    @Test
    public void testAdd() throws Exception {
        Rnd rnd = new Rnd();

        ObjList<String> list = new ObjList<>();
        for (int i = 0; i < 100; i++) {
            list.add(rnd.nextString(10));
        }

        Assert.assertEquals(100, list.size());

        rnd.reset();
        for (int i = 0; i < list.size(); i++) {
            Assert.assertEquals(rnd.nextString(10), list.getQuick(i));
        }
    }

    @Test
    public void testExtendAndSet() throws Exception {
        ObjList<String> list = new ObjList<>();
        list.extendAndSet(10, "XYZ");
        list.extendAndSet(76, "BBB");
        Assert.assertEquals("XYZ", list.getQuick(10));
        Assert.assertEquals(77, list.size());
    }
}
