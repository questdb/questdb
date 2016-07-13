/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2016 Appsicle
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

import com.questdb.misc.Rnd;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;

public class ObjIntHashMapTest {
    @Test
    public void testAddAndIterate() throws Exception {
        Rnd rnd = new Rnd();

        ObjIntHashMap<String> map = new ObjIntHashMap<>();
        HashMap<String, Integer> master = new HashMap<>();

        int n = 1000;
        for (int i = 0; i < n; i++) {
            String s = rnd.nextString(rnd.nextPositiveInt() % 20);
            int v = rnd.nextInt();
            map.put(s, v);
            master.put(s, v);
        }

        for (ObjIntHashMap.Entry<String> e : map) {
            Integer val = master.get(e.key);
            Assert.assertNotNull(val);
            Assert.assertEquals(e.value, val.intValue());
        }
    }
}