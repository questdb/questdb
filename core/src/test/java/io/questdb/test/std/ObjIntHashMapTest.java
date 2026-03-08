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

import io.questdb.std.ObjIntHashMap;
import io.questdb.std.Rnd;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;

public class ObjIntHashMapTest {

    @Test
    public void testAddAndIterate() {
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

    @Test
    public void testClassBehaviour() {
        ObjIntHashMap<Class<?>> map = new ObjIntHashMap<>();
        Assert.assertEquals(-1, map.get(Object.class));
    }
}
