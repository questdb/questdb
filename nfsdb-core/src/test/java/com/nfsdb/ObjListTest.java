/*******************************************************************************
 *   _  _ ___ ___     _ _
 *  | \| | __/ __| __| | |__
 *  | .` | _|\__ \/ _` | '_ \
 *  |_|\_|_| |___/\__,_|_.__/
 *
 *  Copyright (c) 2014-2015. The NFSdb project and its contributors.
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
 ******************************************************************************/
package com.nfsdb;

import com.nfsdb.collections.ObjList;
import com.nfsdb.utils.Rnd;
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

        Rnd rnd2 = new Rnd();
        for (int i = 0; i < list.size(); i++) {
            Assert.assertEquals(rnd2.nextString(10), list.getQuick(i));
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
