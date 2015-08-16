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

package com.nfsdb.ql.parser;

import com.nfsdb.collections.ObjObjHashMap;
import com.nfsdb.ql.ops.Signature;
import com.nfsdb.storage.ColumnType;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class SignatureTest {
    @Test
    public void testMapBehaviour() throws Exception {
        ObjObjHashMap<Signature, String> sigs = new ObjObjHashMap<Signature, String>() {{

            put(new Signature().setName("-").setParamCount(1).paramType(0, ColumnType.INT, false), "sig1");
            // overload
            put(new Signature().setName("-").setParamCount(1).paramType(0, ColumnType.DOUBLE, false), "sig2");
            // overload 2
            put(new Signature().setName("-").setParamCount(2).paramType(0, ColumnType.INT, false).paramType(1, ColumnType.INT, false), "sig3");
            // dupe
            put(new Signature().setName("-").setParamCount(1).paramType(0, ColumnType.INT, false), "sig4");
            put(new Signature().setName("-").setParamCount(1).paramType(0, ColumnType.INT, true), "sig5");
        }};

        Assert.assertEquals(4, sigs.size());
        String[] expected = new String[]{"sig2", "sig3", "sig4", "sig5"};
        String[] actual = new String[sigs.size()];

        int k = 0;
        for (ObjObjHashMap.Entry<Signature, String> e : sigs) {
            actual[k++] = e.value;
        }

        Arrays.sort(actual);
        Assert.assertArrayEquals(expected, actual);
    }
}
