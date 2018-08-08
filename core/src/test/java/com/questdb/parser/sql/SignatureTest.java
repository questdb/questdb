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

package com.questdb.parser.sql;

import com.questdb.ql.ops.Signature;
import com.questdb.std.ObjObjHashMap;
import com.questdb.store.ColumnType;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class SignatureTest {
    @Test
    public void testMapBehaviour() {
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
