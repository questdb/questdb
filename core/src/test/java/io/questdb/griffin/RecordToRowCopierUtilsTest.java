/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.griffin;

import io.questdb.cairo.ColumnType;
import io.questdb.std.IntObjHashMap;
import org.junit.Assert;
import org.junit.Test;

public class RecordToRowCopierUtilsTest extends AbstractGriffinTest {

    private static final IntObjHashMap<String> typeRndFunctions = new IntObjHashMap<>();

    @Test
    public void testAssignableTypeConversions() throws SqlException {
        for (int typeTo = 1, n = ColumnType.MAX; typeTo < n; typeTo++) {
            String typeToName = ColumnType.nameOf(typeTo);
            if (!typeToName.equals("unknown")) {
                compiler.compile("create table test(x " + typeToName + ");", sqlExecutionContext);
                try {
                    for (int typeFrom = 1, m = ColumnType.MAX; typeFrom < m; typeFrom++) {
                        if (ColumnType.isAssignableFrom(typeFrom, typeTo)) {
                            String typeFromName = ColumnType.nameOf(typeFrom);
                            if (!typeFromName.equals("unknown")) {
                                String fn = typeRndFunctions.get(typeFrom);
                                if (fn == null) {
                                    System.out.println(ColumnType.nameOf(typeFrom));
                                }
                                System.out.println("typeFrom: " + typeFromName + " typeTo: " + typeToName);
                                compiler.compile("insert into test select " + typeRndFunctions.get(typeFrom) + " from long_sequence(5)", sqlExecutionContext);
                            }
                        }
                    }
                } finally {
                    compiler.compile("drop table test;", sqlExecutionContext);
                }
            }
        }
    }

    @Test
    public void testIasAssignableFrom() {
        Assert.assertFalse(ColumnType.isAssignableFrom(ColumnType.SHORT, ColumnType.BYTE));
    }

    static {
        typeRndFunctions.put(ColumnType.BOOLEAN, "rnd_boolean()");
        typeRndFunctions.put(ColumnType.BYTE, "rnd_byte()");
        typeRndFunctions.put(ColumnType.SHORT, "rnd_short()");
        typeRndFunctions.put(ColumnType.INT, "rnd_int()");
        typeRndFunctions.put(ColumnType.LONG, "rnd_long()");
        typeRndFunctions.put(ColumnType.FLOAT, "rnd_float()");
        typeRndFunctions.put(ColumnType.DOUBLE, "rnd_double()");
        typeRndFunctions.put(ColumnType.STRING, "rnd_str()");
        typeRndFunctions.put(ColumnType.TIMESTAMP, "rnd_long()");
        typeRndFunctions.put(ColumnType.DATE, "rnd_long()");
        typeRndFunctions.put(ColumnType.SYMBOL, "rnd_symbol('123', '55')");
    }
}