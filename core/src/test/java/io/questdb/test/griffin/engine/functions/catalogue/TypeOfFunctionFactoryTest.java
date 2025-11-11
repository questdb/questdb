/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.test.griffin.engine.functions.catalogue;

import io.questdb.cairo.ColumnType;
import io.questdb.griffin.SqlException;
import io.questdb.std.Chars;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class TypeOfFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testBindVarNotSupported() throws Exception {
        assertException(
                "select typeOf($1) from test",
                "create table test as (select cast(x as varchar) a, timestamp_sequence(0, 1000000) ts from long_sequence(100))",
                14,
                "bind variables are not supported"
        );
    }

    @Test
    public void testOfNull() throws SqlException {
        assertSql("typeOf\n" +
                "NULL\n", "select typeOf(null)"
        );
        assertSql("typeOf\n" +
                "STRING\n", "select typeOf(cast(null as string))"
        );
        assertSql("typeOf\n" +
                "NULL\n", "select typeOf(value) from (select null value from long_sequence(1))"
        );
        assertSql("typeOf\n" +
                "LONG\n", "select typeOf(value) from (select cast(null as long) value from long_sequence(1))"
        );
    }

    @Test
    public void testTooFewArgs() throws Exception {
        assertSyntaxError("select typeOf()");
    }

    @Test
    public void testTooManyArgs() throws Exception {
        assertSyntaxError("select typeOf(1,2)");
    }

    @Test
    public void testTypeOfAllRegularDataTypes() throws SqlException {
        for (int i = ColumnType.BOOLEAN; i < ColumnType.NULL; i++) {
            String name = ColumnType.nameOf(i);
            if (Chars.equals("unknown", name)
                    || i == ColumnType.CURSOR
                    || i == ColumnType.VAR_ARG
                    || i == ColumnType.RECORD
                    || i == ColumnType.GEOHASH
                    || i == ColumnType.LONG128
                    || i == ColumnType.REGCLASS
                    || i == ColumnType.REGPROCEDURE
                    || i == ColumnType.ARRAY_STRING
                    || i == ColumnType.PARAMETER
                    || i == ColumnType.ARRAY
                    || ColumnType.isDecimal(i)
            ) {
                continue;
            }

            assertSql("typeOf\n" + ColumnType.nameOf(i) + "\n", "select typeOf(cast(null as " + name + "  ))");
        }
    }

    @Test
    public void testTypeOfGeoHash() throws SqlException {
        for (int i = 1; i <= ColumnType.GEOLONG_MAX_BITS; i++) {
            int type = ColumnType.getGeoHashTypeWithBits(i);
            sink.clear();
            sink.put("select typeOf(rnd_geohash(").put(i).put("))");
            assertSql("typeOf\n" + ColumnType.nameOf(type) + "\n", sink);
        }
    }

    private void assertSyntaxError(String sql) throws Exception {
        assertMemoryLeak(
                () -> {
                    try {
                        assertExceptionNoLeakCheck(sql);
                    } catch (SqlException e) {
                        Assert.assertEquals(7, e.getPosition());
                        TestUtils.assertContains(e.getFlyweightMessage(), "exactly one argument expected");
                    }
                }
        );
    }
}
