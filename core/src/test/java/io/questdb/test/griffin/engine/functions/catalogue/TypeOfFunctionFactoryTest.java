/*+*****************************************************************************
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
    public void testArgumentIsClosed() throws Exception {
        // typeOf answers from the argument's type alone and keeps no argument, so it owns and must
        // close it; trim() holds two native sinks that only its close() frees
        assertQuery("select typeOf(trim(a)) t from test")
                .ddl("create table test as (select cast(x as varchar) a from long_sequence(4))")
                .expectSize()
                .returns("t\nVARCHAR\nVARCHAR\nVARCHAR\nVARCHAR\n");
    }

    @Test
    public void testBindVarNotSupported() throws Exception {
        assertQuery("select typeOf($1) from test")
                .ddl("create table test as (select cast(x as varchar) a, timestamp_sequence(0, 1000000) ts from long_sequence(100))")
                .fails(14, "bind variables are not supported");
    }

    @Test
    public void testOfNull() throws Exception {
        assertQuery("select typeOf(null)")
                .noLeakCheck()
                .expectSize()
                .returns("""
                        typeOf
                        NULL
                        """);
        assertQuery("select typeOf(cast(null as string))")
                .noLeakCheck()
                .expectSize()
                .returns("""
                        typeOf
                        STRING
                        """);
        assertQuery("select typeOf(value) from (select null value from long_sequence(1))")
                .noLeakCheck()
                .expectSize()
                .returns("""
                        typeOf
                        NULL
                        """);
        assertQuery("select typeOf(value) from (select cast(null as long) value from long_sequence(1))")
                .noLeakCheck()
                .expectSize()
                .returns("""
                        typeOf
                        LONG
                        """);
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
    public void testTypeOfAllRegularDataTypes() throws Exception {
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
                    || i == ColumnType.VARCHAR_SLICE
            ) {
                continue;
            }

            assertQuery("select typeOf(cast(null as " + name + "  ))")
                    .noLeakCheck()
                    .expectSize()
                    .returns("typeOf\n" + ColumnType.nameOf(i) + "\n");
        }
    }

    @Test
    public void testTypeOfDecimal() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table dec (d8 decimal(2,1), d16 decimal(4,0), d32 decimal(9,3), d64 decimal(18,2), d128 decimal(38,10), d256 decimal(76,20))");
            execute("insert into dec values (1.1m, 2m, 3.333m, 4.44m, 5.5m, 6.6m)");
            execute("insert into dec values (null, null, null, null, null, null)");

            // column references, one per storage width
            assertQuery("select typeOf(d8) t8, typeOf(d16) t16, typeOf(d32) t32, typeOf(d64) t64, typeOf(d128) t128, typeOf(d256) t256 from dec")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            t8\tt16\tt32\tt64\tt128\tt256
                            DECIMAL(2,1)\tDECIMAL(4,0)\tDECIMAL(9,3)\tDECIMAL(18,2)\tDECIMAL(38,10)\tDECIMAL(76,20)
                            DECIMAL(2,1)\tDECIMAL(4,0)\tDECIMAL(9,3)\tDECIMAL(18,2)\tDECIMAL(38,10)\tDECIMAL(76,20)
                            """);

            // literal
            assertQuery("select typeOf(123.45m)")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            typeOf
                            DECIMAL(5,2)
                            """);

            // null decimal
            assertQuery("select typeOf(cast(null as decimal(38,10)))")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            typeOf
                            DECIMAL(38,10)
                            """);
        });
    }

    @Test
    public void testTypeOfGeoHash() throws Exception {
        for (int i = 1; i <= ColumnType.GEOLONG_MAX_BITS; i++) {
            int type = ColumnType.getGeoHashTypeWithBits(i);
            sink.clear();
            sink.put("select typeOf(rnd_geohash(").put(i).put("))");
            assertQuery(sink)
                    .noLeakCheck()
                    .expectSize()
                    .returns("typeOf\n" + ColumnType.nameOf(type) + "\n");
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
