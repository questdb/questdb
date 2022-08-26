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

package io.questdb.griffin.engine.functions.analytic;

import io.questdb.griffin.AbstractGriffinTest;
import io.questdb.griffin.SqlException;
import org.junit.Test;

public class LagDoubleFunctionTest extends AbstractGriffinTest {
    @Test(expected = SqlException.class)
    public void testLagInvalid() throws Exception {
        assertQuery(
                "",
                "select lag(a, -1) from tmp1",
                "create table tmp1 as (select rnd_double() a from long_sequence(10))",
                null,
                false,
                true,
                true
        );
    }
    @Test(expected = SqlException.class)
    public void testLagInvalidNonConstant() throws Exception {
        assertQuery(
                "",
                "select lag(a, rnd_int()) from tmp2",
                "create table tmp2 as (select rnd_double() a from long_sequence(10))",
                null,
                false,
                true,
                true
        );
    }
    @Test
    public void testLagDoubleSequence() throws Exception {
        // a
        //0.6607777894187332
        //0.2246301342497259
        //0.08486964232560668
        //0.299199045961845
        //0.20447441837877756
        //0.6508594025855301
        //0.8423410920883345
        //0.9856290845874263
        //0.22452340856088226
        //0.5093827001617407
        assertQuery(
                "lag\n" +
                        "NaN\n" +
                        "0.6607777894187332\n" +
                        "0.2246301342497259\n" +
                        "0.08486964232560668\n" +
                        "0.299199045961845\n" +
                        "0.20447441837877756\n" +
                        "0.6508594025855301\n" +
                        "0.8423410920883345\n" +
                        "0.9856290845874263\n" +
                        "0.22452340856088226\n",
                "select lag(a, 1) from tmp3",
                "create table tmp3 as (select rnd_double() a from long_sequence(10))",
                null,
                false,
                true,
                true
        );
        assertSql(
                "select lag(a, 3) from tmp3",
                "lag\n" +
                        "NaN\n" +
                        "NaN\n" +
                        "NaN\n" +
                        "0.6607777894187332\n" +
                        "0.2246301342497259\n" +
                        "0.08486964232560668\n" +
                        "0.299199045961845\n" +
                        "0.20447441837877756\n" +
                        "0.6508594025855301\n" +
                        "0.8423410920883345\n"

        );
    }

    @Test
    public void testLagDoublePartitioned() throws Exception {
        //a	b
        //0.6607777894187332	false
        //0.12966659791573354	false
        //0.299199045961845	true
        //0.9344604857394011	false
        //0.8423410920883345	true
        assertQuery(
                "a\tb\tlag\n" +
                        "0.6607777894187332\tfalse\t0.9344604857394011\n" +
                        "0.12966659791573354\tfalse\t0.6607777894187332\n" +
                        "0.299199045961845\ttrue\t0.8423410920883345\n" +
                        "0.9344604857394011\tfalse\tNaN\n" +
                        "0.8423410920883345\ttrue\tNaN\n",
                "select a, b, lag(a, 1) over (partition by b order by a desc) from tmp4",
                "create table tmp4 as (select rnd_double() a, rnd_boolean() b from long_sequence(5))",
                null,
                true,
                true,
                false
        );
    }

    @Test
    public void testLagDoublePartitionedBigLag() throws Exception {
        //a	b
        //0.6607777894187332	false
        //0.12966659791573354	false
        //0.299199045961845	true
        //0.9344604857394011	false
        //0.8423410920883345	true
        assertQuery(
                "a\tb\tlag\n" +
                        "0.6607777894187332\tfalse\tNaN\n" +
                        "0.12966659791573354\tfalse\t0.9344604857394011\n" +
                        "0.299199045961845\ttrue\tNaN\n" +
                        "0.9344604857394011\tfalse\tNaN\n" +
                        "0.8423410920883345\ttrue\tNaN\n",
                "select a, b, lag(a, 2) over (partition by b order by a desc) from tmp4",
                "create table tmp4 as (select rnd_double() a, rnd_boolean() b from long_sequence(5))",
                null,
                true,
                true,
                false
        );
    }

    @Test
    public void testLagDoublePartitionedOnlyColumn() throws Exception {
        // if you remove this test, all tests are successful
        // however, with this test included, also testLagDoubleSequence will fail...
        assertQuery(
                "lag\n" +
                        "0.9344604857394011\n" +
                        "0.6607777894187332\n" +
                        "0.8423410920883345\n" +
                        "NaN\n" +
                        "NaN\n",
                "select lag(a, 1) over (partition by b order by a desc) from tmp5",
                "create table tmp5 as (select rnd_double() a, rnd_boolean() b from long_sequence(5))",
                null,
                true,
                true,
                false
        );
    }
}
