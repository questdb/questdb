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
                "select lag(a, -1) from tmp",
                "create table tmp as (select rnd_double() a from long_sequence(10))",
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
                "select lag(a, rnd_int()) from tmp",
                "create table tmp as (select rnd_double() a from long_sequence(10))",
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
                        "-0.43614765516900733\n" +
                        "-0.1397604919241192\n" +
                        "0.2143294036362383\n" +
                        "-0.09472462758306743\n" +
                        "0.44638498420675254\n" +
                        "0.19148168950280442\n" +
                        "0.14328799249909174\n" +
                        "-0.761105676026544\n" +
                        "0.28485929160085843\n",
                "select lag(a, 1) from tmp",
                "create table tmp as (select rnd_double() a from long_sequence(10))",
                null,
                false,
                true,
                true
        );
        assertSql(
                "select lag(a, 3) from tmp",
                "lag\n" +
                        "NaN\n" +
                        "NaN\n" +
                        "NaN\n" +
                        "-0.36157874345688823\n" +
                        "-0.020155715870948332\n" +
                        "0.5659897602599234\n" +
                        "0.5431420461264895\n" +
                        "0.7811546662086487\n" +
                        "-0.42633599402464784\n" +
                        "-0.3329583919265938\n"

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

        // lag
        //0.5311111915029997
        //-0.16953244804611145
        //-0.5431420461264895
        //NaN
        //-0.09211939365106658

        assertQuery(
                "",
                // "select row_number() over (partition by b order by a desc) from tmp",
                "select lag(a, 1) over (partition by b order by a desc) from tmp",
                //"select * from tmp order by b asc, a desc",
                "create table tmp as (select rnd_double() a, rnd_boolean() b from long_sequence(5))",
                null,
                true,
                true,
                false
        );
    }
}
