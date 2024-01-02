/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb.test.griffin.engine.functions.groupby;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

import java.util.UUID;

public class LastNotNullGroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testAllNull() throws Exception {
        assertQuery(
                "a0\ta1\ta2\ta3\ta4\ta5\ta6\ta7\ta8\ta9\ta10\ta11\ta12\ta13\ta14\n" +
                        "\t\tNaN\tNaN\tNaN\tNaN\t\t\t\t\t\t\t\t\t\n",
                "select last_not_null(a0) a0," +
                        "     last_not_null(a1) a1," +
                        "     last_not_null(a2) a2," +
                        "     last_not_null(a3) a3," +
                        "     last_not_null(a4) a4," +
                        "     last_not_null(a5) a5," +
                        "     last_not_null(a6) a6," +
                        "     last_not_null(a7) a7," +
                        "     last_not_null(a8) a8," +
                        "     last_not_null(a9) a9, " +
                        "     last_not_null(a10) a10, " +
                        "     last_not_null(a11) a11, " +
                        "     last_not_null(a12) a12, " +
                        "     last_not_null(a13) a13, " +
                        "     last_not_null(a14) a14 " +
                        "from tab",
                "create table tab as ( " +
                        "select cast(null as char) a0," +
                        "       cast(null as date) a1," +
                        "       cast(null as double) a2," +
                        "       cast(null as float) a3," +
                        "       cast(null as int) a4," +
                        "       cast(null as long) a5," +
                        "       cast(null as symbol) a6," +
                        "       cast(null as timestamp) a7," +
                        "       cast(null as uuid) a8," +
                        "       cast(null as string) a9," +
                        "       cast(null as geohash(5b)) a10, " +
                        "       cast(null as geohash(10b)) a11, " +
                        "       cast(null as geohash(25b)) a12, " +
                        "       cast(null as geohash(35b)) a13, " +
                        "       cast(null as ipv4) a14 " +
                        "from long_sequence(3))",
                null,
                false,
                true
        );
    }

    @Test
    public void testLastNotNull() throws Exception {
        UUID lastUuid = UUID.randomUUID();

        ddl("create table tab (a0 char," +
                "a1 date," +
                "a2 double," +
                "a3 float," +
                "a4 int," +
                "a5 long," +
                "a6 symbol," +
                "a7 timestamp," +
                "a8 uuid," +
                "a9 string," +
                "a10 geohash(5b)," +
                "a11 geohash(10b)," +
                "a12 geohash(25b)," +
                "a13 geohash(35b)," +
                "a14 ipv4 " +
                ")");

        insert("insert into tab values(" +
                "'b'," +
                "to_date('2023-10-22','yyyy-MM-dd')," +
                "22.2," +
                "33.3," +
                "44," +
                "55," +
                "'b_symbol'," +
                "to_timestamp('2023-10-22T01:02:03.000000','yyyy-MM-ddTHH:mm:ss.SSSUUU')," +
                "rnd_uuid4()," +
                "'b_string'," +
                " #v," +
                " #vv," +
                " #vvvvv," +
                " #vvvvvvv, " +
                " '2.0.0.0'" +
                ")");

        insert("insert into tab values(" +
                "'a', " +
                "to_date('2023-10-23','yyyy-MM-dd')," +
                "2.2," +
                "3.3," +
                "4," +
                "5," +
                "'a_symbol'," +
                "to_timestamp('2023-10-23T12:34:59.000000','yyyy-MM-ddTHH:mm:ss.SSSUUU')," +
                "'" + lastUuid + "'," +
                "'a_string'," +
                " #u," +
                " #uu," +
                " #uuuuu," +
                " #uuuuuuu, " +
                " '1.0.0.0'" +
                ")");

        insert("insert into tab (a1) values (null)"); // other columns default to null

        assertSql(
                "a0\ta1\ta2\ta3\ta4\ta5\ta6\ta7\ta8\ta9\ta10\ta11\ta12\ta13\ta14\n" +
                        "a\t2023-10-23T00:00:00.000Z\t2.2\t3.3000\t4\t5\ta_symbol\t2023-10-23T12:34:59.000000Z\t" + lastUuid + "\ta_string\tu\tuu\tuuuuu\tuuuuuuu\t1.0.0.0\n",
                "select last_not_null(a0) a0," +
                        "     last_not_null(a1) a1," +
                        "     last_not_null(a2) a2," +
                        "     last_not_null(a3) a3," +
                        "     last_not_null(a4) a4," +
                        "     last_not_null(a5) a5," +
                        "     last_not_null(a6) a6," +
                        "     last_not_null(a7) a7," +
                        "     last_not_null(a8) a8," +
                        "     last_not_null(a9) a9, " +
                        "     last_not_null(a10) a10, " +
                        "     last_not_null(a11) a11, " +
                        "     last_not_null(a12) a12, " +
                        "     last_not_null(a13) a13, " +
                        "     last_not_null(a14) a14 " +
                        "from tab"
        );
    }
}
