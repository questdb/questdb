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

import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class PgTypeFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testPgTypeFunc() throws Exception {
        assertQuery(
                "oid\ttypname\ttypbasetype\ttyparray\ttypnamespace\ttypnotnull\ttyptypmod\ttyptype\ttypcategory\ttyprelid\ttypelem\ttypreceive\ttypdelim\ttypinput\ttypowner\ttyplen\ttypbyval\ttypispreferred\ttypisdefined\ttypalign\ttypstorage\ttypndims\ttypcollation\ttypdefault\n" +
                        "1043\tvarchar\t0\t1015\t11\tfalse\t0\tb\tS\tnull\t0\t0\t0\t0\t0\t-1\tfalse\tfalse\ttrue\tc\tp\t0\t0\t\n" +
                        "1114\ttimestamp\t0\t1115\t11\tfalse\t0\tb\tD\tnull\t0\t0\t0\t0\t0\t8\tfalse\tfalse\ttrue\tc\tp\t0\t0\t\n" +
                        "701\tfloat8\t0\t1022\t11\tfalse\t0\tb\tN\tnull\t0\t0\t0\t0\t0\t8\tfalse\tfalse\ttrue\tc\tp\t0\t0\t\n" +
                        "700\tfloat4\t0\t1021\t11\tfalse\t0\tb\tN\tnull\t0\t0\t0\t0\t0\t4\tfalse\tfalse\ttrue\tc\tp\t0\t0\t\n" +
                        "23\tint4\t0\t1007\t11\tfalse\t0\tb\tN\tnull\t0\t0\t0\t0\t0\t4\tfalse\tfalse\ttrue\tc\tp\t0\t0\t\n" +
                        "21\tint2\t0\t1005\t11\tfalse\t0\tb\tN\tnull\t0\t0\t0\t0\t0\t2\tfalse\tfalse\ttrue\tc\tp\t0\t0\t0\n" +
                        "1042\tbpchar\t0\t0\t11\tfalse\t0\tb\tZ\tnull\t0\t0\t0\t0\t0\t2\tfalse\tfalse\ttrue\tc\tp\t0\t0\t0\n" +
                        "20\tint8\t0\t1016\t11\tfalse\t0\tb\tN\tnull\t0\t0\t0\t0\t0\t8\tfalse\tfalse\ttrue\tc\tp\t0\t0\t\n" +
                        "16\tbool\t0\t1000\t11\tfalse\t0\tb\tB\tnull\t0\t0\t0\t0\t0\t1\tfalse\tfalse\ttrue\tc\tp\t0\t0\tfalse\n" +
                        "17\tbinary\t0\t0\t11\tfalse\t0\tb\tU\tnull\t0\t0\t0\t0\t0\t-1\tfalse\tfalse\ttrue\tc\tp\t0\t0\t\n" +
                        "1082\tdate\t0\t0\t11\tfalse\t0\tb\tD\tnull\t0\t0\t0\t0\t0\t8\tfalse\tfalse\ttrue\tc\tp\t0\t0\t\n" +
                        "2950\tuuid\t0\t0\t11\tfalse\t0\tb\tU\tnull\t0\t0\t0\t0\t0\t16\tfalse\tfalse\ttrue\tc\tp\t0\t0\t\n" +
                        "2281\tinternal\t0\t0\t11\tfalse\t0\tb\tP\tnull\t0\t0\t0\t0\t0\t8\tfalse\tfalse\ttrue\tc\tp\t0\t0\t\n" +
                        "26\toid\t0\t0\t11\tfalse\t0\tb\tN\tnull\t0\t0\t0\t0\t0\t4\tfalse\tfalse\ttrue\tc\tp\t0\t0\t\n" +
                        "1022\t_float8\t0\t0\t11\tfalse\t0\tb\tA\tnull\t701\t0\t0\t0\t0\t-1\tfalse\tfalse\ttrue\tc\tp\t0\t0\t\n" +
                        "1700\tnumeric\t0\t0\t11\tfalse\t0\tb\tN\tnull\t0\t0\t0\t0\t0\t-1\tfalse\tfalse\ttrue\tc\tp\t0\t0\t\n",
                "pg_type;",
                "create table x(a int)",
                null,
                false,
                true
        );
    }

    @Test
    public void testPrefixedPgTypeFunc() throws Exception {
        assertQuery(
                "oid\ttypname\ttypbasetype\ttyparray\ttypnamespace\ttypnotnull\ttyptypmod\ttyptype\ttypcategory\ttyprelid\ttypelem\ttypreceive\ttypdelim\ttypinput\ttypowner\ttyplen\ttypbyval\ttypispreferred\ttypisdefined\ttypalign\ttypstorage\ttypndims\ttypcollation\ttypdefault\n" +
                        "1043\tvarchar\t0\t1015\t11\tfalse\t0\tb\tS\tnull\t0\t0\t0\t0\t0\t-1\tfalse\tfalse\ttrue\tc\tp\t0\t0\t\n" +
                        "1114\ttimestamp\t0\t1115\t11\tfalse\t0\tb\tD\tnull\t0\t0\t0\t0\t0\t8\tfalse\tfalse\ttrue\tc\tp\t0\t0\t\n" +
                        "701\tfloat8\t0\t1022\t11\tfalse\t0\tb\tN\tnull\t0\t0\t0\t0\t0\t8\tfalse\tfalse\ttrue\tc\tp\t0\t0\t\n" +
                        "700\tfloat4\t0\t1021\t11\tfalse\t0\tb\tN\tnull\t0\t0\t0\t0\t0\t4\tfalse\tfalse\ttrue\tc\tp\t0\t0\t\n" +
                        "23\tint4\t0\t1007\t11\tfalse\t0\tb\tN\tnull\t0\t0\t0\t0\t0\t4\tfalse\tfalse\ttrue\tc\tp\t0\t0\t\n" +
                        "21\tint2\t0\t1005\t11\tfalse\t0\tb\tN\tnull\t0\t0\t0\t0\t0\t2\tfalse\tfalse\ttrue\tc\tp\t0\t0\t0\n" +
                        "1042\tbpchar\t0\t0\t11\tfalse\t0\tb\tZ\tnull\t0\t0\t0\t0\t0\t2\tfalse\tfalse\ttrue\tc\tp\t0\t0\t0\n" +
                        "20\tint8\t0\t1016\t11\tfalse\t0\tb\tN\tnull\t0\t0\t0\t0\t0\t8\tfalse\tfalse\ttrue\tc\tp\t0\t0\t\n" +
                        "16\tbool\t0\t1000\t11\tfalse\t0\tb\tB\tnull\t0\t0\t0\t0\t0\t1\tfalse\tfalse\ttrue\tc\tp\t0\t0\tfalse\n" +
                        "17\tbinary\t0\t0\t11\tfalse\t0\tb\tU\tnull\t0\t0\t0\t0\t0\t-1\tfalse\tfalse\ttrue\tc\tp\t0\t0\t\n" +
                        "1082\tdate\t0\t0\t11\tfalse\t0\tb\tD\tnull\t0\t0\t0\t0\t0\t8\tfalse\tfalse\ttrue\tc\tp\t0\t0\t\n" +
                        "2950\tuuid\t0\t0\t11\tfalse\t0\tb\tU\tnull\t0\t0\t0\t0\t0\t16\tfalse\tfalse\ttrue\tc\tp\t0\t0\t\n" +
                        "2281\tinternal\t0\t0\t11\tfalse\t0\tb\tP\tnull\t0\t0\t0\t0\t0\t8\tfalse\tfalse\ttrue\tc\tp\t0\t0\t\n" +
                        "26\toid\t0\t0\t11\tfalse\t0\tb\tN\tnull\t0\t0\t0\t0\t0\t4\tfalse\tfalse\ttrue\tc\tp\t0\t0\t\n" +
                        "1022\t_float8\t0\t0\t11\tfalse\t0\tb\tA\tnull\t701\t0\t0\t0\t0\t-1\tfalse\tfalse\ttrue\tc\tp\t0\t0\t\n" +
                        "1700\tnumeric\t0\t0\t11\tfalse\t0\tb\tN\tnull\t0\t0\t0\t0\t0\t-1\tfalse\tfalse\ttrue\tc\tp\t0\t0\t\n",
                "pg_catalog.pg_type;",
                "create table x(a int)",
                null,
                false,
                true
        );
    }

    @Test
    public void testWithCategory() throws SqlException {
        assertSql("oid\tswitch\n" +
                        "17\tbinary\n" +
                        "2950\tuuid\n",
                "SELECT oid," +
                        "   CASE " +
                        "    WHEN typcategory = 'E' THEN 'varchar' " +
                        "    ELSE typname " +
                        "  END " +
                        "FROM " +
                        "    pg_type " +
                        "WHERE " +
                        "    typcategory in ('U', 'E') ");
    }
}