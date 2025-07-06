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

package io.questdb.test.griffin.engine.functions.eq;


import io.questdb.griffin.SqlException;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class EqSymTimestampFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testBasicConstant() throws SqlException {
        assertSql("column\n" +
                "true\n", "select '2017-01-01'::symbol = '2017-01-01'::timestamp");

    }

    @Test
    public void testDynamicCast() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select rnd_timestamp(0, 86400000, 0) t from long_sequence(10))");
            execute("alter table x add column sym symbol");
            execute("update x set sym = t::symbol");

            assertSql(
                    "sym\tt\n" +
                            "37847040\t1970-01-01T00:00:37.847040Z\n" +
                            "71892425\t1970-01-01T00:01:11.892425Z\n" +
                            "32891513\t1970-01-01T00:00:32.891513Z\n" +
                            "58263256\t1970-01-01T00:00:58.263256Z\n" +
                            "69433038\t1970-01-01T00:01:09.433038Z\n" +
                            "49660563\t1970-01-01T00:00:49.660563Z\n" +
                            "28354879\t1970-01-01T00:00:28.354879Z\n" +
                            "25030044\t1970-01-01T00:00:25.030044Z\n" +
                            "13225820\t1970-01-01T00:00:13.225820Z\n" +
                            "60453090\t1970-01-01T00:01:00.453090Z\n",
                    "select sym, t from x where sym = t"
            );
        });
    }

    @Test
    public void testDynamicCastConst() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select rnd_timestamp(0, 86400000, 0) t from long_sequence(10))");
            execute("alter table x add column sym symbol");
            execute("update x set sym = t::symbol");

            assertSql(
                    "sym\tt\n" +
                            "37847040\t1970-01-01T00:00:37.847040Z\n",
                    "select sym, t from x where '37847040'::symbol = t"
            );
        });
    }

    @Test
    public void testDynamicCastNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select rnd_timestamp(0, 86400000, 0) t from long_sequence(10))");
            execute("alter table x add column sym symbol");
            execute("update x set sym = t::symbol");

            assertSql(
                    "sym\tt\n",
                    "select sym, t from x where sym = null::timestamp"
            );
        });
    }

    @Test
    public void testDynamicCastNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select rnd_timestamp(0, 86400000, 3) t from long_sequence(10))");
            execute("alter table x add column sym symbol");
            execute("update x set sym = t::symbol");

            assertSql(
                    "sym\tt\n" +
                            "37847040\t1970-01-01T00:00:37.847040Z\n" +
                            "\t\n" +
                            "47753932\t1970-01-01T00:00:47.753932Z\n" +
                            "85842605\t1970-01-01T00:01:25.842605Z\n" +
                            "63734605\t1970-01-01T00:01:03.734605Z\n" +
                            "49228924\t1970-01-01T00:00:49.228924Z\n" +
                            "8395072\t1970-01-01T00:00:08.395072Z\n" +
                            "63602242\t1970-01-01T00:01:03.602242Z\n" +
                            "74504721\t1970-01-01T00:01:14.504721Z\n" +
                            "\t\n",
                    "select sym, t from x where sym = t"
            );
        });
    }

    @Test
    public void testDynamicSymbolTable() throws Exception {
        assertMemoryLeak(() -> assertSql(
                "x\n" +
                        "3\n" +
                        "8\n" +
                        "10\n",
                "select x from long_sequence(10) where rnd_symbol('1','3','5') = 3::timestamp"
        ));
    }

    @Test
    public void testOptimisationWithARuntimeConstantTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select rnd_timestamp(0, 86400000, 0) t from long_sequence(10))");
            execute("alter table x add column sym symbol");
            execute("update x set sym = t::symbol");

            String query = "select sym, t from x where sym = $1";

            bindVariableService.setTimestamp(0, TimestampFormatUtils.parseTimestamp("1970-01-01T00:00:37.847040Z"));
            assertQuery("sym\tt\n" +
                    "37847040\t1970-01-01T00:00:37.847040Z\n", query, "", true, false);

        });
    }

    @Test
    public void testStaticSymbolTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select rnd_symbol('1','3','5') a from long_sequence(10))");
            assertSql(
                    "a\n" +
                            "3\n" +
                            "3\n" +
                            "3\n",
                    "select a from x where a = 3::timestamp"
            );
        });
    }

    @Test
    public void testStaticSymbolTableNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select rnd_symbol('1','3','5', null) a from long_sequence(30))");
            assertSql(
                    "a\n" +
                            "\n" +
                            "\n" +
                            "\n" +
                            "\n" +
                            "\n" +
                            "\n" +
                            "\n" +
                            "\n",
                    "select a from x where a = null::timestamp"
            );
        });
    }
}