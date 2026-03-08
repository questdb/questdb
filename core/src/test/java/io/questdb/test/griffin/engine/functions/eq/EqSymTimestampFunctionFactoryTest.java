/*******************************************************************************
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

package io.questdb.test.griffin.engine.functions.eq;


import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.NanosTimestampDriver;
import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class EqSymTimestampFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testBasicConstant() throws SqlException {
        assertSql("""
                column
                true
                """, "select '2017-01-01'::symbol = '2017-01-01'::timestamp");
        assertSql("""
                column
                true
                """, "select '2017-01-01'::symbol = '2017-01-01'::timestamp_ns");

    }

    @Test
    public void testDynamicCast() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select rnd_timestamp(0, 86400000, 0) t from long_sequence(10))");
            execute("alter table x add column sym symbol");
            execute("update x set sym = t::symbol");

            assertSql(
                    """
                            sym\tt
                            37847040\t1970-01-01T00:00:37.847040Z
                            71892425\t1970-01-01T00:01:11.892425Z
                            32891513\t1970-01-01T00:00:32.891513Z
                            58263256\t1970-01-01T00:00:58.263256Z
                            69433038\t1970-01-01T00:01:09.433038Z
                            49660563\t1970-01-01T00:00:49.660563Z
                            28354879\t1970-01-01T00:00:28.354879Z
                            25030044\t1970-01-01T00:00:25.030044Z
                            13225820\t1970-01-01T00:00:13.225820Z
                            60453090\t1970-01-01T00:01:00.453090Z
                            """,
                    "select sym, t from x where sym = t"
            );
        });
    }

    @Test
    public void testDynamicCast1() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select rnd_timestamp_ns(0, 86400000000, 0) t from long_sequence(10))");
            execute("alter table x add column sym symbol");
            execute("update x set sym = t::symbol");

            assertSql(
                    """
                            sym\tt
                            28258937621\t1970-01-01T00:00:28.258937621Z
                            84704108866\t1970-01-01T00:01:24.704108866Z
                            4684409603\t1970-01-01T00:00:04.684409603Z
                            29566122052\t1970-01-01T00:00:29.566122052Z
                            62471208567\t1970-01-01T00:01:02.471208567Z
                            17048275024\t1970-01-01T00:00:17.048275024Z
                            80408674000\t1970-01-01T00:01:20.408674000Z
                            23080510639\t1970-01-01T00:00:23.080510639Z
                            82509017689\t1970-01-01T00:01:22.509017689Z
                            28729051699\t1970-01-01T00:00:28.729051699Z
                            """,
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
                    """
                            sym\tt
                            37847040\t1970-01-01T00:00:37.847040Z
                            """,
                    "select sym, t from x where '37847040'::symbol = t"
            );
        });
    }

    @Test
    public void testDynamicCastConst1() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select rnd_timestamp_ns(0, 86400000000, 0) t from long_sequence(10))");
            execute("alter table x add column sym symbol");
            execute("update x set sym = t::symbol");

            assertSql(
                    """
                            sym\tt
                            82509017689\t1970-01-01T00:01:22.509017689Z
                            """,
                    "select sym, t from x where '82509017689'::symbol = t"
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
    public void testDynamicCastNull1() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select rnd_timestamp_ns(0, 86400000000, 0) t from long_sequence(10))");
            execute("alter table x add column sym symbol");
            execute("update x set sym = t::symbol");

            assertSql(
                    "sym\tt\n",
                    "select sym, t from x where sym = null::timestamp_ns"
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
                    """
                            sym\tt
                            37847040\t1970-01-01T00:00:37.847040Z
                            \t
                            47753932\t1970-01-01T00:00:47.753932Z
                            85842605\t1970-01-01T00:01:25.842605Z
                            63734605\t1970-01-01T00:01:03.734605Z
                            49228924\t1970-01-01T00:00:49.228924Z
                            8395072\t1970-01-01T00:00:08.395072Z
                            63602242\t1970-01-01T00:01:03.602242Z
                            74504721\t1970-01-01T00:01:14.504721Z
                            \t
                            """,
                    "select sym, t from x where sym = t"
            );
        });
    }

    @Test
    public void testDynamicCastNulls1() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select rnd_timestamp_ns(0, 86400000000, 3) t from long_sequence(10))");
            execute("alter table x add column sym symbol");
            execute("update x set sym = t::symbol");

            assertSql(
                    """
                            sym\tt
                            28258937621\t1970-01-01T00:00:28.258937621Z
                            \t
                            17536982995\t1970-01-01T00:00:17.536982995Z
                            35652982957\t1970-01-01T00:00:35.652982957Z
                            65390153277\t1970-01-01T00:01:05.390153277Z
                            15568952078\t1970-01-01T00:00:15.568952078Z
                            74965011151\t1970-01-01T00:01:14.965011151Z
                            12591706140\t1970-01-01T00:00:12.591706140Z
                            23253230564\t1970-01-01T00:00:23.253230564Z
                            \t
                            """,
                    "select sym, t from x where sym = t"
            );
        });
    }

    @Test
    public void testDynamicSymbolTable() throws Exception {
        assertMemoryLeak(() -> {
            assertSql(
                    """
                            x
                            3
                            8
                            10
                            """,
                    "select x from long_sequence(10) where rnd_symbol('1','3','5') = 3::timestamp"
            );

            assertSql(
                    """
                            x
                            1
                            3
                            4
                            5
                            8
                            10
                            """,
                    "select x from long_sequence(10) where rnd_symbol('1','3','5') = 3::timestamp_ns"
            );
        });
    }

    @Test
    public void testOptimisationWithARuntimeConstantNanoTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select rnd_timestamp_ns(0, 86400000000, 0) t from long_sequence(10))");
            execute("alter table x add column sym symbol");
            execute("update x set sym = t::symbol");

            String query = "select sym, t from x where sym = $1";

            bindVariableService.setTimestampNano(0, NanosTimestampDriver.INSTANCE.parseFloorLiteral("1970-01-01T00:00:28.258937621Z"));
            assertQuery("""
                    sym\tt
                    28258937621\t1970-01-01T00:00:28.258937621Z
                    """, query, "", true, false);

        });
    }

    @Test
    public void testOptimisationWithARuntimeConstantTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select rnd_timestamp(0, 86400000, 0) t from long_sequence(10))");
            execute("alter table x add column sym symbol");
            execute("update x set sym = t::symbol");

            String query = "select sym, t from x where sym = $1";

            bindVariableService.setTimestamp(0, MicrosTimestampDriver.INSTANCE.parseFloorLiteral("1970-01-01T00:00:37.847040Z"));
            assertQuery("""
                    sym\tt
                    37847040\t1970-01-01T00:00:37.847040Z
                    """, query, "", true, false);

        });
    }

    @Test
    public void testStaticSymbolTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select rnd_symbol('1','3','5') a from long_sequence(10))");
            assertSql(
                    """
                            a
                            3
                            3
                            3
                            """,
                    "select a from x where a = 3::timestamp"
            );
        });
    }

    @Test
    public void testStaticSymbolTableNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select rnd_symbol('1','3','5', null) a from long_sequence(30))");
            assertSql(
                    """
                            a
                            
                            
                            
                            
                            
                            
                            
                            
                            """,
                    "select a from x where a = null::timestamp"
            );
            assertSql(
                    """
                            a
                            
                            
                            
                            
                            
                            
                            
                            
                            """,
                    "select a from x where a = null::timestamp_ns"
            );
        });
    }
}
