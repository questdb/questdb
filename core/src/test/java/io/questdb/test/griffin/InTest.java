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

package io.questdb.test.griffin;

import io.questdb.std.Numbers;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class InTest extends AbstractCairoTest {

    @Test
    public void testInChar_const() throws Exception {
        // single-char constant
        assertQuery("select * from tab WHERE ch in ('1'::char)")
                .ddl("create table tab as (select timestamp_sequence('2020-01-01', 10 * 60 * 1000000L) ts, x::string::char ch from long_sequence(9))" +
                        " timestamp(ts) PARTITION BY MONTH")
                .timestamp("ts")
                .returns("""
                        ts\tch
                        2020-01-01T00:00:00.000000Z\t1
                        """);

        // '' literal is treated as a zero char
        assertQuery("select ts, ch from tab2 where ch in ('')")
                .ddl("create table tab2 as (select timestamp_sequence('2020-01-01', 10 * 60 * 1000000L) ts, (x-1)::char ch from long_sequence(9))" +
                        " timestamp(ts) PARTITION BY MONTH")
                .timestamp("ts")
                .returns("""
                        ts\tch
                        2020-01-01T00:00:00.000000Z\t
                        """);

        // empty varchar is also treated as a zero char
        assertQuery("select ts, ch from tab3 where ch in (''::varchar)")
                .ddl("create table tab3 as (select timestamp_sequence('2020-01-01', 10 * 60 * 1000000L) ts, (x-1)::char ch from long_sequence(9))" +
                        " timestamp(ts) PARTITION BY MONTH")
                .timestamp("ts")
                .returns("""
                        ts\tch
                        2020-01-01T00:00:00.000000Z\t
                        """);
    }

    @Test
    public void testInDouble_const() throws Exception {
        assertQuery("select * from tab WHERE d in (null, 1::byte, 2::short, 3::int, 4::long, 5::float, 6::double, '7'::string, '8'::symbol, '9'::varchar)")
                .ddl("create table tab as (select timestamp_sequence('2020-01-01', 10 * 60 * 1000000L) ts, x::double d from long_sequence(20))" +
                        " timestamp(ts) PARTITION BY MONTH")
                .timestamp("ts")
                .returns("""
                        ts\td
                        2020-01-01T00:00:00.000000Z\t1.0
                        2020-01-01T00:10:00.000000Z\t2.0
                        2020-01-01T00:20:00.000000Z\t3.0
                        2020-01-01T00:30:00.000000Z\t4.0
                        2020-01-01T00:40:00.000000Z\t5.0
                        2020-01-01T00:50:00.000000Z\t6.0
                        2020-01-01T01:00:00.000000Z\t7.0
                        2020-01-01T01:10:00.000000Z\t8.0
                        2020-01-01T01:20:00.000000Z\t9.0
                        """);
    }

    @Test
    public void testInDouble_nonConst() throws Exception {
        assertQuery("select * from tab WHERE d in (fixed_byte, fixed_short, fixed_int, fixed_long, fixed_float, fixed_double, fixed_string, fixed_symbol, fixed_varchar)")
                .ddl("create table tab as (select timestamp_sequence('2020-01-01', 10 * 60 * 1000000L) ts, " +
                        "x::double d, " +
                        "1::byte as fixed_byte, " +
                        "2::short as fixed_short, " +
                        "3::int as fixed_int, " +
                        "4::long as fixed_long, " +
                        "5::float as fixed_float, " +
                        "6::double as fixed_double, " +
                        "7::string as fixed_string, " +
                        "8::symbol as fixed_symbol, " +
                        "9::varchar as fixed_varchar, " +
                        "from long_sequence(20))" +
                        " timestamp(ts) PARTITION BY MONTH")
                .timestamp("ts")
                .returns("""
                        ts\td\tfixed_byte\tfixed_short\tfixed_int\tfixed_long\tfixed_float\tfixed_double\tfixed_string\tfixed_symbol\tfixed_varchar
                        2020-01-01T00:00:00.000000Z\t1.0\t1\t2\t3\t4\t5.0\t6.0\t7\t8\t9
                        2020-01-01T00:10:00.000000Z\t2.0\t1\t2\t3\t4\t5.0\t6.0\t7\t8\t9
                        2020-01-01T00:20:00.000000Z\t3.0\t1\t2\t3\t4\t5.0\t6.0\t7\t8\t9
                        2020-01-01T00:30:00.000000Z\t4.0\t1\t2\t3\t4\t5.0\t6.0\t7\t8\t9
                        2020-01-01T00:40:00.000000Z\t5.0\t1\t2\t3\t4\t5.0\t6.0\t7\t8\t9
                        2020-01-01T00:50:00.000000Z\t6.0\t1\t2\t3\t4\t5.0\t6.0\t7\t8\t9
                        2020-01-01T01:00:00.000000Z\t7.0\t1\t2\t3\t4\t5.0\t6.0\t7\t8\t9
                        2020-01-01T01:10:00.000000Z\t8.0\t1\t2\t3\t4\t5.0\t6.0\t7\t8\t9
                        2020-01-01T01:20:00.000000Z\t9.0\t1\t2\t3\t4\t5.0\t6.0\t7\t8\t9
                        """);
    }

    @Test
    public void testInLong_const() throws Exception {
        assertQuery("select * from tab WHERE l in (null, 1::byte, 2::short, 3::int, 4::long, 5::timestamp, '6'::string, '7'::symbol, '8'::varchar)")
                .ddl("create table tab as (select timestamp_sequence('2020-01-01', 10 * 60 * 1000000L) ts, x::long l from long_sequence(20))" +
                        " timestamp(ts) PARTITION BY MONTH")
                .timestamp("ts")
                .returns("""
                        ts\tl
                        2020-01-01T00:00:00.000000Z\t1
                        2020-01-01T00:10:00.000000Z\t2
                        2020-01-01T00:20:00.000000Z\t3
                        2020-01-01T00:30:00.000000Z\t4
                        2020-01-01T00:40:00.000000Z\t5
                        2020-01-01T00:50:00.000000Z\t6
                        2020-01-01T01:00:00.000000Z\t7
                        2020-01-01T01:10:00.000000Z\t8
                        """);
    }

    @Test
    public void testInLong_nonConst() throws Exception {
        assertQuery("select * from tab WHERE l in (fixed_byte, fixed_short, fixed_int, fixed_long, fixed_timestamp, fixed_string, fixed_symbol, fixed_varchar)")
                .ddl("create table tab as (select timestamp_sequence('2020-01-01', 10 * 60 * 1000000L) ts, " +
                        "x::long l, " +
                        "1::byte as fixed_byte, " +
                        "2::short as fixed_short, " +
                        "3::int as fixed_int, " +
                        "4::long as fixed_long, " +
                        "5::timestamp as fixed_timestamp, " +
                        "6::string as fixed_string, " +
                        "7::symbol as fixed_symbol, " +
                        "8::varchar as fixed_varchar, " +
                        "from long_sequence(20))" +
                        " timestamp(ts) PARTITION BY MONTH")
                .timestamp("ts")
                .returns("""
                        ts\tl\tfixed_byte\tfixed_short\tfixed_int\tfixed_long\tfixed_timestamp\tfixed_string\tfixed_symbol\tfixed_varchar
                        2020-01-01T00:00:00.000000Z\t1\t1\t2\t3\t4\t1970-01-01T00:00:00.000005Z\t6\t7\t8
                        2020-01-01T00:10:00.000000Z\t2\t1\t2\t3\t4\t1970-01-01T00:00:00.000005Z\t6\t7\t8
                        2020-01-01T00:20:00.000000Z\t3\t1\t2\t3\t4\t1970-01-01T00:00:00.000005Z\t6\t7\t8
                        2020-01-01T00:30:00.000000Z\t4\t1\t2\t3\t4\t1970-01-01T00:00:00.000005Z\t6\t7\t8
                        2020-01-01T00:40:00.000000Z\t5\t1\t2\t3\t4\t1970-01-01T00:00:00.000005Z\t6\t7\t8
                        2020-01-01T00:50:00.000000Z\t6\t1\t2\t3\t4\t1970-01-01T00:00:00.000005Z\t6\t7\t8
                        2020-01-01T01:00:00.000000Z\t7\t1\t2\t3\t4\t1970-01-01T00:00:00.000005Z\t6\t7\t8
                        2020-01-01T01:10:00.000000Z\t8\t1\t2\t3\t4\t1970-01-01T00:00:00.000005Z\t6\t7\t8
                        """);
    }

    @Test
    public void testInStr_const() throws Exception {
        assertQuery("select * from tab WHERE s in (null, 1::string, 2::varchar, 3::symbol, '4'::char)")
                .ddl("create table tab as (select timestamp_sequence('2020-01-01', 10 * 60 * 1000000L) ts, x::string s from long_sequence(20))" +
                        " timestamp(ts) PARTITION BY MONTH")
                .timestamp("ts")
                .returns("""
                        ts\ts
                        2020-01-01T00:00:00.000000Z\t1
                        2020-01-01T00:10:00.000000Z\t2
                        2020-01-01T00:20:00.000000Z\t3
                        2020-01-01T00:30:00.000000Z\t4
                        """);
    }

    @Test
    public void testInSymbol_const() throws Exception {
        assertQuery("select * from tab WHERE s in (null, 1::string, 2::varchar, 3::symbol, '4'::char)")
                .ddl("create table tab as (select timestamp_sequence('2020-01-01', 10 * 60 * 1000000L) ts, x::symbol s from long_sequence(20))" +
                        " timestamp(ts) PARTITION BY MONTH")
                .timestamp("ts")
                .returns("""
                        ts\ts
                        2020-01-01T00:00:00.000000Z\t1
                        2020-01-01T00:10:00.000000Z\t2
                        2020-01-01T00:20:00.000000Z\t3
                        2020-01-01T00:30:00.000000Z\t4
                        """);
    }

    @Test
    public void testInSymbol_escapedConstant() throws Exception {
        assertQuery("select * from tab WHERE s in ('1''suffix', '2''suffix', '3''suffix', '4''suffix')")
                .ddl("create table tab as (select timestamp_sequence('2020-01-01', 10 * 60 * 1000000L) ts, concat(x::symbol, '''', 'suffix')::symbol s from long_sequence(20))" +
                        " timestamp(ts) PARTITION BY MONTH")
                .timestamp("ts")
                .returns("""
                        ts\ts
                        2020-01-01T00:00:00.000000Z\t1'suffix
                        2020-01-01T00:10:00.000000Z\t2'suffix
                        2020-01-01T00:20:00.000000Z\t3'suffix
                        2020-01-01T00:30:00.000000Z\t4'suffix
                        """);
    }

    @Test
    public void testInUuid_const() throws Exception {
        assertQuery("select * from tab where u in ('0010cde8-12ce-40ee-8010-a928bb8b9650', '9f9b2131-d49f-4d1d-ab81-39815c50d341'::varchar, '7bcd48d8-c77a-4655-b2a2-15ba0462ad15'::symbol);")
                .ddl("create table tab as (select timestamp_sequence('2020-01-01', 10 * 60 * 1000000L) ts, rnd_uuid4() u from long_sequence(20))" +
                        " timestamp(ts) PARTITION BY MONTH")
                .timestamp("ts")
                .returns("""
                        ts\tu
                        2020-01-01T00:00:00.000000Z\t0010cde8-12ce-40ee-8010-a928bb8b9650
                        2020-01-01T00:10:00.000000Z\t9f9b2131-d49f-4d1d-ab81-39815c50d341
                        2020-01-01T00:20:00.000000Z\t7bcd48d8-c77a-4655-b2a2-15ba0462ad15
                        """);
    }

    @Test
    public void testIntInWithSentinelValues() throws Exception {
        execute("CREATE TABLE anomaly_log AS (" +
                "SELECT " +
                "  timestamp_sequence('2025-10-24', 3600000000L) ts, " +
                "  rnd_symbol('S3', 'O1', 'O2') type, " +
                "  rnd_int(1, 3, 0) risk, " +
                "  rnd_int(-2, 0, 3) action " +
                "FROM long_sequence(1000)" +
                ") TIMESTAMP(ts) PARTITION BY MONTH");

        assertQuery("SELECT count(*) FROM anomaly_log " +
                        "where action IN (-2, 0, -1) ")
                .withPlan("""
                        Count
                            Async JIT Filter workers: 1
                              filter: action in [-2,-1,0]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: anomaly_log
                        """)
                .noRandomAccess()
                .expectSize()
                .returns("count\n889\n");
        assertQuery("SELECT count(*) FROM anomaly_log " +
                        "where action IN (null, -2, 0, -1) ")
                .withPlan("""
                        Count
                            Async JIT Filter workers: 1
                              filter: action in [null,-2,-1,0]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: anomaly_log
                        """)
                .noRandomAccess()
                .expectSize()
                .returns("count\n1000\n");

        bindVariableService.setInt("a", -1);
        bindVariableService.setInt("b", -2);
        bindVariableService.setInt("c", 0);
        bindVariableService.setInt("d", Numbers.INT_NULL);
        assertQuery("SELECT count(*) FROM anomaly_log " +
                        "where action IN (:a, :b, :c, :d) ")
                .withPlan("""
                        Count
                            Async JIT Filter workers: 1
                              filter: action in [null,-2,-1,0]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: anomaly_log
                        """)
                .noRandomAccess()
                .expectSize()
                .returns("count\n1000\n");
    }

    @Test
    public void testSymbolInVarcharSubquery() throws Exception {
        //
        assertQuery("""
                select *
                from x
                where sym in (select vch from x where sym = 'baz')
                """)
                .ddl("""
                        create table x as (
                          select x as value,
                                 rnd_symbol('foo','bar','baz') sym,
                                 rnd_symbol('foo','bar','baz')::varchar vch,
                                 cast(x as timestamp) ts
                          from long_sequence(10)
                        ) timestamp(ts) partition by day""")
                .timestamp("ts")
                .returns("""
                        value\tsym\tvch\tts
                        2\tbar\tbaz\t1970-01-01T00:00:00.000002Z
                        3\tbaz\tbaz\t1970-01-01T00:00:00.000003Z
                        4\tbaz\tbar\t1970-01-01T00:00:00.000004Z
                        6\tbar\tbaz\t1970-01-01T00:00:00.000006Z
                        7\tbar\tbar\t1970-01-01T00:00:00.000007Z
                        8\tbar\tfoo\t1970-01-01T00:00:00.000008Z
                        """);
    }
}
