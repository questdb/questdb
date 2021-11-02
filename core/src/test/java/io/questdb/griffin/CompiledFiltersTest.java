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

import org.junit.Test;

public class CompiledFiltersTest extends AbstractGriffinTest {

    @Test
    public void testEqConst() throws Exception {
        final String query = "select * from x where i8 = 1 and i16 = 1 and i32 = 1 and i64 = 1 and f32 = 1.0 and f64 = 1.0";
        final String expected = "k\ti8\ti16\ti32\ti64\tf32\tf64\n" +
                "1970-01-05T15:06:40.000000Z\t1\t1\t1\t1\t1.0000\t1.0\n";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " cast(x as byte) i8," +
                " cast(x as short) i16," +
                " cast(x as int) i32," +
                " cast(x as long) i64," +
                " cast(x as float) f32," +
                " cast(x as double) f64" +
                " from long_sequence(1000)) timestamp(k) partition by DAY";
        assertQuery(expected,
                query,
                ddl,
                "k",
                false);
    }

    @Test
    public void testNotEqConst() throws Exception {
        final String query = "select * from x where i8 <> 1 and i16 <> 1 and i32 <> 1 and i64 <> 1 and f32 <> 1.0000 and f64 <> 1.0";
        final String expected = "k\ti8\ti16\ti32\ti64\tf32\tf64\n" +
                "1970-01-05T15:15:00.000000Z\t2\t2\t2\t2\t2.0000\t2.0\n" +
                "1970-01-05T15:23:20.000000Z\t3\t3\t3\t3\t3.0000\t3.0\n" +
                "1970-01-05T15:31:40.000000Z\t4\t4\t4\t4\t4.0000\t4.0\n" +
                "1970-01-05T15:40:00.000000Z\t5\t5\t5\t5\t5.0000\t5.0\n" +
                "1970-01-05T15:48:20.000000Z\t6\t6\t6\t6\t6.0000\t6.0\n" +
                "1970-01-05T15:56:40.000000Z\t7\t7\t7\t7\t7.0000\t7.0\n" +
                "1970-01-05T16:05:00.000000Z\t8\t8\t8\t8\t8.0000\t8.0\n" +
                "1970-01-05T16:13:20.000000Z\t9\t9\t9\t9\t9.0000\t9.0\n" +
                "1970-01-05T16:21:40.000000Z\t10\t10\t10\t10\t10.0000\t10.0\n";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " cast(x as byte) i8," +
                " cast(x as short) i16," +
                " cast(x as int) i32," +
                " cast(x as long) i64," +
                " cast(x as float) f32," +
                " cast(x as double) f64" +
                " from long_sequence(10)) timestamp(k)";
        assertQuery(expected,
                query,
                ddl,
                "k",
                false);
    }

    @Test
    public void testLtConst() throws Exception {
        final String query = "select * from x where i8 < 2 and i16 < 2 and i32 < 2 and i64 < 2 and f32 < 2.0000 and f64 < 2.0";
        final String expected = "k\ti8\ti16\ti32\ti64\tf32\tf64\n" +
                "1970-01-05T15:06:40.000000Z\t1\t1\t1\t1\t1.0000\t1.0\n";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " cast(x as byte) i8," +
                " cast(x as short) i16," +
                " cast(x as int) i32," +
                " cast(x as long) i64," +
                " cast(x as float) f32," +
                " cast(x as double) f64" +
                " from long_sequence(10)) timestamp(k)";
        assertQuery(expected,
                query,
                ddl,
                "k",
                false);
    }

    @Test
    public void testLeConst() throws Exception {
        final String query = "select * from x where i8 <= 2 and i16 <= 2 and i32 <= 2 and i64 <= 2 and f32 <= 2.0000 and f64 <= 2.0";
        final String expected = "k\ti8\ti16\ti32\ti64\tf32\tf64\n" +
                "1970-01-05T15:06:40.000000Z\t1\t1\t1\t1\t1.0000\t1.0\n" +
                "1970-01-05T15:15:00.000000Z\t2\t2\t2\t2\t2.0000\t2.0\n";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " cast(x as byte) i8," +
                " cast(x as short) i16," +
                " cast(x as int) i32," +
                " cast(x as long) i64," +
                " cast(x as float) f32," +
                " cast(x as double) f64" +
                " from long_sequence(10)) timestamp(k)";
        assertQuery(expected,
                query,
                ddl,
                "k",
                false);
    }

    @Test
    public void testGtConst() throws Exception {
        final String query = "select * from x where i8 > 9 and i16 > 9 and i32 > 9 and i64 > 9 and f32 > 9.0000 and f64 > 9.0";
        final String expected = "k\ti8\ti16\ti32\ti64\tf32\tf64\n" +
                "1970-01-05T16:21:40.000000Z\t10\t10\t10\t10\t10.0000\t10.0\n";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " cast(x as byte) i8," +
                " cast(x as short) i16," +
                " cast(x as int) i32," +
                " cast(x as long) i64," +
                " cast(x as float) f32," +
                " cast(x as double) f64" +
                " from long_sequence(10)) timestamp(k)";
        assertQuery(expected,
                query,
                ddl,
                "k",
                false);
    }

    @Test
    public void testGeConst() throws Exception {
        final String query = "select * from x where i8 >= 9 and i16 >= 9 and i32 >= 9 and i64 >= 9 and f32 >= 9.0000 and f64 >= 9.0";
        final String expected = "k\ti8\ti16\ti32\ti64\tf32\tf64\n" +
                "1970-01-05T16:13:20.000000Z\t9\t9\t9\t9\t9.0000\t9.0\n" +
                "1970-01-05T16:21:40.000000Z\t10\t10\t10\t10\t10.0000\t10.0\n";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " cast(x as byte) i8," +
                " cast(x as short) i16," +
                " cast(x as int) i32," +
                " cast(x as long) i64," +
                " cast(x as float) f32," +
                " cast(x as double) f64" +
                " from long_sequence(10)) timestamp(k)";
        assertQuery(expected,
                query,
                ddl,
                "k",
                false);
    }

    @Test
    public void testAddConst() throws Exception {
        final String query = "select * from x where i8 + 1 = 10 and i16 + 1 = 10 and i32 + 1 = 10 and i64 + 1 = 10 " +
                "and f32 + 1.0 = 10.0 and f64 + 1.0 = 10.0";
        final String expected = "k\ti8\ti16\ti32\ti64\tf32\tf64\n" +
                "1970-01-05T16:13:20.000000Z\t9\t9\t9\t9\t9.0000\t9.0\n";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " cast(x as byte) i8," +
                " cast(x as short) i16," +
                " cast(x as int) i32," +
                " cast(x as long) i64," +
                " cast(x as float) f32," +
                " cast(x as double) f64" +
                " from long_sequence(10)) timestamp(k)";
        assertQuery(expected,
                query,
                ddl,
                "k",
                false);
    }

    @Test
    public void testSubConst() throws Exception {
        final String query = "select * from x where i8 - 1 = 2 and i16 - 1 = 2 and i32 - 1 = 2 and i64 - 1 = 2 " +
                "and f32 - 1.0 = 2.0 and f64 - 1.0 = 2.0";
        final String expected = "k\ti8\ti16\ti32\ti64\tf32\tf64\n" +
                "1970-01-05T15:23:20.000000Z\t3\t3\t3\t3\t3.0000\t3.0\n";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " cast(x as byte) i8," +
                " cast(x as short) i16," +
                " cast(x as int) i32," +
                " cast(x as long) i64," +
                " cast(x as float) f32," +
                " cast(x as double) f64" +
                " from long_sequence(10)) timestamp(k)";
        assertQuery(expected,
                query,
                ddl,
                "k",
                false);
    }

    @Test
    public void testMulConst() throws Exception {
        final String query = "select * from x where i8 * 2 = 4 and i16 * 2 = 4 and i32 * 2 = 4 and i64 * 2 = 4" +
                " and f32 * 2.0 = 4.0 and f64 * 2.0 = 4.0";
        final String expected = "k\ti8\ti16\ti32\ti64\tf32\tf64\n" +
                "1970-01-05T15:15:00.000000Z\t2\t2\t2\t2\t2.0000\t2.0\n";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " cast(x as byte) i8," +
                " cast(x as short) i16," +
                " cast(x as int) i32," +
                " cast(x as long) i64," +
                " cast(x as float) f32," +
                " cast(x as double) f64" +
                " from long_sequence(10)) timestamp(k)";
        assertQuery(expected,
                query,
                ddl,
                "k",
                false);
    }

    @Test
    public void testDivConst() throws Exception {
        final String query = "select * from x where i8 / 2 = 4 and i16 / 2 = 4 and i32 / 2 = 4 and i64 / 2 = 4" +
                " and f32 / 2.0 = 4.0 and f64 / 2.0 = 4.0";
        final String expected = "k\ti8\ti16\ti32\ti64\tf32\tf64\n" +
                "1970-01-05T16:05:00.000000Z\t8\t8\t8\t8\t8.0000\t8.0\n";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " cast(x as byte) i8," +
                " cast(x as short) i16," +
                " cast(x as int) i32," +
                " cast(x as long) i64," +
                " cast(x as float) f32," +
                " cast(x as double) f64" +
                " from long_sequence(10)) timestamp(k)";
        assertQuery(expected,
                query,
                ddl,
                "k",
                false);
    }

    @Test
    public void testIntFloatCast() throws Exception {
        final String query = "select * from x where " +
                "i8 = f32 and " +
                "i8 = f64 and " +
                "i16 = f32 and " +
                "i16 = f64 and " +
                "i32 = f32 and " +
                "i32 = f64 and " +
                "i64 = f32 and " +
                "i64 = f64";
        final String expected = "k\ti8\ti16\ti32\ti64\tf32\tf64\n" +
                "1970-01-05T15:06:40.000000Z\t1\t1\t1\t1\t1.0000\t1.0\n" +
                "1970-01-05T15:15:00.000000Z\t2\t2\t2\t2\t2.0000\t2.0\n" +
                "1970-01-05T15:23:20.000000Z\t3\t3\t3\t3\t3.0000\t3.0\n" +
                "1970-01-05T15:31:40.000000Z\t4\t4\t4\t4\t4.0000\t4.0\n" +
                "1970-01-05T15:40:00.000000Z\t5\t5\t5\t5\t5.0000\t5.0\n" +
                "1970-01-05T15:48:20.000000Z\t6\t6\t6\t6\t6.0000\t6.0\n" +
                "1970-01-05T15:56:40.000000Z\t7\t7\t7\t7\t7.0000\t7.0\n" +
                "1970-01-05T16:05:00.000000Z\t8\t8\t8\t8\t8.0000\t8.0\n" +
                "1970-01-05T16:13:20.000000Z\t9\t9\t9\t9\t9.0000\t9.0\n" +
                "1970-01-05T16:21:40.000000Z\t10\t10\t10\t10\t10.0000\t10.0\n";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " cast(x as byte) i8," +
                " cast(x as short) i16," +
                " cast(x as int) i32," +
                " cast(x as long) i64," +
                " cast(x as float) f32," +
                " cast(x as double) f64" +
                " from long_sequence(10)) timestamp(k)";
        assertQuery(expected,
                query,
                ddl,
                "k",
                false);
    }

    @Test
    public void testNegConst() throws Exception {
        final String query = "select * from x where 20 + -i8 = 10 and -f32 * 2 = -20.0";
        final String expected = "k\ti8\ti16\ti32\ti64\tf32\tf64\n" +
                "1970-01-05T16:21:40.000000Z\t10\t10\t10\t10\t10.0000\t10.0\n";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " cast(x as byte) i8," +
                " cast(x as short) i16," +
                " cast(x as int) i32," +
                " cast(x as long) i64," +
                " cast(x as float) f32," +
                " cast(x as double) f64" +
                " from long_sequence(10)) timestamp(k)";
        assertQuery(expected,
                query,
                ddl,
                "k",
                false);
    }

    @Test
    public void testOrNot() throws Exception {
        final String query = "select * from x where i8 / 2 = 4 or i8 > 6 and not f64 = 10 and not i64 = 7";
        final String expected = "k\ti8\ti16\ti32\ti64\tf32\tf64\n" +
                "1970-01-05T16:05:00.000000Z\t8\t8\t8\t8\t8.0000\t8.0\n" +
                "1970-01-05T16:13:20.000000Z\t9\t9\t9\t9\t9.0000\t9.0\n";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " cast(x as byte) i8," +
                " cast(x as short) i16," +
                " cast(x as int) i32," +
                " cast(x as long) i64," +
                " cast(x as float) f32," +
                " cast(x as double) f64" +
                " from long_sequence(10)) timestamp(k)";
        assertQuery(expected,
                query,
                ddl,
                "k",
                false);
    }

    // test const expr
    // test null
    // test filter on subquery
    // test interval and filter
    // test wrong type expression a+b
    // test join
    // test latestby
}
