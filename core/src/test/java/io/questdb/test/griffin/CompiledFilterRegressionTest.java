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

package io.questdb.test.griffin;

import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.SqlException;
import io.questdb.jit.JitUtil;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Basic tests that compare compiled filter output with the Java implementation.
 */
public class CompiledFilterRegressionTest extends AbstractCairoTest {

    private static final Log LOG = LogFactory.getLog(CompiledFilterRegressionTest.class);
    private static final int N_SIMD = 512;
    private static final int N_SIMD_WITH_SCALAR_TAIL = N_SIMD + 3;

    private static final StringSink jitSink = new StringSink();

    @Override
    @Before
    public void setUp() {
        // Disable the test suite on ARM64.
        Assume.assumeTrue(JitUtil.isJitSupported());
        super.setUp();
//        compiler.setEnableJitNullChecks(true);
    }

    @Test
    public void testBoolean() throws Exception {
        final String query = "select * from x where bool1 or bool2 = false";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_boolean() bool1," +
                " rnd_boolean() bool2" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertQueryNotNull(query, ddl);
    }

    @Test
    public void testBooleanOperators() throws Exception {
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_byte() i8," +
                " rnd_short() i16," +
                " rnd_int() i32," +
                " rnd_long() i64," +
                " rnd_float() f32," +
                " rnd_double() f64 " +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        FilterGenerator gen = new FilterGenerator()
                .withOptionalNot().withAnyOf("i8 / 2 = 4")
                .withBooleanOperator()
                .withOptionalNot().withAnyOf("i16 < 0")
                .withBooleanOperator()
                .withOptionalNot().withAnyOf("i32 > 0")
                .withBooleanOperator()
                .withOptionalNot().withAnyOf("i64 <= 0")
                .withBooleanOperator()
                .withOptionalNot().withAnyOf("f32 <= 0.34")
                .withBooleanOperator()
                .withOptionalNot().withAnyOf("f64 > 7.5");
        assertGeneratedQueryNotNull("select * from x", ddl, gen);
    }

    @Test
    public void testChar() throws Exception {
        final String query = "select * from x where ch > 'A' and ch < 'Z'";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_char() ch" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertQueryNotNull(query, ddl);
    }

    @Test
    public void testColumnArithmetics() throws Exception {
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_byte() i8," +
                " rnd_short() i16," +
                " rnd_int() i32," +
                " rnd_long() i64," +
                " rnd_float() f32," +
                " rnd_double() f64 " +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        FilterGenerator gen = new FilterGenerator()
                .withOptionalNegation().withAnyOf("i8", "i16", "i32", "i64", "f32", "f64")
                .withArithmeticOperator()
                .withOptionalNegation().withAnyOf("i8", "i16", "i32", "i64", "f32", "f64")
                .withAnyOf(" = 1");
        assertGeneratedQueryNotNull("select * from x", ddl, gen);
    }

    @Test
    public void testColumnArithmeticsNullComparison() throws Exception {
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_int(-10, 10, 10) i32," +
                " rnd_long(-10, 10, 10) i64," +
                " rnd_float(10) f32," +
                " rnd_double(10) f64 " +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        FilterGenerator gen = new FilterGenerator()
                .withOptionalNegation().withAnyOf("i32", "i64", "f32", "f64")
                .withArithmeticOperator()
                .withOptionalNegation().withAnyOf("i32", "i64", "f32", "f64")
                .withAnyOf(" = ", " <> ")
                .withAnyOf("null");
        assertGeneratedQueryNullable("select * from x", ddl, gen);
    }

    @Test
    public void testColumnFloatConstantComparison() throws Exception {
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_int() i32," +
                " rnd_long() i64," +
                " rnd_float() f32," +
                " rnd_double() f64 " +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        FilterGenerator gen = new FilterGenerator()
                .withOptionalNegation().withAnyOf("i32", "i64", "f32", "f64")
                .withComparisonOperator()
                .withAnyOf("-42.5", "0.0", "0.000", "42.5");
        assertGeneratedQueryNotNull("select * from x", ddl, gen);
    }

    @Test
    public void testColumnIntConstantComparison() throws Exception {
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_byte() i8," +
                " rnd_short() i16," +
                " rnd_int() i32," +
                " rnd_long() i64," +
                " rnd_float() f32," +
                " rnd_double() f64 " +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        FilterGenerator gen = new FilterGenerator()
                .withOptionalNegation().withAnyOf("i8", "i16", "i32", "i64", "f32", "f64")
                .withComparisonOperator()
                .withAnyOf("-50", "0", "50");
        assertGeneratedQueryNotNull("select * from x", ddl, gen);
    }

    @Test
    public void testColumnIntConstantComparisonBoundaryMatch() throws Exception {
        final int boundary = 42;
        Assert.assertTrue("boundary should be within the range", N_SIMD_WITH_SCALAR_TAIL > boundary);
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " cast(x as byte) i8," +
                " cast(x as short) i16," +
                " cast(x as int) i32," +
                " x i64," +
                " cast(x as float) f32," +
                " cast(x as double) f64 " +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        FilterGenerator gen = new FilterGenerator()
                .withAnyOf("i8", "i16", "i32", "i64", "f32", "f64")
                .withComparisonOperator()
                .withAnyOf(String.valueOf(boundary));
        assertGeneratedQueryNotNull("select * from x", ddl, gen);
    }

    @Test
    public void testConstantColumnArithmetics() throws Exception {
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_byte() i8," +
                " rnd_short() i16," +
                " rnd_int() i32," +
                " rnd_long() i64," +
                " rnd_float() f32," +
                " rnd_double() f64 " +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        FilterGenerator gen = new FilterGenerator()
                .withOptionalNegation().withAnyOf("i8", "i16", "i32", "i64")
                .withArithmeticOperator()
                .withAnyOf("3", "-3.5")
                .withAnyOf(" + ")
                .withAnyOf("42.5", "-42")
                .withArithmeticOperator()
                .withOptionalNegation().withAnyOf("f32", "f64")
                .withAnyOf(" > 1");
        assertGeneratedQueryNotNull("select * from x", ddl, gen);
    }

    @Test
    public void testCount() throws Exception {
        final String query = "select count() from x where price > 0 and sym = 'HBC'";
        final String ddl = "create table x as " +
                "(select rnd_symbol('ABB','HBC','DXR') sym, \n" +
                " rnd_double() price, \n" +
                " timestamp_sequence(172800000000, 360000000) ts \n" +
                "from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp (ts)";
        assertQueryNotNull(query, ddl);
    }

    @Test
    public void testDate() throws Exception {
        final String query = "select * from x where d1 != d2";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_date(to_date('2020', 'yyyy'), to_date('2021', 'yyyy'), 0) d1," +
                " rnd_date(to_date('2020', 'yyyy'), to_date('2021', 'yyyy'), 0) d2" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertQueryNotNull(query, ddl);
    }

    @Test
    public void testDateNull() throws Exception {
        final String query = "select * from x where d <> null";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_date(to_date('2020', 'yyyy'), to_date('2021', 'yyyy'), 5) d" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertQueryNullable(query, ddl);
    }

    @Test
    public void testGeoHashConstant() throws Exception {
        final String query = "select * from x " +
                "where geo8 != ##1001 and geo16 != ##100110011001 and geo32 != ##1001100110011001 and geo64 != ##10011001100110011001100110011001";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_geohash(4) geo8," +
                " rnd_geohash(12) geo16," +
                " rnd_geohash(16) geo32," +
                " rnd_geohash(32) geo64" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertQueryNotNull(query, ddl);
    }

    @Test
    public void testGeoHashNull() throws Exception {
        final String query = "select * from x where geo8 <> null or geo16 <> null or geo32 <> null or geo64 <> null";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_geohash(4) geo8," +
                " rnd_geohash(15) geo16," +
                " rnd_geohash(16) geo32," +
                " rnd_geohash(40) geo64" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertQueryNotNull(query, ddl);
    }

    @Test
    public void testGeoHashValue() throws Exception {
        final String query = "select * from x where geo8a = geo8b";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_geohash(4) geo8a," +
                " rnd_geohash(4) geo8b" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertQueryNotNull(query, ddl);
    }

    @Test
    public void testGroupBy() throws Exception {
        final String query = "select sum(price)/count(sym) from x where price > 0";
        final String ddl = "create table x as " +
                "(select rnd_symbol('ABB','HBC','DXR') sym, \n" +
                " rnd_double() price, \n" +
                " timestamp_sequence(172800000000, 360000000) ts \n" +
                "from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp (ts)";
        assertQueryNotNull(query, ddl);
    }

    @Test
    public void testHugeFilter() throws Exception {
        final int N = 682; // depends on memory configuration for a jit IR
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_long() i64 " +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";

        FilterGenerator gen = new FilterGenerator();
        for (int i = 0; i < N; i++) {
            if (i > 0) {
                gen.withAnyOf(" and ");
            }
            gen.withAnyOf("i64 != 0");
        }
        assertGeneratedQueryNotNull("select * from x", ddl, gen);
    }

    @Test
    public void testIntConstantColumnComparisonBoundaryMatch() throws Exception {
        final int boundary = 101;
        Assert.assertTrue("boundary should be within the range", N_SIMD_WITH_SCALAR_TAIL > boundary);
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " cast(x as byte) i8," +
                " cast(x as short) i16," +
                " cast(x as int) i32," +
                " x i64," +
                " cast(x as float) f32," +
                " cast(x as double) f64 " +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        FilterGenerator gen = new FilterGenerator()
                .withAnyOf(String.valueOf(boundary))
                .withComparisonOperator()
                .withAnyOf("i8", "i16", "i32", "i64", "f32", "f64");
        assertGeneratedQueryNotNull("select * from x", ddl, gen);
    }

    @Test
    public void testIntFloatColumnsComparison() throws Exception {
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_byte() i8," +
                " rnd_short() i16," +
                " rnd_int() i32," +
                " rnd_long() i64," +
                " rnd_float() f32," +
                " rnd_double() f64 " +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        FilterGenerator gen = new FilterGenerator()
                .withOptionalNegation().withAnyOf("i8", "i16", "i32", "i64")
                .withComparisonOperator()
                .withOptionalNegation().withAnyOf("f32", "f64");
        assertGeneratedQueryNotNull("select * from x", ddl, gen);
    }

    @Test
    public void testIntFloatColumnsComparisonFilterOutNulls() throws Exception {
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_int(-10, 10, 10) i32," +
                " rnd_long(-10, 10, 10) i64," +
                " rnd_float(10) f32," +
                " rnd_double(10) f64 " +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        FilterGenerator gen = new FilterGenerator()
                .withOptionalNegation().withAnyOf("i32", "i64")
                .withComparisonOperator()
                .withOptionalNegation().withAnyOf("f32", "f64");
        assertGeneratedQueryNullable("select * from x", ddl, gen);
    }

    @Test
    public void testInterval() throws Exception {
        final String query = "select * from x where k in '2021-11-29' and i32 > 0";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(to_timestamp('2021-11-29T10:00:00', 'yyyy-MM-ddTHH:mm:ss'), 500000000) as k," +
                " rnd_int() i32" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertQueryNotNull(query, ddl);
    }

    @Test
    public void testNullComparison() throws Exception {
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_int(-10, 10, 10) i32," +
                " rnd_long(-10, 10, 10) i64," +
                " rnd_float(10) f32," +
                " rnd_double(10) f64 " +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        FilterGenerator gen = new FilterGenerator()
                .withOptionalNegation().withAnyOf("i32", "i64")
                .withAnyOf(" = ", " <> ")
                .withAnyOf("null")
                .withBooleanOperator()
                .withOptionalNegation().withAnyOf("f32", "f64")
                .withAnyOf(" = ", " <> ")
                .withAnyOf("null");
        assertGeneratedQueryNullable("select * from x", ddl, gen);
    }

    @Test
    public void testNullValueComparison() throws Exception {
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_int(-10, 10, 10) i32," +
                " rnd_long(-10, 10, 10) i64," +
                " rnd_float(10) f32," +
                " rnd_double(10) f64 " +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        FilterGenerator gen = new FilterGenerator()
                .withAnyOf("i32", "i64", "f32", "f64")
                .withComparisonOperator()
                .withAnyOf("1");
        assertGeneratedQueryNullable("select * from x", ddl, gen);
    }

    @Test
    public void testOrderByAsc() throws Exception {
        testOrderBy("order by ts asc");
    }

    @Test
    public void testOrderByDesc() throws Exception {
        testOrderBy("order by ts desc");
    }

    @Test
    public void testSymbolKnownConstant() throws Exception {
        // The column order is important here, since we want
        // query and table column indexes to be different.
        final String query = "select price, sym from x where sym = 'HBC' or sym = 'DXR'";
        final String ddl = "create table x as " +
                "(select rnd_symbol('ABB','HBC','DXR') sym, \n" +
                " rnd_double() price, \n" +
                " timestamp_sequence(172800000000, 360000000) ts \n" +
                "from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp (ts)";
        assertQueryNotNull(query, ddl);
    }

    @Test
    public void testSymbolNull() throws Exception {
        final String query = "select * from x where sym <> null";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_symbol(10,1,3,5) sym" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertQueryNullable(query, ddl);
    }

    @Test
    public void testTimestampComparison() throws Exception {
        final String query = "select * from x where t1 != t2";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_timestamp(to_timestamp('2020', 'yyyy'), to_timestamp('2021', 'yyyy'), 0) t1," +
                " rnd_timestamp(to_timestamp('2020', 'yyyy'), to_timestamp('2021', 'yyyy'), 0) t2" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertQueryNotNull(query, ddl);
    }

    @Test
    public void testTimestampNull() throws Exception {
        final String query = "select * from x where t <> null";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_timestamp(to_timestamp('2020', 'yyyy'), to_timestamp('2021', 'yyyy'), 5) t" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertQueryNullable(query, ddl);
    }

    @Test
    public void testTimestampNullValueComparison() throws Exception {
        final String query = "select * from x where ts >= 0";
        final String ddl = "create table x as " +
                "(select case when x < 10 then cast(NULL as TIMESTAMP) else cast(x as TIMESTAMP) end ts" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + "))";
        assertQueryNullable(query, ddl);
    }

    @Test
    public void testUuidConstantComparison() throws Exception {
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_uuid4() uuid1, " +
                " rnd_uuid4() uuid2 " +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        FilterGenerator gen = new FilterGenerator()
                .withAnyOf("uuid1", "uuid2")
                .withEqualityOperator()
                .withAnyOf("'22222222-2222-2222-2222-222222222222'", "'33333333-3333-3333-3333-333333333333'")
                .withBooleanOperator()
                .withOptionalNot()
                .withAnyOf("uuid1", "uuid2")
                .withEqualityOperator()
                .withAnyOf("'22222222-2222-2222-2222-222222222222'", "'33333333-3333-3333-3333-333333333333'");
        assertGeneratedQueryNullable("select * from x", ddl, gen);
    }

    @Test
    public void testUuidConstantIntMixedComparison() throws Exception {
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_int() int, " +
                " rnd_uuid4() uuid " +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        FilterGenerator gen = new FilterGenerator()
                .withAnyOf("int")
                .withEqualityOperator()
                .withAnyOf("3", "-1", "null")
                .withBooleanOperator()
                .withOptionalNot()
                .withAnyOf("uuid")
                .withEqualityOperator()
                .withAnyOf("'22222222-2222-2222-2222-222222222222'", "null");
        assertGeneratedQueryNullable("select * from x", ddl, gen);
    }

    @Test
    public void testUuidNullComparison() throws Exception {
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_uuid4() uuid1, " +
                " rnd_uuid4() uuid2 " +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        FilterGenerator gen = new FilterGenerator()
                .withAnyOf("uuid1", "uuid2")
                .withEqualityOperator()
                .withAnyOf("null")
                .withBooleanOperator()
                .withAnyOf("uuid1", "uuid2")
                .withEqualityOperator()
                .withAnyOf("null");
        assertGeneratedQueryNullable("select * from x", ddl, gen);
    }

    private void assertGeneratedQuery(CharSequence baseQuery, CharSequence ddl, FilterGenerator gen, boolean notNull) throws Exception {
        assertMemoryLeak(() -> {
            if (ddl != null) {
                ddl(ddl);
            }

            long maxSize = 0;
            List<String> filters = gen.generate();
            LOG.info().$("generated ").$(filters.size()).$(" filter expressions for base query: ").$(baseQuery).$();
            Assert.assertFalse(filters.isEmpty());
            for (String filter : filters) {
                long size = runQuery(baseQuery + " where " + filter);
                maxSize = Math.max(maxSize, size);

                assertJitQuery(baseQuery + " where " + filter, notNull);
            }
            Assert.assertTrue("at least one query is expected to return rows", maxSize > 0);
        });
    }

    private void assertGeneratedQueryNotNull(CharSequence baseQuery, CharSequence ddl, FilterGenerator gen) throws Exception {
        assertGeneratedQuery(baseQuery, ddl, gen, true);
    }

    private void assertGeneratedQueryNullable(CharSequence baseQuery, CharSequence ddl, FilterGenerator gen) throws Exception {
        assertGeneratedQuery(baseQuery, ddl, gen, false);
    }

    private void assertJitQuery(CharSequence query, boolean notNull) throws SqlException {
//        compiler.setEnableJitNullChecks(true);

        sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_FORCE_SCALAR);
        runJitQuery(query);
        TestUtils.assertEquals("[scalar mode] result mismatch for query: " + query, sink, jitSink);

        sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
        runJitQuery(query);
        TestUtils.assertEquals("[vectorized mode] result mismatch for query: " + query, sink, jitSink);

        // At the moment, there is no way for users to disable null checks in the
        // JIT compiler output. Yet, we want to test this part of the compiler.
        if (notNull) {
            //          compiler.setEnableJitNullChecks(false);

            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_FORCE_SCALAR);
            runJitQuery(query);
            TestUtils.assertEquals("[scalar mode, not null] result mismatch for query: " + query, sink, jitSink);

            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
            runJitQuery(query);
            TestUtils.assertEquals("[vectorized mode, not null] result mismatch for query: " + query, sink, jitSink);
        }
    }

    private void assertQuery(CharSequence query, CharSequence ddl, boolean notNull) throws Exception {
        assertMemoryLeak(() -> {
            if (ddl != null) {
                ddl(ddl);
            }

            long size = runQuery(query);
            Assert.assertTrue("query is expected to return rows", size > 0);

            assertJitQuery(query, notNull);
        });
    }

    private void assertQueryNotNull(CharSequence query, CharSequence ddl) throws Exception {
        assertQuery(query, ddl, false);
    }

    private void assertQueryNullable(CharSequence query, CharSequence ddl) throws Exception {
        assertQuery(query, ddl, true);
    }

    private void runJitQuery(CharSequence query) throws SqlException {
        try (final RecordCursorFactory factory = select(query)) {
            Assert.assertTrue("JIT was not enabled for query: " + query, factory.usesCompiledFilter());
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                TestUtils.printCursor(cursor, factory.getMetadata(), true, jitSink, printer);
            }
        }
    }

    private long runQuery(CharSequence query) throws SqlException {
        long resultSize;

        sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
        try (RecordCursorFactory factory = select(query)) {
            Assert.assertFalse("JIT was enabled for query: " + query, factory.usesCompiledFilter());
            try (CountingRecordCursor cursor = new CountingRecordCursor(factory.getCursor(sqlExecutionContext))) {
                TestUtils.printCursor(cursor, factory.getMetadata(), true, sink, printer);
                resultSize = cursor.count();
            }
        }

        return resultSize;
    }

    private void testOrderBy(String orderByClause) throws Exception {
        final String query = "select * from x where price > 0 " + orderByClause;
        final String ddl = "create table x as " +
                "(select rnd_symbol('ABB','HBC','DXR') sym, \n" +
                " rnd_double() price, \n" +
                " timestamp_sequence(172800000000, 360000000) ts \n" +
                "from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp (ts)";
        assertQueryNotNull(query, ddl);
    }

    private static class CountingRecordCursor implements RecordCursor {

        private final RecordCursor delegate;
        private long count;

        public CountingRecordCursor(RecordCursor delegate) {
            this.delegate = delegate;
        }

        @Override
        public void close() {
            delegate.close();
        }

        public long count() {
            return count;
        }

        @Override
        public Record getRecord() {
            return delegate.getRecord();
        }

        @Override
        public Record getRecordB() {
            return delegate.getRecordB();
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            return delegate.getSymbolTable(columnIndex);
        }

        @Override
        public boolean hasNext() {
            boolean hasNext = delegate.hasNext();
            if (hasNext) {
                count++;
            }
            return hasNext;
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            return delegate.newSymbolTable(columnIndex);
        }

        @Override
        public void recordAt(Record record, long atRowId) {
            delegate.recordAt(record, atRowId);
        }

        @Override
        public long size() {
            return delegate.size();
        }

        @Override
        public void toTop() {
            delegate.toTop();
        }
    }

    private static class FilterGenerator {

        private static final String[] ARITHMETIC_OPERATORS = new String[]{" + ", " - ", " * ", " / "};
        private static final String[] BOOLEAN_OPERATORS = new String[]{" and ", " or "};
        private static final String[] COMPARISON_OPERATORS = new String[]{" = ", " != ", " > ", " >= ", " < ", " <= "};
        private static final String[] EQUALITY_OPERATORS = new String[]{" = ", " != ", " <> "};
        private static final String[] OPTIONAL_NEGATION = new String[]{"", "-"};
        private static final String[] OPTIONAL_NOT = new String[]{"", " not "};
        private final List<String[]> filterParts = new ArrayList<>();

        /**
         * Generates a simple cartesian product of the given filter expression parts.
         * <p>
         * The algorithm originates from Generating All n-tuple, of The Art Of Computer
         * Programming by Knuth.
         */
        public List<String> generate() {
            if (filterParts.isEmpty()) {
                return Collections.emptyList();
            }

            int combinations = 1;
            for (String[] parts : filterParts) {
                combinations *= parts.length;
            }

            final List<String> filters = new ArrayList<>();
            final StringBuilder sb = new StringBuilder();

            for (int i = 0; i < combinations; i++) {
                int j = 1;
                for (String[] parts : filterParts) {
                    sb.append(parts[(i / j) % parts.length]);
                    j *= parts.length;
                }
                filters.add(sb.toString());
                sb.setLength(0);
            }
            return filters;
        }

        public FilterGenerator withAnyOf(String... parts) {
            filterParts.add(parts);
            return this;
        }

        public FilterGenerator withArithmeticOperator() {
            filterParts.add(ARITHMETIC_OPERATORS);
            return this;
        }

        public FilterGenerator withBooleanOperator() {
            filterParts.add(BOOLEAN_OPERATORS);
            return this;
        }

        public FilterGenerator withComparisonOperator() {
            filterParts.add(COMPARISON_OPERATORS);
            return this;
        }

        public FilterGenerator withEqualityOperator() {
            filterParts.add(EQUALITY_OPERATORS);
            return this;
        }

        public FilterGenerator withOptionalNegation() {
            filterParts.add(OPTIONAL_NEGATION);
            return this;
        }

        public FilterGenerator withOptionalNot() {
            filterParts.add(OPTIONAL_NOT);
            return this;
        }
    }
}
