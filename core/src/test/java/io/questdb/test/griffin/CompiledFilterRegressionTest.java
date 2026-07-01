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

import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
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
        // compiler.setEnableJitNullChecks(true);
    }

    @Test
    public void testBoolean() throws Exception {
        final String query = "x where bool1 or bool2 = false";
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
        assertGeneratedQueryNotNull(ddl, gen);
    }

    @Test
    public void testChar() throws Exception {
        final String query = "x where ch > 'A' and ch < 'Z'";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_char() ch" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertQueryNotNull(query, ddl);
    }

    @Test
    public void testColumnAddressHoistingExceedsCacheCapacity() throws Exception {
        // Tests column address hoisting with more than 8 columns (exceeds cache capacity)
        // The backend address cache has capacity of 8 elements
        final String query = "x where " +
                "c1 > 0 and c2 > 0 and c3 > 0 and c4 > 0 " +
                "and c5 > 0 and c6 > 0 and c7 > 0 and c8 > 0 " +
                "and c9 > 0 and c10 > 0 and c11 > 0 and c12 > 0";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_long() c1," +
                " rnd_long() c2," +
                " rnd_long() c3," +
                " rnd_long() c4," +
                " rnd_long() c5," +
                " rnd_long() c6," +
                " rnd_long() c7," +
                " rnd_long() c8," +
                " rnd_long() c9," +
                " rnd_long() c10," +
                " rnd_long() c11," +
                " rnd_long() c12" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertQueryNotNull(query, ddl);
    }

    @Test
    public void testColumnAddressHoistingMixedTypes() throws Exception {
        // Tests column address hoisting with mixed types exceeding cache capacity
        final String query = "x where " +
                "i1 > 0 and i2 > 0 and i3 > 0 and l1 > 0 and l2 > 0 and l3 > 0 " +
                "and f1 > 0.0 and f2 > 0.0 and f3 > 0.0 " +
                "and d1 > 0.0 and d2 > 0.0 and d3 > 0.0";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_int() i1," +
                " rnd_int() i2," +
                " rnd_int() i3," +
                " rnd_long() l1," +
                " rnd_long() l2," +
                " rnd_long() l3," +
                " rnd_float() f1," +
                " rnd_float() f2," +
                " rnd_float() f3," +
                " rnd_double() d1," +
                " rnd_double() d2," +
                " rnd_double() d3" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertQueryNotNull(query, ddl);
    }

    @Test
    public void testColumnAddressHoistingWithinCacheCapacity() throws Exception {
        // Tests column address hoisting with 8 columns (within cache capacity of 8)
        final String query = "x where " +
                "c1 > 0 and c2 > 0 and c3 > 0 and c4 > 0 " +
                "and c5 > 0 and c6 > 0 and c7 > 0 and c8 > 0";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_long() c1," +
                " rnd_long() c2," +
                " rnd_long() c3," +
                " rnd_long() c4," +
                " rnd_long() c5," +
                " rnd_long() c6," +
                " rnd_long() c7," +
                " rnd_long() c8" +
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
        assertGeneratedQueryNotNull(ddl, gen);
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
        assertGeneratedQueryNullable(ddl, gen);
    }

    @Test
    public void testColumnFloatComparisonWithNulls() throws Exception {
        // Regression test for ARM64 NaN condition code handling.
        // ARM64 fcmp with NaN sets NZCV=0011. Ordered less-than must use MI (not LT),
        // and ordered less-or-equal must use LS (not LE), otherwise NaN compares as true.
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_float(10) f32," +
                " rnd_double(10) f64 " +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        FilterGenerator gen = new FilterGenerator()
                .withAnyOf("f32", "f64")
                .withComparisonOperator()
                .withAnyOf("0.5", "-0.5")
                .withBooleanOperator()
                .withAnyOf("f32", "f64")
                .withComparisonOperator()
                .withAnyOf("0.3");
        assertGeneratedQueryNullable(ddl, gen);
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
        assertGeneratedQueryNotNull(ddl, gen);
    }

    @Test
    public void testColumnFloatEpsilonWithNulls() throws Exception {
        // Regression test for ARM64 epsilon comparison NaN handling.
        // Float equality uses epsilon comparison internally. ARM64 must use
        // GT (ordered, false for NaN) not HI (unsigned, true for NaN).
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_float(10) f32," +
                " rnd_double(10) f64 " +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        FilterGenerator gen = new FilterGenerator()
                .withAnyOf("f32", "f64")
                .withAnyOf(" = ", " != ", " <> ")
                .withAnyOf("0.5", "-0.5", "0.0")
                .withBooleanOperator()
                .withAnyOf("f32", "f64")
                .withAnyOf(" = ", " != ")
                .withAnyOf("null");
        assertGeneratedQueryNullable(ddl, gen);
    }

    @Test
    public void testColumnFloatNegativeConstant() throws Exception {
        // Regression test for is_float() misclassifying negative floats.
        // Previously is_float() used numeric_limits<float>::min() (smallest positive
        // normal) as lower bound, causing all negative floats to be promoted to f64.
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_float() f32," +
                " rnd_double() f64 " +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        FilterGenerator gen = new FilterGenerator()
                .withAnyOf("f32")
                .withComparisonOperator()
                .withAnyOf("-0.5", "-1.0", "-100.0", "-3.4028235E38");
        assertGeneratedQueryNotNull(ddl, gen);
    }

    @Test
    public void testColumnFloatSharedConstant() throws Exception {
        // Regression test for constant cache type mismatch on ARM64.
        // When f32 and f64 columns compare against the same constant value (e.g. 0.0),
        // the ConstantCache shares an entry. The cached Vec register may have wrong
        // element size (S vs D), requiring fcvt conversion.
        final String query = "x where f32 > 0.0 and f64 > 0.0 and f32 < 0.5 and f64 < 0.5";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_float() f32," +
                " rnd_double() f64 " +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertQueryNotNull(query, ddl);
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
        assertGeneratedQueryNotNull(ddl, gen);
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
        assertGeneratedQueryNotNull(ddl, gen);
    }

    @Test
    public void testColumnLessThanNullComparison() throws Exception {
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_int(-10, 10, 10) i32," +
                " rnd_long(-10, 10, 10) i64," +
                " rnd_float(10) f32," +
                " rnd_double(10) f64 " +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        FilterGenerator gen = new FilterGenerator()
                .withOptionalNegation().withAnyOf("i32", "i64", "f32", "f64")
                .withAnyOf(" <= ", " >= ", " = ")
                .withAnyOf("null");
        assertGeneratedQueryNullable(ddl, gen);
    }

    @Test
    public void testColumnTimestampLiteralComparison() throws Exception {
        final String ddl = "create table x as " +
                "(select rnd_timestamp(to_timestamp('2019','yyyy'),to_timestamp('2021','yyyy'),2) ts" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + "))";
        FilterGenerator gen = new FilterGenerator()
                .withAnyOf("ts")
                .withComparisonOperator()
                .withAnyOf("'2020-01-01T01:01:01.111111Z'");
        assertGeneratedQueryNotNull(ddl, gen);
    }

    @Test
    public void testColumnValueCacheExceedsCapacity() throws Exception {
        // Tests when column value cache exceeds capacity (8 elements)
        // Each column access creates a cache entry
        final String query = "x where " +
                "c1 > 0 and c1 < 100 and c2 > 0 and c2 < 100 " +
                "and c3 > 0 and c3 < 100 and c4 > 0 and c4 < 100 " +
                "and c5 > 0 and c5 < 100";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_long(0, 200, 0) c1," +
                " rnd_long(0, 200, 0) c2," +
                " rnd_long(0, 200, 0) c3," +
                " rnd_long(0, 200, 0) c4," +
                " rnd_long(0, 200, 0) c5" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertQueryNotNull(query, ddl);
    }

    @Test
    public void testColumnValueCachingInArithmetic() throws Exception {
        // Tests column value caching when column is used in arithmetic expressions
        final String query = "x where " +
                "i64 + i64 > 100 and i64 * 2 < 180";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_long(0, 100, 0) i64" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertQueryNotNull(query, ddl);
    }

    @Test
    public void testColumnValueCachingMixedTypes() throws Exception {
        // Tests column value caching with multiple columns of different types
        final String query = "x where " +
                "i32 > 10 and i32 < 90 and i64 > 20 and i64 < 80 " +
                "and f64 > 0.1 and f64 < 0.9";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_int(0, 100, 0) i32," +
                " rnd_long(0, 100, 0) i64," +
                " rnd_double() f64" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertQueryNotNull(query, ddl);
    }

    @Test
    public void testColumnValueCachingSameColumnMultipleTimes() throws Exception {
        // Tests column value caching when the same column is used multiple times
        // The backend caches loaded column values within a single row iteration
        final String query = "x where " +
                "i64 > 10 and i64 < 90 and i64 != 50";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_long(0, 100, 0) i64" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertQueryNotNull(query, ddl);
    }

    @Test
    public void testCombinedHoistingExceedsBothCaches() throws Exception {
        // Tests both constant and column address hoisting exceeding cache capacities
        final String query = "x where " +
                "c1 > 1 and c2 > 2 and c3 > 3 and c4 > 4 " +
                "and c5 > 5 and c6 > 6 and c7 > 7 and c8 > 8 " +
                "and c9 > 9 and c10 > 10";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_long(0, 100, 0) c1," +
                " rnd_long(0, 100, 0) c2," +
                " rnd_long(0, 100, 0) c3," +
                " rnd_long(0, 100, 0) c4," +
                " rnd_long(0, 100, 0) c5," +
                " rnd_long(0, 100, 0) c6," +
                " rnd_long(0, 100, 0) c7," +
                " rnd_long(0, 100, 0) c8," +
                " rnd_long(0, 100, 0) c9," +
                " rnd_long(0, 100, 0) c10" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertQueryNotNull(query, ddl);
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
        assertGeneratedQueryNotNull(ddl, gen);
    }

    @Test
    public void testConstantHoistingExceedsCacheCapacity() throws Exception {
        // Tests constant hoisting with more than 8 constants (exceeds cache capacity)
        // The backend constant cache has capacity of 8 elements
        final String query = "x where " +
                "i64 > 1 and i64 < 100 and i64 != 10 and i64 != 20 " +
                "and i64 != 30 and i64 != 40 and i64 != 50 and i64 != 60 " +
                "and i64 != 70 and i64 != 80 and i64 != 90";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_long(0, 200, 0) i64" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertQueryNotNull(query, ddl);
    }

    @Test
    public void testConstantHoistingMixedTypes() throws Exception {
        // Tests constant hoisting with mixed types exceeding cache capacity
        final String query = "x where " +
                "i32 > 1 and i32 < 100 and i64 > 2 and i64 < 200 " +
                "and f32 > 0.1 and f32 < 0.9 and f64 > 0.2 and f64 < 0.8 " +
                "and i32 != 50 and i64 != 100 and f32 != 0.5 and f64 != 0.5";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_int(0, 200, 0) i32," +
                " rnd_long(0, 300, 0) i64," +
                " rnd_float() f32," +
                " rnd_double() f64" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertQueryNotNull(query, ddl);
    }

    @Test
    public void testConstantHoistingWithinCacheCapacity() throws Exception {
        // Tests constant hoisting with 8 constants (within cache capacity of 8)
        final String query = "x where " +
                "i64 > 1 and i64 < 100 and i64 != 50 and i64 != 51 " +
                "and i64 != 52 and i64 != 53 and i64 != 54 and i64 != 55";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_long(0, 200, 0) i64" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertQueryNotNull(query, ddl);
    }

    @Test
    public void testConstantFoldDivisionAtIntWidth() throws Exception {
        // A folded constant with a non-modular operator (division) must replicate
        // the Java filter's per-op INT wrapping at an INT-width comparison.
        // (1000000 * 1000000) / 7 folds in long to 142857142857, whose low 32
        // bits are +1123222089, but DivInt#getInt computes
        // (int) (1000000 * 1000000) / 7 = -103911424. Folding in long and
        // truncating diverged: the JIT returned 0 rows where the Java filter
        // returned every row.
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " cast(x - 100 as byte) i8," +
                " cast(x - 100 as short) i16," +
                " cast(x - 100 as int) i32," +
                " (x - 100) i64" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertMemoryLeak(() -> {
            execute(ddl);
            // Per-op INT wrap = -103911424; every narrow/INT row (>= -99) exceeds it.
            assertQueryNotNullNoLeakCheck("x where i8 > (1000000 * 1000000) / 7");
            assertQueryNotNullNoLeakCheck("x where i16 > (1000000 * 1000000) / 7");
            assertQueryNotNullNoLeakCheck("x where i32 > (1000000 * 1000000) / 7");
            // LONG column reads full long width via DivInt#getLong = 142857142857;
            // no i64 row reaches it, and the I8 fold path already agreed here.
            assertQueryNullableNoLeakCheck("x where i64 > (1000000 * 1000000) / 7");
        });
    }

    @Test
    public void testConstantFoldIntNullCollision() throws Exception {
        // An inner constant product that wraps to EXACTLY INT_NULL (-2^31)
        // poisons the rest of an INT-width fold: the Java filter's
        // MulInt/AddInt#getInt return INT_NULL once an operand is INT_NULL, so
        // i8 > (65536 * 32768) * 2 collapses to i8 > NULL (no rows). The JIT I4
        // fold did pure modular arithmetic (-2^31 * 2 = 0) and returned i8 > 0
        // (rows). tryFoldConstantArithI4 now propagates INT_NULL like the
        // runtime ops, so both paths agree.
        assertMemoryLeak(() -> {
            execute("create table x as (select timestamp_sequence(0, 1000000) k," +
                    " cast(x - 2 as byte) i8," +
                    " cast(x - 2 as short) i16," +
                    " cast(x - 2 as int) i32," +
                    " (x - 2) i64" +
                    " from long_sequence(5)) timestamp(k)");
            // INT-width comparisons: inner 65536 * 32768 wraps to INT_NULL, so
            // the whole fold is NULL and no row matches.
            assertJitMatchesJava("x where i8 > (65536 * 32768) * 2", true);
            assertJitMatchesJava("x where i16 > (65536 * 32768) * 2", true);
            assertJitMatchesJava("x where i32 > (65536 * 32768) * 2", true);
            // LONG column promotes to long width: getLong() never wraps onto the
            // sentinel, so the constant is the genuine 4294967296 on both paths.
            assertJitMatchesJava("x where i64 > (65536 * 32768) * 2", true);
        });
    }

    @Test
    public void testConstantFoldRootUnderLongWithFloat() throws Exception {
        // An overflowing constant product (258558 * -259815) nested under a LONG
        // add (c0 + ...) is read at long width by AddLong#getLong, so the Java
        // filter never wraps it. A FLOAT in the predicate suppressed the global
        // narrow-i64 widening, and isLongTypedConstArith only checks LONG *leaves*,
        // so the JIT folded the product to a wrapped I4 IMM and diverged (JIT all
        // rows, Java none). The serializer now tags such fold roots for a full I8
        // IMM.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (c0 LONG, c8 INT, c9 FLOAT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t SELECT rnd_long(-1000000, 1000000, 8), " +
                    "rnd_int(-1000000, 1000000, 8), rnd_float(8), " +
                    "timestamp_sequence(to_timestamp('2024-01-01', 'yyyy-MM-dd'), 1800000000L) " +
                    "FROM long_sequence(122)");
            // Previously diverging shapes: fold root under a genuine LONG op with a
            // FLOAT comparison. Still JIT-compiled, now correct.
            assertJitMatchesJava("SELECT * FROM t WHERE c9 <= (c0 + (258558 * -259815))", true);
            assertJitMatchesJava("SELECT * FROM t WHERE c9 <= (c0 - (258558 * -259815))", true);
            assertJitMatchesJava("SELECT * FROM t WHERE c9 <= ((258558 * -259815) + c0)", true);
            // Control: direct float-vs-overflow comparison still wraps via
            // getDouble(getInt()), so the wrapped I4 fold remains correct.
            assertJitMatchesJava("SELECT * FROM t WHERE c9 <= (258558 * -259815)", true);
            // Control: pure-INT arithmetic under a float wraps (no genuine LONG).
            assertJitMatchesJava("SELECT * FROM t WHERE c9 <= (c8 + (258558 * -259815))", true);
        });
    }

    @Test
    public void testConstantFoldWidthUnderBooleanEquality() throws Exception {
        // A boolean equality of two comparisons -- (cmp) = (cmp) -- forms a
        // SINGLE predicate context, so a predicate-global width signal applies
        // one width to both comparisons. An overflowing INT fold then took the
        // wrong width:
        //   - I4-where-I8: (1000000 * 1000000 > i64) = (f32 > 0) -- the fold
        //     feeds a LONG comparison and must read at long width, but the float
        //     suppressed global widening and the fold root went unmarked, so the
        //     JIT emitted a wrapped I4 (Java 2 rows, JIT 0).
        //   - I8-where-I4: (ci > 1000000 * 1000000) = (cl > 0) -- the fold feeds
        //     an INT comparison and must wrap, but the predicate-global
        //     long-widening (driven by the sibling LONG comparison) forced it to
        //     I8 (Java 3 rows, JIT 0).
        // The fold width is now derived from the fold's own comparison context.
        assertMemoryLeak(() -> {
            execute("create table x as (select timestamp_sequence(0, 1000000) k, x id," +
                    " (case when x = 1 then 0L when x = 2 then 5L else 2000000000000L end) i64," +
                    " x::int ci, x::long cl, 1.0f f32" +
                    " from long_sequence(3)) timestamp(k)");
            // Previously diverging shapes, both directions.
            assertJitMatchesJava("select id from x where (1000000 * 1000000 > i64) = (f32 > 0)", true);
            assertJitMatchesJava("select id from x where (ci > 1000000 * 1000000) = (cl > 0)", true);
            // Controls: single comparison, AND, OR each split into separate
            // predicate contexts and were always correct.
            assertJitMatchesJava("select id from x where ci > 1000000 * 1000000", true);
            assertJitMatchesJava("select id from x where ci > 1000000 * 1000000 and cl > 0", true);
            assertJitMatchesJava("select id from x where ci > 1000000 * 1000000 or cl > 0", true);
            // Control: the LONG column reads the fold at long width on both paths.
            assertJitMatchesJava("select id from x where i64 > 1000000 * 1000000", true);
        });
    }

    @Test
    public void testConstantOverflowFoldOnByteColumn() throws Exception {
        // Overflowing INT constant arithmetic must agree across JIT off/scalar/
        // vectorized. A BYTE column compares at INT width, so the JIT folds the
        // constant to a wrapped I4 IMM, matching the Java filter's getInt() wrap.
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " cast(x - 100 as byte) i8" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertMemoryLeak(() -> {
            execute(ddl);
            // -1.04e17 wraps to +1_699_321_072 at INT width; no BYTE exceeds it.
            assertQueryNullableNoLeakCheck("x where i8 > -286452 * (-952151 * -382988)");
            // Symmetric: +1.04e17 wraps to -1_699_321_072, below every BYTE.
            assertQueryNotNullNoLeakCheck("x where i8 > 286452 * (952151 * 382988)");
        });
    }

    @Test
    public void testConstantOverflowFoldOnIntColumn() throws Exception {
        // INT column compares at INT width: the JIT wraps the constant (I4) like
        // the Java filter rather than widening to i64.
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " cast(x - 100 as int) i32" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertMemoryLeak(() -> {
            execute(ddl);
            // Wraps to +1_699_321_072: no INT row exceeds it.
            assertQueryNullableNoLeakCheck("x where i32 > -286452 * (-952151 * -382988)");
            // Wraps to -1_699_321_072: below every INT row.
            assertQueryNotNullNoLeakCheck("x where i32 > 286452 * (952151 * 382988)");
        });
    }

    @Test
    public void testConstantOverflowFoldOnLongColumn() throws Exception {
        // A LONG column promotes the comparison to LONG: both the Java filter
        // (getLong()) and the JIT (i64 fold) read full long width and never wrap.
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " (x - 100) i64" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertMemoryLeak(() -> {
            execute(ddl);
            // -1.04e17 stays -1.04e17 at LONG width; every LONG row exceeds it.
            assertQueryNotNullNoLeakCheck("x where i64 > -286452 * (-952151 * -382988)");
            // +1.04e17 stays +1.04e17; no LONG row exceeds it.
            assertQueryNullableNoLeakCheck("x where i64 > 286452 * (952151 * 382988)");
        });
    }

    @Test
    public void testConstantOverflowFoldOnShortColumn() throws Exception {
        // A SHORT column promotes to INT, so the constant wraps at INT width.
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " cast(x - 100 as short) i16" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertMemoryLeak(() -> {
            execute(ddl);
            // Wraps to +1_699_321_072: no SHORT row exceeds it.
            assertQueryNullableNoLeakCheck("x where i16 > -286452 * (-952151 * -382988)");
            // Wraps to -1_699_321_072: below every SHORT row.
            assertQueryNotNullNoLeakCheck("x where i16 > 286452 * (952151 * 382988)");
        });
    }

    @Test
    public void testConstantOverflowFoldVariousOps() throws Exception {
        // The fold path covers +, -, *, /, and unary minus uniformly.
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " cast(x - 100 as byte) i8," +
                " (x - 100) i64" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertMemoryLeak(() -> {
            execute(ddl);
            // i64 values lie in [-99, 414]; the four constants all evaluate to a
            // value below every i64 row, so > matches every row and exercises
            // the fold path uniformly across all four operators.
            assertQueryNotNullNoLeakCheck("x where i64 > -5000000000 + -5000000000");
            assertQueryNotNullNoLeakCheck("x where i64 > -5000000000 - 5000000000");
            assertQueryNotNullNoLeakCheck("x where i64 > -100000 * 100000");
            assertQueryNotNullNoLeakCheck("x where i64 > -1000000000000 / 1");
            // Unary minus over an overflowing product. The i64 column promotes
            // to LONG and reads full width (-1.04e17); every row passes.
            assertQueryNotNullNoLeakCheck("x where i64 > -(286452 * (-952151 * -382988))");
            // Same constant on a BYTE column compares at INT width: wraps to
            // +1_699_321_072, which no BYTE row exceeds (wrapped-I4 fold path).
            assertQueryNullableNoLeakCheck("x where i8 > -(286452 * (-952151 * -382988))");
        });
    }

    @Test
    public void testCount() throws Exception {
        final String query = "select count() from x where price > 0 and sym = 'HBC'";
        final String ddl = "create table x as " +
                "(select rnd_symbol('ABB','HBC','DXR') sym, \n" +
                " rnd_double() price, \n" +
                " timestamp_sequence(172800000000, 360000000) ts \n" +
                "from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp (ts)";
        assertQueryNotNullNoCount(query, ddl);
    }

    @Test
    public void testDate() throws Exception {
        final String query = "x where d1 != d2";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_date(to_date('2020', 'yyyy'), to_date('2021', 'yyyy'), 0) d1," +
                " rnd_date(to_date('2020', 'yyyy'), to_date('2021', 'yyyy'), 0) d2" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertQueryNotNull(query, ddl);
    }

    @Test
    public void testDateNull() throws Exception {
        final String query = "x where d <> null";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_date(to_date('2020', 'yyyy'), to_date('2021', 'yyyy'), 5) d" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertQueryNullable(query, ddl);
    }

    @Test
    public void testGeoHashConstant() throws Exception {
        final String query = "x " +
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
        final String query = "x where geo8 <> null or geo16 <> null or geo32 <> null or geo64 <> null";
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
        final String query = "x where geo8a = geo8b";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_geohash(4) geo8a," +
                " rnd_geohash(4) geo8b" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertQueryNotNull(query, ddl);
    }

    @Test
    public void testGroupBy() throws Exception {
        // We don't want parallel GROUP BY to kick in, so we cast string column to symbol to avoid that.
        final String query = "select str::symbol, sum(price)/count() from x where price > 0";
        final String ddl = "create table x as " +
                "(select rnd_str('ABB','HBC','DXR') str, \n" +
                " rnd_double() price, \n" +
                " timestamp_sequence(172800000000, 360000000) ts \n" +
                "from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp (ts)";
        assertQueryNotNullNoCount(query, ddl);
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
        assertGeneratedQueryNotNull(ddl, gen);
    }

    @Test
    public void testInOperatorChained() throws Exception {
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_int(1, 10, 0) a," +
                " rnd_int(1, 10, 0) b," +
                " rnd_int(1, 10, 0) c" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        FilterGenerator gen = new FilterGenerator()
                .withAnyOf("a")
                .withAnyOf(" in ", " not in ")
                .withAnyOf("(1, 2)")
                .withBooleanOperator()
                .withAnyOf("b")
                .withAnyOf(" in ", " not in ")
                .withAnyOf("(3, 4)")
                .withBooleanOperator()
                .withAnyOf("c")
                .withAnyOf(" in ", " not in ")
                .withAnyOf("(5, 6)");
        assertGeneratedQueryNotNull(ddl, gen);
    }

    @Test
    public void testInOperatorFloat() throws Exception {
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_float() f32," +
                " rnd_double() f64" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        FilterGenerator gen = new FilterGenerator()
                .withAnyOf("f32", "f64")
                .withAnyOf(" in ", " not in ")
                .withAnyOf("(0.1, 0.2, 0.3)", "(0.5, 0.6)", "(-0.1, 0.0, 0.1)");
        assertGeneratedQueryNotNull(ddl, gen);
    }

    @Test
    public void testInOperatorFloatWithNull() throws Exception {
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_float(5) f32," +
                " rnd_double(5) f64" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        FilterGenerator gen = new FilterGenerator()
                .withAnyOf("f32", "f64")
                .withAnyOf(" in ", " not in ")
                .withAnyOf("(0.1, 0.2, null)", "(null, 0.5)", "(0.0, null, 0.1)");
        assertGeneratedQueryNullable(ddl, gen);
    }

    @Test
    public void testInOperatorInt() throws Exception {
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_int(1, 10, 0) i32," +
                " rnd_long(1, 10, 0) i64" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        FilterGenerator gen = new FilterGenerator()
                .withAnyOf("i32", "i64")
                .withAnyOf(" in ", " not in ")
                .withAnyOf("(1, 2, 3)", "(4, 5)", "(6, 7, 8, 9)");
        assertGeneratedQueryNotNull(ddl, gen);
    }

    @Test
    public void testInOperatorIntWithNull() throws Exception {
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_int(1, 10, 5) i32," +
                " rnd_long(1, 10, 5) i64" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        FilterGenerator gen = new FilterGenerator()
                .withAnyOf("i32", "i64")
                .withAnyOf(" in ", " not in ")
                .withAnyOf("(1, 2, null)", "(null, 4, 5)", "(6, null)");
        assertGeneratedQueryNullable(ddl, gen);
    }

    @Test
    public void testInOperatorManyValues() throws Exception {
        // Tests IN operator with many values (exceeds typical unroll thresholds)
        final String query = "x where " +
                "i64 in (1, 2, 3, 4, 5, 6, 7, 8, 9)";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_long(1, 20, 0) i64" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertQueryNotNull(query, ddl);
    }

    @Test
    public void testInOperatorNestedWithAndOr() throws Exception {
        // Tests IN operator nested within AND/OR expressions
        final String query = "x where " +
                "(a in (1, 2) and b > 5) or (a in (8, 9) and b < 3)";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_int(1, 10, 0) a," +
                " rnd_int(1, 10, 0) b" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertQueryNotNull(query, ddl);
    }

    @Test
    public void testInOperatorOverflowFoldMatchesJavaOnLongColumn() throws Exception {
        // An overflowing INT arithmetic fold inside an IN() list against a LONG column: the
        // Java filter reads the IN element at long width (10^12), so the JIT must too. The
        // table carries both the widened value (10^12) and its wrapped INT image
        // (-727379968) so a wrong fold matches the wrong row instead of returning empty.
        // markI64WidenFoldRoots used to bail on the IN node (it is a FUNCTION, not an
        // OPERATION) for the single-value form, and could not derive the comparison width
        // for the multi-value form (the operands live in args with lhs/rhs null), so it
        // emitted a wrapped I4 IMM and diverged from the Java filter.
        assertMemoryLeak(() -> {
            execute("create table x as (select cast(v as long) i64 " +
                    "from (select 1000000000000 v union all select -727379968 v))");
            assertJitMatchesJava("x where i64 in (1000000 * 1000000)", true);          // single value
            assertJitMatchesJava("x where i64 in (1, 1000000 * 1000000)", true);       // two values
            assertJitMatchesJava("x where i64 in (1, 2, 1000000 * 1000000)", true);    // multi value
            assertJitMatchesJava("x where i64 not in (1, 1000000 * 1000000)", true);   // inverse
            assertJitMatchesJava("x where i64 = 1000000 * 1000000", true);             // control: plain '='
        });
    }

    @Test
    public void testInOperatorOverflowFoldMatchesJavaOnNarrowArithKey() throws Exception {
        // C1 regression: an overflowing INT *arithmetic* KEY on the left of IN(). The narrow-int
        // IN fix wrapped the IN-list elements at INT width (matching '=') but every InLong*.getBool
        // still read the KEY via getLong(), which WIDENS an overflowing INT arithmetic. So IN
        // disagreed with '=' even with the JIT off, and the JIT (which computes the key at I4 and
        // wraps) disagreed with the Java IN. The key is now read at INT width too, symmetric with
        // the elements. a + a = 3*10^9 wraps to INT -1294967296; '=' (EqInt) and the JIT both wrap,
        // so a correctly-wrapping IN matches the single row.
        assertMemoryLeak(() -> {
            execute("create table x as (select cast(1500000000 as int) a, cast(1500000000 as int) b)");

            // const IN shapes: the JIT wraps the key at I4, the Java IN must wrap too.
            assertJitMatchesJava("x where (a + a) in (1500000000 + 1500000000)", true);        // single const
            assertJitMatchesJava("x where (a + a) in (1, 1500000000 + 1500000000)", true);     // two const
            assertJitMatchesJava("x where (a + a) in (1, 2, 1500000000 + 1500000000)", true);  // multi const
            assertJitMatchesJava("x where (a + a) not in (1500000000 + 1500000000)", true);    // inverse
            // an in-range literal equal to the wrapped key must match too (pre-existing slice).
            assertJitMatchesJava("x where (a + a) in (-1294967296)", true);
            assertJitMatchesJava("x where (a + a) = 1500000000 + 1500000000", true);           // control: '='

            // Java (JIT-disabled) IN must agree with '=' across the const InLong* variants.
            Assert.assertEquals(1, runQuery("x where (a + a) in (1500000000 + 1500000000)"));      // single
            Assert.assertEquals(1, runQuery("x where (a + a) in (1, 1500000000 + 1500000000)"));   // two
            Assert.assertEquals(1, runQuery("x where (a + a) in (1, 2, 1500000000 + 1500000000)"));// multi
            Assert.assertEquals(0, runQuery("x where (a + a) not in (1500000000 + 1500000000)"));  // inverse
            Assert.assertEquals(1, runQuery("x where (a + a) = 1500000000 + 1500000000"));         // '='

            // runtime-const variant: a bind variable forces InLongRuntimeConstFunction. The const
            // element still matches the wrapped key; :p (= 0) does not.
            bindVariableService.setInt("p", 0);
            Assert.assertEquals(1, runQuery("x where (a + a) in (1500000000 + 1500000000, :p)"));

            // var variant: a non-constant element forces InLongVarFunction.
            Assert.assertEquals(1, runQuery("x where (a + a) in (b + b)"));
            Assert.assertEquals(1, runQuery("x where (a + a) = (b + b)"));
        });
    }

    @Test
    public void testInOperatorOverflowFoldMatchesJavaOnNarrowColumn() throws Exception {
        // An overflowing INT arithmetic fold inside an IN() list against a NARROW integer
        // key (INT/SHORT/BYTE). '=' (EqInt) wraps the fold mod 2^32 and the JIT emits the
        // wrapped I4, but the Java IN path read the element via getLong() and WIDENED it to
        // the full-width product. So IN disagreed with '=' even with the JIT off, and the
        // JIT disagreed with the Java IN. InLongFunctionFactory now reads narrow-key IN
        // elements at INT width (wrap), matching '='. The columns hold the wrapped image of
        // each overflow product so the correct (wrapping) behaviour matches a row.
        assertMemoryLeak(() -> {
            // 1000000 * 1000000 = 10^12 wraps to INT -727379968; 3 * 1431655767 = 2^32 + 5
            // wraps to INT 5 (matchable by SHORT/BYTE keys too).
            execute("create table x as (select cast(-727379968 as int) i32b, " +
                    "cast(5 as int) i32, cast(5 as short) i16, cast(5 as byte) i8)");

            // INT key, canonical 10^12 product, JIT-vs-Java parity across IN forms.
            assertJitMatchesJava("x where i32b in (1000000 * 1000000)", true);         // single value
            assertJitMatchesJava("x where i32b in (1, 1000000 * 1000000)", true);      // two values
            assertJitMatchesJava("x where i32b in (1, 2, 1000000 * 1000000)", true);   // multi value
            assertJitMatchesJava("x where i32b not in (1, 1000000 * 1000000)", true);  // inverse
            assertJitMatchesJava("x where i32b = 1000000 * 1000000", true);            // control: '='

            // The Java (JIT-disabled) IN must agree with '=' -- it widened before the fix.
            Assert.assertEquals(1, runQuery("x where i32b in (1000000 * 1000000)"));
            Assert.assertEquals(1, runQuery("x where i32b = 1000000 * 1000000"));

            // INT/SHORT/BYTE keys all wrap the element to 5 and match.
            assertJitMatchesJava("x where i32 in (3 * 1431655767)", true);
            assertJitMatchesJava("x where i16 in (3 * 1431655767)", true);
            assertJitMatchesJava("x where i8 in (3 * 1431655767)", true);
            Assert.assertEquals(1, runQuery("x where i32 in (3 * 1431655767)"));
            Assert.assertEquals(1, runQuery("x where i16 in (3 * 1431655767)"));
            Assert.assertEquals(1, runQuery("x where i8 in (3 * 1431655767)"));
            Assert.assertEquals(1, runQuery("x where i32 = 3 * 1431655767"));
            Assert.assertEquals(1, runQuery("x where i16 = 3 * 1431655767"));
            Assert.assertEquals(1, runQuery("x where i8 = 3 * 1431655767"));
        });
    }

    @Test
    public void testInOperatorOverflowFoldMatchesJavaOnNarrowKeyWithLongElement() throws Exception {
        // C4 regression: a narrow/INT KEY IN() list that mixes an overflowing INT-arithmetic
        // element with a genuine-LONG element. The Java InLong path reads the INT element at
        // the narrow key's width (getInt -> wrap), so the JIT must too. But
        // markI64WidenFoldRoots used to fold ONE comparison width across the whole IN list,
        // so a single coexisting LONG element (3000000000) promoted the list to I8 and WIDENED
        // the overflowing INT element to its full-width product -- the JIT then read 10^12
        // while the Java filter wrapped to -727379968 == the stored key, so the row matched in
        // Java but not in the JIT. The width is now derived per element (key vs that element),
        // so the INT element wraps (matching the key) while the LONG element stays at long
        // width (and never matches the narrow key either way).
        assertMemoryLeak(() -> {
            execute("create table x as (select cast(-727379968 as int) c)"); // -727379968 = (int)(1000000*1000000)

            // overflow-INT element coexists with a genuine-LONG element against the narrow key.
            assertJitMatchesJava("x where c in (1000000 * 1000000, 3000000000)", true);        // RED on HEAD
            assertJitMatchesJava("x where c in (3000000000, 1000000 * 1000000)", true);        // element order swapped
            assertJitMatchesJava("x where c in (1, 1000000 * 1000000, 3000000000)", true);     // plus a plain element
            assertJitMatchesJava("x where c not in (1000000 * 1000000, 3000000000)", true);    // inverse

            // The Java (JIT-disabled) path is the oracle: c matches the wrapped INT element only.
            Assert.assertEquals(1, runQuery("x where c in (1000000 * 1000000, 3000000000)"));
            Assert.assertEquals(0, runQuery("x where c not in (1000000 * 1000000, 3000000000)"));

            // control: an all-INT list against the narrow key already wrapped correctly.
            assertJitMatchesJava("x where c in (1, 1000000 * 1000000)", true);
        });
    }

    @Test
    public void testInOperatorOverflowWidenKeyOnLongElement() throws Exception {
        // C1 regression: an overflowing INT *arithmetic* KEY on the left of IN() against a
        // LONG element. 'in (LONG)' is 'key = LONG', which promotes to long, so the key must
        // WIDEN via getLong() (a * b = 10^12), NOT wrap via getInt() (= -727379968). The
        // narrow-int IN fix read the key at INT width for every element, so a LONG-column
        // element wrongly matched the wrapped key -- IN returned a different row than '=',
        // CAST and the JIT even with the JIT disabled. The IN path now derives the compare
        // width per element: a LONG/TIMESTAMP element widens the key, an INT element wraps it.
        //
        // Rows: a*b widens to 10^12 (both rows); el row1 = -727379968 (the wrapped image),
        // el row2 = 10^12 (the widened value); ei = -727379968 (the wrapped image, both rows).
        assertMemoryLeak(() -> {
            execute("create table x (id long, a int, b int, el long, ei int, k timestamp) timestamp(k)");
            execute("insert into x values (1, 1000000, 1000000, -727379968, -727379968, 1)," +
                    " (2, 1000000, 1000000, 1000000000000, -727379968, 2)");

            // A LONG COLUMN element forces InLongVarFunction (non-constant). The key widens,
            // so it matches the widened image (id 2), NOT the wrapped image (id 1). '=', CAST
            // and the JIT all agree, so JIT-vs-Java parity holds too.
            assertJitMatchesJava("select id from x where (a * b) in (el)", true);
            assertJitMatchesJava("select id from x where (a * b) not in (el)", true);
            assertJitMatchesJava("select id from x where (a * b) = el", true);              // control: '='
            // absolute correctness: the widened key selects row 2, not the wrapped row 1.
            Assert.assertEquals("id\n2\n", runJavaToString("select id from x where (a * b) in (el)"));
            Assert.assertEquals("id\n1\n", runJavaToString("select id from x where (a * b) not in (el)"));
            Assert.assertEquals(
                    runJavaToString("select id from x where (a * b) = el"),
                    runJavaToString("select id from x where (a * b) in (el)"));
            Assert.assertEquals(
                    runJavaToString("select id from x where cast(a * b as long) in (el)"),
                    runJavaToString("select id from x where (a * b) in (el)"));

            // A LONG CONSTANT element widens the key too (matching '='). Both rows carry the
            // same a*b = 10^12, so the widened key matches both; a wrapped key would match
            // neither. Const IN forms: single / two / multi. Interpreted IN must equal '='.
            // (The JIT wraps an overflowing arithmetic key against a genuine-LONG constant --
            // a pre-existing narrow-vs-long-const asymmetry that predates this PR and exists
            // for '=' too -- so JIT parity is deliberately not asserted for these.)
            Assert.assertEquals("id\n1\n2\n", runJavaToString("select id from x where (a * b) = 1000000000000"));
            Assert.assertEquals("id\n1\n2\n", runJavaToString("select id from x where (a * b) in (1000000000000)"));         // single
            Assert.assertEquals("id\n1\n2\n", runJavaToString("select id from x where (a * b) in (1000000000000, 5)"));      // two
            Assert.assertEquals("id\n1\n2\n", runJavaToString("select id from x where (a * b) in (1000000000000, 5, 6)"));   // multi
            Assert.assertEquals("id\n", runJavaToString("select id from x where (a * b) not in (1000000000000)"));

            // A runtime-constant (bind variable) LONG element forces InLongRuntimeConstFunction.
            bindVariableService.setLong("p", 1000000000000L);
            Assert.assertEquals("id\n1\n2\n", runJavaToString("select id from x where (a * b) in (:p)"));
            Assert.assertEquals("id\n1\n2\n", runJavaToString("select id from x where (a * b) = :p"));

            // Control -- an INT element must still WRAP the key (matching EqInt and the JIT).
            // master read the key via getLong() and WIDENED it, so IN wrongly returned no row
            // where '=' matched both; the wrapped key now matches the stored wrapped image.
            assertJitMatchesJava("select id from x where (a * b) in (ei)", true);
            assertJitMatchesJava("select id from x where (a * b) = ei", true);
            Assert.assertEquals("id\n1\n2\n", runJavaToString("select id from x where (a * b) in (ei)"));
            Assert.assertEquals("id\n1\n2\n", runJavaToString("select id from x where (a * b) in (1000000 * 1000000)"));
        });
    }

    @Test
    public void testInOperatorSingleValue() throws Exception {
        // Tests single-value IN() which has a special unrolled code path in CompiledFilterIRSerializer
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_int(1, 10, 0) i32," +
                " rnd_long(1, 10, 0) i64," +
                " rnd_float() f32," +
                " rnd_double() f64" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        FilterGenerator gen = new FilterGenerator()
                .withAnyOf("i32", "i64", "f32", "f64")
                .withAnyOf(" in ", " not in ")
                .withAnyOf("(5)");
        assertGeneratedQueryNotNull(ddl, gen);
    }

    @Test
    public void testInOperatorSingleValueChained() throws Exception {
        // Tests multiple single-value IN() conditions chained with AND/OR
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_int(1, 5, 0) a," +
                " rnd_int(1, 5, 0) b," +
                " rnd_int(1, 5, 0) c" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        FilterGenerator gen = new FilterGenerator()
                .withAnyOf("a")
                .withAnyOf(" in ", " not in ")
                .withAnyOf("(1)")
                .withBooleanOperator()
                .withAnyOf("b")
                .withAnyOf(" in ", " not in ")
                .withAnyOf("(2)")
                .withBooleanOperator()
                .withAnyOf("c")
                .withAnyOf(" in ", " not in ")
                .withAnyOf("(3)");
        assertGeneratedQueryNotNull(ddl, gen);
    }

    @Test
    public void testInOperatorSingleValueWithBooleanOperators() throws Exception {
        // Tests single-value IN() combined with AND/OR for short-circuit evaluation
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_int(1, 10, 0) i32," +
                " rnd_long(1, 10, 0) i64" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        FilterGenerator gen = new FilterGenerator()
                .withAnyOf("i32")
                .withAnyOf(" in ", " not in ")
                .withAnyOf("(5)")
                .withBooleanOperator()
                .withAnyOf("i64")
                .withAnyOf(" in ", " not in ")
                .withAnyOf("(7)");
        assertGeneratedQueryNotNull(ddl, gen);
    }

    @Test
    public void testInOperatorSingleValueWithNull() throws Exception {
        // Tests single-value IN() with null, special unrolled code path
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_int(1, 10, 5) i32," +
                " rnd_long(1, 10, 5) i64," +
                " rnd_float(5) f32," +
                " rnd_double(5) f64" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        FilterGenerator gen = new FilterGenerator()
                .withAnyOf("i32", "i64", "f32", "f64")
                .withAnyOf(" in ", " not in ")
                .withAnyOf("(null)", "(5)");
        assertGeneratedQueryNullable(ddl, gen);
    }

    @Test
    public void testInOperatorTwoValues() throws Exception {
        // Tests two-value IN() which also has an unrolled code path (args.size() < 3)
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_int(1, 10, 0) i32," +
                " rnd_long(1, 10, 0) i64," +
                " rnd_float() f32," +
                " rnd_double() f64" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        FilterGenerator gen = new FilterGenerator()
                .withAnyOf("i32", "i64", "f32", "f64")
                .withAnyOf(" in ", " not in ")
                .withAnyOf("(3, 7)");
        assertGeneratedQueryNotNull(ddl, gen);
    }

    @Test
    public void testInOperatorTwoValuesWithNull() throws Exception {
        // Tests two-value IN() with null, special unrolled code path
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_int(1, 10, 5) i32," +
                " rnd_long(1, 10, 5) i64," +
                " rnd_float(5) f32," +
                " rnd_double(5) f64" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        FilterGenerator gen = new FilterGenerator()
                .withAnyOf("i32", "i64", "f32", "f64")
                .withAnyOf(" in ", " not in ")
                .withAnyOf("(3, null)", "(null, 7)");
        assertGeneratedQueryNullable(ddl, gen);
    }

    @Test
    public void testInOperatorWithBooleanOperators() throws Exception {
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_int(1, 20, 0) i32," +
                " rnd_long(1, 20, 0) i64," +
                " rnd_float() f32," +
                " rnd_double() f64" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        FilterGenerator gen = new FilterGenerator()
                .withAnyOf("i32", "i64")
                .withAnyOf(" in ", " not in ")
                .withAnyOf("(1, 2, 3)", "(10, 11, 12)")
                .withBooleanOperator()
                .withAnyOf("f32", "f64")
                .withComparisonOperator()
                .withAnyOf("0.5");
        assertGeneratedQueryNotNull(ddl, gen);
    }

    @Test
    public void testInOperatorWithBooleanOperatorsAndNull() throws Exception {
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_int(1, 20, 5) i32," +
                " rnd_long(1, 20, 5) i64," +
                " rnd_float(5) f32," +
                " rnd_double(5) f64" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        FilterGenerator gen = new FilterGenerator()
                .withAnyOf("i32", "i64")
                .withAnyOf(" in ", " not in ")
                .withAnyOf("(1, 2, null)", "(null, 10, 11)")
                .withBooleanOperator()
                .withAnyOf("f32", "f64")
                .withAnyOf(" = ", " <> ")
                .withAnyOf("null");
        assertGeneratedQueryNullable(ddl, gen);
    }

    @Test
    public void testInOperatorWithOrChain() throws Exception {
        // Tests IN operator combined with OR chain
        final String query = "x where " +
                "a in (1, 2, 3) or b in (4, 5, 6) or c > 90";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_int(1, 10, 0) a," +
                " rnd_int(1, 10, 0) b," +
                " rnd_long(0, 100, 0) c" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertQueryNotNull(query, ddl);
    }

    @Test
    public void testIntColumnsCount() throws Exception {
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " cast(x as byte) i8," +
                " cast(x as short) i16," +
                " cast(x as int) i32," +
                " x i64" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        FilterGenerator gen = new FilterGenerator()
                .withAnyOf("i8", "i16", "i32", "i64")
                .withAnyOf(" != 0", " < 42");
        assertGeneratedQueryNotNull(ddl, gen);
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
        assertGeneratedQueryNotNull(ddl, gen);
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
        assertGeneratedQueryNotNull(ddl, gen);
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
        assertGeneratedQueryNullable(ddl, gen);
    }

    @Test
    public void testInterval() throws Exception {
        final String query = "x where k in '2021-11-29' and i32 > 0";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(to_timestamp('2021-11-29T10:00:00', 'yyyy-MM-ddTHH:mm:ss'), 500000000) as k," +
                " rnd_int() i32" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertQueryNotNull(query, ddl);
    }

    @Test
    public void testNarrowIntArithUnderLongWithFloat() throws Exception {
        // A narrow INT arithmetic subtree (c8 * -776782) that overflows int32
        // and feeds a LONG-width multiply diverged between the JIT and the Java
        // filter when a FLOAT comparison suppressed the narrow-to-i64 widening:
        // the JIT wrapped the inner product mod 2^32 while the Java filter read
        // it at long width via MulInt#getLong. The serializer now sign-extends
        // exactly the narrow leaves under the LONG-width subtree, so the JIT
        // stays on and both paths agree.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (c0 LONG, c2 SHORT, c8 INT, c9 FLOAT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t SELECT rnd_long(-1000000, 1000000, 8), rnd_short(), " +
                    "rnd_int(-1000000, 1000000, 8), rnd_float(8), " +
                    "timestamp_sequence(to_timestamp('2024-01-01', 'yyyy-MM-dd'), 1800000000L) " +
                    "FROM long_sequence(122)");

            // Previously diverging shapes: still JIT-compiled, now correct.
            assertJitMatchesJava("SELECT * FROM t WHERE c9 <= ((c0 - c2) * (c8 * -776782))", true);
            assertJitMatchesJava("SELECT * FROM t WHERE c9 <= (c0 * (c8 * -776782))", true);
            assertJitMatchesJava("SELECT * FROM t WHERE c9 <= (c0 + (c8 * -776782))", true);
            assertJitMatchesJava("SELECT * FROM t WHERE c9 <= (c0 * (c8 + 2000000000))", true);
            // Control shapes that were always correct under JIT.
            assertJitMatchesJava("SELECT * FROM t WHERE c9 <= (c8 * -776782)", true);
            assertJitMatchesJava("SELECT * FROM t WHERE c9 <= (c0 * c8)", true);
            assertJitMatchesJava("SELECT * FROM t WHERE (c8 * -776782) > 0", true);
        });
    }

    @Test
    public void testNotInOperatorFloat() throws Exception {
        // Tests NOT IN operator with floats
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_double() f64" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        FilterGenerator gen = new FilterGenerator()
                .withAnyOf("f64")
                .withAnyOf(" not in ")
                .withAnyOf("(0.1, 0.2, 0.3)", "(0.5, 0.6)");
        assertGeneratedQueryNotNull(ddl, gen);
    }

    @Test
    public void testNotInOperatorInt() throws Exception {
        // Tests NOT IN operator with integers
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_int(1, 10, 0) i32" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        FilterGenerator gen = new FilterGenerator()
                .withAnyOf("i32")
                .withAnyOf(" not in ")
                .withAnyOf("(1, 2, 3)", "(5, 6, 7, 8)");
        assertGeneratedQueryNotNull(ddl, gen);
    }

    @Test
    public void testNotInOperatorWithNull() throws Exception {
        // Tests NOT IN operator with null in the list
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_int(1, 10, 5) i32" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        FilterGenerator gen = new FilterGenerator()
                .withAnyOf("i32")
                .withAnyOf(" not in ")
                .withAnyOf("(1, 2, null)", "(5, null, 7)");
        assertGeneratedQueryNullable(ddl, gen);
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
        assertGeneratedQueryNullable(ddl, gen);
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
        assertGeneratedQueryNullable(ddl, gen);
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
    public void testShortCircuitAndDeepChain() throws Exception {
        // Tests short-circuit AND with a deep chain of predicates
        final String query = "x where " +
                "i64 > 10 and i64 < 90 and i64 != 20 and i64 != 30 " +
                "and i64 != 40 and i64 != 50 and i64 != 60 and i64 != 70";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_long(0, 100, 0) i64" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertQueryNotNull(query, ddl);
    }

    @Test
    public void testShortCircuitAndEarlyExit() throws Exception {
        // Tests short-circuit AND where first predicate is usually false
        // This tests early exit optimization
        final String query = "x where " +
                "i64 > 95 and i64 < 100 and i32 > 0";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_long(0, 100, 0) i64," +
                " rnd_int() i32" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertQueryNotNull(query, ddl);
    }

    @Test
    public void testShortCircuitFlagOptimizationAndEq() throws Exception {
        // Tests flag-based optimization for equality in AND chains.
        // When EQ is followed by And_Sc, the backend emits CMP + JNE directly
        // instead of CMP + SETE + TEST + JZ (kFlagsEq optimization).
        final String query = "x " +
                "where i64 = 95 and i32 = 13 and i16 = 12107";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_long(0, 100, 0) i64," +
                " rnd_int(0, 50, 0) i32," +
                " rnd_short() i16" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertQueryNotNull(query, ddl);
    }

    @Test
    public void testShortCircuitFlagOptimizationAndNeq() throws Exception {
        // Tests flag-based optimization for inequality in AND chains.
        // When NE is followed by And_Sc, the backend emits CMP + JE directly
        // instead of CMP + SETNE + TEST + JZ (kFlagsNe optimization).
        final String query = "x " +
                "where i64 != 50 and i32 != 25 and i16 != 10";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_long(0, 100, 0) i64," +
                " rnd_int(0, 50, 0) i32," +
                " rnd_short() i16" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertQueryNotNull(query, ddl);
    }

    @Test
    public void testShortCircuitFlagOptimizationMixedEqNeq() throws Exception {
        // Tests flag-based optimization with mixed EQ and NE in the same chain.
        final String query = "x " +
                "where i64 = 26 and i32 != 42 and i16 = 6201";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_long(0, 100, 0) i64," +
                " rnd_int(0, 50, 0) i32," +
                " rnd_short() i16" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertQueryNotNull(query, ddl);
    }

    @Test
    public void testShortCircuitFlagOptimizationOrEq() throws Exception {
        // Tests flag-based optimization for equality in OR chains.
        // When EQ is followed by Or_Sc, the backend emits CMP + JE directly
        // instead of CMP + SETE + TEST + JNZ (kFlagsEq optimization).
        final String query = "x where " +
                "i64 = 50 or i32 = 25 or i16 = 10";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_long(0, 100, 0) i64," +
                " rnd_int(0, 50, 0) i32," +
                " rnd_short() i16" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertQueryNotNull(query, ddl);
    }

    @Test
    public void testShortCircuitFlagOptimizationOrNeq() throws Exception {
        // Tests flag-based optimization for inequality in OR chains.
        // When NE is followed by Or_Sc, the backend emits CMP + JNE directly
        // instead of CMP + SETNE + TEST + JNZ (kFlagsNe optimization).
        final String query = "x where " +
                "i64 != 50 or i32 != 25 or i16 != 10";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_long(0, 100, 0) i64," +
                " rnd_int(0, 50, 0) i32," +
                " rnd_short() i16" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertQueryNotNull(query, ddl);
    }

    @Test
    public void testShortCircuitFlagOptimizationUuid() throws Exception {
        // Tests flag-based optimization for UUID (i128) comparisons.
        // UUID comparison uses pcmpeqb + pmovmskb + cmp, then JE/JNE.
        final String query = "x " +
                "where uuid1 = 'd37facdc-c648-4f32-887c-c184027ff724' and i64 = 57";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_uuid4() uuid1," +
                " rnd_long(0, 100, 0) i64" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertQueryNotNull(query, ddl);
    }

    @Test
    public void testShortCircuitFlagOptimizationUuidNeq() throws Exception {
        // Tests flag-based optimization for UUID (i128) inequality comparisons.
        final String query = "x where " +
                "uuid1 != '11111111-1111-1111-1111-111111111111' and i64 = 50";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_uuid4() uuid1," +
                " rnd_long(0, 100, 0) i64" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertQueryNotNull(query, ddl);
    }

    @Test
    public void testShortCircuitMixedAndOr() throws Exception {
        // Tests mixed AND/OR chains with short-circuit evaluation
        final String query = "x where " +
                "(i64 > 20 and i64 < 40) or (i64 > 60 and i64 < 80)";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_long(0, 100, 0) i64" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertQueryNotNull(query, ddl);
    }

    @Test
    public void testShortCircuitNestedAndOr() throws Exception {
        // Tests nested AND/OR with multiple levels
        final String query = "x where " +
                "(a > 10 and b > 10) or (c > 10 and d > 10) or (a < 5 and c < 5)";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_long(0, 100, 0) a," +
                " rnd_long(0, 100, 0) b," +
                " rnd_long(0, 100, 0) c," +
                " rnd_long(0, 100, 0) d" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertQueryNotNull(query, ddl);
    }

    @Test
    public void testShortCircuitOrDeepChain() throws Exception {
        // Tests short-circuit OR with a deep chain of predicates
        final String query = "x where " +
                "i64 = 10 or i64 = 20 or i64 = 30 or i64 = 40 " +
                "or i64 = 50 or i64 = 60 or i64 = 70 or i64 = 80";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_long(0, 100, 0) i64" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertQueryNotNull(query, ddl);
    }

    @Test
    public void testShortCircuitOrEarlyExit() throws Exception {
        // Tests short-circuit OR where first predicate is usually true
        // This tests early exit optimization
        final String query = "x where " +
                "i64 < 95 or i64 = 99 or i32 < 0";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_long(0, 100, 0) i64," +
                " rnd_int() i32" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertQueryNotNull(query, ddl);
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
        assertQueryNotNullNoCount(query, ddl);
    }

    @Test
    public void testSymbolNull() throws Exception {
        final String query = "x where sym <> null";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_symbol(10,1,3,5) sym" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertQueryNullable(query, ddl);
    }

    @Test
    public void testTimestampComparison() throws Exception {
        final String query = "x where t1 != t2";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_timestamp(to_timestamp('2020', 'yyyy'), to_timestamp('2021', 'yyyy'), 0) t1," +
                " rnd_timestamp(to_timestamp('2020', 'yyyy'), to_timestamp('2021', 'yyyy'), 0) t2" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertQueryNotNull(query, ddl);
    }

    @Test
    public void testTimestampComparison2() throws Exception {
        final String query = "x where ts >= 0";
        final String ddl = "create table x as " +
                "(select case when x < 10 then cast(NULL as TIMESTAMP) else cast(x as TIMESTAMP) end ts" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + "))";
        assertQueryNullable(query, ddl);
    }

    @Test
    public void testTimestampNull() throws Exception {
        final String query = "x where t <> null";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_timestamp(to_timestamp('2020', 'yyyy'), to_timestamp('2021', 'yyyy'), 5) t" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
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
        assertGeneratedQueryNullable(ddl, gen);
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
        assertGeneratedQueryNullable(ddl, gen);
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
        assertGeneratedQueryNullable(ddl, gen);
    }

    @Test
    public void testUuidSameConstantAndChain() throws Exception {
        final String query = "x " +
                "where uuid1 != '11111111-1111-1111-1111-111111111111'" +
                "  and uuid2 != '11111111-1111-1111-1111-111111111111'" +
                "  and uuid3 != '11111111-1111-1111-1111-111111111111'";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_uuid4() uuid1," +
                " rnd_uuid4() uuid2," +
                " rnd_uuid4() uuid3" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertQueryNotNull(query, ddl);
    }

    @Test
    public void testVarSizeNullComparison() throws Exception {
        final String ddl = "create table x as (select" +
                " x," +
                " timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_str(2, 1, 5, 3) string_value," +
                " rnd_varchar(1, 5, 3) varchar_value," +
                " rnd_bin(1, 32, 3) binary_value" +
                " from long_sequence(1000)) timestamp(k)";
        final FilterGenerator gen = new FilterGenerator()
                .withAnyOf("string_value", "varchar_value", "binary_value")
                .withEqualityOperator()
                .withAnyOf("null")
                .withBooleanOperator()
                .withAnyOf("string_value", "varchar_value", "binary_value")
                .withEqualityOperator()
                .withAnyOf("null");
        assertGeneratedQueryNullable(ddl, gen);
    }

    @Test
    public void testVarcharNullComparison() throws Exception {
        final String ddl = "create table x as (select" +
                " x," +
                " timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_varchar(1, 5, 3) varchar_value" +
                " from long_sequence(1000)) timestamp(k)";
        final FilterGenerator gen = new FilterGenerator()
                .withAnyOf("varchar_value")
                .withEqualityOperator()
                .withAnyOf("null");
        assertGeneratedQueryNullable(ddl, gen);
    }

    private void assertGeneratedQuery(CharSequence ddl, FilterGenerator gen, boolean notNull) throws Exception {
        assertMemoryLeak(() -> {
            if (ddl != null) {
                execute(ddl);
            }

            long maxCount = 0;
            List<String> filters = gen.generate();
            LOG.info().$("generated ").$(filters.size()).$(" filter expressions for base query: select * from x").$();
            Assert.assertFalse(filters.isEmpty());
            for (String filter : filters) {
                long count = runQuery("x where " + filter);
                maxCount = Math.max(maxCount, count);

                assertJitQuery("x where " + filter, notNull);
                assertJitCountQuery("select count() from x where " + filter, count);
            }
            Assert.assertTrue("at least one query is expected to return rows", maxCount > 0);
        });
    }

    private void assertGeneratedQueryNotNull(CharSequence ddl, FilterGenerator gen) throws Exception {
        assertGeneratedQuery(ddl, gen, true);
    }

    private void assertGeneratedQueryNullable(CharSequence ddl, FilterGenerator gen) throws Exception {
        assertGeneratedQuery(ddl, gen, false);
    }

    /**
     * Runs {@code query} with JIT disabled and with JIT enabled and asserts the
     * two cursors produce identical output. {@code expectJit} pins whether the
     * JIT-enabled run is expected to compile a filter (true) or fall back to the
     * Java filter (false), guarding against both the divergence and over-eager
     * fallback.
     */
    private void assertJitMatchesJava(CharSequence query, boolean expectJit) throws SqlException {
        StringSink javaSink = new StringSink();
        sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
        try (RecordCursorFactory factory = select(query)) {
            Assert.assertFalse("JIT was enabled for query: " + query, factory.usesCompiledFilter());
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                CursorPrinter.println(cursor, factory.getMetadata(), javaSink);
            }
        }

        StringSink jit = new StringSink();
        sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
        try (RecordCursorFactory factory = select(query)) {
            Assert.assertEquals("unexpected compiled-filter usage for query: " + query,
                    expectJit, factory.usesCompiledFilter());
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                CursorPrinter.println(cursor, factory.getMetadata(), jit);
            }
        }
        TestUtils.assertEquals("JIT vs Java result mismatch for query: " + query, javaSink, jit);
    }

    private void assertJitCountQuery(CharSequence countQuery, long expectedCount) throws SqlException {
        sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_FORCE_SCALAR);
        long actualCount = runJitCountQuery(countQuery);
        Assert.assertEquals("[scalar mode] count mismatch for query: " + countQuery, expectedCount, actualCount);

        sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
        runJitQuery(countQuery);
        Assert.assertEquals("[vectorized mode] count mismatch for query: " + countQuery, expectedCount, actualCount);
    }

    private void assertJitQuery(CharSequence query, boolean notNull) throws SqlException {
        sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_FORCE_SCALAR);
        runJitQuery(query);
        TestUtils.assertEquals("[scalar mode] result mismatch for query: " + query, sink, jitSink);

        sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
        runJitQuery(query);
        TestUtils.assertEquals("[vectorized mode] result mismatch for query: " + query, sink, jitSink);

        // At the moment, there is no way for users to disable null checks in the
        // JIT compiler output. Yet, we want to test this part of the compiler.
        if (notNull) {
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
                execute(ddl);
            }

            long count = runQuery(query);
            Assert.assertTrue("query is expected to return rows", count > 0);

            assertJitQuery(query, notNull);
            assertJitCountQuery("select count() from " + query, count);
        });
    }

    private void assertQueryNotNull(CharSequence query, CharSequence ddl) throws Exception {
        assertQuery(query, ddl, false);
    }

    private void assertQueryNotNullNoCount(CharSequence query, CharSequence ddl) throws Exception {
        assertMemoryLeak(() -> {
            if (ddl != null) {
                execute(ddl);
            }

            long count = runQuery(query);
            Assert.assertTrue("query is expected to return rows", count > 0);

            assertJitQuery(query, false);
        });
    }

    /**
     * Same checks as {@link #assertQueryNotNull} but without a surrounding
     * {@code assertMemoryLeak} - call this from inside a single
     * {@code assertMemoryLeak} when running multiple queries against shared
     * DDL.
     */
    private void assertQueryNotNullNoLeakCheck(CharSequence query) throws SqlException {
        long count = runQuery(query);
        Assert.assertTrue("query is expected to return rows", count > 0);
        assertJitQuery(query, false);
        assertJitCountQuery("select count() from " + query, count);
    }

    private void assertQueryNullable(CharSequence query, CharSequence ddl) throws Exception {
        assertQuery(query, ddl, true);
    }

    /**
     * No-leak-check counterpart of {@link #assertQueryNullable}; intended for
     * sharing a single DDL across multiple query assertions inside one
     * {@code assertMemoryLeak} block.
     */
    private void assertQueryNullableNoLeakCheck(CharSequence query) throws SqlException {
        long count = runQuery(query);
        assertJitQuery(query, true);
        assertJitCountQuery("select count() from " + query, count);
    }

    private long runJitCountQuery(CharSequence countQuery) throws SqlException {
        try (RecordCursorFactory factory = select(countQuery)) {
            Assert.assertTrue("JIT was not enabled for query: " + countQuery, factory.usesCompiledFilter());
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Assert.assertTrue(cursor.hasNext());
                return cursor.getRecord().getLong(0);
            }
        }
    }

    private void runJitQuery(CharSequence query) throws SqlException {
        try (RecordCursorFactory factory = select(query)) {
            Assert.assertTrue("JIT was not enabled for query: " + query, factory.usesCompiledFilter());
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                RecordMetadata metadata = factory.getMetadata();
                CursorPrinter.println(cursor, metadata, jitSink);
            }
        }
    }

    private String runJavaToString(CharSequence query) throws SqlException {
        StringSink javaSink = new StringSink();
        sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
        try (RecordCursorFactory factory = select(query)) {
            Assert.assertFalse("JIT was enabled for query: " + query, factory.usesCompiledFilter());
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                CursorPrinter.println(cursor, factory.getMetadata(), javaSink);
            }
        }
        return javaSink.toString();
    }

    private long runQuery(CharSequence query) throws SqlException {
        long resultSize;
        sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
        try (RecordCursorFactory factory = select(query)) {
            Assert.assertFalse("JIT was enabled for query: " + query, factory.usesCompiledFilter());
            try (CountingRecordCursor cursor = new CountingRecordCursor(factory.getCursor(sqlExecutionContext))) {
                println(factory, cursor);
                resultSize = cursor.count();
            }
        }
        return resultSize;
    }

    private void testOrderBy(String orderByClause) throws Exception {
        final String query = "x where price > 0 " + orderByClause;
        final String ddl = "create table x as " +
                "(select rnd_symbol('ABB','HBC','DXR') sym, \n" +
                " rnd_double() price, \n" +
                " timestamp_sequence(172800000000, 360000000) ts \n" +
                "from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp (ts)";
        assertQueryNotNullNoCount(query, ddl);
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
        public long preComputedStateSize() {
            return delegate.preComputedStateSize();
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
