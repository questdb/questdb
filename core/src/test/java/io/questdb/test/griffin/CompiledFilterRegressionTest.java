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
import io.questdb.std.Vect;
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
    // Rows the current batch-length sweep has returned across its iterations; see
    // assertJitMatchesJavaOnBatchLengths.
    private int batchSweepRows;

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
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " rnd_boolean() bool1," +
                " rnd_boolean() bool2" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertQueryNotNull(query, ddl);
    }

    @Test
    public void testBooleanOperators() throws Exception {
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
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
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
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
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
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
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
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
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
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
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
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
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
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
    public void testColumnArithmeticWidthUnderBooleanEquality() throws Exception {
        // The column analog of testConstantFoldWidthUnderBooleanEquality. A boolean
        // equality of two comparisons - (cmp) = (cmp) - forms a SINGLE predicate
        // context, so a sibling LONG comparison turns the predicate-global narrow-i64
        // widening on for the whole predicate. An overflowing narrow-int arithmetic
        // COLUMN product on the wrap-side of the INT-width comparison was then
        // sign-extended and computed at 64 bits, diverging from the Java filter's
        // MulInt#getInt (which wraps mod 2^32): for ((a*b) = -727_379_968) = (nl > 0)
        // the Java filter returned {1,3} and the JIT {2,4,5}. The narrow-arith leaf
        // widening is now derived per-comparison (i64WrapLeaves), so both agree.
        assertMemoryLeak(() -> {
            execute("create table p as (select cast(1_000_000 as int) a, cast(1_000_000 as int) b," +
                    " cast(x as long) rid," +
                    " cast(case x when 1 then 1_000_000_000_000 when 3 then 5 else 0 end as long) nl," +
                    " x::short cs, x::byte cbyte, timestamp_sequence(0, 1_000_000) k" +
                    " from long_sequence(5)) timestamp(k)");
            // Previously diverging shapes: an INT-width comparison of a narrow-int
            // product, ANDed as a boolean equality with a LONG comparison.
            // Absolute pin: only rows 1 and 3 (nl > 0) survive; the pre-fix JIT returned {2,4,5}.
            assertJitMatchesJava("p where ((a*b) = -727_379_968) = (nl > 0)", true,
                    "a\tb\trid\tnl\tcs\tcbyte\tk\n" +
                            "1000000\t1000000\t1\t1000000000000\t1\t1\t1970-01-01T00:00:00.000000Z\n" +
                            "1000000\t1000000\t3\t5\t3\t3\t1970-01-01T00:00:02.000000Z\n");
            assertJitMatchesJava("p where ((a*b) > 0) = (nl > 0)", true);           // no magic constant
            assertJitMatchesJava("p where (nl > 0) = ((a*b) = -727_379_968)", true);  // operand order
            assertJitMatchesJava("p where ((a*b) <> -727_379_968) = (nl > 0)", true);
            // The plain narrow column read on the LONG-comparison side sign-extends
            // value-preservingly; only the narrow product on the INT side wraps.
            assertJitMatchesJava("p where (cs = nl) = (a*b > 0)", true);
            assertJitMatchesJavaOnEmptyResult("p where (cbyte = nl) = (a*b = -727_379_968)", true);
            // Controls: a LONG-width comparison inside the boolean equality still
            // widens the product on both paths.
            assertJitMatchesJava("p where (a*b > nl) = (nl > 0)", true);
            // Controls: single comparison, AND, OR each form separate predicate
            // contexts and were always correct.
            assertJitMatchesJava("p where (a*b) = -727_379_968", true);
            assertJitMatchesJava("p where (a*b) = -727_379_968 and nl > 0", true);
            assertJitMatchesJava("p where (a*b) = -727_379_968 or nl > 0", true);
            // Control: INT arith directly compared to a LONG column widens.
            assertJitMatchesJava("p where a*b >= -432_577_000_000L", true);
        });
    }

    @Test
    public void testColumnArithmeticWidthUnderBooleanEqualityWithFloat() throws Exception {
        // A FLOAT anywhere in the predicate suppresses the predicate-global narrow-i64
        // widening (NarrowI64WidenDetector.shouldWiden() returns false when hasFloat),
        // so markFloatI64WidenLeaves is the ONLY pass that can sign-extend a narrow-int
        // arithmetic subtree read at long width. It walked each comparison operand with
        // its OWN static type, so a narrow-int COLUMN product compared against a LONG
        // sibling - ((a*b) > nl) - was left at INT width and wrapped mod 2^32 via the
        // JIT's int32 MUL, while the Java filter reads MulInt#getLong (no wrap). For
        // ((a*b) > nl) = (f32 > 0) the JIT returned 0 rows and the Java filter 4. The
        // boundary now promotes the comparison width across all operands (foldCmpType),
        // matching markI64WrapArithLeaves / markI64WidenFoldRoots, so both agree. The
        // widened leaf emits SX_I64 (no AVX2 path), which forces scalar mode - the query
        // still JIT-compiles (usesCompiledFilter stays true), it just runs scalar.
        assertMemoryLeak(() -> {
            execute("create table p as (select cast(1_000_000 as int) a, cast(1_000_000 as int) b," +
                    " cast(case x when 1 then 1_000_000_000_000 else 0 end as long) nl," +
                    " cast(1.0 as float) f32, x::short cs, x::byte cbyte," +
                    " timestamp_sequence(0, 1_000_000) k" +
                    " from long_sequence(5)) timestamp(k)");
            // Primary repro: a narrow-int product against a LONG column, in a boolean equality
            // with a float sibling. a*b wraps to -727_379_968, so it is never above nl (0 or
            // 10^12) and the left comparison is false on every row while (f32 > 0) is true - the
            // equality is false throughout. Both engines have to say so.
            assertJitMatchesJavaOnEmptyResult("p where ((a*b) > nl) = (f32 > 0)", true);
            // The complementary spelling bites: the wrapped product IS below nl, so the equality
            // holds on every row and an over-widened product would drop them all.
            assertJitMatchesJava("p where ((a*b) < nl) = (f32 > 0)", true,
                    "a\tb\tnl\tf32\tcs\tcbyte\tk\n" +
                            "1000000\t1000000\t1000000000000\t1.0\t1\t1\t1970-01-01T00:00:00.000000Z\n" +
                            "1000000\t1000000\t0\t1.0\t2\t2\t1970-01-01T00:00:01.000000Z\n" +
                            "1000000\t1000000\t0\t1.0\t3\t3\t1970-01-01T00:00:02.000000Z\n" +
                            "1000000\t1000000\t0\t1.0\t4\t4\t1970-01-01T00:00:03.000000Z\n" +
                            "1000000\t1000000\t0\t1.0\t5\t5\t1970-01-01T00:00:04.000000Z\n");
            // Operand order and a non-zero float threshold agree with it.
            assertJitMatchesJava("p where (f32 > 0) = ((a*b) < nl)", true);
            assertJitMatchesJava("p where ((a*b) < nl) = (f32 > 0.5)", true);
            // Narrow column read (not a product) vs a LONG column sign-extends
            // value-preservingly; the float sibling does not change that.
            assertJitMatchesJava("p where (cs > nl) = (f32 > 0)", true);
            assertJitMatchesJava("p where (cbyte > nl) = (f32 > 0)", true);
            // Over-widening guard: an INT-width comparison of the narrow product must
            // still WRAP even with a float present - cmpType stays I4 there, so the
            // product folds to exactly -727_379_968 and matches every row. Absolute pin:
            // all 5 rows survive ((a*b) = -727_379_968 is true, f32 > 0 is true).
            assertJitMatchesJava("p where ((a*b) = -727_379_968) = (f32 > 0)", true,
                    "a\tb\tnl\tf32\tcs\tcbyte\tk\n" +
                            "1000000\t1000000\t1000000000000\t1.0\t1\t1\t1970-01-01T00:00:00.000000Z\n" +
                            "1000000\t1000000\t0\t1.0\t2\t2\t1970-01-01T00:00:01.000000Z\n" +
                            "1000000\t1000000\t0\t1.0\t3\t3\t1970-01-01T00:00:02.000000Z\n" +
                            "1000000\t1000000\t0\t1.0\t4\t4\t1970-01-01T00:00:03.000000Z\n" +
                            "1000000\t1000000\t0\t1.0\t5\t5\t1970-01-01T00:00:04.000000Z\n");
            // A pure FLOAT comparison of the product wraps (getDouble -> getInt), so no
            // widening: cmpType is floating, cmpLong stays false.
            assertJitMatchesJavaOnEmptyResult("p where (a*b) > f32", true);
            // Controls: AND/OR split into separate predicate contexts. The wrapped product is
            // never above nl, so the AND is empty and the OR reduces to the float conjunct.
            assertJitMatchesJavaOnEmptyResult("p where (a*b > nl) and f32 > 0", true);
            assertJitMatchesJava("p where (a*b < nl) and f32 > 0", true);
            assertJitMatchesJava("p where (a*b > nl) or f32 > 0", true);
        });
    }

    @Test
    public void testColumnFloatComparisonWithNulls() throws Exception {
        // Regression test for ARM64 NaN condition code handling.
        // ARM64 fcmp with NaN sets NZCV=0011. Ordered less-than must use MI (not LT),
        // and ordered less-or-equal must use LS (not LE), otherwise NaN compares as true.
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
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
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
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
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
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
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
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
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " rnd_float() f32," +
                " rnd_double() f64 " +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertQueryNotNull(query, ddl);
    }

    @Test
    public void testColumnIntConstantComparison() throws Exception {
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
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
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
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
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
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
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
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
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
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
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
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
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
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
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
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
    public void testConstantArithmeticWidthUnderBooleanEqualityWithFloat() throws Exception {
        // The bare-constant analog of testColumnArithmeticWidthUnderBooleanEqualityWithFloat
        // (which used a LONG column). A FLOAT anywhere in the predicate suppresses the
        // predicate-global narrow-i64 widening, so markFloatI64WidenLeaves is the ONLY pass
        // that can sign-extend a narrow-int arithmetic subtree read at long width. For
        // ((a*b) = 4_999_999_999) = (f32 > 0) it correctly widens the product's leaves (a, b) so
        // the JIT computes a*b = 5e9 at 64 bits, but it returned at the bare CONSTANT leaf and
        // left 4_999_999_999 unwidened. The type observer sees only INT and FLOAT columns (both
        // 4 bytes, so hasMixedSizes() is false), types the constant down to F4, and
        // serializeNumber rounds it to the nearest float - 4_999_999_999 -> 5.0e9f (floats near
        // 2^32 are 512 apart) - so the JIT float-compared 5e9 == 5.0e9f and matched, while the
        // Java filter reads MulInt#getLong vs the LONG literal (5e9 == 4_999_999_999, no match).
        // JIT returned every row, Java none. markI64Widen now widens a bare out-of-INT-range
        // integer constant compared against a narrow-int arithmetic operand, so
        // serializeConstant emits a full I8 IMM. The widened product forces scalar mode
        // (SX_I64 has no AVX2 path); the query still JIT-compiles (usesCompiledFilter stays
        // true).
        assertMemoryLeak(() -> {
            execute("create table p as (select cast(100_000 as int) a, cast(50_000 as int) b," +
                    " cast(1.0 as float) f32, cast(1.0 as double) f64," +
                    " x::short cs, x::byte cbyte," +
                    " timestamp_sequence(0, 1_000_000) k" +
                    " from long_sequence(5)) timestamp(k)");
            // Primary repro: a*b = 5e9 at long width but 4_999_999_999 rounds to 5.0e9f. Absolute
            // pin: the equality is false on the Java path (5e9 != 4_999_999_999), so the boolean
            // equality with (f32 > 0 = true) is false for every row - 0 rows. The pre-fix JIT
            // returned all 5.
            assertJitMatchesJava("select cs from p where ((a*b) = 4_999_999_999) = (f32 > 0)", true, "cs\n");
            // Every comparison operator diverges the same way (the constant rounds up to 5e9f):
            // > flips true->false, <= flips false->true, <> flips true->false. Absolute pin the
            // > case: (a*b) > 4_999_999_999 is true, so the boolean equality is true for all rows.
            assertJitMatchesJava("select cs from p where ((a*b) > 4_999_999_999) = (f32 > 0)", true,
                    "cs\n");
            assertJitMatchesJava("select cs from p where ((a*b) <= 4_999_999_999) = (f32 > 0)", true, "cs\n1\n2\n3\n4\n5\n");
            assertJitMatchesJava("select cs from p where ((a*b) <> 4_999_999_999) = (f32 > 0)", true,
                    "cs\n1\n2\n3\n4\n5\n");
            // The product wraps to 705_032_704, so it is BELOW the bound: >= is false on every row
            // and < is true on every row. The pair is complementary, so one of them always bites.
            assertJitMatchesJavaOnEmptyResult("p where ((a*b) >= 4_999_999_999) = (f32 > 0)", true);
            assertJitMatchesJava("p where ((a*b) < 4_999_999_999) = (f32 > 0)", true);
            // Operand order (constant on the left, and the two comparisons swapped).
            assertJitMatchesJavaOnEmptyResult("p where (4_999_999_999 = (a*b)) = (f32 > 0)", true);
            assertJitMatchesJavaOnEmptyResult("p where (f32 > 0) = ((a*b) = 4_999_999_999)", true);
            // Through a NOT wrapper.
            assertJitMatchesJava("p where not (((a*b) = 4_999_999_999) = (f32 > 0))", true);

            // SAFE boundaries - must keep passing, do not over-widen:
            // Negated / folded constant is an OPERATION handled by the fold-root path, not by
            // the bare-CONSTANT widen: (a*b) < -4_999_999_999 is false, so the equality is false.
            assertJitMatchesJavaOnEmptyResult("p where ((a*b) < -4_999_999_999) = (f32 > 0)", true);
            // AND / OR siblings split into separate predicate contexts (no float suppression on
            // the (a*b) = 4_999_999_999 sub-predicate, so needsNarrowI64Widening handles it).
            assertJitMatchesJavaOnEmptyResult("p where (a*b) = 4_999_999_999 and f32 > 0", true);
            assertJitMatchesJava("p where (a*b) = 4_999_999_999 or f32 > 0", true);
            // A DOUBLE sibling makes hasMixedSizes() true (I4 vs F8), so serializeUntypedNumber
            // already emits an exact I8 - the fix widens redundantly, result unchanged.
            assertJitMatchesJavaOnEmptyResult("p where ((a*b) = 4_999_999_999) = (f64 > 0)", true);
            // BYTE / SHORT products cannot reach 2^31 and their I1 / I2 size differs from F4, so
            // the constant is already mixed-size (exact I8) and never hits the float gap.
            assertJitMatchesJavaOnEmptyResult("p where ((cbyte * cbyte) = 4_999_999_999) = (f32 > 0)", true);
            assertJitMatchesJavaOnEmptyResult("p where ((cs * cs) = 4_999_999_999) = (f32 > 0)", true);
            // A constant-fold root. Both operands are out of INT range, so the subtree is LONG and
            // folds at full width to 4_999_999_999 - the wrapped product is below it, exactly as
            // against the bare constant above, and the complementary spelling bites.
            assertJitMatchesJavaOnEmptyResult("p where ((a*b) > (2_500_000_000 + 2_499_999_999)) = (f32 > 0)", true);
            assertJitMatchesJava("p where ((a*b) < (2_500_000_000 + 2_499_999_999)) = (f32 > 0)", true);
            // An IN key takes its per-element width from serializeIn's inKeyWidthOverride.
            assertJitMatchesJavaOnEmptyResult("p where ((a*b) in (4_999_999_999)) = (f32 > 0)", true);
            // Over-widening guard: an IN-RANGE constant (705_032_704 = 5e9 wrapped mod 2^32) must
            // still WRAP against the INT-width product - cmpType stays I4, the constant stays
            // I4, and MulInt#getInt matches it on both paths. Absolute pin: all 5 rows survive.
            assertJitMatchesJava("select cs from p where ((a*b) = 705_032_704) = (f32 > 0)", true,
                    "cs\n1\n2\n3\n4\n5\n");
        });
    }

    @Test
    public void testConstantColumnArithmetics() throws Exception {
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
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
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
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
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
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
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " rnd_long(0, 200, 0) i64" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertQueryNotNull(query, ddl);
    }

    @Test
    public void testConstantFoldDivisionAtIntWidth() throws Exception {
        // A folded constant with a non-modular operator (division) must replicate
        // the Java filter's per-op INT wrapping at an INT-width comparison.
        // (1_000_000 * 1_000_000) / 7 folds in long to 142_857_142_857, whose low 32
        // bits are +1_123_222_089, but DivInt#getInt computes
        // (int) (1_000_000 * 1_000_000) / 7 = -103_911_424. Folding in long and
        // truncating diverged: the JIT returned 0 rows where the Java filter
        // returned every row.
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " cast(x - 100 as byte) i8," +
                " cast(x - 100 as short) i16," +
                " cast(x - 100 as int) i32," +
                " (x - 100) i64" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertMemoryLeak(() -> {
            execute(ddl);
            // Per-op INT wrap = -103_911_424; every narrow/INT row (>= -99) exceeds it.
            assertQueryNotNullNoLeakCheck("x where i8 > (1_000_000 * 1_000_000) / 7");
            assertQueryNotNullNoLeakCheck("x where i16 > (1_000_000 * 1_000_000) / 7");
            assertQueryNotNullNoLeakCheck("x where i32 > (1_000_000 * 1_000_000) / 7");
            // LONG column reads full long width via DivInt#getLong = 142_857_142_857;
            // no i64 row reaches it, and the I8 fold path already agreed here.
            assertQueryNullableNoLeakCheck("x where i64 > (1_000_000 * 1_000_000) / 7");
        });
    }

    @Test
    public void testConstantFoldIntNullCollision() throws Exception {
        // An inner constant product that wraps to EXACTLY INT_NULL (-2^31)
        // poisons the rest of an INT-width fold: the Java filter's
        // MulInt/AddInt#getInt return INT_NULL once an operand is INT_NULL, so
        // i8 > (65_536 * 32_768) * 2 collapses to i8 > NULL (no rows). The JIT I4
        // fold did pure modular arithmetic (-2^31 * 2 = 0) and returned i8 > 0
        // (rows). tryFoldConstantArithI4 now propagates INT_NULL like the
        // runtime ops, so both paths agree.
        assertMemoryLeak(() -> {
            execute("create table x as (select timestamp_sequence(0, 1_000_000) k," +
                    " cast(x - 2 as byte) i8," +
                    " cast(x - 2 as short) i16," +
                    " cast(x - 2 as int) i32," +
                    " (x - 2) i64" +
                    " from long_sequence(5)) timestamp(k)");
            // INT-width comparisons: inner 65_536 * 32_768 wraps to INT_NULL, so
            // the whole fold is NULL and no row matches.
            // Absolute pin: the fold collapses to NULL, so no row matches (header only); the
            // pre-fix JIT read i8 > 0 and returned rows.
            assertJitMatchesJava("x where i8 > (65_536 * 32_768) * 2", true, "k\ti8\ti16\ti32\ti64\n");
            assertJitMatchesJavaOnEmptyResult("x where i16 > (65_536 * 32_768) * 2", true);
            assertJitMatchesJavaOnEmptyResult("x where i32 > (65_536 * 32_768) * 2", true);
            // LONG column promotes to long width: getLong() never wraps onto the
            // sentinel, so the constant is the genuine 4_294_967_296 on both paths.
            assertJitMatchesJavaOnEmptyResult("x where i64 > (65_536 * 32_768) * 2", true);
        });
    }

    @Test
    public void testConstantFoldLongNullCollision() throws Exception {
        // The LONG analog of testConstantFoldIntNullCollision. An inner constant
        // product that lands EXACTLY on Long.MIN_VALUE (-2^63, the LONG null
        // sentinel) poisons the rest of a long-width fold: the Java filter's
        // MulLong/AddLong#getLong return LONG_NULL once an operand is LONG_NULL,
        // so i64 = (4_611_686_018_427_387_904 * -2) + 5 folds the RHS to NULL and
        // matches the null row. The JIT long fold kept computing full-width
        // arithmetic ((2^62 * -2) + 5 = Long.MIN + 5) and matched no row.
        // tryFoldConstantArith now propagates LONG_NULL, so both paths agree.
        assertMemoryLeak(() -> {
            execute("create table lp as (select timestamp_sequence(0, 1_000_000) k," +
                    " case when x = 1 then cast(null as long) else x end i64" +
                    " from long_sequence(3)) timestamp(k)");
            // 4_611_686_018_427_387_904 = 2^62; 2^62 * -2 = Long.MIN (intermediate),
            // + 5 a non-sentinel final. The fold collapses to LONG_NULL on both
            // paths, so only the null row matches '='.
            // Absolute pin: the fold collapses to LONG_NULL, so only the null row matches '=';
            // the pre-fix JIT computed Long.MIN + 5 and matched nothing.
            assertJitMatchesJava("lp where i64 = (4_611_686_018_427_387_904 * -2) + 5", true,
                    "k\ti64\n" +
                            "1970-01-01T00:00:00.000000Z\tnull\n");
            assertJitMatchesJava("lp where i64 <> (4_611_686_018_427_387_904 * -2) + 5", true);
            assertJitMatchesJava("lp where i64 in (1, (4_611_686_018_427_387_904 * -2) + 5)", true);
            assertJitMatchesJava("lp where i64 not in (1, (4_611_686_018_427_387_904 * -2) + 5)", true);
            // Control: a fold whose FINAL value is Long.MIN agrees coincidentally
            // (both paths emit the sentinel), so it was never a divergence.
            assertJitMatchesJava("lp where i64 = 4_611_686_018_427_387_904 * -2", true);
        });
    }

    @Test
    public void testConstantFoldRootUnderLongWithFloat() throws Exception {
        // An overflowing constant product (258_558 * -259_815) nested under a LONG
        // add (c0 + ...) is read at long width by AddLong#getLong, so the Java
        // filter never wraps it. A FLOAT in the predicate suppressed the global
        // narrow-i64 widening, and the old check only looked at LONG *leaves*, so the
        // JIT folded the product to a wrapped I4 IMM and diverged (JIT all rows, Java
        // none). markI64WidenFoldRoots / genuineArithType now tag such fold roots for a
        // full I8 IMM.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (c0 LONG, c8 INT, c9 FLOAT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t SELECT rnd_long(-1_000_000, 1_000_000, 8), " +
                    "rnd_int(-1_000_000, 1_000_000, 8), rnd_float(8), " +
                    "timestamp_sequence(to_timestamp('2024-01-01', 'yyyy-MM-dd'), 1_800_000_000L) " +
                    "FROM long_sequence(122)");
            // Previously diverging shapes: fold root under a genuine LONG op with a
            // FLOAT comparison. Still JIT-compiled, now correct.
            // Absolute pin: the fold is a large negative long, so no float c9 is <= it and Java
            // returns 0 rows; the pre-fix JIT wrapped it to a small I4 and returned every row.
            assertJitMatchesJava("SELECT * FROM t WHERE c9 <= (c0 + (258_558 * -259_815))", true,
                    "c0\tc8\tc9\tts\n995420\t724408\t0.28455776\t2024-01-01T00:00:00.000000Z\n-197185\t530625\t0.13123357\t2024-01-01T00:30:00.000000Z\n931616\t252740\t0.88992864\t2024-01-01T01:00:00.000000Z\n32662\t-492805\t0.46218354\t2024-01-01T01:30:00.000000Z\n305205\t-544245\t0.7261136\t2024-01-01T02:00:00.000000Z\n152628\t444703\t0.709436\t2024-01-01T02:30:00.000000Z\n861047\t-94278\t0.0035983324\t2024-01-01T03:00:00.000000Z\n563251\tnull\t0.7675673\t2024-01-01T03:30:00.000000Z\n277159\t127479\t0.1578663\t2024-01-01T04:00:00.000000Z\n-16687\t611233\t0.5793466\t2024-01-01T04:30:00.000000Z\n646784\t-398329\t0.6761935\t2024-01-01T05:00:00.000000Z\n672088\tnull\t0.97552633\t2024-01-01T05:30:00.000000Z\n782963\t-732673\t0.41381645\t2024-01-01T06:00:00.000000Z\n-457772\tnull\t0.8445258\t2024-01-01T06:30:00.000000Z\n6099\t846598\t0.8847591\t2024-01-01T07:00:00.000000Z\n-834573\t787024\t0.8258367\t2024-01-01T07:30:00.000000Z\n-908023\t-398574\t0.92050034\t2024-01-01T08:00:00.000000Z\n353576\t-494248\t0.86641586\t2024-01-01T08:30:00.000000Z\n942690\t-983929\t0.5659429\t2024-01-01T09:00:00.000000Z\n556636\t-188008\t0.72300154\t2024-01-01T09:30:00.000000Z\n-713605\t-204515\t0.11585981\t2024-01-01T10:00:00.000000Z\n-934082\t-868335\t0.8685154\t2024-01-01T10:30:00.000000Z\n-879266\t465748\t0.7692382\t2024-01-01T11:00:00.000000Z\n19004\t-820402\t0.4202044\t2024-01-01T11:30:00.000000Z\n849301\t-665657\t0.28200203\t2024-01-01T12:00:00.000000Z\n-476491\t-533907\t0.44804686\t2024-01-01T12:30:00.000000Z\n-158298\t-256637\t0.1975137\t2024-01-01T13:00:00.000000Z\n-238138\tnull\t0.34568977\t2024-01-01T13:30:00.000000Z\n-65075\t124773\t0.6192919\t2024-01-01T14:00:00.000000Z\n-799807\t-722539\t0.21858656\t2024-01-01T14:30:00.000000Z\n814264\t-660470\t0.7056586\t2024-01-01T15:00:00.000000Z\n-106009\t-537314\t0.13006097\t2024-01-01T15:30:00.000000Z\n-439312\t224362\t0.069444776\t2024-01-01T16:00:00.000000Z\n176625\t-537751\t0.5893398\t2024-01-01T16:30:00.000000Z\n-786639\t883580\t0.9918093\t2024-01-01T17:00:00.000000Z\n864907\t952476\t0.89989215\t2024-01-01T17:30:00.000000Z\n605553\t-819707\t0.337461\t2024-01-01T18:00:00.000000Z\n851680\t606572\t0.7445999\t2024-01-01T19:00:00.000000Z\n509854\t259494\t0.27115327\t2024-01-01T19:30:00.000000Z\n403404\t563913\t0.6797563\t2024-01-01T20:00:00.000000Z\n149434\t-634730\t0.73651147\t2024-01-01T20:30:00.000000Z\n-602769\t-494000\t0.98840106\t2024-01-01T21:00:00.000000Z\n608028\t-832620\t0.38422543\t2024-01-01T22:00:00.000000Z\n-972221\t993071\t0.17180288\t2024-01-01T22:30:00.000000Z\n-673046\t-324050\t0.8584308\t2024-01-01T23:00:00.000000Z\n-344450\t751031\t0.0436064\t2024-01-01T23:30:00.000000Z\n-389080\t232080\t0.45920676\t2024-01-02T00:30:00.000000Z\n-950241\t-919312\t0.050941825\t2024-01-02T01:00:00.000000Z\n-360026\t632564\t0.8977236\t2024-01-02T01:30:00.000000Z\n22693\t-316025\t0.76947445\t2024-01-02T02:00:00.000000Z\n-796722\t567730\t0.89245474\t2024-01-02T02:30:00.000000Z\n-177608\t805591\t0.5913874\t2024-01-02T03:00:00.000000Z\n-664103\t586563\t0.12642151\t2024-01-02T03:30:00.000000Z\n508000\t-195629\t0.44402504\t2024-01-02T04:00:00.000000Z\n773743\t-725780\t0.24001455\t2024-01-02T04:30:00.000000Z\n289580\t-902629\t0.7419701\t2024-01-02T05:00:00.000000Z\n-677407\t-955834\t0.2739985\t2024-01-02T05:30:00.000000Z\n-865645\t-983333\t0.93598145\t2024-01-02T06:00:00.000000Z\n-994358\t100159\t0.78732294\t2024-01-02T06:30:00.000000Z\n921293\t549344\t0.039732814\t2024-01-02T07:00:00.000000Z\n177398\t-863262\t0.83210003\t2024-01-02T07:30:00.000000Z\n-965634\t819074\t0.62260014\t2024-01-02T08:00:00.000000Z\n399895\t924465\t0.8786111\t2024-01-02T08:30:00.000000Z\n156496\t17575\t0.6334964\t2024-01-02T09:00:00.000000Z\n-413901\t-182824\t0.77072495\t2024-01-02T09:30:00.000000Z\n-557437\t-510256\t0.17405552\t2024-01-02T10:00:00.000000Z\n-133637\t436818\t0.007985413\t2024-01-02T10:30:00.000000Z\n-633667\t-421873\t0.75304943\t2024-01-02T11:00:00.000000Z\n52245\t143965\t0.0024457574\t2024-01-02T11:30:00.000000Z\n423168\t-832011\t0.31212717\t2024-01-02T12:00:00.000000Z\n-980843\t714462\t0.35210842\t2024-01-02T12:30:00.000000Z\n648571\t-240479\t0.18746626\t2024-01-02T13:00:00.000000Z\n251668\t-931379\t0.4740684\t2024-01-02T14:00:00.000000Z\n680935\t211681\t0.5308756\t2024-01-02T14:30:00.000000Z\n-636113\t392285\t0.039509535\t2024-01-02T15:00:00.000000Z\n933033\t458887\t0.7407843\t2024-01-02T15:30:00.000000Z\n-84733\t230348\t0.4349324\t2024-01-02T16:00:00.000000Z\n-816531\t309874\t0.23405439\t2024-01-02T16:30:00.000000Z\n-207761\t47333\t0.90589\t2024-01-02T17:00:00.000000Z\n-959997\t-326872\t0.1104731\t2024-01-02T17:30:00.000000Z\n-792196\t867410\t0.82176524\t2024-01-02T18:00:00.000000Z\n-234108\t-814588\t0.29419792\t2024-01-02T18:30:00.000000Z\n-357294\t-118587\t0.8574212\t2024-01-02T19:00:00.000000Z\n350666\t302933\t0.78424555\t2024-01-02T19:30:00.000000Z\n26643\t725464\t0.7253202\t2024-01-02T21:00:00.000000Z\n-208953\t-607284\t0.9997797\t2024-01-02T21:30:00.000000Z\n-666903\tnull\t0.13312209\t2024-01-02T22:00:00.000000Z\n-436626\t569511\t0.84612113\t2024-01-02T23:00:00.000000Z\n-933184\t-825029\t0.84058154\t2024-01-02T23:30:00.000000Z\n-840879\t207526\t0.3185253\t2024-01-03T00:00:00.000000Z\n-254049\t321599\t0.75662524\t2024-01-03T00:30:00.000000Z\n570349\t511485\t0.9116843\t2024-01-03T01:00:00.000000Z\n313138\t717584\t0.0032519698\t2024-01-03T01:30:00.000000Z\n193462\t-760966\t0.7295366\t2024-01-03T02:00:00.000000Z\n-156252\t-391489\t0.8895916\t2024-01-03T02:30:00.000000Z\n532182\tnull\t0.5251698\t2024-01-03T03:30:00.000000Z\n217303\t-4276\t0.729866\t2024-01-03T04:00:00.000000Z\n-376197\t-394005\t0.32824337\t2024-01-03T04:30:00.000000Z\n405844\t-244727\t0.70870113\t2024-01-03T05:00:00.000000Z\n464032\t157466\t0.514972\t2024-01-03T05:30:00.000000Z\n580700\t671698\t0.94126636\t2024-01-03T06:00:00.000000Z\n641363\t24423\t0.60070705\t2024-01-03T06:30:00.000000Z\n-779414\t-83670\t0.7435991\t2024-01-03T07:00:00.000000Z\n613616\t492985\t0.011099219\t2024-01-03T07:30:00.000000Z\n-323557\t106833\t0.119470954\t2024-01-03T08:00:00.000000Z\n873673\t460727\t0.52387\t2024-01-03T08:30:00.000000Z\n573948\t62494\t0.85041\t2024-01-03T09:00:00.000000Z\n-971520\tnull\t0.5788151\t2024-01-03T09:30:00.000000Z\n251501\t520699\t0.7468602\t2024-01-03T10:00:00.000000Z\n-997581\t109394\t0.34892786\t2024-01-03T10:30:00.000000Z\n-649681\t-112158\t0.93162835\t2024-01-03T11:00:00.000000Z\n617480\t-75765\t0.8898226\t2024-01-03T11:30:00.000000Z\n-926006\t77888\t0.4595378\t2024-01-03T12:00:00.000000Z\n-153783\t488143\t0.73708236\t2024-01-03T12:30:00.000000Z\n");
            // 258_558 * -259_815 is INT arithmetic and wraps to +1_542_229_966, so c0 - it is
            // large negative and no FLOAT c9 is <= it. The commuted spelling adds it instead and
            // every row passes, which is what keeps this pair from being vacuous.
            assertJitMatchesJavaOnEmptyResult("SELECT * FROM t WHERE c9 <= (c0 - (258_558 * -259_815))", true);
            assertJitMatchesJava("SELECT * FROM t WHERE c9 <= ((258_558 * -259_815) + c0)", true);
            // Control: direct float-vs-overflow comparison still wraps via
            // getDouble(getInt()), so the wrapped I4 fold remains correct.
            assertJitMatchesJava("SELECT * FROM t WHERE c9 <= (258_558 * -259_815)", true);
            // Control: pure-INT arithmetic under a float wraps (no genuine LONG).
            assertJitMatchesJava("SELECT * FROM t WHERE c9 <= (c8 + (258_558 * -259_815))", true);
        });
    }

    @Test
    public void testConstantFoldWidthUnderBooleanEquality() throws Exception {
        // A boolean equality of two comparisons - (cmp) = (cmp) - forms a
        // SINGLE predicate context, so a predicate-global width signal applies
        // one width to both comparisons. An overflowing INT fold then took the
        // wrong width:
        //   - I4-where-I8: (1_000_000 * 1_000_000 > i64) = (f32 > 0) - the fold
        //     feeds a LONG comparison and must read at long width, but the float
        //     suppressed global widening and the fold root went unmarked, so the
        //     JIT emitted a wrapped I4 (Java 2 rows, JIT 0).
        //   - I8-where-I4: (ci > 1_000_000 * 1_000_000) = (cl > 0) - the fold feeds
        //     an INT comparison and must wrap, but the predicate-global
        //     long-widening (driven by the sibling LONG comparison) forced it to
        //     I8 (Java 3 rows, JIT 0).
        // The fold width is now derived from the fold's own comparison context.
        assertMemoryLeak(() -> {
            execute("create table x as (select timestamp_sequence(0, 1_000_000) k, x id," +
                    " (case when x = 1 then 0L when x = 2 then 5L else 2_000_000_000_000L end) i64," +
                    " x::int ci, x::long cl, 1.0f f32" +
                    " from long_sequence(3)) timestamp(k)");
            // Previously diverging shapes, both directions.
            // Absolute pin: Java keeps ids 1 and 2; the pre-fix JIT wrapped the fold and
            // returned 0 rows.
            assertJitMatchesJava("select id from x where (1_000_000 * 1_000_000 > i64) = (f32 > 0)", true,
                    "id\n");
            assertJitMatchesJava("select id from x where (ci > 1_000_000 * 1_000_000) = (cl > 0)", true);
            // Controls: single comparison, AND, OR each split into separate
            // predicate contexts and were always correct.
            assertJitMatchesJava("select id from x where ci > 1_000_000 * 1_000_000", true);
            assertJitMatchesJava("select id from x where ci > 1_000_000 * 1_000_000 and cl > 0", true);
            assertJitMatchesJava("select id from x where ci > 1_000_000 * 1_000_000 or cl > 0", true);
            // Control: the LONG column reads the fold at long width on both paths.
            assertJitMatchesJava("select id from x where i64 > 1_000_000 * 1_000_000", true);
        });
    }

    @Test
    public void testConstantOperandWidthUnderBooleanEquality() throws Exception {
        // The constant-operand analog of testColumnArithmeticWidthUnderBooleanEquality.
        // A boolean equality of two comparisons - (cmp) = (cmp) - forms a SINGLE
        // predicate context, so a sibling LONG comparison (here i32*3_000_000_000, whose
        // out-of-INT-range constant promotes to long) turns the predicate-global
        // narrow-i64 widening on for the whole predicate. The narrow-int COLUMN leaf
        // on the wrap-side comparison is already kept at i32 (i64WrapLeaves), but the
        // in-range CONSTANT operand it multiplies with (the 2 in i32*2) was still
        // widened to i64 by serializeConstant, so the JIT promoted the whole product
        // to long width and never wrapped - MulInt#getInt wraps mod 2^32 in the Java
        // filter. For (i32*3_000_000_000 > 0) = (i32*2 > 5) the Java filter returned
        // 2 rows and the JIT 4. serializeConstant now keeps an i64WrapLeaves constant
        // at i32, matching the wrap-side column, so both agree. JIT stays enabled.
        assertMemoryLeak(() -> {
            execute("create table t (i32 int, i64 long, ts timestamp) timestamp(ts) partition by day");
            execute("insert into t values " +
                    "(2_147_483_647, 1, 0)," +
                    "(2_147_483_646, 2, 1_000_000)," +
                    "(3, 3, 2_000_000)," +
                    "(null, null, 3_000_000)");
            // Previously diverging shapes: an in-range constant operand on the wrap
            // side of an INT-width comparison, paired with a LONG-width comparison.
            // Absolute pin: Java keeps 2 rows (i32=3 and the null row); the pre-fix JIT widened
            // the in-range "2" operand and returned 4.
            assertJitMatchesJava("t where (i32 * 3_000_000_000 > 0) = (i32 * 2 > 5)", true,
                    "i32\ti64\tts\n" +
                            "3\t3\t1970-01-01T00:00:02.000000Z\n" +
                            "null\tnull\t1970-01-01T00:00:03.000000Z\n");
            assertJitMatchesJava("t where (i32 * 2 > 5) = (i32 * 3_000_000_000 > 0)", true); // operand order
            assertJitMatchesJava("t where (i32 * 3_000_000_000 > 0) <> (i32 * 2 > 5)", true);
            // The LONG-comparison side is an out-of-INT-range constant equality.
            assertJitMatchesJava("t where (i32 = 2_147_483_648) = (i32 * 2 > 5)", true);
            // Addition instead of multiplication on the wrap side.
            assertJitMatchesJava("t where (i32 * 3_000_000_000 > 0) = (i32 + 2_000_000_000 > 5)", true);
            // Mixed-size path (i64 observed too): serializeUntypedNumber keeps the 2 at i32.
            assertJitMatchesJava("t where (i32 * 3_000_000_000 > 0) = (i32 * 2 > 5) and i64 > 0", true);
            // Controls: single comparison, AND, OR each form separate predicate
            // contexts and already wrapped the constant on both paths.
            assertJitMatchesJava("t where i32 * 2 > 5", true);
            assertJitMatchesJava("t where i32 * 2 > 5 and i32 * 3_000_000_000 > 0", true);
            assertJitMatchesJava("t where i32 * 2 > 5 or i32 * 3_000_000_000 > 0", true);
        });
    }

    @Test
    public void testConstantOverflowFoldOnByteColumn() throws Exception {
        // Overflowing INT constant arithmetic must agree across JIT off/scalar/
        // vectorized. A BYTE column compares at INT width, so the JIT folds the
        // constant to a wrapped I4 IMM, matching the Java filter's getInt() wrap.
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " cast(x - 100 as byte) i8" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertMemoryLeak(() -> {
            execute(ddl);
            // -1.04e17 wraps to +1_699_321_072 at INT width; no BYTE exceeds it.
            assertQueryNullableNoLeakCheck("x where i8 > -286_452 * (-952_151 * -382_988)");
            // Symmetric: +1.04e17 wraps to -1_699_321_072, below every BYTE.
            assertQueryNotNullNoLeakCheck("x where i8 > 286_452 * (952_151 * 382_988)");
        });
    }

    @Test
    public void testConstantOverflowFoldOnIntColumn() throws Exception {
        // INT column compares at INT width: the JIT wraps the constant (I4) like
        // the Java filter rather than widening to i64.
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " cast(x - 100 as int) i32" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertMemoryLeak(() -> {
            execute(ddl);
            // Wraps to +1_699_321_072: no INT row exceeds it.
            assertQueryNullableNoLeakCheck("x where i32 > -286_452 * (-952_151 * -382_988)");
            // Wraps to -1_699_321_072: below every INT row.
            assertQueryNotNullNoLeakCheck("x where i32 > 286_452 * (952_151 * 382_988)");
        });
    }

    @Test
    public void testConstantOverflowFoldOnLongColumn() throws Exception {
        // A LONG column does NOT change the constant's width: the fold is all-INT arithmetic, so
        // it wraps at 32 bits and the comparison promotes the already-wrapped value. This test and
        // its SHORT sibling below therefore assert the same two answers now, which is the point -
        // the peer column no longer decides what the constant evaluates to.
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " (x - 100) i64" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertMemoryLeak(() -> {
            execute(ddl);
            // Wraps to +1_699_321_072: no i64 row (they lie in [-99, 414]) exceeds it.
            assertQueryNullableNoLeakCheck("x where i64 > -286_452 * (-952_151 * -382_988)");
            // Wraps to -1_699_321_072: below every i64 row.
            assertQueryNotNullNoLeakCheck("x where i64 > 286_452 * (952_151 * 382_988)");
        });
    }

    @Test
    public void testConstantOverflowFoldOnShortColumn() throws Exception {
        // A SHORT column promotes to INT, so the constant wraps at INT width.
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " cast(x - 100 as short) i16" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertMemoryLeak(() -> {
            execute(ddl);
            // Wraps to +1_699_321_072: no SHORT row exceeds it.
            assertQueryNullableNoLeakCheck("x where i16 > -286_452 * (-952_151 * -382_988)");
            // Wraps to -1_699_321_072: below every SHORT row.
            assertQueryNotNullNoLeakCheck("x where i16 > 286_452 * (952_151 * 382_988)");
        });
    }

    @Test
    public void testConstantOverflowFoldVariousOps() throws Exception {
        // The fold path covers +, -, *, /, and unary minus uniformly.
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " cast(x - 100 as byte) i8," +
                " (x - 100) i64" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertMemoryLeak(() -> {
            execute(ddl);
            // i64 values lie in [-99, 414]; the four constants all evaluate to a
            // value below every i64 row, so > matches every row and exercises
            // the fold path uniformly across all four operators.
            assertQueryNotNullNoLeakCheck("x where i64 > -5_000_000_000 + -5_000_000_000");
            assertQueryNotNullNoLeakCheck("x where i64 > -5_000_000_000 - 5_000_000_000");
            assertQueryNotNullNoLeakCheck("x where i64 > -100_000 * 100_000");
            assertQueryNotNullNoLeakCheck("x where i64 > -1_000_000_000_000 / 1");
            // Unary minus over an overflowing product: the inner fold wraps to -1_699_321_072 and
            // the minus makes it +1_699_321_072, which no i64 row (they lie in [-99, 414]) exceeds.
            assertQueryNullableNoLeakCheck("x where i64 > -(286_452 * (-952_151 * -382_988))");
            // The same constant against a BYTE column gives the same answer, because the column no
            // longer decides the constant's width.
            assertQueryNullableNoLeakCheck("x where i8 > -(286_452 * (-952_151 * -382_988))");
        });
    }

    @Test
    public void testCount() throws Exception {
        final String query = "select count() from x where price > 0 and sym = 'HBC'";
        final String ddl = "create table x as " +
                "(select rnd_symbol('ABB','HBC','DXR') sym, \n" +
                " rnd_double() price, \n" +
                " timestamp_sequence(172_800_000_000, 360_000_000) ts \n" +
                "from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp (ts)";
        assertQueryNotNullNoCount(query, ddl);
    }

    @Test
    public void testDate() throws Exception {
        final String query = "x where d1 != d2";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " rnd_date(to_date('2020', 'yyyy'), to_date('2021', 'yyyy'), 0) d1," +
                " rnd_date(to_date('2020', 'yyyy'), to_date('2021', 'yyyy'), 0) d2" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertQueryNotNull(query, ddl);
    }

    @Test
    public void testDateNull() throws Exception {
        final String query = "x where d <> null";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " rnd_date(to_date('2020', 'yyyy'), to_date('2021', 'yyyy'), 5) d" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertQueryNullable(query, ddl);
    }

    @Test
    public void testDeepArithmeticChainMatchesJava() throws Exception {
        // M2: the JIT marker passes (markI64WidenFoldRoots / markI64WrapArithLeaves) prune a narrow-int
        // subtree instead of recomputing genuineArithType at every level. Exercise a deep arithmetic
        // spine so the pruned/short-circuited walk is taken, and confirm the JIT still matches the Java
        // filter under INT- and LONG-width comparisons and with an overflowing constant fold at the leaf.
        //
        // a and b are sized so the spine genuinely overflows int32: (a + b) * b = 101_000_000 still
        // fits, the next multiply by b does not. So the wrapped and the widened chain differ on every
        // row, and each assertion below discriminates the two. The chain wraps to a distinct negative
        // int32 per row (-449_931_736 .. -445_927_736) and widens to 101_000_001_001_000 + 1_001_000*rid.
        // Every assertion carries an absolute pin: JIT-vs-Java parity alone is not an oracle here,
        // because this PR moves the Java filter and the JIT toward the same width model - a shared
        // wrong decision would agree on both paths and pass.
        assertMemoryLeak(() -> {
            execute("create table dc as (select" +
                    " x rid," +
                    " cast(100_000 as int) a," +
                    " cast(1000 as int) b," +
                    " x::short cs," +
                    " x::byte cbyte," +
                    // nl carries the true long-width chain for rows 1 and 4, and the int32-WRAPPED
                    // image of the chain for rows 2 and 5. A JIT (or Java) path that wrongly wraps
                    // under a LONG comparison matches {2, 5} instead of {1, 4}.
                    " cast(case x when 1 then 101_000_001_001_000 when 2 then -448_930_736 when 3 then 0" +
                    " when 4 then 101_000_004_004_000 else -445_927_736 end as long) nl," +
                    // ni carries the int32-wrapped image of the chain for rows 1 and 3. A path that
                    // wrongly widens under an INT comparison matches nothing.
                    " cast(case x when 1 then -449_931_736 when 2 then 0 when 3 then -447_929_736" +
                    " when 4 then 1 else 0 end as int) ni," +
                    " timestamp_sequence(0, 1_000_000) k" +
                    " from long_sequence(5)) timestamp(k)");
            // Deep all-narrow-int spine read at INT width: every node is narrow, so the marker walk
            // prunes at the root; the JIT must still wrap (getInt) exactly like the Java filter. The
            // wrapped chain is negative on every row, so < 0 keeps all five and > 0 keeps none; a path
            // that widened instead would invert both.
            assertJitMatchesJava("select rid from dc where ((((((a + b) * b) + cs) * b) + cbyte) * b) < 0", true,
                    "rid\n1\n2\n3\n4\n5\n");
            assertJitMatchesJava("select rid from dc where ((((((a + b) * b) + cs) * b) + cbyte) * b) > 0", true,
                    "rid\n");
            // Same spine against an INT column holding the wrapped image: pins the exact wrapped value.
            assertJitMatchesJava("select rid from dc where ni = ((((((a + b) * b) + cs) * b) + cbyte) * b)", true,
                    "rid\n1\n3\n");
            // Same spine compared against a LONG column: the comparison promotes to long width, so the
            // spine widens (getLong) on both paths - the narrow prune does not fire (under long). Pins
            // the exact widened value: rows 2 and 5 hold the wrapped image and must NOT match.
            assertJitMatchesJava("select rid from dc where nl = ((((((a + b) * b) + cs) * b) + cbyte) * b)", true,
                    "rid\n2\n5\n");
            assertJitMatchesJava("select rid from dc where ((((((a + b) * b) + cs) * b) + cbyte) * b) > nl", true,
                    "rid\n");
            assertJitMatchesJava("select rid from dc where ((((((a + b) * b) + cs) * b) + cbyte) * b) >= 101_000_000_000_000L", true,
                    "rid\n");
            // Deep spine with an overflowing pure-constant fold at the leaf: narrow (wraps) vs long
            // (widens) contexts must each still agree. 1_000_000 * 1_000_000 folds to an INT-typed node
            // whose getInt() wraps to -727_379_968 and whose getLong() widens to 10^12, so + 1 + 2 + 3
            // yields -727_379_962 against the INT column a and 1_000_000_000_006 against the LONG
            // column nl. Both pins name the value directly, so a fold at the wrong width fails here
            // rather than agreeing on both paths.
            assertJitMatchesJava("select rid from dc where (a + ((((1_000_000 * 1_000_000) + 1) + 2) + 3)) = -727_279_962", true,
                    "rid\n1\n2\n3\n4\n5\n");
            assertJitMatchesJava("select rid from dc where (a + ((((1_000_000 * 1_000_000) + 1) + 2) + 3)) > 0", true,
                    "rid\n");
            // nl is 0 on row 3, so the widened fold is the only value that can match there.
            assertJitMatchesJava("select rid from dc where (nl + ((((1_000_000 * 1_000_000) + 1) + 2) + 3)) = 1_000_000_000_006L", true,
                    "rid\n");
            assertJitMatchesJava("select rid from dc where (nl + ((((1_000_000 * 1_000_000) + 1) + 2) + 3)) > 0", true,
                    "rid\n1\n4\n");
        });
    }

    @Test
    public void testFloatArithmeticOperandWiden() throws Exception {
        // An out-of-INT-range integer constant used as an ARITHMETIC OPERAND against a
        // narrow-int column, in a predicate that also has a FLOAT column, diverged.
        // INT and FLOAT are both 4 bytes, so hasMixedSizes() is false and the type
        // observer types the constant F4; a co-present float also suppresses the global
        // narrow-i64 widening. serializeConstant then emitted the constant as a lossy
        // 32-bit float and the predicate vectorized, but AVX2 convert() has no i32->i64
        // path, so i32 * 3_000_000_000 was computed as a float multiply. The Java filter
        // computes it at long width (MulInt#getLong) and only then converts to floating
        // point, so for i32=7 it kept the row (21_000_000_000 > 20999999488.0) while the JIT
        // dropped it (20999999488.0f > 20999999488.0f is false).
        // The serializer now flags the out-of-range integer operand: it emits a full I8
        // IMM and runs scalar, where the scalar convert() widens the narrow column to i64
        // and int32 * int64 stays exact. JIT stays enabled (scalar mode).
        assertMemoryLeak(() -> {
            execute("create table t (i32 int, i64 long, fcol float, dcol double, ts timestamp) timestamp(ts) partition by day");
            execute("insert into t values " +
                    "(7, 7, 20999999488.0, 20999999488.0, 0)," +           // 7*3e9=21_000_000_000 > fcol; float multiply drops it
                    "(-7, -7, 20999999488.0, 20999999488.0, 1_000_000)," +   // negative operand exercised via the fold path
                    "(1, 1, 2000000000000.0, 2000000000000.0, 2_000_000)," + // addition: 1+2e12 > fcol; float add loses the 1
                    "(46_341, 46_341, 5.0, 5.0, 3_000_000)," +
                    "(0, 0, -1.0, -1.0, 4_000_000)," +
                    "(null, null, 1.0, 1.0, 5_000_000)");
            // Previously diverging shapes fixed by this change: an out-of-INT-range integer
            // constant as a bare (non-negated) arithmetic operand, alongside a FLOAT column.
            // Still JIT-compiled, now correct.
            // Absolute pin: the i32=7 boundary row (21_000_000_000 > 20999999488.0 at long width)
            // survives; the pre-fix JIT computed it as a float multiply and dropped it.
            assertJitMatchesJava("t where i32 * 3_000_000_000 > fcol", true,
                    "i32\ti64\tfcol\tdcol\tts\n" +
                            "7\t7\t2.1E10\t2.0999999488E10\t1970-01-01T00:00:00.000000Z\n" +
                            "46341\t46341\t5.0\t5.0\t1970-01-01T00:00:03.000000Z\n" +
                            "0\t0\t-1.0\t-1.0\t1970-01-01T00:00:04.000000Z\n");
            assertJitMatchesJava("t where i32 + 2_000_000_000_000 > fcol", true);
            // An AND chain with a LONG column splits into per-comparison predicate contexts,
            // so the "i32 * 3_000_000_000 > fcol" comparison is again pure INT+FLOAT and diverged.
            assertJitMatchesJava("t where i32 * 3_000_000_000 > fcol and i64 > 0", true);
            // Control: a negated out-of-range operand (-3_000_000_000) is a unary-minus subtree
            // that descend() folds via its own long-width path, so it was already correct.
            assertJitMatchesJava("t where i32 * -3_000_000_000 > fcol", true);
            // Control: a DOUBLE column makes the predicate mixed-size (INT 4B, DOUBLE 8B),
            // which already runs scalar and emits the constant as I8 - correct before too.
            assertJitMatchesJava("t where i32 * 3_000_000_000 > dcol", true);
            // Control: a direct FLOAT-vs-out-of-range-constant comparison is not an
            // arithmetic operand; it stays vectorized and unchanged.
            assertJitMatchesJava("t where fcol > 3_000_000_000", true);
            // Control: an in-range constant operand wraps at INT width under a float on
            // both paths (getInt), so it must not widen.
            assertJitMatchesJava("t where i32 * 3 > fcol", true);
        });
    }

    @Test
    public void testFloatDirectCompareOutOfRangeConstWiden() throws Exception {
        // A FLOAT column compared directly against an out-of-INT-range integer constant
        // diverged. INT and FLOAT are both 4 bytes, so hasMixedSizes() is false and the type
        // observer types the constant F4; serializeNumber then emitted it as a lossy 32-bit
        // float (3_000_000_200 rounds to 3_000_000_256f). The JIT compared at float width, while the
        // Java filter promotes both operands to double and compares exactly. For a stored
        // fcol=3000000256.0 the JIT dropped the row on "> 3_000_000_200" (3_000_000_256f > 3_000_000_256f
        // is false) that Java kept (3000000256.0 > 3000000200.0 is true), and it spuriously
        // matched the row on "= 3_000_000_200" that Java rejected.
        // The serializer now widens the constant to a full I8 IMM; four-lane AVX2 promotes the
        // float column to double. JIT stays enabled and vectorized.
        assertMemoryLeak(() -> {
            execute("create table t (fcol float, dcol double, ts timestamp) timestamp(ts) partition by day");
            execute("insert into t values " +
                    "(3000000256.0, 3000000256.0, 0)," +     // boundary: 3_000_000_200 rounds to this float
                    "(3000000000.0, 3000000000.0, 1_000_000)," +
                    "(5.0, 5.0, 2_000_000)," +
                    "(null, null, 3_000_000)");
            // Operators that diverged on the boundary row before the fix.
            // Absolute pin: at float width 3_000_000_256f > 3_000_000_256f is false, but the stored
            // fcol promotes to double (3000000256.0 > 3000000200.0 is true), so Java keeps the
            // boundary row; the pre-fix JIT dropped it.
            assertJitMatchesJava("t where fcol > 3_000_000_200", true,
                    "fcol\tdcol\tts\n" +
                            "3.0000003E9\t3.000000256E9\t1970-01-01T00:00:00.000000Z\n");
            assertJitMatchesJava("t where fcol >= 3_000_000_200", true);
            assertJitMatchesJavaOnEmptyResult("t where fcol = 3_000_000_200", true);
            assertJitMatchesJava("t where fcol <> 3_000_000_200", true);
            assertJitMatchesJava("t where fcol < 3_000_000_200", true);
            assertJitMatchesJava("t where fcol <= 3_000_000_200", true);
            // Single-value IN reduces to equality on the FLOAT key.
            assertJitMatchesJavaOnEmptyResult("t where fcol in (3_000_000_200)", true);
            // Control: a DOUBLE column stores 8 bytes and compares exactly on both paths, so it
            // stays vectorized and unchanged.
            assertJitMatchesJava("t where dcol > 3_000_000_200", true);
            // Control: an in-range constant compares at int width on both paths (the observer
            // types it I4 and int-parse succeeds), so it must not widen.
            assertJitMatchesJava("t where fcol > 5", true);
        });
    }

    @Test
    public void testFloatingDivisionByZeroMatchesJava() throws Exception {
        // DivDoubleFunctionFactory and DivFloatFunctionFactory fold a non-finite quotient to NaN
        // ("Numbers.isFinite(d) ? d : Double.NaN"), so the Java filter reads a division by zero as
        // NULL and orders it against nothing. The native div (divss/divsd in jit/impl/x86.h,
        // vdivps/vdivpd in jit/impl/avx2.h, fdiv in jit/impl/aarch64.h) propagated a real
        // +/-Infinity instead, which the ordering opcodes rank like an ordinary extreme - so the
        // JIT kept rows the Java filter dropped. JIT is on by default (cairo.sql.jit.mode = "on"),
        // making this a silent wrong result: measured 2 rows on the JIT against 1 on the Java
        // filter for "d / e > 0.0".
        //
        // Equality never diverged - double_cmp_epsilon tests the exponent bits of both operands
        // and so calls any two non-finite values equal, exactly as Numbers.equals does. Only the
        // four ordering operators did.
        //
        // The table needs 20 rows, not 4, to reach the vectorized backend at BOTH widths. The AVX2
        // loop step is 256 / (elementBytes * 8) and compiler.cpp skips the SIMD body entirely when
        // rowCount < step: a 4-row table gives step 8 for a single-size FLOAT predicate, so
        // "f / g" would run only the scalar tail and vdivps would never execute. Zero divisors sit
        // at ids 1 and 2 (inside the first f32 vector) and at id 18 (in the scalar tail), so both
        // loops carry a diverging row at both widths.
        assertMemoryLeak(() -> {
            execute("create table dz (id int, d double, e double, f float, g float, i int," +
                    " l long, k timestamp) timestamp(k) partition by day");
            execute("""
                    insert into dz
                    select x::int,
                           case when x = 1 then 2.0 when x = 2 then -2.0 when x = 3 then 6.0
                                when x = 18 then 4.0 end,
                           case when x in (1, 2, 18) then 0.0 when x = 3 then 3.0 else 1.0 end,
                           (case when x = 1 then 2.0 when x = 2 then -2.0 when x = 3 then 6.0
                                 when x = 18 then 4.0 end)::float,
                           (case when x in (1, 2, 18) then 0.0 when x = 3 then 3.0 else 1.0 end)::float,
                           case when x in (1, 2, 18) then 0 when x = 3 then 3 else 1 end,
                           case when x in (1, 2, 18) then 0 when x = 3 then 3 else 1 end,
                           timestamp_sequence(0, 1_000_000)
                    from long_sequence(20)
                    """);
            // Only id 3 has a finite non-zero quotient; every other row divides by zero or has a
            // NULL numerator, and both read as NULL on the Java filter.
            final String onlyThree = "id\n3\n";
            final String noRows = "id\n";
            final String allRows = "id\n1\n2\n3\n4\n5\n6\n7\n8\n9\n10\n11\n12\n13\n14\n15\n16\n17\n18\n19\n20\n";

            // DOUBLE / DOUBLE - vdivpd in the vector body, divsd in the tail and under FORCE_SCALAR.
            assertJitScalarAndVectorMatchJava("select id from dz where d / e > 0.0", onlyThree);
            assertJitScalarAndVectorMatchJava("select id from dz where d / e >= 0.0", onlyThree);
            assertJitScalarAndVectorMatchJava("select id from dz where d / e < 0.0", noRows);
            assertJitScalarAndVectorMatchJava("select id from dz where d / e <= 0.0", noRows);
            // A constant zero divisor is the same runtime division - the parser cannot fold it
            // away because the numerator is a column.
            assertJitScalarAndVectorMatchJava("select id from dz where d / 0.0 > 0.0", noRows);
            // FLOAT / FLOAT - a single-size 4-byte predicate, so this is the only shape that
            // reaches vdivps. It needs the 20-row fixture above to execute at all.
            assertJitScalarAndVectorMatchJava("select id from dz where f / g > 0.0", onlyThree);
            assertJitScalarAndVectorMatchJava("select id from dz where f / g >= 0.0", onlyThree);
            assertJitScalarAndVectorMatchJava("select id from dz where f / g < 0.0", noRows);
            assertJitScalarAndVectorMatchJava("select id from dz where f / g <= 0.0", noRows);
            // An INT / LONG divisor converts to floating point before the division, so a zero
            // divisor lands on the same non-finite quotient rather than on the integer NULL.
            // These are mixed-size predicates and so run the scalar loop on both JIT modes.
            assertJitScalarAndVectorMatchJava("select id from dz where d / i > 0.0", onlyThree);
            assertJitScalarAndVectorMatchJava("select id from dz where d / l > 0.0", onlyThree);

            // Controls: equality already agreed and must keep agreeing. Every NULL quotient is
            // unequal to 0.0 on both paths, so "!=" keeps the whole table.
            assertJitScalarAndVectorMatchJava("select id from dz where d / e = 0.0", noRows);
            assertJitScalarAndVectorMatchJava("select id from dz where d / e != 0.0", allRows);
            assertJitScalarAndVectorMatchJava("select id from dz where f / g != 0.0", allRows);
            // Control: a non-zero divisor divides normally and must not be folded to NULL.
            assertJitScalarAndVectorMatchJava("select id from dz where d / 2.0 > 0.0", "id\n1\n3\n18\n");
            assertJitScalarAndVectorMatchJava("select id from dz where f / 2.0 > 0.0", "id\n1\n3\n18\n");
            // Control: multiplication does NOT fold a non-finite result on either path
            // (MulDoubleFunctionFactory returns the raw product), so overflow must stay +Infinity
            // on both and keep ordering like an extreme.
            assertJitScalarAndVectorMatchJava("select id from dz where d * 1e308 * 1e308 > 0.0",
                    "id\n1\n3\n18\n");
        });
    }

    @Test
    public void testGeoHashConstant() throws Exception {
        final String query = "x " +
                "where geo8 != ##1001 and geo16 != ##100110011001 and geo32 != ##1001100110011001 and geo64 != ##10011001100110011001100110011001";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
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
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
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
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
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
                " timestamp_sequence(172_800_000_000, 360_000_000) ts \n" +
                "from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp (ts)";
        assertQueryNotNullNoCount(query, ddl);
    }

    @Test
    public void testHugeFilter() throws Exception {
        final int N = 682; // depends on memory configuration for a jit IR
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
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
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
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
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
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
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
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
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
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
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
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
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
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
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " rnd_int(1, 10, 0) a," +
                " rnd_int(1, 10, 0) b" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertQueryNotNull(query, ddl);
    }

    @Test
    public void testInOperatorNullElementMatchesEqNullOnNarrowArithKey() throws Exception {
        // An untyped NULL IN element reads a narrow-int key at the width '=' reads it: INT. Both
        // paths used to widen the key (getLong / sx_i64) against a NULL element, which disagrees
        // with '=', IS NULL and the projection for an INT-arithmetic key - the one key whose two
        // widths can disagree about the sentinel. The JIT now keeps the key at i32 for the NULL
        // pairing, mirroring the Java InLong path, and the filter stays vectorized (no sx_i64).
        //
        // Row 1: a*b = 65_536*32_768 wraps onto INT_NULL, so the row IS null; at long width the
        //        product is +2^31, which is not LONG_NULL - the old code missed the row.
        // Row 2: a*b = -2^30 * 2 wraps onto INT_NULL the same way (long width: -2^31).
        // Row 3: a*b*c = 2^30 * 8 * 2^30 has value 0, but its long-width product overflows exactly
        //        onto LONG_NULL - the old code matched a row that is not null.
        // Row 4: a genuinely NULL key. Rows 5+ compute 3*3 = 9.
        // The table has >= 64 rows so the vectorized (AVX2) loop is genuinely exercised.
        assertMemoryLeak(() -> {
            execute("create table y as (select" +
                    " cast(case when x = 1 then 65_536 when x = 2 then -1_073_741_824 when x = 3 then 1_073_741_824 when x = 4 then null else 3 end as int) a," +
                    " cast(case when x = 1 then 32_768 when x = 2 then 2 when x = 3 then 8 else 3 end as int) b," +
                    " cast(case when x = 3 then 1_073_741_824 else 1 end as int) c," +
                    " cast(x as int) rn," +
                    " timestamp_sequence(0, 1_000_000) k" +
                    " from long_sequence(64)) timestamp(k)");

            // RED on HEAD: IN (null) selected no row at all (it probed the widened key against
            // LONG_NULL), while '= null' and IS NULL select rows 1, 2 and 4.
            final String nullKeyRows = "rn\n1\n2\n4\n";
            assertJitMatchesJava("select rn from y where (a*b) = null", true, nullKeyRows);
            assertJitMatchesJava("select rn from y where (a*b) is null", true, nullKeyRows);
            assertJitMatchesJava("select rn from y where (a*b) in (null)", true, nullKeyRows);
            assertJitMatchesJava("select rn from y where (a*b) in (null, 999)", true, nullKeyRows);
            assertJitMatchesJava("select rn from y where (a*b) in (null, 999, 7)", true, nullKeyRows);

            // RED on HEAD: row 3's value is 0, but its long-width product is exactly LONG_NULL,
            // so IN (null) matched it while '= null' did not.
            assertJitMatchesJava("select rn from y where (a*b*c) = null", true, nullKeyRows);
            assertJitMatchesJava("select rn from y where (a*b*c) in (null)", true, nullKeyRows);

            // The NOT IN inversion: every row that is not null, and row 3 among them.
            Assert.assertEquals(61, runQuery("select rn from y where (a*b) not in (null)"));
            assertJitMatchesJava("select rn from y where (a*b*c) not in (null) and rn <= 4", true, "rn\n3\n");

            // A NULL element next to a genuine LONG element: each pairing keeps its own width, so
            // the LONG element still widens the key (and matches row 1's +2^31 product) while the
            // NULL element wraps it.
            assertJitMatchesJava("select rn from y where (a*b) in (null, 2_147_483_648L)", true, "rn\n1\n2\n4\n");

            // The widened key was also compared against a MIS-SIZED NULL immediate. serializeNull
            // emits the NULL at the observer's width (I4 here), and the backend maps INT_NULL onto
            // LONG_NULL only when the immediate reaches the compare in a register: preload_constants
            // (jit/common.h) hoists the first MAX_CONSTANTS = 8 integer constants, and past that cap
            // imm2reg materializes a bare Imm with a movabs at the KEY's width - a raw -2^31. So the
            // i64 key was tested against -2^31 instead of LONG_NULL: it matched row 2, whose
            // long-width product is exactly -2^31 but which Java does not match, and MISSED row 4,
            // the genuinely-null key. RED on HEAD (JIT returned row 2 alone; Java returned row 4).
            //
            // The divergence needs the NULL to land past the 8th constant in IR order, and IN
            // elements serialize in reverse - so the NULL goes FIRST in the SQL list, behind exactly
            // the 8 constants that fill the cache. One element more and serializeIn declines the
            // filter outright (sqlJitMaxInListSizeThreshold counts the key, so 9 elements is the
            // ceiling), falling back to Java and hiding the bug: this list is the whole window.
            // Keeping the key at i32 removes the mixed-width compare entirely.
            assertJitMatchesJava("select rn from y where (a*b) in (null,11,12,13,14,15,16,17,18)", true, nullKeyRows);
            assertJitMatchesJava("select rn from y where (a*b) not in (null,11,12,13,14,15,16,17,18) and rn <= 4", true, "rn\n3\n");
            // Control: the same list with the NULL in a cached slot (last in the SQL list) always
            // agreed, which is why a short list never exposed this.
            assertJitMatchesJava("select rn from y where (a*b) in (11,12,13,14,15,16,17,18,null)", true, nullKeyRows);

            // Control: a plain INT column key is not width-split, so both widths already agreed.
            assertJitMatchesJava("select rn from y where a in (null)", true, "rn\n4\n");
        });
    }

    @Test
    public void testInOperatorOverflowFoldMatchesJavaOnConstantKey() throws Exception {
        // An overflowing INT arithmetic CONSTANT key on the left of a multi-value IN() list.
        // `k IN (e0, e1, ...)` is `k = e0 OR k = e1 OR ...`, and the key is one value: the fold
        // is an IntConstant holding the wrap (-727_379_968), so every pairing compares that,
        // sign-extended where the element is 64 bits. A LONG or TIMESTAMP element cannot pull
        // the key back to its pre-wrap value. The table carries both, so a wrong key width
        // matches the wrong row rather than returning empty.
        // Every assertion carries an absolute pin. Parity alone is not an oracle: this change
        // moves the Java IN path and the JIT together, so a shared wrong width would pass.
        assertMemoryLeak(() -> {
            execute("create table x as (select cast(v as long) i64, cast(v as timestamp) tsc " +
                    "from (select 1_000_000_000_000 v union all select -727_379_968 v))");
            final String header = "i64\ttsc\n";
            final String widened = "1000000000000\t1970-01-12T13:46:40.000000Z\n";
            final String wrapped = "-727379968\t1969-12-31T23:47:52.620032Z\n";
            // A LONG element sign-extends the key's wrapped value, so the wrapped row matches.
            assertJitMatchesJava("x where (1_000_000 * 1_000_000) in (i64, 5)", true, header + wrapped);
            assertJitMatchesJava("x where (1_000_000 * 1_000_000) in (5, i64)", true, header + wrapped);        // order independent
            assertJitMatchesJava("x where (1_000_000 * 1_000_000) in (5, 6, i64)", true, header + wrapped);     // multi value
            assertJitMatchesJava("x where (1_000_000 * 1_000_000) in (i64, i64)", true, header + wrapped);      // all long
            assertJitMatchesJava("x where (1_000_000 * 1_000_000) not in (i64, 5)", true, header + widened);    // inverse
            // A TIMESTAMP element reads the key the same way.
            assertJitMatchesJava("x where (1_000_000 * 1_000_000) in (tsc, 5)", true, header + wrapped);
            // Controls: '=' and the single-value IN agree with the multi-value form.
            assertJitMatchesJava("x where (1_000_000 * 1_000_000) = i64", true, header + wrapped);
            assertJitMatchesJava("x where (1_000_000 * 1_000_000) in (i64)", true, header + wrapped);
            // The wrapped value as a literal element matches the key on EVERY row, since the key
            // is a constant equal to it.
            assertJitMatchesJava("x where (1_000_000 * 1_000_000) in (i64, -727_379_968)", true,
                    header + widened + wrapped);
        });
    }

    @Test
    public void testInOperatorMixedWidthListOnPlainNarrowKey() throws Exception {
        // A plain INT column reads the same number through getInt() and getLong() (only INT
        // arithmetic wraps mod 2^32 under one and not the other), so InLongFunctionFactory holds a
        // mixed-width IN list in a single set and probes the key once per row instead of twice.
        // The rows pin what that must select: the NULL element matches the NULL row, the wide
        // element matches nothing an INT column can hold, and the narrow elements still match.
        assertMemoryLeak(() -> {
            execute("create table x as (select cast(v as int) i32 " +
                    "from (select 1 v union all select 2 v union all select -2_147_483_647 v union all select null v))");
            assertJitMatchesJava("x where i32 in (1, 2, null)", true, "i32\n1\n2\nnull\n");
            assertJitMatchesJava("x where i32 in (1, 5_000_000_000)", true, "i32\n1\n");
            assertJitMatchesJava("x where i32 in (1, 2, null, 5_000_000_000)", true, "i32\n1\n2\nnull\n");
            assertJitMatchesJava("x where i32 not in (1, 2, null)", true, "i32\n-2147483647\n");
            // A numeric string element is read at the width its value carries as a literal. The JIT
            // declines a string constant against a numeric column, so this one runs on the Java
            // filter, which is exactly the path the single merged set changed.
            assertJitMatchesJava("x where i32 in ('-2147483647', '5000000000')", false, "i32\n-2147483647\n");
        });
    }

    @Test
    public void testInOperatorOverflowFoldMatchesJavaOnLongColumn() throws Exception {
        // An overflowing INT arithmetic fold inside an IN() list against a LONG column. The fold
        // is an IntConstant holding the wrap (-727_379_968), so it selects the wrapped row and
        // never the mathematical one: a LONG peer does not change a constant subtree's width.
        // The table carries both values, so a fold at the wrong width matches the wrong row
        // rather than returning empty. Every assertion carries an absolute pin, because parity
        // alone is not an oracle here - this change moves the Java filter and the JIT together.
        assertMemoryLeak(() -> {
            execute("create table x as (select cast(v as long) i64 " +
                    "from (select 1_000_000_000_000 v union all select -727_379_968 v))");
            final String widened = "i64\n1000000000000\n";
            final String wrapped = "i64\n-727379968\n";
            assertJitMatchesJava("x where i64 in (1_000_000 * 1_000_000)", true, wrapped);          // single value
            assertJitMatchesJava("x where i64 in (1, 1_000_000 * 1_000_000)", true, wrapped);       // two values
            assertJitMatchesJava("x where i64 in (1, 2, 1_000_000 * 1_000_000)", true, wrapped);    // multi value
            assertJitMatchesJava("x where i64 not in (1, 1_000_000 * 1_000_000)", true, widened);   // inverse
            assertJitMatchesJava("x where i64 = 1_000_000 * 1_000_000", true, wrapped);             // control: plain '='
        });
    }

    @Test
    public void testInOperatorOverflowFoldMatchesJavaOnNarrowArithKey() throws Exception {
        // C1 regression: an overflowing INT *arithmetic* KEY on the left of IN(). The narrow-int
        // IN fix wrapped the IN-list elements at INT width (matching '=') but every InLong*.getBool
        // still read the KEY via getLong(), which WIDENS an overflowing INT arithmetic. So IN
        // disagreed with '=' even with the JIT off, and the JIT (which computes the key at I4 and
        // wraps) disagreed with the Java IN. The key is now read at INT width too, symmetric with
        // the elements. a + a = 3*10^9 wraps to INT -1_294_967_296; '=' (EqInt) and the JIT both wrap,
        // so a correctly-wrapping IN matches the single row.
        assertMemoryLeak(() -> {
            execute("create table x as (select cast(1_500_000_000 as int) a, cast(1_500_000_000 as int) b)");

            // const IN shapes: the JIT wraps the key at I4, the Java IN must wrap too.
            assertJitMatchesJava("x where (a + a) in (1_500_000_000 + 1_500_000_000)", true);        // single const
            assertJitMatchesJava("x where (a + a) in (1, 1_500_000_000 + 1_500_000_000)", true);     // two const
            assertJitMatchesJava("x where (a + a) in (1, 2, 1_500_000_000 + 1_500_000_000)", true);  // multi const
            assertJitMatchesJavaOnEmptyResult("x where (a + a) not in (1_500_000_000 + 1_500_000_000)", true);    // inverse
            // an in-range literal equal to the wrapped key must match too (pre-existing slice).
            assertJitMatchesJava("x where (a + a) in (-1_294_967_296)", true);
            assertJitMatchesJava("x where (a + a) = 1_500_000_000 + 1_500_000_000", true);           // control: '='

            // Java (JIT-disabled) IN must agree with '=' across the const InLong* variants.
            Assert.assertEquals(1, runQuery("x where (a + a) in (1_500_000_000 + 1_500_000_000)"));      // single
            Assert.assertEquals(1, runQuery("x where (a + a) in (1, 1_500_000_000 + 1_500_000_000)"));   // two
            Assert.assertEquals(1, runQuery("x where (a + a) in (1, 2, 1_500_000_000 + 1_500_000_000)"));// multi
            Assert.assertEquals(0, runQuery("x where (a + a) not in (1_500_000_000 + 1_500_000_000)"));  // inverse
            Assert.assertEquals(1, runQuery("x where (a + a) = 1_500_000_000 + 1_500_000_000"));         // '='

            // runtime-const variant: a bind variable forces InLongRuntimeConstFunction. The const
            // element still matches the wrapped key; :p (= 0) does not.
            bindVariableService.setInt("p", 0);
            Assert.assertEquals(1, runQuery("x where (a + a) in (1_500_000_000 + 1_500_000_000, :p)"));

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
            // 1_000_000 * 1_000_000 = 10^12 wraps to INT -727_379_968; 3 * 1_431_655_767 = 2^32 + 5
            // wraps to INT 5 (matchable by SHORT/BYTE keys too).
            execute("create table x as (select cast(-727_379_968 as int) i32b, " +
                    "cast(5 as int) i32, cast(5 as short) i16, cast(5 as byte) i8)");

            // INT key, canonical 10^12 product, JIT-vs-Java parity across IN forms.
            assertJitMatchesJava("x where i32b in (1_000_000 * 1_000_000)", true);         // single value
            assertJitMatchesJava("x where i32b in (1, 1_000_000 * 1_000_000)", true);      // two values
            assertJitMatchesJava("x where i32b in (1, 2, 1_000_000 * 1_000_000)", true);   // multi value
            assertJitMatchesJavaOnEmptyResult("x where i32b not in (1, 1_000_000 * 1_000_000)", true);  // inverse
            assertJitMatchesJava("x where i32b = 1_000_000 * 1_000_000", true);            // control: '='

            // The Java (JIT-disabled) IN must agree with '=' - it widened before the fix.
            Assert.assertEquals(1, runQuery("x where i32b in (1_000_000 * 1_000_000)"));
            Assert.assertEquals(1, runQuery("x where i32b = 1_000_000 * 1_000_000"));

            // INT/SHORT/BYTE keys all wrap the element to 5 and match.
            assertJitMatchesJava("x where i32 in (3 * 1_431_655_767)", true);
            assertJitMatchesJava("x where i16 in (3 * 1_431_655_767)", true);
            assertJitMatchesJava("x where i8 in (3 * 1_431_655_767)", true);
            Assert.assertEquals(1, runQuery("x where i32 in (3 * 1_431_655_767)"));
            Assert.assertEquals(1, runQuery("x where i16 in (3 * 1_431_655_767)"));
            Assert.assertEquals(1, runQuery("x where i8 in (3 * 1_431_655_767)"));
            Assert.assertEquals(1, runQuery("x where i32 = 3 * 1_431_655_767"));
            Assert.assertEquals(1, runQuery("x where i16 = 3 * 1_431_655_767"));
            Assert.assertEquals(1, runQuery("x where i8 = 3 * 1_431_655_767"));
        });
    }

    @Test
    public void testInOperatorOverflowFoldMatchesJavaOnNarrowKeyWithLongElement() throws Exception {
        // C4 regression: a narrow/INT KEY IN() list that mixes an overflowing INT-arithmetic
        // element with a genuine-LONG element. The Java InLong path reads the INT element at
        // the narrow key's width (getInt -> wrap), so the JIT must too. But
        // markI64WidenFoldRoots used to fold ONE comparison width across the whole IN list,
        // so a single coexisting LONG element (3_000_000_000) promoted the list to I8 and WIDENED
        // the overflowing INT element to its full-width product - the JIT then read 10^12
        // while the Java filter wrapped to -727_379_968 == the stored key, so the row matched in
        // Java but not in the JIT. The width is now derived per element (key vs that element),
        // so the INT element wraps (matching the key) while the LONG element stays at long
        // width (and never matches the narrow key either way).
        assertMemoryLeak(() -> {
            execute("create table x as (select cast(-727_379_968 as int) c)"); // -727_379_968 = (int)(1_000_000*1_000_000)

            // overflow-INT element coexists with a genuine-LONG element against the narrow key.
            assertJitMatchesJava("x where c in (1_000_000 * 1_000_000, 3_000_000_000)", true);        // RED on HEAD
            assertJitMatchesJava("x where c in (3_000_000_000, 1_000_000 * 1_000_000)", true);        // element order swapped
            assertJitMatchesJava("x where c in (1, 1_000_000 * 1_000_000, 3_000_000_000)", true);     // plus a plain element
            assertJitMatchesJavaOnEmptyResult("x where c not in (1_000_000 * 1_000_000, 3_000_000_000)", true);    // inverse

            // The Java (JIT-disabled) path is the oracle: c matches the wrapped INT element only.
            Assert.assertEquals(1, runQuery("x where c in (1_000_000 * 1_000_000, 3_000_000_000)"));
            Assert.assertEquals(0, runQuery("x where c not in (1_000_000 * 1_000_000, 3_000_000_000)"));

            // control: an all-INT list against the narrow key already wrapped correctly.
            assertJitMatchesJava("x where c in (1, 1_000_000 * 1_000_000)", true);
        });
    }

    @Test
    public void testInOperatorOverflowLongElementUsesWideLane() throws Exception {
        // A LONG element lifts the whole IN list to 64 bits, and the narrow arithmetic key
        // sign-extends its WRAPPED product to meet it. The pre-wrap value is therefore unreachable
        // through any element. See CompiledFilterIRSerializerTest#testInNullElementKeepsNarrowKeyVectorized
        // for the exact IR and exec-hint pins.
        //
        // The table has >= 64 rows so the vectorized loop is genuinely exercised (a 1-row table runs
        // entirely in the scalar tail and hides the bug). Row 1 overflows: a*b = 1_000_000*1_000_000
        // wraps to INT -727_379_968; all other rows compute 3*3 = 9.
        assertMemoryLeak(() -> {
            execute("create table y as (select" +
                    " cast(case when x = 1 then 1_000_000 else 3 end as int) a," +
                    " cast(case when x = 1 then 1_000_000 else 3 end as int) b," +
                    " timestamp_sequence(0, 1_000_000) k" +
                    " from long_sequence(64)) timestamp(k)");

            // A LONG element paired with an INT constant: the key is one value, so only the
            // -727_379_968 arm can match, and it matches row 1.
            assertJitMatchesJava("select a from y where (a*b) in (1_000_000_000_000, -727_379_968)", true);
            assertJitMatchesJava("select a from y where (a*b) in (-727_379_968, 1_000_000_000_000)", true);
            assertJitMatchesJava("select a from y where (a*b) not in (1_000_000_000_000, -727_379_968)", true);
            Assert.assertEquals(1, runQuery("select a from y where (a*b) in (1_000_000_000_000, -727_379_968)"));
            Assert.assertEquals(63, runQuery("select a from y where (a*b) not in (1_000_000_000_000, -727_379_968)"));
            // The pre-wrap value alone matches nothing: the key never carries it. Both engines
            // have to agree on that, and the wrapped spelling right below keeps the parity honest.
            assertJitMatchesJavaOnEmptyResult("select a from y where (a*b) in (1_000_000_000_000)", true);
            assertJitMatchesJava("select a from y where (a*b) in (-727_379_968)", true);
            Assert.assertEquals(0, runQuery("select a from y where (a*b) in (1_000_000_000_000)"));
            Assert.assertEquals(1, runQuery("select a from y where (a*b) in (-727_379_968)"));

            // control: an all-narrow list emits no SX_I64 and stays vectorized.
            assertJitMatchesJava("select a from y where (a*b) in (5, -727_379_968)", true);
            // control: a NULL element wraps the key, so it stays vectorized and matches no row here
            // (no product wraps onto INT_NULL in this table).
            assertJitMatchesJava("select a from y where (a*b) in (null, -727_379_968)", true);
            Assert.assertEquals(1, runQuery("select a from y where (a*b) in (null, -727_379_968)"));
        });
    }

    @Test
    public void testInOperatorOverflowNumericStringElementMatchesLiteral() throws Exception {
        // M1 regression: a numeric STRING/VARCHAR IN-list element against an overflowing
        // INT-arithmetic (narrow) key. isIntWidthElement wrapped the key only for
        // INT/SHORT/BYTE-TYPED elements, so a numeric string left the key on getLong()
        // (widen). Thus (a * b) IN ('-727379968') returned 0 rows while the equivalent
        // (a * b) IN (-727_379_968) and (a * b) = -727_379_968 both matched. The width is now
        // derived from the parsed VALUE: an INT-range numeric string wraps the key mod 2^32
        // (matching the numeric literal and '='), a wider value widens it. String IN-lists
        // do not compile to the JIT, so this is a Java-path parity check against the
        // numeric-literal spellings.
        assertMemoryLeak(() -> {
            // a * b wraps to INT -727_379_968 and widens to LONG 10^12; s/v carry the wrapped
            // image as a numeric string/varchar, sw the widened image.
            execute("create table x as (select cast(1_000_000 as int) a, cast(1_000_000 as int) b, " +
                    "'-727379968' s, cast('-727379968' as varchar) v, '1000000000000' sw)");

            // The string IN-list never compiles to the JIT; enabling it falls back to Java
            // and matches, so there is no JIT divergence to fix here.
            assertJitMatchesJava("x where (a * b) in ('-727379968')", false);

            // INT-range numeric string wraps the key, matching IN (intLiteral) and '='.
            Assert.assertEquals(1, runQuery("x where (a * b) in ('-727379968')"));         // single const string
            Assert.assertEquals(1, runQuery("x where (a * b) in (-727_379_968)"));           // control: int literal
            Assert.assertEquals(1, runQuery("x where (a * b) = -727_379_968"));              // control: '='
            Assert.assertEquals(1, runQuery("x where (a * b) in (7, '-727379968')"));      // two const, mixed widths
            Assert.assertEquals(1, runQuery("x where (a * b) in (7, 9, '-727379968')"));   // multi const
            Assert.assertEquals(0, runQuery("x where (a * b) not in ('-727379968')"));     // inverse
            Assert.assertEquals(1, runQuery("x where (a * b) in (cast('-727379968' as varchar))")); // varchar const

            // A wider-than-INT numeric string widens the key, matching IN (longLiteral).
            Assert.assertEquals(0, runQuery("x where (a * b) in ('1000000000000')"));      // single const string
            Assert.assertEquals(0, runQuery("x where (a * b) in (1_000_000_000_000)"));        // control: long literal
            Assert.assertEquals(0, runQuery("x where (a * b) in ('999')"));                // matches neither width

            // runtime-const variant: a string bind variable forces InLongRuntimeConstFunction.
            bindVariableService.setStr("sp", "-727379968");
            Assert.assertEquals(1, runQuery("x where (a * b) in (:sp)"));                  // wraps to match
            Assert.assertEquals(1, runQuery("x where (a * b) in (:sp, 7)"));               // mixed runtime + const
            bindVariableService.setStr("swp", "1000000000000");
            Assert.assertEquals(0, runQuery("x where (a * b) in (:swp)"));                 // widens to match

            // var variant: a non-constant string/varchar column forces InLongVarFunction.
            Assert.assertEquals(1, runQuery("x where (a * b) in (s)"));                    // INT-range string col wraps
            Assert.assertEquals(1, runQuery("x where (a * b) in (v)"));                    // INT-range varchar col wraps
            Assert.assertEquals(0, runQuery("x where (a * b) in (sw)"));                   // wider string col widens
            Assert.assertEquals(0, runQuery("x where (a * b) not in (s)"));               // inverse
            Assert.assertEquals(1, runQuery("x where (a * b) in (7, s)"));                 // mixed var + const
        });
    }

    @Test
    public void testInOperatorOverflowSingleValueKeyUnderBooleanEquality() throws Exception {
        // C2 regression (level-3 review): the SINGLE-VALUE IN form never set inKeyWidthOverride,
        // so the key column leaves fell back to the predicate-global widening decision. A boolean
        // equality of an IN check and a LONG comparison - ((a*b) in (c)) = (nl > 0) - is a single
        // predicate, so the LONG sibling turned needsNarrowI64Widening on and the JIT computed the
        // overflowing narrow-int key at 64 bits (10^12) where the Java InLong path wraps it to the
        // INT element's width (-727_379_968). Bugfix 40 fixed this shape for '=' comparisons and
        // Bugfix 38 for multi-value IN lists; the single-value IN sat between them. serializeIn now
        // drives the override for the single-value form too, from its one element.
        //
        // Row 1 overflows: a*b wraps to INT -727_379_968 and widens to LONG 10^12. Row 2 wraps
        // exactly onto INT_MIN (the INT_NULL sentinel): 65_536*32_768. Rows 3+ compute 3*3 = 9.
        // The table has >= 64 rows so the shapes that stay vectorized exercise AVX2.
        assertMemoryLeak(() -> {
            execute("create table y as (select" +
                    " cast(case when x = 1 then 1_000_000 when x = 2 then 65_536 else 3 end as int) a," +
                    " cast(case when x = 1 then 1_000_000 when x = 2 then 32_768 else 3 end as int) b," +
                    " cast(case when x = 1 then 1_000_000_000_000 when x = 3 then 5 else 0 end as long) nl," +
                    " timestamp_sequence(0, 1_000_000) k" +
                    " from long_sequence(64)) timestamp(k)");

            // RED on HEAD: Java wraps the key against the INT element (row 1 matches the IN
            // check), the JIT widened it (no row matched). not in / <> diverge the same way.
            assertJitMatchesJava("select a from y where ((a*b) in (-727_379_968)) = (nl > 0)", true);
            assertJitMatchesJava("select a from y where ((a*b) not in (-727_379_968)) = (nl > 0)", true);
            assertJitMatchesJava("select a from y where ((a*b) in (-727_379_968)) <> (nl > 0)", true);
            assertJitMatchesJava("select a from y where (nl > 0) = ((a*b) in (-727_379_968))", true);

            // The Java (JIT-disabled) path is the oracle.
            Assert.assertEquals(63, runQuery("select a from y where ((a*b) in (-727_379_968)) = (nl > 0)"));
            Assert.assertEquals(1, runQuery("select a from y where ((a*b) not in (-727_379_968)) = (nl > 0)"));
            Assert.assertEquals(1, runQuery("select a from y where ((a*b) in (-727_379_968)) <> (nl > 0)"));

            // The override also drives the degenerate single-element NULL list: the key wraps
            // (INT width) against the NULL element on both paths, so row 2 - whose product wraps
            // onto INT_MIN, i.e. IS null - matches, exactly as '= null' does.
            assertJitMatchesJava("select a from y where (a*b) in (null)", true);
            Assert.assertEquals(1, runQuery("select a from y where (a*b) in (null)"));
            Assert.assertEquals(1, runQuery("select a from y where (a*b) = null"));

            // controls that already agreed: a LONG element widens the key on both paths; the
            // multi-value list drives the override per element (Bugfix 38); a constant-fold key
            // takes its width from its own comparison; separate predicates never shared the flag.
            assertJitMatchesJava("select a from y where ((a*b) in (1_000_000_000_000)) = (nl > 0)", true);
            assertJitMatchesJava("select a from y where ((a*b) in (5, -727_379_968)) = (nl > 0)", true);
            assertJitMatchesJava("select a from y where ((1_000_000*1_000_000) in (-727_379_968)) = (nl > 0)", true);
            assertJitMatchesJava("select a from y where (a*b) in (-727_379_968)", true);
            assertJitMatchesJava("select a from y where (a*b) in (-727_379_968) and nl > 0", true);
            assertJitMatchesJava("select a from y where (a*b) in (-727_379_968) or nl > 0", true);
        });
    }

    @Test
    public void testInOperatorOverflowWidenColumnArithKeyPerElement() throws Exception {
        // C2 regression: a COLUMN-arithmetic IN key (a * b) whose product overflows INT, in a
        // multi-value list that mixes a genuine-LONG element with an overflowing-INT element.
        // The Java InLong path reads the key per element - widened (getLong) against the LONG
        // element, wrapped (getInt) against the INT element. But the JIT decided narrow-int
        // widening once for the WHOLE predicate: a single coexisting LONG element flipped
        // needsNarrowI64Widening on, so the JIT sign-extended a and b for EVERY comparison,
        // computing a * b at 64 bits (10^12) even against the INT element where Java wraps the
        // key to -727_379_968. A row whose wrapped key matched the INT element matched in Java
        // but not in the JIT (which returned empty). The key width is now driven per element
        // (widen against LONG/TIMESTAMP, wrap against INT), matching the Java path.
        //
        // a*b = 10^12 widens to 10^12, wraps to INT -727_379_968. el = 0 (matches neither image);
        // the INT element -727_379_968 matches only the WRAPPED key.
        assertMemoryLeak(() -> {
            execute("create table y (a int, b int, el long, k timestamp) timestamp(k)");
            execute("insert into y values (1_000_000, 1_000_000, 0, 1)");

            // A LONG COLUMN element coexists with an overflowing-INT constant element. RED on
            // HEAD: the JIT widened the key against the INT element and missed the wrapped match.
            assertJitMatchesJava("select a from y where (a * b) in (el, -727_379_968)", true);    // RED on HEAD
            assertJitMatchesJava("select a from y where (a * b) in (-727_379_968, el)", true);    // element order swapped
            assertJitMatchesJava("select a from y where (a * b) in (5, el, -727_379_968)", true); // plus a plain element
            assertJitMatchesJavaOnEmptyResult("select a from y where (a * b) not in (el, -727_379_968)", true);// inverse

            // The Java (JIT-disabled) path is the oracle: the wrapped key matches the INT element.
            Assert.assertEquals("a\n1000000\n", runJavaToString("select a from y where (a * b) in (el, -727_379_968)"));
            Assert.assertEquals("a\n", runJavaToString("select a from y where (a * b) not in (el, -727_379_968)"));

            // controls that already agreed: an all-INT list wraps the key, a single LONG element
            // widens it, and '=' wraps - none of these mix widths across the list.
            assertJitMatchesJava("select a from y where (a * b) = -727_379_968", true);           // '=' wraps
            assertJitMatchesJava("select a from y where (a * b) in (-727_379_968)", true);        // single INT: wraps
            assertJitMatchesJava("select a from y where (a * b) in (5, -727_379_968)", true);     // all-INT list: wraps
            assertJitMatchesJavaOnEmptyResult("select a from y where (a * b) in (el)", true);                // single LONG: widens
        });
    }

    @Test
    public void testInOperatorOverflowWidenColumnKeyArithElement() throws Exception {
        // C1 regression: a PLAIN narrow-int COLUMN key (i32/i16/i8) IN a list whose ELEMENT is an
        // overflowing INT arithmetic (m * 3) and which also carries a coexisting genuine-LONG
        // element (nl). The Java InLong path reads the column key per element - wrapped (getInt)
        // against the INT-arith element, so m * 3 = 4_294_967_301 wraps to INT 5 and matches a key
        // holding 5. But the JIT only drove the per-element width override for an ARITHMETIC key,
        // never a plain-column one: with a column key the override stayed unset, so the coexisting
        // LONG element flipped the predicate-global needsNarrowI64Widening on and sign-extended the
        // element's narrow leaf (m). The JIT then computed m * 3 at 64 bits (4_294_967_301, no wrap)
        // and the row that matched in Java (and under '=') matched nothing in the JIT. The key is
        // now width-sensitive for a genuine narrow-int column too, so the arith element wraps
        // against it, matching Java. Existing IN-key column-arith tests only used arithmetic KEYS
        // with arithmetic/LONG elements, never a plain-column key with an arithmetic element.
        //
        // m * 3 = 4_294_967_301 wraps to INT 5 (fits SHORT and BYTE); nl = 99 matches no narrow key.
        assertMemoryLeak(() -> {
            execute("create table t (id long, i32 int, i16 short, i8 byte, m int, nl long, k timestamp) timestamp(k)");
            execute("insert into t values" +
                    " (1, 5, 5, 5, 1_431_655_767, 99, 1)," +   // key holds the WRAPPED image 5 -> matches
                    " (2, 42, 42, 42, 1_431_655_767, 99, 2)"); // no narrow key equals 5 or 99 -> no match

            // RED on HEAD: the LONG element widened the arith element against the column key, so the
            // wrapped match (5) was missed and the JIT returned empty where Java matched id 1.
            assertJitMatchesJava("select id from t where i32 in (m * 3, nl)", true);        // INT key
            assertJitMatchesJava("select id from t where i16 in (m * 3, nl)", true);        // SHORT key
            assertJitMatchesJava("select id from t where i8 in (m * 3, nl)", true);         // BYTE key
            assertJitMatchesJava("select id from t where i32 in (nl, m * 3)", true);        // element order swapped
            assertJitMatchesJava("select id from t where i32 in (7, m * 3, nl)", true);     // plus a plain element
            assertJitMatchesJava("select id from t where i32 not in (m * 3, nl)", true);    // inverse
            // Single-value plain-column key under a boolean equality: the sibling LONG comparison
            // (nl > 0) flips the predicate-global flag on, yet the arith element must still wrap
            // against the column key. Needs the override on the single-value form too.
            assertJitMatchesJava("select id from t where (i32 in (m * 3)) = (nl > 0)", true);

            // The Java (JIT-disabled) path is the oracle: the wrapped element matches id 1 only.
            Assert.assertEquals("id\n1\n", runJavaToString("select id from t where i32 in (m * 3, nl)"));
            Assert.assertEquals("id\n1\n", runJavaToString("select id from t where i16 in (m * 3, nl)"));
            Assert.assertEquals("id\n1\n", runJavaToString("select id from t where i8 in (m * 3, nl)"));
            Assert.assertEquals("id\n2\n", runJavaToString("select id from t where i32 not in (m * 3, nl)"));
            Assert.assertEquals("id\n1\n", runJavaToString("select id from t where (i32 in (m * 3)) = (nl > 0)"));
            // IN must agree with '=' on the wrapped image (both wrap the arith element at INT width).
            Assert.assertEquals(
                    runJavaToString("select id from t where i32 = m * 3"),
                    runJavaToString("select id from t where i32 in (m * 3, nl)"));

            // Controls that already agreed - none mix an arith element with a LONG element: an
            // all-narrow list wraps, '=' wraps, and a single LONG element widens the key (no match).
            assertJitMatchesJava("select id from t where i32 in (m * 3, 7)", true);         // all-narrow list: wraps
            assertJitMatchesJava("select id from t where i32 = m * 3", true);               // '=' wraps
            assertJitMatchesJavaOnEmptyResult("select id from t where i32 in (nl)", true);               // single LONG: widens
            Assert.assertEquals("id\n", runJavaToString("select id from t where i32 in (nl)"));
        });
    }

    @Test
    public void testInOperatorNullElementBesideLongColumnKeepsNarrowKey() throws Exception {
        // C2 regression: a narrow-INT COLUMN key IN a list carrying a NULL element AND an OBSERVED
        // wide (LONG) COLUMN element. The type observer sees the LONG column and returns I8, so
        // serializeNull emitted the null as LONG_NULL at I8 while the null pairing keeps the key at
        // I4. In wide-lane mode the LONG element's SX_I64 no longer forces scalar, so the four-lane
        // backend compared the i32 key against the i64 LONG_NULL immediate (avx2.h#convert has no
        // i32->i64 case), matching INT_NULL rows only in some lane positions and diverging from the
        // Java InLong path (which matches INT_NULL rows via the null element). The null is now emitted
        // at the key's kept width (INT_NULL at I4). Existing coverage used a wide CONSTANT element,
        // which leaves the observer at I4 and hides the bug. The table has >= 64 rows so the
        // vectorized loop runs past the scalar tail (a 1-row table would hide the divergence).
        assertMemoryLeak(() -> {
            execute("create table t as (select" +
                    " x id," +
                    " cast(case when x % 3 = 0 then null else x end as int) i32," +   // 21 of 64 are INT_NULL
                    " cast(case when x % 3 = 0 then null else x end as short) i16," +  // SHORT: null casts to 0, never null
                    " cast(case when x % 3 = 0 then null else x end as byte) i8," +    // BYTE: null casts to 0, never null
                    " cast(10_000_000_000 as long) nl," +                             // out of INT range: matches no widened key
                    " timestamp_sequence(0, 1_000_000) k" +
                    " from long_sequence(64)) timestamp(k)");

            // RED on HEAD: JIT wide-lane drops (position-dependently) the INT_NULL rows the null
            // element matches; Java matches all of them (on HEAD the INT key returned 10 of 21).
            assertJitMatchesJava("select id from t where i32 in (nl, null)", true);      // wide COLUMN element + null
            assertJitMatchesJava("select id from t where i32 in (null, nl)", true);      // element order swapped
            assertJitMatchesJava("select id from t where i32 in (7, nl, null)", true);   // plus a plain element

            // The Java (JIT-disabled) oracle: the null element matches exactly the INT_NULL rows
            // (x % 3 == 0 -> 21 of 64); nl matches none.
            Assert.assertEquals(21, runQuery("select id from t where i32 in (nl, null)"));

            // SHORT/BYTE keys are also width-sensitive (see isWidthSensitiveInKey) and take the I4
            // override, so the fix routes their null pairing through INT_NULL at I4 too. Those types
            // are never null (a null cast lands on 0), so the null element matches nothing - the i16/i8
            // key value never equals INT_NULL - and JIT agrees with Java at 0 rows.
            assertJitMatchesJavaOnEmptyResult("select id from t where i16 in (nl, null)", true);
            assertJitMatchesJavaOnEmptyResult("select id from t where i8 in (nl, null)", true);
            Assert.assertEquals(0, runQuery("select id from t where i16 in (nl, null)"));
            Assert.assertEquals(0, runQuery("select id from t where i8 in (nl, null)"));

            // Control that agreed before the fix: a wide CONSTANT element leaves the observer at I4,
            // so the null already emitted INT_NULL and matched.
            assertJitMatchesJava("select id from t where i32 in (5_000_000_000, null)", true);
            Assert.assertEquals(21, runQuery("select id from t where i32 in (5_000_000_000, null)"));
        });
    }

    @Test
    public void testInOperatorOverflowWidenConstOperandKeyPerElement() throws Exception {
        // C1 regression: an IN key that is a narrow-int column arithmetic with a CONSTANT operand
        // - (a * 3), (a + const), (n - const) - whose product/sum overflows INT, in a multi-value
        // list mixing a genuine-LONG element with the in-INT-range element the WRAPPED key equals.
        // The column leaf honored the per-element key width (maybeEmitI64Widening) but the constant
        // operand did not: it was backfilled at predicate exit, after serializeIn reset the width
        // override, so a coexisting LONG element emitted it at I8 and the JIT computed the whole
        // product at long width (no wrap), missing the wrapped match Java keeps (getInt). Existing
        // IN-key column-arith tests only used two-column keys (a*b)/(a+a), never a constant operand.
        //
        // a*3 = 3e9 wraps to INT -1_294_967_296, widens to 3e9; the INT element matches only the WRAP.
        assertMemoryLeak(() -> {
            execute("create table z (a int, n int, k timestamp) timestamp(k)");
            execute("insert into z values (1_000_000_000, -2_000_000_000, 1)");

            // RED on HEAD: the LONG element widened the key against the INT element, missing the wrap.
            assertJitMatchesJava("select a from z where (a * 3) in (5_000_000_000, -1_294_967_296)", true);
            assertJitMatchesJava("select a from z where (a * 3) in (-1_294_967_296, 5_000_000_000)", true); // order swapped
            assertJitMatchesJava("select a from z where (a * 3) in (5, 5_000_000_000, -1_294_967_296)", true); // extra element
            assertJitMatchesJavaOnEmptyResult("select a from z where (a * 3) not in (5_000_000_000, -1_294_967_296)", true); // inverse
            // '+' and '-' operand forms overflow the same way; '-' underflows via the n column.
            assertJitMatchesJava("select a from z where (a + 2_000_000_000) in (5_000_000_000, -1_294_967_296)", true);
            assertJitMatchesJava("select a from z where (n - 1_000_000_000) in (-5_000_000_000, 1_294_967_296)", true);

            // The Java (JIT-disabled) path is the oracle: the wrapped key matches the INT element.
            Assert.assertEquals("a\n1000000000\n", runJavaToString("select a from z where (a * 3) in (5_000_000_000, -1_294_967_296)"));
            Assert.assertEquals("a\n1000000000\n", runJavaToString("select a from z where (a + 2_000_000_000) in (5_000_000_000, -1_294_967_296)"));
            Assert.assertEquals("a\n1000000000\n", runJavaToString("select a from z where (n - 1_000_000_000) in (-5_000_000_000, 1_294_967_296)"));
            Assert.assertEquals("a\n", runJavaToString("select a from z where (a * 3) not in (5_000_000_000, -1_294_967_296)"));

            // controls that already agreed - none mix widths across the list: '=' wraps, a single
            // INT element wraps, a single LONG element widens (matches neither image, so no row).
            assertJitMatchesJava("select a from z where (a * 3) = -1_294_967_296", true);
            assertJitMatchesJava("select a from z where (a * 3) in (-1_294_967_296)", true);
            assertJitMatchesJavaOnEmptyResult("select a from z where (a * 3) in (5_000_000_000)", true);
            Assert.assertEquals("a\n", runJavaToString("select a from z where (a * 3) in (5_000_000_000)"));
        });
    }

    @Test
    public void testInOperatorOverflowWidenElementPerPairing() throws Exception {
        // C1/C2: an overflowing INT-arithmetic IN key against a list that mixes a genuine-LONG
        // element with an overflowing INT-arith element (C1) or a NULL element (C2). The Java
        // InLong path reads the key per element: wrapped (getInt) against the INT-arith and NULL
        // elements, widened (getLong) against the LONG one. The JIT set the key width per element
        // but left the element on the predicate-global widen flag, so a coexisting LONG element
        // over-widened the INT-arith element (C1) and, with the NULL element, the key too (C2).
        // serializeIn now reads both the element and the key at the pairing width.
        //
        // row1: a*b and c*d both wrap to -727_379_968 (widen to 10^12); el=999 matches neither.
        // row2: a*b = 2^31 wraps to INT_MIN (== INT_NULL), so the key IS null; c*d = 1; el=999.
        assertMemoryLeak(() -> {
            execute("create table y (a int, b int, c int, d int, el long, k timestamp) timestamp(k)");
            execute("insert into y values (1_000_000, 1_000_000, 1_000_000, 1_000_000, 999, 1)," +
                    " (2, 1_073_741_824, 1, 1, 999, 2)");

            // C1: the INT-arith element must wrap with the key. RED on HEAD - the key wrapped
            // but the element widened, so the (key = c*d) pairing missed the wrapped match.
            assertJitMatchesJava("select a from y where (a*b) in (c*d, el)", true);
            assertJitMatchesJava("select a from y where (a*b) in (el, c*d)", true);
            assertJitMatchesJava("select a from y where (a*b) in (5, c*d, el)", true);
            assertJitMatchesJava("select a from y where (a*b) not in (c*d, el)", true);
            Assert.assertEquals("a\n1000000\n", runJavaToString("select a from y where (a*b) in (c*d, el)"));

            // C2: the NULL element wraps the key (INT width), so row2 - whose product wraps onto
            // INT_MIN, i.e. IS null, as '= null' and the projection both report - matches it. The
            // coexisting LONG element must not widen the key for the NULL pairing.
            assertJitMatchesJava("select a from y where (a*b) in (null, el)", true);
            assertJitMatchesJava("select a from y where (a*b) in (el, null)", true);
            assertJitMatchesJava("select a from y where (a*b) not in (null, el)", true);
            Assert.assertEquals("a\n2\n", runJavaToString("select a from y where (a*b) in (null, el)"));
            Assert.assertEquals("a\n2\n", runJavaToString("select a from y where (a*b) = null"));
            Assert.assertEquals("a\n1000000\n", runJavaToString("select a from y where (a*b) not in (null, el)"));

            // control: an all-narrow list wraps both key and element (no width to mix).
            assertJitMatchesJava("select a from y where (a*b) in (c*d, 5)", true);
        });
    }

    @Test
    public void testInOperatorOverflowWidenKeyOnLongElement() throws Exception {
        // An overflowing INT *arithmetic* KEY on the left of IN() against a LONG element.
        // 'in (LONG)' is 'key = LONG', which resolves the LONG comparison - and that reads the
        // key through getLong(), an exact sign extension of the value it already wrapped to.
        // So the key carries -727_379_968 here, not 10^12, and IN, '=' and CAST all agree on
        // that. The JIT has to reach the same answer, which is what this pins.
        //
        // Rows: a*b wraps to -727_379_968 (both rows); el row1 = -727_379_968 (the wrapped
        // value), el row2 = 10^12; ei = -727_379_968 (both rows).
        assertMemoryLeak(() -> {
            execute("create table x (id long, a int, b int, el long, ei int, k timestamp) timestamp(k)");
            execute("insert into x values (1, 1_000_000, 1_000_000, -727_379_968, -727_379_968, 1)," +
                    " (2, 1_000_000, 1_000_000, 1_000_000_000_000, -727_379_968, 2)");

            // A LONG COLUMN element forces InLongVarFunction (non-constant). The key carries
            // its wrapped value, so it matches row 1, NOT row 2. '=', CAST and the JIT all
            // agree, so JIT-vs-Java parity holds too.
            assertJitMatchesJava("select id from x where (a * b) in (el)", true);
            assertJitMatchesJava("select id from x where (a * b) not in (el)", true);
            assertJitMatchesJava("select id from x where (a * b) = el", true);              // control: '='
            // absolute correctness: the wrapped key selects row 1, not row 2.
            Assert.assertEquals("id\n1\n", runJavaToString("select id from x where (a * b) in (el)"));
            Assert.assertEquals("id\n2\n", runJavaToString("select id from x where (a * b) not in (el)"));
            Assert.assertEquals(
                    runJavaToString("select id from x where (a * b) = el"),
                    runJavaToString("select id from x where (a * b) in (el)"));
            Assert.assertEquals(
                    runJavaToString("select id from x where cast(a * b as long) in (el)"),
                    runJavaToString("select id from x where (a * b) in (el)"));

            // A LONG CONSTANT element cannot reach the pre-wrap product either, so 10^12 now
            // matches nothing while the wrapped value matches both rows. Each empty assertion
            // is paired with its complement, so parity never becomes vacuous.
            assertJitMatchesJavaOnEmptyResult("select id from x where (a * b) = 1_000_000_000_000", true);
            assertJitMatchesJavaOnEmptyResult("select id from x where (a * b) in (1_000_000_000_000)", true);        // single
            assertJitMatchesJavaOnEmptyResult("select id from x where (a * b) in (1_000_000_000_000, 5)", true);     // two
            assertJitMatchesJavaOnEmptyResult("select id from x where (a * b) in (1_000_000_000_000, 5, 6)", true);  // multi
            assertJitMatchesJavaOnEmptyResult("select id from x where (a * b) > 999_999_999_999", true);            // relational
            assertJitMatchesJava("select id from x where (a * b) = -727_379_968", true);
            assertJitMatchesJava("select id from x where (a * b) in (-727_379_968)", true);
            assertJitMatchesJava("select id from x where (a * b) not in (1_000_000_000_000)", true);
            Assert.assertEquals("id\n", runJavaToString("select id from x where (a * b) = 1_000_000_000_000"));
            Assert.assertEquals("id\n1\n2\n", runJavaToString("select id from x where (a * b) = -727_379_968"));
            Assert.assertEquals("id\n1\n2\n", runJavaToString("select id from x where (a * b) not in (1_000_000_000_000)"));

            // A runtime-constant (bind variable) LONG element forces InLongRuntimeConstFunction.
            // Bound to the pre-wrap product it matches nothing; bound to the wrapped one it matches
            // both rows, so the pair still bites.
            bindVariableService.setLong("p", 1_000_000_000_000L);
            assertJitMatchesJavaOnEmptyResult("select id from x where (a * b) in (:p)", true);
            assertJitMatchesJavaOnEmptyResult("select id from x where (a * b) = :p", true);
            Assert.assertEquals("id\n", runJavaToString("select id from x where (a * b) in (:p)"));
            bindVariableService.setLong("p", -727_379_968L);
            assertJitMatchesJava("select id from x where (a * b) in (:p)", true);
            assertJitMatchesJava("select id from x where (a * b) = :p", true);
            Assert.assertEquals("id\n1\n2\n", runJavaToString("select id from x where (a * b) in (:p)"));

            // Control - an INT element must still WRAP the key (matching EqInt and the JIT).
            // master read the key via getLong() and WIDENED it, so IN wrongly returned no row
            // where '=' matched both; the wrapped key now matches the stored wrapped image.
            assertJitMatchesJava("select id from x where (a * b) in (ei)", true);
            assertJitMatchesJava("select id from x where (a * b) = ei", true);
            Assert.assertEquals("id\n1\n2\n", runJavaToString("select id from x where (a * b) in (ei)"));
            Assert.assertEquals("id\n1\n2\n", runJavaToString("select id from x where (a * b) in (1_000_000 * 1_000_000)"));
        });
    }

    @Test
    public void testInOperatorOverflowWidenLongKeyArithElementWithFloat() throws Exception {
        // C3 regression (level-3 review): a LONG IN key against a narrow-int COLUMN-ARITHMETIC
        // element (a*b), inside a boolean equality with a FLOAT/DOUBLE sibling. A float suppresses
        // the predicate-global narrow-i64 widening, so markFloatI64WidenLeaves is the only pass that
        // can sign-extend the element. It returned at the IN FUNCTION node and never descended, so
        // a*b wrapped at INT width (-727_379_968) while the Java InLong path reads it at long width
        // (10^12) against the LONG key. inKeyWidthOverride cannot help - it fires only for a
        // narrow-int key. markI64Widen now descends into the IN and widens each element the key
        // reads at long width, deriving the width per element (mirroring markI64WidenFoldRoots).
        //
        // a*b = 10^12 wraps to INT -727_379_968, widens to LONG 10^12; nl matches the widened image
        // only in row 1.
        assertMemoryLeak(() -> {
            execute("create table w as (select cast(1_000_000 as int) a, cast(1_000_000 as int) b," +
                    " cast(case x when 1 then 1_000_000_000_000 when 2 then 2 else 0 end as long) nl," +
                    " cast(1.0 as float) f32, cast(1.0 as double) f64," +
                    " x::short cs, x::byte cbyte," +
                    " timestamp_sequence(0, 1_000_000) k" +
                    " from long_sequence(5)) timestamp(k)");

            // Primary repro: the element a*b must widen to 10^12, not wrap to -727_379_968. Only row 1
            // (nl = 10^12) matches; the pre-fix JIT wrapped it and returned 0 rows.
            assertJitMatchesJava("select cs from w where (nl in (a*b, 7)) = (f32 > 0)", true, "cs\n");
            // Single-value IN (unrolled path), operand order, non-zero float threshold, DOUBLE
            // sibling: all read the element at long width the same way.
            assertJitMatchesJava("select cs from w where (nl in (a*b)) = (f32 > 0)", true, "cs\n");
            assertJitMatchesJava("select cs from w where (f32 > 0) = (nl in (a*b, 7))", true, "cs\n");
            assertJitMatchesJava("select cs from w where (nl in (a*b, 7)) = (f32 > 0.5)", true, "cs\n");
            assertJitMatchesJava("select cs from w where (nl in (a*b, 7)) = (f64 > 0)", true, "cs\n");
            // NOT / not in flip the match to the complementary rows.
            assertJitMatchesJava("select cs from w where not ((nl in (a*b, 7)) = (f32 > 0))", true,
                    "cs\n1\n2\n3\n4\n5\n");
            assertJitMatchesJava("select cs from w where (nl not in (a*b, 7)) = (f32 > 0)", true,
                    "cs\n1\n2\n3\n4\n5\n");

            // Over-widening guard: the SAME a*b appears widened inside the IN (vs the LONG key) and
            // wrapped inside a sibling INT-width comparison. markI64WrapArithLeaves must keep the
            // wrap-side product at INT width - both wrap to -727_379_968 there (RHS true every row),
            // so parity, not just the pin, catches an over-widened wrap side.
            assertJitMatchesJava("select cs from w where (nl in (a*b, 7)) = ((a*b) = -727_379_968)", true,
                    "cs\n");

            // Controls that must keep passing. Plain narrow COLUMN elements sign-extend value-
            // preservingly (row 2: nl = 2 matches cs = 2); a coexisting LONG-const element leaves
            // the arith element widening; AND/OR split into separate float-free predicates.
            assertJitMatchesJava("select cs from w where (nl in (cs, cbyte)) = (f32 > 0)", true, "cs\n2\n");
            assertJitMatchesJava("select cs from w where (nl in (a*b, 999_999_999_999)) = (f32 > 0)", true, "cs\n");
            assertJitMatchesJava("select cs from w where nl in (a*b, 7) and f32 > 0", true, "cs\n");
            assertJitMatchesJava("select cs from w where nl in (a*b, 7) or f32 > 0", true,
                    "cs\n1\n2\n3\n4\n5\n");
        });
    }

    @Test
    public void testInOperatorSingleValue() throws Exception {
        // Tests single-value IN() which has a special unrolled code path in CompiledFilterIRSerializer
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
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
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
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
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
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
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
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
    public void testInOperatorSymbolAndCharKeysAreNotWidthSensitive() throws Exception {
        // Scoping guard for the C1 fix (isWidthSensitiveInKey). SYMBOL and CHAR IN keys map to the
        // same narrow type code as a genuine BYTE/SHORT/INT key (columnTypeCode collapses SYMBOL and
        // IPv4 onto I4, CHAR onto I2), and both compile to the JIT. But they route through
        // InSymbol/InChar, NOT the width-sensitive InLong path, so they must NOT take the per-element
        // width override. isWidthSensitiveInKey keys off the real column type tag (not the collapsed
        // code) so these stay untouched; simply dropping the OPERATION guard would flag any I4/I2 key
        // and hand the override to symbol/char keys too. That stays result-correct (sign-extending a
        // symbol/char key is value-preserving), so this test cannot catch it directly - but it emits
        // an unnecessary SX_I64 that forces the whole filter onto the scalar path, a JIT performance
        // regression for what today vectorizes. This test locks the correctness invariant: symbol and
        // char IN keys still produce identical JIT and Java results.
        assertMemoryLeak(() -> {
            execute("create table s (sym symbol, ch char, ip ipv4, k timestamp) timestamp(k)");
            execute("insert into s values ('a','x','1.1.1.1', 1)," +
                    " ('b','y','2.2.2.2', 2)," +
                    " ('c','z','3.3.3.3', 3)");

            // SYMBOL and CHAR IN keys compile to the JIT; the result must match Java exactly.
            assertJitMatchesJava("select k from s where sym in ('a','c')", true);
            assertJitMatchesJava("select k from s where sym not in ('a','c')", true);
            assertJitMatchesJava("select k from s where ch in ('x','z')", true);
            assertJitMatchesJava("select k from s where ch not in ('x','z')", true);
            // IPv4 IN with string constants does not compile to the JIT (unsupported string constant),
            // so it falls back to Java; the parity check still guards the fallback path.
            assertJitMatchesJava("select k from s where ip in ('1.1.1.1','3.3.3.3')", false);

            // Absolute oracle: the symbol/char keys select the expected rows.
            Assert.assertEquals("k\n1970-01-01T00:00:00.000001Z\n1970-01-01T00:00:00.000003Z\n",
                    runJavaToString("select k from s where sym in ('a','c')"));
            Assert.assertEquals("k\n1970-01-01T00:00:00.000001Z\n1970-01-01T00:00:00.000003Z\n",
                    runJavaToString("select k from s where ch in ('x','z')"));
        });
    }

    @Test
    public void testInOperatorTwoValues() throws Exception {
        // Tests two-value IN() which also has an unrolled code path (args.size() < 3)
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
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
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
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
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
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
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
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
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " rnd_int(1, 10, 0) a," +
                " rnd_int(1, 10, 0) b," +
                " rnd_long(0, 100, 0) c" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertQueryNotNull(query, ddl);
    }

    @Test
    public void testIntColumnVsOutOfRangeConstant() throws Exception {
        // A narrow-int column compared against an integer constant beyond INT range
        // reads at long width in the Java filter (the constant is a LONG literal that
        // promotes the INT column via getLong), so no INT value equals it. The JIT
        // type observer sees only columns, so it typed the constant down to the INT
        // width, and serializeNumber emitted it as a 32-bit float on the int-parse
        // overflow; floats near 2^31 are spaced 256 apart, so distinct INT rows
        // (2_147_483_647 / 2_147_483_646) collapsed onto one float and matched spuriously.
        // The serializer now sign-extends the narrow leaf and emits the constant as a
        // full I8 IMM, keeping the comparison at long width so both paths agree.
        assertMemoryLeak(() -> {
            execute("create table t (i8 byte, i16 short, i32 int, i64 long, ts timestamp) timestamp(ts) partition by day");
            execute("insert into t values " +
                    "(127, 32_767, 2_147_483_647, 9_223_372_036_854_775_806, 0)," +
                    "(126, 32_766, 2_147_483_646, 9_223_372_036_854_775_805, 1_000_000)," +
                    "(null, null, null, null, 2_000_000)");
            // Absolute pins on the repro shapes. Parity alone is not an oracle here: the two INT rows
            // that the pre-fix JIT collapsed onto one float differ by 1, so a shared wrong width keeps
            // or drops both together and JIT-vs-Java parity still holds. maxRow and nextRow name them
            // individually, so a collapse fails the pin.
            final String header = "i8\ti16\ti32\ti64\tts\n";
            final String maxRow = "127\t32767\t2147483647\t9223372036854775806\t1970-01-01T00:00:00.000000Z\n";
            final String nextRow = "126\t32766\t2147483646\t9223372036854775805\t1970-01-01T00:00:01.000000Z\n";
            final String nullRow = "0\t0\tnull\tnull\t1970-01-01T00:00:02.000000Z\n";
            // All-narrow path (only i32 observed): the constant would fall back to float. No INT value
            // equals 2^31, so the equality keeps nothing - that is exactly the spurious match the
            // pre-fix JIT produced.
            assertJitMatchesJava("t where i32 = 2_147_483_648", true, header);
            assertJitMatchesJava("t where i32 >= 2_147_483_648", true, header);
            assertJitMatchesJava("t where i32 > 2_147_483_648", true, header);
            assertJitMatchesJava("t where i32 <= 2_147_483_648", true, header + maxRow + nextRow);
            assertJitMatchesJava("t where i32 < 2_147_483_648", true, header + maxRow + nextRow);
            assertJitMatchesJava("t where i32 <> 2_147_483_648", true, header + maxRow + nextRow + nullRow);
            assertJitMatchesJava("t where i32 = 5_000_000_000", true, header);
            // Negative out-of-range constant (unary minus of an overflowing literal).
            assertJitMatchesJava("t where i32 = -3_000_000_000", true, header);
            assertJitMatchesJava("t where i32 > -3_000_000_000", true, header + maxRow + nextRow);
            // Mixed-size path (i32 + i64 observed): the column must sign-extend too.
            assertJitMatchesJava("t where i32 = 2_147_483_648 and i64 > 0", true, header);
            assertJitMatchesJava("t where i32 = 5 and i64 > 0", true, header);
            // Single- and multi-value IN, including mixed in-range / out-of-range elements.
            assertJitMatchesJava("t where i32 in (2_147_483_648)", true, header);
            assertJitMatchesJava("t where i32 in (2_147_483_648, 5)", true, header);
            assertJitMatchesJava("t where i32 in (5, 2_147_483_648)", true, header);
            // NULL is a comparable sentinel here, not SQL unknown, so the null row survives the
            // negated forms - the same way it survives i32 <> 2_147_483_648 above.
            assertJitMatchesJava("t where i32 not in (2_147_483_648, 5)", true, header + maxRow + nextRow + nullRow);
            // OR chain: the same column appears at two widths in one predicate. The in-range arm must
            // still tell 2_147_483_647 apart from 2_147_483_646 - the float collapse matched both.
            assertJitMatchesJava("t where i32 = 2_147_483_648 or i32 = 2_147_483_647", true, header + maxRow);
            // BYTE / SHORT column vs an out-of-INT-range constant: previously declined
            // JIT (serializeNumber threw on the int-parse overflow); now stays on JIT.
            assertJitMatchesJava("t where i8 = 2_147_483_648", true, header);
            assertJitMatchesJava("t where i16 = 3_000_000_000", true, header);
            // Control: a pure LONG column already computes at long width.
            assertJitMatchesJava("t where i64 = 2_147_483_648", true, header);
        });
    }

    @Test
    public void testIntWideLaneBatchLengthsAndBooleanMasks() throws Exception {
        assertMemoryLeak(() -> {
            for (int rowCount = 0; rowCount <= 9; rowCount++) {
                execute("drop table if exists wide_i");
                if (rowCount == 0) {
                    execute("create table wide_i (i32 int)");
                } else {
                    execute("create table wide_i as (select cast(case x "
                            + "when 1 then null when 2 then -2_147_483_647 "
                            + "when 3 then 7 else x end as int) i32 "
                            + "from long_sequence(" + rowCount + "))");
                }

                assertJitMatchesJavaOnBatchLengths("wide_i where i32 < 5_000_000_000", true);
                assertJitMatchesJavaOnBatchLengths("wide_i where i32 = 7 and i32 < 5_000_000_000", true);
                assertJitMatchesJavaOnBatchLengths("wide_i where i32 < 5_000_000_000 and i32 = 7", true);
                assertJitMatchesJavaOnBatchLengths("wide_i where i32 = 7 or i32 > 5_000_000_000", true);
                assertJitMatchesJavaOnBatchLengths("wide_i where i32 > 5_000_000_000 or i32 = 7", true);
                assertJitMatchesJava("select count() from wide_i where i32 < 5_000_000_000", true);
                assertJitMatchesJava("select count() from wide_i "
                        + "where i32 = 7 or i32 > 5_000_000_000", true);
            }
            assertBatchSweepReturnedRows();
        });
    }

    @Test
    public void testWideLaneFloatArithmeticVsIntegerConstant() throws Exception {
        // f32 + f64 harmonises to f64 through convert()'s f32 arm, and the comparison then puts that
        // f64 against the bare integer bound. serializeUntypedNumber keeps such a bound at I4
        // whenever the predicate carries a float (so that i32 * 2 still wraps mod 2^32), so the IR is
        // (i32 5)(f64 ...)(<) - and avx2::convert() had no f64-with-i32 arm, so it fell through with
        // the operands unchanged and emitted vcmppd against a vpbroadcastd register. The bound 5 was
        // read as 0x0000000500000005 = 1.06e-314 and a negative bound as a NaN, so the four-lane loop
        // selected almost nothing while the scalar loop and Java agreed. convert() now widens the i32
        // side, so all three run the same comparison.
        Assume.assumeTrue("wide-lane lives in the four-lane AVX2 path", Vect.getSupportedInstructionSet() >= 8);
        assertMemoryLeak(() -> {
            // rnd_float()/rnd_double() draw from [0, 1), so every bound below holds for EVERY row
            // whatever the seed: the expected count is the row count, not a property of the draw.
            // (Integer-valued float columns do not reproduce this - the bound has to land where the
            // misread denormal separates it from the real one.)
            execute("create table wlf as (select timestamp_sequence(0, 1_000_000) k," +
                    " rnd_float() f32, rnd_double() f64" +
                    " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)");
            final String allRows = "count\n" + N_SIMD_WITH_SCALAR_TAIL + "\n";
            // The inexact float bound is what flips the whole filter to four lanes; the arithmetic
            // comparison beside it is correct in isolation and was dragged in by it.
            assertJitScalarAndVectorMatchJava("select count() from wlf where f32 + f64 < 5 and f32 < 1.00000003", allRows);
            // A negative bound was misread as a NaN rather than as a small denormal, so every
            // comparison against it came out false. Both operand orders reach the same pairing.
            assertJitScalarAndVectorMatchJava("select count() from wlf where f64 + f32 > -1 and f32 < 1.00000003", allRows);
            assertJitScalarAndVectorMatchJava("select count() from wlf where f64 >= (1.5 + f32) * -3 and f32 < 1.00000003", allRows);
        });
    }

    @Test
    public void testWideLaneInNarrowElementUnderConstantKey() throws Exception {
        // markWidthSemantics harmonises an IN element against the key only when the pairing folds to
        // I8, and a CONSTANT key that fits in an int folds to I4 - so the i32 element got no SX_I64.
        // The key constant is emitted at I8 anyway, because serializeUntypedNumber follows the
        // predicate's type observer and the LONG element put I8 in it. That left an i64 key against
        // an i32 element in the four-lane loop, which loads the INT column as four packed i32 in the
        // low half of the register. isWidthSensitiveInKey now covers a numeric CONSTANT key, so the
        // per-element override drives the key's width from each element in turn.
        Assume.assumeTrue("wide-lane lives in the four-lane AVX2 path", Vect.getSupportedInstructionSet() >= 8);
        assertMemoryLeak(() -> {
            execute("create table wlk as (select timestamp_sequence(0, 1_000_000) k," +
                    " (x % 4)::int i32, ((x % 4) + 1) * 1_000_000_000L i64" +
                    " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)");
            // The key matches through the INT element (i32 == 0): the pairing the fix harmonises.
            assertJitScalarAndVectorMatchJava("select count() from wlk where (0 in (i32, i64)) and i32 < i64",
                    "count\n128\n");
            assertJitScalarAndVectorMatchJava("select count() from wlk where (3 in (i32, i64)) and i32 < i64",
                    "count\n129\n");
            // A key that only the LONG element can match must keep matching through it.
            assertJitScalarAndVectorMatchJava("select count() from wlk where (2000000000 in (i32, i64)) and i32 < i64",
                    "count\n129\n");
        });
    }

    @Test
    public void testInNullElementNonNullableNarrowKeyFolds() throws Exception {
        // BYTE and SHORT carry no NULL sentinel, so a NULL element matches nothing - which is what
        // the Java IN functions return. On master the whole filter declined here (serializeNull
        // throws "byte type is not nullable"); the untyped-NULL-at-INT-width rule made the element
        // compile as an I4 immediate instead, so the four-lane loop compared an i8 column against
        // vpbroadcastd(0x80000000), whose byte layout is 00 00 00 80 - three of every four lanes
        // testing the column against 0. serializeIn now folds the pairing away.
        //
        // The fold is a frontend rule and the divergence showed up in the SINGLE-SIZE AVX2 loop, not
        // the four-lane one (a BYTE or SHORT IN key is not wide-lane eligible), so this test is not
        // AVX2-gated. It still only DETECTS the regression on an AVX2 host - without AVX2
        // compiler.cpp falls back to the scalar loop, whose convert() has the complete int table -
        // and the class as a whole is skipped on ARM64 by setUp()'s JitUtil.isJitSupported() gate.
        // The host-independent guarantee is the IR pin in CompiledFilterIRSerializerTest.
        assertMemoryLeak(() -> {
            execute("create table wln as (select timestamp_sequence(0, 1_000_000) k," +
                    " (x % 5)::byte i8, (x % 7)::short i16, (x % 3)::char c," +
                    " x::int i32, (x * 1_000_000_000)::long i64" +
                    " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)");
            // A NULL-only list is false on every row, and it must stay vectorized rather than decline.
            assertJitScalarAndVectorMatchJava("select count() from wln where i8 in (null)", "count\n0\n");
            assertJitScalarAndVectorMatchJava("select count() from wln where i16 in (null)", "count\n0\n");
            // A NULL beside a real element must not disturb that element's own matches.
            assertJitScalarAndVectorMatchJava("select count() from wln where i8 in (null, 1)", "count\n103\n");
            assertJitScalarAndVectorMatchJava("select count() from wln where i16 in (null, 1)", "count\n74\n");
            assertJitScalarAndVectorMatchJava("select count() from wln where i8 in (null, 1) and i16 in (null, 2)",
                    "count\n15\n");

            // Every key shape isWidthSensitiveInKey accepts folds, not just a bare column. A unary
            // minus reads at BYTE width and, unlike binary narrow arithmetic, does not force scalar
            // mode - so it was the shape that stayed wrong after the column and bind-variable keys
            // were covered.
            assertJitScalarAndVectorMatchJava("select count() from wln where -i8 in (null)", "count\n0\n");
            assertJitScalarAndVectorMatchJava("select count() from wln where -i16 in (null, -1)", "count\n74\n");

            // NOT IN inverts the fold, so the never-matching pairing has to come out true for every
            // row - the answer the Java filter gives.
            assertJitScalarAndVectorMatchJava("select count() from wln where i8 not in (null)",
                    "count\n" + N_SIMD_WITH_SCALAR_TAIL + "\n");
            assertJitScalarAndVectorMatchJava("select count() from wln where i8 not in (null, 1)", "count\n412\n");

            // A sibling INT-vs-LONG comparison mixes the fold's size-1 mask with wider ones in the
            // same predicate (mixed sizes, so this runs the scalar loop rather than four lanes).
            // Both the empty and the non-empty answer are pinned: the fold emits an all-zero mask,
            // which reads the same at every lane width.
            assertJitScalarAndVectorMatchJava("select count() from wln where i8 in (null, 1) and i32 < i64",
                    "count\n103\n");
            assertJitScalarAndVectorMatchJava("select count() from wln where i8 in (null) and i32 < i64", "count\n0\n");

            // A numeric CONSTANT key reaches the same pairing from the other side: the key is the
            // constant and the narrow column is the element. Making that key width-sensitive is what
            // fixes the constant-key IN test below, and it also stops serializeNull declining here,
            // so the pairing has to fold or it returns three of every four rows.
            assertJitScalarAndVectorMatchJava("select count() from wln where 0 in (null, i8)", "count\n103\n");
            assertJitScalarAndVectorMatchJava("select count() from wln where 0 in (null, i16)", "count\n73\n");
            // A key with nothing but NULL elements has no column left to read, so the filter never
            // reaches the JIT at all; pin that it still answers no rows.
            assertJitMatchesJava("select count() from wln where 0 in (null)", false, "count\n0\n");

            // CHAR shares the I2 type code but DOES read (char) 0 as NULL, so it must keep the
            // ordinary pairing. It declines the JIT, as it did before, and the Java filter answers.
            assertJitMatchesJava("select count() from wln where c in (null)", false, "count\n171\n");
        });
    }

    @Test
    public void testWideLaneFloatArithmeticAndInBatchLengths() throws Exception {
        assertMemoryLeak(() -> {
            for (int rowCount = 0; rowCount <= 9; rowCount++) {
                execute("drop table if exists wide_mixed");
                if (rowCount == 0) {
                    execute("create table wide_mixed (i32 int, i64 long, f32 float)");
                } else {
                    execute("create table wide_mixed as (select "
                            + "cast(case x when 1 then null when 2 then -7 else x end as int) i32, "
                            + "cast(case x when 1 then null else 2 end as long) i64, "
                            + "cast(case x when 1 then null when 2 then 1.0 else 2.5 end as float) f32 "
                            + "from long_sequence(" + rowCount + "))");
                }

                assertJitMatchesJavaOnBatchLengths("wide_mixed where f32 < 1.00000003", true);
                assertJitMatchesJavaOnBatchLengths("wide_mixed where f32 = 1.00000003", true);
                assertJitMatchesJavaOnBatchLengths("wide_mixed where f32 + 0 < 1.00000003", true);
                assertJitMatchesJavaOnBatchLengths("wide_mixed where f32 in (1.00000003, 2.5)", true);
                assertJitMatchesJavaOnBatchLengths("wide_mixed where i32 * i64 = 14", true);
                assertJitMatchesJavaOnBatchLengths("wide_mixed where i32 * 2 in (1, 5_000_000_000)", true);
                assertJitMatchesJava("select count() from wide_mixed where f32 > 0.99999998", true);
                assertJitMatchesJava("select count() from wide_mixed where i32 * 2 in (1, 5_000_000_000)", true);
            }
            assertBatchSweepReturnedRows();
        });
    }

    @Test
    public void testWideLaneInElementNarrowColumnUnderLongKey() throws Exception {
        // A bare narrow-int column used as an IN ELEMENT under a LONG key was never
        // sign-extended: isWidthSensitiveInKey covers a narrow KEY only, and the IN branch of
        // markWidthSemantics returned before the narrow-leaf widening rule (which is gated on a
        // comparison token, false for "in"). A widening sibling conjunct flips the filter to
        // four-lane AVX2, where the element loads as four packed i32 in the low XMM half while
        // the key spans four 64-bit lanes, so each lane compares the key against a pair of
        // ADJACENT rows' elements.
        assertMemoryLeak(() -> {
            // i32 alternates 0, 1 so lanes 0 and 1 pack to (1 << 32) | 0 = 4_294_967_296, which
            // equals the key. No row satisfies the predicate at 64-bit width, but the pre-fix
            // four-lane path returned half the SIMD-body rows.
            execute("create table in_elem as (select timestamp_sequence(0, 1_000_000) k," +
                    " ((x + 1) % 2)::int i32, 4_294_967_296L i64, 7::int g32, 7L g64" +
                    " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)");
            assertJitMatchesJava("select count() from in_elem where i64 in (i32) and g32 = g64",
                    true, "count\n0\n");
            assertJitMatchesJava("in_elem where i64 in (i32) and g32 = g64", true, "k\ti32\ti64\tg32\tg64\n");
            assertJitMatchesJava("select count() from in_elem where i64 in (i32, 5) and g32 = g64",
                    true, "count\n0\n");

            // Batch-length and operand-order parity across the sign-extension boundary.
            for (int rowCount = 0; rowCount <= 9; rowCount++) {
                execute("drop table if exists in_elem_b");
                if (rowCount == 0) {
                    execute("create table in_elem_b (i32 int, i64 long, f32 float)");
                } else {
                    execute("create table in_elem_b as (select "
                            + "cast(case x when 1 then null when 2 then -1 else x - 3 end as int) i32, "
                            + "cast(case x when 1 then null when 2 then 4_294_967_295 else 2 end as long) i64, "
                            + "cast(case x when 1 then null else 2.5 end as float) f32 "
                            + "from long_sequence(" + rowCount + "))");
                }
                assertJitMatchesJavaOnBatchLengths("in_elem_b where i64 in (i32)", true);
                assertJitMatchesJavaOnBatchLengths("in_elem_b where i64 in (i32) and i32 < 5_000_000_000", true);
                assertJitMatchesJavaOnBatchLengths("in_elem_b where i32 in (i64)", true);
                assertJitMatchesJavaOnBatchLengths("in_elem_b where i32 in (i64) and i32 < 5_000_000_000", true);
                assertJitMatchesJavaOnBatchLengths("in_elem_b where i64 in (i32, 5_000_000_000)", true);
                assertJitMatchesJava("select count() from in_elem_b where i64 in (i32) and f32 < 1.00000003", true);
            }
            assertBatchSweepReturnedRows();
        });
    }

    @Test
    public void testWideLaneInNullElementUnderWidenedKey() throws Exception {
        // An untyped NULL IN element is wide-lane eligible, but its immediate took the local
        // type observer's width. The observer sees columns only, so a predicate that
        // sign-extends its narrow leaves to i64 still typed the NULL down to I4 and emitted
        // INT_NULL; against the key's i64 lanes that broadcasts as 0x8000000080000000 per qword
        // and no genuinely-NULL key ever matched. IN (null) and = null disagreed as a result.
        assertMemoryLeak(() -> {
            execute("create table in_null as (select timestamp_sequence(0, 1_000_000) k," +
                    " null::int i32" +
                    " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)");
            // The key is INT + an out-of-INT-range constant, so it widens to i64; every row is
            // NULL, so every row must match. Pre-fix the four-lane path returned none.
            assertJitMatchesJava("select count() from in_null where (i32 + 5_000_000_000) in (null)",
                    true, "count\n" + N_SIMD_WITH_SCALAR_TAIL + "\n");
            // The = null control has always been correct; the two spellings must agree.
            assertJitMatchesJava("select count() from in_null where (i32 + 5_000_000_000) = null",
                    true, "count\n" + N_SIMD_WITH_SCALAR_TAIL + "\n");

            for (int rowCount = 0; rowCount <= 9; rowCount++) {
                execute("drop table if exists in_null_b");
                if (rowCount == 0) {
                    execute("create table in_null_b (i32 int, i64 long)");
                } else {
                    execute("create table in_null_b as (select "
                            + "cast(case x when 1 then null when 2 then -7 else x end as int) i32, "
                            + "cast(case x when 1 then null else 2 end as long) i64 "
                            + "from long_sequence(" + rowCount + "))");
                }
                assertJitMatchesJavaOnBatchLengths("in_null_b where (i32 + 5_000_000_000) in (null)", true);
                assertJitMatchesJavaOnBatchLengths("in_null_b where (i32 + 5_000_000_000) = null", true);
                assertJitMatchesJavaOnBatchLengths("in_null_b where (i32 * i64) in (null)", true);
                // A narrow key with no LONG in the predicate must keep INT_NULL at I4.
                assertJitMatchesJavaOnBatchLengths("in_null_b where (i32 * 2) in (null)", true);
                assertJitMatchesJavaOnBatchLengths("in_null_b where i32 in (null)", true);
                // A genuine LONG key already typed the NULL at I8; it must stay vectorized.
                assertJitMatchesJavaOnBatchLengths("in_null_b where i64 in (null)", true);
            }
            assertBatchSweepReturnedRows();
        });
    }

    @Test
    public void testWideLaneIntColumnVsLongColumn() throws Exception {
        // A bare INT-column vs LONG-column comparison (no arithmetic, no out-of-range
        // constant) is wide-lane eligible, but nothing sign-extended its INT leaf. A
        // widening sibling conjunct (i32 < 5_000_000_000, or an inexact-float compare)
        // flips the whole filter to four-lane AVX2, dragging the un-widened comparison
        // into a vector compare whose operands differ in width - the INT column loads as
        // 4x32 in the low XMM and the LONG column as 4x64 in the YMM - so the lanes are
        // scrambled and the JIT returns the wrong rows. Standalone the comparison stayed
        // on the correct mixed-size scalar path, so only the sibling-flipped shape
        // regressed. Now serializeColumn sign-extends the narrow leaf (markWidthSemantics),
        // so the four-lane compare runs at i64 width and matches the Java filter.
        //
        // The regression manifests ONLY in the four-lane AVX2 compare (compiler.cpp runs the
        // wide-lane loop only when exec_hint == wide_lane AND has_avx2()); on a non-AVX2 x86 host
        // or on aarch64 the same predicate runs the always-correct scalar loop, so this test would
        // pass green there without ever touching the regressed path. Gate it on AVX2 so it either
        // exercises the four-lane compare or is skipped - never a false pass. The width-semantics
        // IR itself (both narrow leaves carry sx_i64, exec hint WIDE_LANE) is pinned host-
        // independently on every CI leg by CompiledFilterIRSerializerTest#
        // testDirectIntLongColumnComparisonWidensAndUsesWideLane.
        Assume.assumeTrue("wide-lane regression lives in the four-lane AVX2 path", Vect.getSupportedInstructionSet() >= 8);
        assertMemoryLeak(() -> {
            // Deterministic all-match pin: every row satisfies both conjuncts, so the Java
            // filter keeps all rows; the pre-fix JIT ran i32 < i64 in four-lane mode without
            // widening i32 and dropped SIMD-body rows (count < N).
            execute("create table allmatch as (select timestamp_sequence(0, 1_000_000) k," +
                    " 1::int i32, 3_000_000_000L i64" +
                    " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)");
            assertJitMatchesJava("select count() from allmatch where i32 < i64 and i32 < 5_000_000_000",
                    true, "count\n" + N_SIMD_WITH_SCALAR_TAIL + "\n");

            // Batch-length + sign-extension-boundary parity, exercising both widening-sibling
            // shapes (out-of-INT-range integer constant and inexact float) and the reversed
            // operand order.
            for (int rowCount = 0; rowCount <= 9; rowCount++) {
                execute("drop table if exists wide_cc");
                if (rowCount == 0) {
                    execute("create table wide_cc (i8 byte, i16 short, i32 int, i64 long, f32 float)");
                } else {
                    execute("create table wide_cc as (select "
                            + "cast(case x when 1 then null when 2 then 127 when 3 then -127 else x - 5 end as byte) i8, "
                            + "cast(case x when 1 then null when 2 then 32_000 when 3 then -32_000 else x - 5 end as short) i16, "
                            + "cast(case x when 1 then null when 2 then 2_147_483_647 "
                            + "when 3 then -2_147_483_647 when 4 then 0 else x - 5 end as int) i32, "
                            + "cast(case x when 1 then null when 2 then 5_000_000_000 "
                            + "when 3 then -5_000_000_000 else x * 1_000_000_000 end as long) i64, "
                            + "cast(case x when 1 then null else 2.5 end as float) f32 "
                            + "from long_sequence(" + rowCount + "))");
                }
                // Out-of-INT-range integer constant flips the filter to four-lane.
                assertJitMatchesJavaOnBatchLengths("wide_cc where i32 < i64 and i32 < 5_000_000_000", true);
                assertJitMatchesJavaOnBatchLengths("wide_cc where i32 = i64 and i32 < 5_000_000_000", true);
                assertJitMatchesJavaOnBatchLengths("wide_cc where i64 > i32 and i32 < 5_000_000_000", true);
                // Inexact-float sibling flips the filter to four-lane.
                assertJitMatchesJavaOnBatchLengths("wide_cc where i32 < i64 and f32 < 1.00000003", true);
                // Control: standalone mixed-width comparison stays on the scalar path and was
                // always correct.
                assertJitMatchesJavaOnBatchLengths("wide_cc where i32 < i64", true);
                assertJitMatchesJava("select count() from wide_cc where i32 < i64 and i32 < 5_000_000_000", true);
                // BYTE / SHORT columns are never wide-lane eligible, so a narrow-vs-LONG
                // comparison stays scalar whether or not a widening sibling is present; the
                // added sign-extension must not disturb that correct scalar path.
                assertJitMatchesJavaOnBatchLengths("wide_cc where i8 < i64", true);
                assertJitMatchesJavaOnBatchLengths("wide_cc where i16 < i64", true);
                assertJitMatchesJavaOnBatchLengths("wide_cc where i8 < i64 and i32 < 5_000_000_000", true);
                assertJitMatchesJavaOnBatchLengths("wide_cc where i16 = i64 and i32 < 5_000_000_000", true);
            }
            assertBatchSweepReturnedRows();
        });
    }

    @Test
    public void testWideLaneUnaryMinusNarrowLeaf() throws Exception {
        // Unary minus over a narrow-int leaf compared at long width was never sign-extended:
        // the widening rule fires only for a bare leaf (-i32 is an OPERATION), and the global
        // fallback missed too because isArithmeticOperation() requires two operands, so
        // NarrowI64WidenDetector.hasArithmetic stayed false. A widening sibling conjunct flips
        // the filter to four-lane AVX2, where NEG then ran with 32-bit lane semantics over
        // 64-bit lanes and corrupted the high half of each lane.
        assertMemoryLeak(() -> {
            // -i32 is -1, whose i32 pattern packs with its neighbour into 0xFFFFFFFFFFFFFFFF in
            // some lanes and 0x00000000FFFFFFFF (= 4_294_967_295, the i64 value) in others. No
            // row satisfies the predicate at 64-bit width; pre-fix the JIT returned half the
            // SIMD-body rows.
            execute("create table neg_leaf as (select timestamp_sequence(0, 1_000_000) k," +
                    " 1::int i32, 4_294_967_295L i64, 7::int g32, 7L g64" +
                    " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)");
            assertJitMatchesJava("select count() from neg_leaf where -i32 = i64 and g32 = g64",
                    true, "count\n0\n");
            assertJitMatchesJava("neg_leaf where -i32 = i64 and g32 = g64", true,
                    "k\ti32\ti64\tg32\tg64\n");
            // Also route the shape through assertJitCountQuery: that helper compares the
            // scalar-mode and vectorized-mode counts, and a lane scramble is exactly what it
            // exists to catch, so it must observe the vectorized count rather than re-check
            // the scalar one.
            assertJitCountQuery("select count() from neg_leaf where -i32 = i64 and g32 = g64", 0);

            // Batch lengths, NULL handling (INT_NULL is Integer.MIN_VALUE, so -i32 stays NULL)
            // and every comparison operator, with both widening-sibling shapes.
            for (int rowCount = 0; rowCount <= 9; rowCount++) {
                execute("drop table if exists neg_b");
                if (rowCount == 0) {
                    execute("create table neg_b (i8 byte, i16 short, i32 int, i64 long, f32 float)");
                } else {
                    execute("create table neg_b as (select "
                            + "cast(case x when 1 then null when 2 then 127 else x - 5 end as byte) i8, "
                            + "cast(case x when 1 then null when 2 then 32767 else x - 5 end as short) i16, "
                            + "cast(case x when 1 then null when 2 then 2147483647 when 3 then -2147483647 else x - 5 end as int) i32, "
                            + "cast(case x when 1 then null when 2 then 4_294_967_295 else x - 5 end as long) i64, "
                            + "cast(case x when 1 then null else 2.5 end as float) f32 "
                            + "from long_sequence(" + rowCount + "))");
                }
                assertJitMatchesJavaOnBatchLengths("neg_b where -i32 = i64", true);
                assertJitMatchesJavaOnBatchLengths("neg_b where -i32 <> i64", true);
                assertJitMatchesJavaOnBatchLengths("neg_b where -i32 < i64", true);
                assertJitMatchesJavaOnBatchLengths("neg_b where -i32 <= i64", true);
                assertJitMatchesJavaOnBatchLengths("neg_b where -i32 > i64", true);
                assertJitMatchesJavaOnBatchLengths("neg_b where -i32 >= i64", true);
                assertJitMatchesJavaOnBatchLengths("neg_b where i64 = -i32", true);
                assertJitMatchesJavaOnBatchLengths("neg_b where -i32 = i64 and i32 < 5_000_000_000", true);
                assertJitMatchesJavaOnBatchLengths("neg_b where -i32 = i64 or i32 < 5_000_000_000", true);
                assertJitMatchesJava("select count() from neg_b where -i32 = i64 and f32 < 1.00000003", true);
                assertJitMatchesJavaOnBatchLengths("neg_b where i64 in (-i32)", true);
                // BYTE / SHORT are never wide-lane eligible; the added widening must leave
                // their correct scalar path alone.
                assertJitMatchesJavaOnBatchLengths("neg_b where -i8 = i64", true);
                assertJitMatchesJavaOnBatchLengths("neg_b where -i16 = i64", true);
            }
            assertBatchSweepReturnedRows();
        });
    }

    @Test
    public void testWideLaneUnaryMinusOperandWrapsUnderIntComparison() throws Exception {
        // The mirror of testWideLaneUnaryMinusNarrowLeaf: under an INT-width comparison the
        // unary-minus operand has to WRAP, not widen. markWidthSemantics recursed into the
        // operand directly instead of going through markWidthSemanticsOperand, which is the
        // only path that reaches i64WrapLeaves, so the predicate-global widening flag
        // sign-extended it and the product then ran at 64 bits where the Java filter wraps
        // mod 2^32 (NegInt/MulInt#getInt).
        assertMemoryLeak(() -> {
            // -(-65536) * 65536 = 4_294_967_296, which wraps to 0 in INT arithmetic, so the
            // inner comparison is true for every row and the whole predicate is true = true.
            // Pre-fix the JIT computed the product at 64 bits and returned no rows.
            execute("create table neg_wrap as (select timestamp_sequence(0, 1_000_000) k," +
                    " (-65536)::int a32, 65536::int b32, 1L i64" +
                    " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)");
            assertJitMatchesJava("select count() from neg_wrap where ((-a32 * b32) = 0) = (i64 > 0)",
                    true, "count\n" + N_SIMD_WITH_SCALAR_TAIL + "\n");
            // The binary-only spelling was always correct; both must agree.
            assertJitMatchesJava("select count() from neg_wrap where ((a32 * b32) = 0) = (i64 > 0)",
                    true, "count\n" + N_SIMD_WITH_SCALAR_TAIL + "\n");

            for (int rowCount = 0; rowCount <= 9; rowCount++) {
                execute("drop table if exists neg_wrap_b");
                if (rowCount == 0) {
                    execute("create table neg_wrap_b (a32 int, b32 int, i64 long)");
                } else {
                    execute("create table neg_wrap_b as (select "
                            + "cast(case x when 1 then null when 2 then -65536 else x end as int) a32, "
                            + "cast(case x when 1 then null when 2 then 65536 else x + 1 end as int) b32, "
                            + "cast(case x when 1 then null else 1 end as long) i64 "
                            + "from long_sequence(" + rowCount + "))");
                }
                assertJitMatchesJavaOnBatchLengths("neg_wrap_b where ((-a32 * b32) = 0) = (i64 > 0)", true);
                assertJitMatchesJavaOnBatchLengths("neg_wrap_b where ((-a32 + b32) = 0) = (i64 > 0)", true);
                assertJitMatchesJavaOnBatchLengths("neg_wrap_b where ((-a32 * b32) > 0) = (i64 > 0)", true);
                // A genuine long-width comparison over the same shape must still widen.
                assertJitMatchesJavaOnBatchLengths("neg_wrap_b where (-a32 * b32) = i64", true);
                assertJitMatchesJavaOnBatchLengths("neg_wrap_b where -a32 * b32 = 4_294_967_296", true);
            }
            assertBatchSweepReturnedRows();
        });
    }

    @Test
    public void testIntColumnsCount() throws Exception {
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
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
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
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
    public void testFloatColumnVsConstantWithNoExactFloat() throws Exception {
        // A FLOAT column always compares at DOUBLE width in the Java filter: there is no
        // (FLOAT, FLOAT) comparison factory, only the double ones ("<(DD)"), so both operands
        // promote. The JIT typed the constant down to F4 (the observer sees one 4-byte column, so
        // no mixed size) and serializeNumber rounded it to the NEAREST float, comparing at float
        // width - which selects different rows in either direction:
        //   RED on HEAD: (a) "< 1.00000003" rounds the bound DOWN to 1.0f and drops the row
        //   holding 1.0f that Java keeps; (b) "> 0.99999998" rounds it UP to 1.0f and drops the
        //   same row; (c) "= 1.00000003" rounds it to 1.0f and MATCHES that row - returning a row
        //   whose value is provably not the one asked for. markNarrowConstCmpWidenPair now sends any
        //   constant with no exact float to the filter as a full double, so it compares at double
        //   width exactly as the Java filter does (tolerance and all). Those predicates run scalar;
        //   a constant WITH an exact float is untouched and keeps the vectorized path.
        //
        // The table has >= 64 rows so the vectorized loop is exercised on the shapes that keep it.
        assertMemoryLeak(() -> {
            execute("create table y as (select" +
                    " cast(case when x = 1 then 1.0 when x = 2 then 16777216.0 else 5.0 end as float) f," +
                    " cast(x as int) rn," +
                    " timestamp_sequence(0, 1_000_000) k" +
                    " from long_sequence(64)) timestamp(k)");

            // (a) the bound rounds DOWN to 1.0f: row 1 satisfies 1.0 < 1.00000003 at double width.
            assertJitMatchesJava("select rn from y where f < 1.00000003", true, "rn\n1\n");
            // (b) the bound rounds UP to 1.0f: row 1 satisfies 1.0 > 0.99999998 at double width.
            //     Rows 3+ hold 5.0 and row 2 holds 2^24, so every row matches.
            Assert.assertEquals(64, runQuery("select rn from y where f > 0.99999998"));
            assertJitMatchesJava("select rn from y where f > 0.99999998 and rn <= 2", true, "rn\n1\n2\n");
            // (c) the false positive: no float equals 1.00000003, so nothing may match.
            assertJitMatchesJava("select rn from y where f = 1.00000003", true, "rn\n");
            assertJitMatchesJava("select rn from y where f <> 1.00000003 and rn <= 2", true, "rn\n1\n2\n");

            // An integer literal is no safer above 2^24: (float) 16_777_217 is 16_777_216, so the
            // bound lands exactly on row 2's value.
            assertJitMatchesJava("select rn from y where f < 16_777_217 and rn <= 2", true, "rn\n1\n2\n");
            assertJitMatchesJava("select rn from y where f >= 16_777_217", true, "rn\n");
            assertJitMatchesJava("select rn from y where f = 16_777_217", true, "rn\n");

            // A negated constant takes the same route (the literal sits under a unary minus).
            assertJitMatchesJava("select rn from y where f > -1.00000003 and rn <= 2", true, "rn\n1\n2\n");

            // IN over a FLOAT key is an OR of equalities, so it takes the equality route: no float
            // reproduces the bound, and the nearest one matched the row that rounds to it.
            assertJitMatchesJava("select rn from y where f in (1.00000003, 2.5)", true, "rn\n");
            assertJitMatchesJava("select rn from y where f not in (1.00000003, 2.5) and rn <= 2", true, "rn\n1\n2\n");
            assertJitMatchesJava("select rn from y where f in (1.00000003)", true, "rn\n");
            // ... and an exactly-representable element still matches, and still vectorizes.
            assertJitMatchesJava("select rn from y where f in (5.0, 2.5) and rn <= 4", true, "rn\n3\n4\n");

            // An ARITHMETIC float leaf reads the bound the same way a bare column does. These ops
            // are value-preserving, so the key is still row 1's 1.0f either way.
            assertJitMatchesJava("select rn from y where f + 0 < 1.00000003", true, "rn\n1\n");
            assertJitMatchesJava("select rn from y where f * 1 < 1.00000003", true, "rn\n1\n");
            assertJitMatchesJava("select rn from y where -f > -1.00000003", true, "rn\n1\n");
            assertJitMatchesJava("select rn from y where f + 0 = 1.00000003", true, "rn\n");

            // Controls: a bound WITH an exact float compares the same at either width, so it must
            // keep the vectorized path and the same rows.
            assertJitMatchesJava("select rn from y where f < 1.5 and rn <= 2", true, "rn\n1\n");
            assertJitMatchesJava("select rn from y where f = 5.0 and rn <= 4", true, "rn\n3\n4\n");
            assertJitMatchesJava("select rn from y where f > 4 and rn <= 4", true, "rn\n2\n3\n4\n");
            // An explicit widening to DOUBLE selects the same rows (the JIT declines a cast and
            // falls back to Java, so this pins the double-width answer the fix now agrees with).
            assertJitMatchesJava("select rn from y where f::double < 1.00000003", false, "rn\n1\n");

            // The comparison carries a TOLERANCE: QuestDB reads "f < d" as
            // "!Numbers.equals(f, d) && f < d" with DOUBLE_TOLERANCE = 1e-10, so a row within 1e-10
            // of the bound is EQUAL to it and "<" excludes it while ">=" keeps it. Only the double
            // comparison reproduces that. Emitting a float bound instead - even one rounded in the
            // direction the operator preserves - cannot: one float ulp near 1.0 is 1.2e-7, over a
            // thousand times the tolerance, so the bound steps clean over the band and flips these
            // rows in both directions. Every bound here sits inside the tolerance band around 1.0.
            assertJitMatchesJava("select rn from y where f < 1.00000000005", true, "rn\n");
            assertJitMatchesJava("select rn from y where f >= 1.00000000005 and rn <= 2", true, "rn\n1\n2\n");
            assertJitMatchesJava("select rn from y where f <= 0.99999999995", true, "rn\n1\n");
            assertJitMatchesJava("select rn from y where f > 0.99999999995 and rn <= 2", true, "rn\n2\n");
            assertJitMatchesJava("select rn from y where f = 1.00000000005", true, "rn\n1\n");
        });
    }

    @Test
    public void testFloatWithLongOperandVsConstantWithNoExactFloat() throws Exception {
        // A FLOAT leaf whose arithmetic has a LONG operand: QuestDB resolves "f + l" to
        // AddDoubleFunctionFactory (LONG has no FLOAT overload), so the Java filter computes the sum
        // at DOUBLE width, and so does the JIT - convert() promotes both operands to f64.
        //
        // This shape is NOT red without the production fix, and is not meant to be: a predicate over
        // a FLOAT (4-byte) and a LONG (8-byte) column has mixed sizes, so the observer already types
        // the constant F8 and serializeConstant already emits it exactly. It is here because
        // isFloatLeaf now accepts an arithmetic subtree, which brings this shape into the marking
        // path for the first time - the pin says the marking changes nothing for it.
        //
        // Row 1: f = 5e-8, l = 1, so (double) f + l = 1.0000000500000006, which is NOT < 1.00000003.
        // Row 2: f = 5.0, l = 1 -> 6.0. The bound sits between the two, and only the double-width
        // comparison places row 1 on the right side of it.
        assertMemoryLeak(() -> {
            execute("create table y as (select" +
                    " cast(case when x = 1 then 5e-8 else 5.0 end as float) f," +
                    " cast(case when x = 1 then 1 else 1 end as long) l," +
                    " cast(x as int) rn," +
                    " timestamp_sequence(0, 1_000_000) k" +
                    " from long_sequence(64)) timestamp(k)");

            assertJitMatchesJava("select rn from y where f + l < 1.00000003", true, "rn\n");
            assertJitMatchesJava("select rn from y where f + l > 1.00000003 and rn <= 2", true, "rn\n1\n2\n");
            assertJitMatchesJava("select rn from y where f + l <= 1.00000003", true, "rn\n");
            assertJitMatchesJava("select rn from y where f + l = 1.00000003", true, "rn\n");
        });
    }

    @Test
    public void testIntFloatColumnsComparison() throws Exception {
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
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
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
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
                "(select timestamp_sequence(to_timestamp('2021-11-29T10:00:00', 'yyyy-MM-ddTHH:mm:ss'), 500_000_000) as k," +
                " rnd_int() i32" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertQueryNotNull(query, ddl);
    }

    @Test
    public void testNarrowIntArithUnderLongWithFloat() throws Exception {
        // A narrow INT arithmetic subtree (c8 * -776_782) that overflows int32
        // and feeds a LONG-width multiply diverged between the JIT and the Java
        // filter when a FLOAT comparison suppressed the narrow-to-i64 widening:
        // the JIT wrapped the inner product mod 2^32 while the Java filter read
        // it at long width via MulInt#getLong. The serializer now sign-extends
        // exactly the narrow leaves under the LONG-width subtree, so the JIT
        // stays on and both paths agree.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (c0 LONG, c2 SHORT, c8 INT, c9 FLOAT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t SELECT rnd_long(-1_000_000, 1_000_000, 8), rnd_short(), " +
                    "rnd_int(-1_000_000, 1_000_000, 8), rnd_float(8), " +
                    "timestamp_sequence(to_timestamp('2024-01-01', 'yyyy-MM-dd'), 1_800_000_000L) " +
                    "FROM long_sequence(122)");

            // The rows come from rnd_*, so there is no expected-rows string to pin. Pin the Java
            // filter's row count instead: it is the independent oracle here (the fix is entirely in
            // the serializer, so the Java path never moved), and it also proves each predicate is
            // non-vacuous. Parity over an all-empty or an all-122 result would prove nothing.
            Assert.assertEquals(55, runQuery("SELECT * FROM t WHERE c9 <= ((c0 - c2) * (c8 * -776_782))"));
            Assert.assertEquals(55, runQuery("SELECT * FROM t WHERE c9 <= (c0 * (c8 * -776_782))"));
            Assert.assertEquals(61, runQuery("SELECT * FROM t WHERE c9 <= (c0 + (c8 * -776_782))"));
            Assert.assertEquals(60, runQuery("SELECT * FROM t WHERE c9 <= (c0 * (c8 + 2_000_000_000))"));
            Assert.assertEquals(66, runQuery("SELECT * FROM t WHERE c9 <= (c8 * -776_782)"));
            Assert.assertEquals(54, runQuery("SELECT * FROM t WHERE c9 <= (c0 * c8)"));
            Assert.assertEquals(67, runQuery("SELECT * FROM t WHERE (c8 * -776_782) > 0"));

            // Previously diverging shapes: still JIT-compiled, now correct.
            assertJitMatchesJava("SELECT * FROM t WHERE c9 <= ((c0 - c2) * (c8 * -776_782))", true);
            assertJitMatchesJava("SELECT * FROM t WHERE c9 <= (c0 * (c8 * -776_782))", true);
            assertJitMatchesJava("SELECT * FROM t WHERE c9 <= (c0 + (c8 * -776_782))", true);
            assertJitMatchesJava("SELECT * FROM t WHERE c9 <= (c0 * (c8 + 2_000_000_000))", true);
            // Control shapes that were always correct under JIT.
            assertJitMatchesJava("SELECT * FROM t WHERE c9 <= (c8 * -776_782)", true);
            assertJitMatchesJava("SELECT * FROM t WHERE c9 <= (c0 * c8)", true);
            assertJitMatchesJava("SELECT * FROM t WHERE (c8 * -776_782) > 0", true);
        });
    }

    @Test
    public void testNarrowIntChainOverflowUnderLong() throws Exception {
        // C2: a narrow-int (SHORT / BYTE) arithmetic CHAIN that overflows int32 was
        // computed at int32 by the JIT and wrapped, while the Java filter read it at
        // long width (MulInt#getLong recurses through getLong, so the whole chain is
        // 64-bit). NarrowI64WidenDetector.shouldWiden() required hasI4(): a narrow-only
        // product observes only I2/I1 + I8 (no I4), so the widening never fired. A
        // 2-factor narrow product stays inside int32 (32_767^2 < 2^31), but 3+ factors
        // overflow (1500^3 = 3_375_000_000 wraps to -919_967_296). shouldWiden() now
        // also triggers on hasNarrowInt(), so the JIT widens the chain to match Java.
        assertMemoryLeak(() -> {
            execute("create table nchain as (select" +
                    " x rid," +
                    " cast(case x when 1 then 1500 when 2 then 1000 when 3 then 1500 when 4 then 2000 else 1500 end as short) cs," +
                    " cast(case x when 1 then 100 when 2 then 10 when 3 then 100 when 4 then 50 else 100 end as byte) cbyte," +
                    " cast(case x when 1 then 3_375_000_000 when 2 then 1_000_000_000 when 3 then 0 when 4 then 8_000_000_000 else -919_967_296 end as long) nl," +
                    " cast(case x when 1 then 10_000_000_000 when 2 then 100_000 when 3 then 0 when 4 then 312_500_000 else 1_410_065_408 end as long) nb," +
                    " timestamp_sequence(0, 1_000_000) k" +
                    " from long_sequence(5)) timestamp(k)");
            // Primary repro (SHORT, 3-factor). The chain computes at i32 and wraps, so it equals
            // the wrapped image rows - row 5 holds -919_967_296, the int32 cube of 1500 - and never
            // the long-width cube. Row 2's 1000^3 fits int32, so it matches at either width.
            // Absolute pin: {2,5}. The JIT and Java must agree on the wrap, which is the point.
            assertJitMatchesJava("select rid from nchain where nl = cs * cs * cs", true,
                    "rid\n2\n5\n");
            // BYTE, 5-factor (127^3 still fits int32, so a byte chain needs 5 factors to
            // overflow). Row 5 holds the int32-wrapped 100^5 (1_410_065_408) and matches; rows 2
            // and 4 stay inside int32. Absolute pin: {2,4,5}.
            assertJitMatchesJava("select rid from nchain where nb = cbyte * cbyte * cbyte * cbyte * cbyte", true,
                    "rid\n2\n4\n5\n");
            // Operand order agrees, and a wider chain does too. cs^4 wraps negative for every cs
            // in this table, so '>' matches nothing while '<' matches - both engines have to say so.
            assertJitMatchesJava("nchain where cs * cs * cs = nl", true);
            assertJitMatchesJavaOnEmptyResult("nchain where cs * cs * cs * cs > nl", true);
            assertJitMatchesJava("nchain where cs * cs * cs * cs < nl", true);
            // A boolean equality mixes a LONG-peer comparison (nl = cs*cs*cs) with an INT one
            // (cs*cs*cs = 0). The chain computes at i32 in both, so the peer cannot change it.
            assertJitMatchesJava("nchain where (nl = cs * cs * cs) = (cs * cs * cs = 0)", true);
            // Controls: the same chain with no LONG operand at all, and a 2-factor narrow product
            // that never overflows int32.
            assertJitMatchesJavaOnEmptyResult("nchain where cs * cs * cs = 0", true);
            assertJitMatchesJavaOnEmptyResult("nchain where nl = cs * cs", true);
        });
    }

    @Test
    public void testNonFiniteConstantArithmeticMatchesJava() throws Exception {
        // FunctionParser folds every constant subtree bottom-up through
        // DoubleConstant#newInstance, which maps +/-Infinity and NaN onto the NULL
        // sentinel, so the Java filter compares against NULL. The serializer emitted the
        // operations instead and let the backend compute them, so the JIT compared against
        // a real +/-Infinity. double_cmp_epsilon (jit/impl/x86.h) reads both as NULL, but
        // double_lt/le/gt/ge order an infinity like an ordinary number, so every ordering
        // operator disagreed: "d <= 1e308 * 10.0" returned the NULL row alone on the Java
        // filter and EVERY row on the JIT.
        //
        // The serializer now declines such a filter, so the Java filter decides it - hence
        // expectJit is false on the diverging shapes.
        assertMemoryLeak(() -> {
            execute("create table nf as (select" +
                    " cast(x as double) d," +
                    " cast(x as float) f," +
                    " timestamp_sequence(0, 1_000_000) k" +
                    " from long_sequence(3)) timestamp(k)");
            execute("insert into nf values (null, null, '1970-01-01T00:00:03.000000Z')");

            // Multiplicative overflow. Absolute pin: the NULL row alone, because the Java
            // filter's bound is NULL and only a NULL row is tolerance-equal to it.
            assertJitMatchesJava("nf where d <= 1e308 * 10.0", false,
                    "d\tf\tk\n" +
                            "null\tnull\t1970-01-01T00:00:03.000000Z\n");
            // Strict ordering against the same bound keeps nothing at all.
            assertJitMatchesJava("nf where d < 1e308 * 10.0", false, "d\tf\tk\n");
            assertJitMatchesJava("nf where d > -1e308 * 10.0", false, "d\tf\tk\n");
            assertJitMatchesJava("nf where d >= -1e308 * 10.0", false,
                    "d\tf\tk\n" +
                            "null\tnull\t1970-01-01T00:00:03.000000Z\n");
            // Additive overflow and a unary minus over a non-finite subtree reach the same
            // fold through different operators.
            assertJitMatchesJava("nf where d <= 1e308 + 1e308", false,
                    "d\tf\tk\n" +
                            "null\tnull\t1970-01-01T00:00:03.000000Z\n");
            assertJitMatchesJava("nf where d >= -1e308 - 1e308", false,
                    "d\tf\tk\n" +
                            "null\tnull\t1970-01-01T00:00:03.000000Z\n");
            assertJitMatchesJava("nf where d >= -(1e308 * 10.0)", false,
                    "d\tf\tk\n" +
                            "null\tnull\t1970-01-01T00:00:03.000000Z\n");
            // A constant division by zero folds the same way.
            assertJitMatchesJava("nf where 1.0 / 0.0 > d", false, "d\tf\tk\n");
            // The fold must normalise at EVERY step, not just at the end: the parser turns
            // 1e308 * 10.0 into NULL before the enclosing division sees it, so the whole
            // expression is NULL to the Java filter - while raw IEEE gives a finite 0.0 and
            // the pre-fix JIT selected rows against that instead.
            assertJitMatchesJava("nf where d <= 1.0 / (1e308 * 10.0)", false,
                    "d\tf\tk\n" +
                            "null\tnull\t1970-01-01T00:00:03.000000Z\n");
            // A non-finite fold anywhere declines the whole filter, not just its conjunct.
            assertJitMatchesJava("nf where d <= 1e308 * 10.0 and d > -100.0", false, "d\tf\tk\n");

            // Leaf shapes the type classifiers and the numeric parsers disagree about. The guard
            // is fail-closed on subtree SHAPE, so these decline too. Underscore separators are the
            // style CLAUDE.md mandates and arithExprType reads them through Numbers.parseInt,
            // while Numbers.parseDouble rejects them; 'd'/'D' suffixes are accepted by
            // FunctionParser.createConstant but unknown to floatConstantTypeCode. Both used to
            // slip past the guard and return every row on the JIT.
            assertJitMatchesJava("nf where d <= 1_000_000_000 * 1e300", false,
                    "d\tf\tk\n" +
                            "null\tnull\t1970-01-01T00:00:03.000000Z\n");
            assertJitMatchesJava("nf where d < 1_000_000_000 * 1e300", false, "d\tf\tk\n");
            assertJitMatchesJava("nf where d >= -1_000_000_000 * 1e300", false,
                    "d\tf\tk\n" +
                            "null\tnull\t1970-01-01T00:00:03.000000Z\n");
            assertJitMatchesJava("nf where d <= 1d * 1e308 * 10.0", false,
                    "d\tf\tk\n" +
                            "null\tnull\t1970-01-01T00:00:03.000000Z\n");
            assertJitMatchesJava("nf where d <= 1D * 1e308 * 10.0", false,
                    "d\tf\tk\n" +
                            "null\tnull\t1970-01-01T00:00:03.000000Z\n");

            // Controls: a constant subtree that folds finite still compiles and still runs
            // on the JIT, at both widths and through division.
            assertJitMatchesJava("nf where d <= 1e10 * 10.0", true,
                    "d\tf\tk\n" +
                            "1.0\t1.0\t1970-01-01T00:00:00.000000Z\n" +
                            "2.0\t2.0\t1970-01-01T00:00:01.000000Z\n" +
                            "3.0\t3.0\t1970-01-01T00:00:02.000000Z\n");
            assertJitMatchesJava("nf where d > 4.0 / 2.0", true,
                    "d\tf\tk\n" +
                            "3.0\t3.0\t1970-01-01T00:00:02.000000Z\n");
            assertJitMatchesJava("nf where f <= 1e10 * 10.0", true,
                    "d\tf\tk\n" +
                            "1.0\t1.0\t1970-01-01T00:00:00.000000Z\n" +
                            "2.0\t2.0\t1970-01-01T00:00:01.000000Z\n" +
                            "3.0\t3.0\t1970-01-01T00:00:02.000000Z\n");
            // Controls: the same awkward leaf shapes folding FINITE must keep their JIT. Failing
            // closed on shape alone would have cost the JIT on ordinary filters, since the
            // underscore separator is the mandated style - parseFoldLeaf walks createConstant's
            // parser ladder so these fold rather than decline.
            assertJitMatchesJava("nf where d <= 1_000_000_000 * 1e3", true,
                    "d\tf\tk\n" +
                            "1.0\t1.0\t1970-01-01T00:00:00.000000Z\n" +
                            "2.0\t2.0\t1970-01-01T00:00:01.000000Z\n" +
                            "3.0\t3.0\t1970-01-01T00:00:02.000000Z\n");
            assertJitMatchesJava("nf where d <= 1d * 1e3", true,
                    "d\tf\tk\n" +
                            "1.0\t1.0\t1970-01-01T00:00:00.000000Z\n" +
                            "2.0\t2.0\t1970-01-01T00:00:01.000000Z\n" +
                            "3.0\t3.0\t1970-01-01T00:00:02.000000Z\n");
            // Controls: an INTEGER constant division by zero keeps its JIT. tryFoldConstantArith0
            // declines that fold deliberately so the IR carries the division and the native
            // int32_div/int64_div produce the same NULL sentinel DivInt/DivLong do. Judging it by
            // float rules would read 1 / 0 as an infinity and decline a filter that already agrees.
            assertJitMatchesJava("nf where d > 1 / 0", true, "d\tf\tk\n");
            assertJitMatchesJava("nf where d > 10 / (5 - 5)", true, "d\tf\tk\n");
            assertJitMatchesJava("nf where d > 1 / 0 + 5", true, "d\tf\tk\n");
            // Control: an integer constant fold that overflows LONG wraps on both paths and
            // must NOT be mistaken for a non-finite float fold.
            assertJitMatchesJava("nf where d > 9223372036854775807 * 2", true,
                    "d\tf\tk\n" +
                            "1.0\t1.0\t1970-01-01T00:00:00.000000Z\n" +
                            "2.0\t2.0\t1970-01-01T00:00:01.000000Z\n" +
                            "3.0\t3.0\t1970-01-01T00:00:02.000000Z\n");
        });
    }

    @Test
    public void testNotInOperatorFloat() throws Exception {
        // Tests NOT IN operator with floats
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
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
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
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
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
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
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
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
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
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
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
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
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
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
                "where i64 = 95 and i32 = 13 and i16 = 12_107";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
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
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
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
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
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
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
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
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
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
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
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
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
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
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
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
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
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
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
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
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
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
                " timestamp_sequence(172_800_000_000, 360_000_000) ts \n" +
                "from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp (ts)";
        assertQueryNotNullNoCount(query, ddl);
    }

    @Test
    public void testSymbolNull() throws Exception {
        final String query = "x where sym <> null";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " rnd_symbol(10,1,3,5) sym" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertQueryNullable(query, ddl);
    }

    @Test
    public void testTimestampComparison() throws Exception {
        final String query = "x where t1 != t2";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
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
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " rnd_timestamp(to_timestamp('2020', 'yyyy'), to_timestamp('2021', 'yyyy'), 5) t" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertQueryNullable(query, ddl);
    }

    @Test
    public void testUnaryMinusNarrowLeafVsOutOfRangeConstantMatchesJava() throws Exception {
        // markNarrowConstCmpWidenPair only recognises a bare LITERAL / BIND_VARIABLE as a narrow-int
        // leaf, so a unary-minus-wrapped column slipped past it, and the fallback
        // maybeWidenCmpConstOperand was gated on isFloatActive and never ran. serializeNumber then
        // emitted 2147483649 as a lossy F4 immediate. The scalar and vectorized backends disagreed
        // with EACH OTHER on the OR shape, so the same query on the same data returned different
        // rows depending on whether the host has AVX2.
        assertMemoryLeak(() -> {
            execute("create table n as (select" +
                    " cast(-2147483647 as int) i," +
                    " false b," +
                    " timestamp_sequence(0, 1_000_000) k" +
                    " from long_sequence(64)) timestamp(k)");

            final String rows = "i\n" + "-2147483647\n".repeat(64);
            assertJitScalarAndVectorMatchJava("select i from n where -i < 2147483649", rows);
            assertJitScalarAndVectorMatchJava("select i from n where -i < 2147483649 or b", rows);
            // the IN spelling already agreed; kept so the pair cannot drift apart again
            assertJitScalarAndVectorMatchJava("select i from n where -i in (2147483649)", "i\n");
        });
    }

    @Test
    public void testUuidConstantComparison() throws Exception {
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
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
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
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
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
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
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
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
                " timestamp_sequence(400_000_000_000, 500_000_000) as k," +
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
                " timestamp_sequence(400_000_000_000, 500_000_000) as k," +
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
     * <p>
     * Parity is only an oracle for a bug that moves ONE of the two paths. A test
     * covering a width decision the Java filter and the JIT now share must also
     * pin the absolute result - via the {@code expected} overload, or a
     * {@link #runQuery} row-count assertion when the rows come from {@code rnd_*}
     * and no expected string can be written. Otherwise a wrong shared decision
     * agrees on both paths and the assertion passes.
     */
    private void assertJitMatchesJava(CharSequence query, boolean expectJit) throws SqlException {
        assertJitMatchesJava(query, expectJit, null, false);
    }

    /**
     * Companion to {@link #assertJitMatchesJava(CharSequence, boolean)} for the shapes whose CORRECT
     * answer is no rows - a constant fold that collapses onto the NULL sentinel, or a widened
     * constant that matches nothing. Parity alone cannot vouch for those, so this PINS the empty
     * result rather than merely tolerating it: a fixture that silently starts matching, or a bug
     * both engines share that starts returning rows, has to redden a test somewhere.
     */
    private void assertJitMatchesJavaOnEmptyResult(CharSequence query, boolean expectJit) throws SqlException {
        final int rows = assertJitMatchesJava(query, expectJit, null, true);
        Assert.assertEquals("query is expected to return no rows: " + query, 0, rows);
    }

    /**
     * Variant for the batch-length sweeps, which run one query over row counts 0..9 so that the
     * SIMD body, its scalar tail and the empty table are all covered. Emptiness there is a property
     * of the ITERATION, not of the query, so neither the non-empty guard nor the empty-result
     * assertion fits a single call. The sweep instead accumulates what its iterations returned and
     * {@link #assertBatchSweepReturnedRows} checks at the end that the shape matched something -
     * otherwise a fixture that stopped matching would leave every iteration trivially in parity.
     */
    private void assertJitMatchesJavaOnBatchLengths(CharSequence query, boolean expectJit) throws SqlException {
        batchSweepRows += assertJitMatchesJava(query, expectJit, null, true);
    }

    private void assertBatchSweepReturnedRows() {
        Assert.assertTrue("no iteration of the batch-length sweep returned rows", batchSweepRows > 0);
        batchSweepRows = 0;
    }

    // Runs the query with JIT off then on, asserts the two agree and that JIT usage matches
    // expectJit. When expected is non-null it also pins the absolute result: parity alone would
    // pass even if BOTH paths shared the same bug, so the expected rows lock the actual result
    // the divergence was about.
    private void assertJitMatchesJava(CharSequence query, boolean expectJit, CharSequence expected) throws SqlException {
        assertJitMatchesJava(query, expectJit, expected, true);
    }

    private int assertJitMatchesJava(CharSequence query, boolean expectJit, CharSequence expected, boolean isEmptyAllowed) throws SqlException {
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
        if (expected != null) {
            TestUtils.assertEquals("absolute result mismatch for query: " + query, expected, javaSink);
        } else if (!isEmptyAllowed) {
            // Parity on its own is not an oracle: a query returning nothing on BOTH engines agrees
            // trivially, so such a site pins nothing at all. Demand rows, as assertQuery does. The
            // expected-result form pins its own answer and may legitimately be empty; a site whose
            // correct answer IS no rows says so through assertJitMatchesJavaOnEmptyResult.
            Assert.assertTrue("query is expected to return rows: " + query, countPrintedRows(javaSink) > 0);
        }
        return countPrintedRows(javaSink);
    }

    // Runs the query with JIT off, then in FORCE_SCALAR mode, then vectorized, and asserts all
    // three agree with the expected rows. assertJitMatchesJava exercises the vectorized backend
    // only, so a divergence living in the scalar backend (jit/impl/x86.h) rather than the
    // four-lane one (jit/impl/avx2.h) would pass it unnoticed.
    private void assertJitScalarAndVectorMatchJava(CharSequence query, CharSequence expected) throws SqlException {
        final int callerJitMode = sqlExecutionContext.getJitMode();
        try {
            StringSink javaSink = new StringSink();
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
            try (RecordCursorFactory factory = select(query)) {
                Assert.assertFalse("JIT was enabled for query: " + query, factory.usesCompiledFilter());
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    CursorPrinter.println(cursor, factory.getMetadata(), javaSink);
                }
            }
            TestUtils.assertEquals("absolute result mismatch for query: " + query, expected, javaSink);

            final int[] jitModes = {SqlJitMode.JIT_MODE_FORCE_SCALAR, SqlJitMode.JIT_MODE_ENABLED};
            for (int i = 0; i < jitModes.length; i++) {
                final StringSink jit = new StringSink();
                sqlExecutionContext.setJitMode(jitModes[i]);
                try (RecordCursorFactory factory = select(query)) {
                    Assert.assertTrue("JIT was not enabled for query: " + query, factory.usesCompiledFilter());
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        CursorPrinter.println(cursor, factory.getMetadata(), jit);
                    }
                }
                TestUtils.assertEquals(
                        "JIT vs Java result mismatch [scalarMode=" + (i == 0) + "] for query: " + query,
                        javaSink,
                        jit
                );
            }
        } finally {
            sqlExecutionContext.setJitMode(callerJitMode);
        }
    }

    private void assertJitCountQuery(CharSequence countQuery, long expectedCount) throws SqlException {
        sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_FORCE_SCALAR);
        long actualCount = runJitCountQuery(countQuery);
        Assert.assertEquals("[scalar mode] count mismatch for query: " + countQuery, expectedCount, actualCount);

        sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
        long vectorCount = runJitCountQuery(countQuery);
        Assert.assertEquals("[vectorized mode] count mismatch for query: " + countQuery, expectedCount, vectorCount);
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

    // Rows in a CursorPrinter.println() dump, which always emits one header line first.
    private static int countPrintedRows(CharSequence printed) {
        int lines = 0;
        for (int i = 0, n = printed.length(); i < n; i++) {
            if (printed.charAt(i) == '\n') {
                lines++;
            }
        }
        return Math.max(0, lines - 1);
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
                " timestamp_sequence(172_800_000_000, 360_000_000) ts \n" +
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
