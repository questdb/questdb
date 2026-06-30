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

package io.questdb.test.griffin;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

/**
 * Pins the agreement between the constant-folded path and the column/bind path for
 * INT arithmetic that overflows. The query fuzzer's literal-vs-bind oracle surfaced a
 * divergence: a constant {@code INT * INT} product that overflowed was folded to a
 * wider LONG (2764486628), while the same expression over a column wrapped to INT
 * (-1530480668). That flipped a {@code > 2} comparison and changed a row count.
 * <p>
 * The fix keeps the overflowing arithmetic at INT static type (so both paths wrap),
 * while wider numeric/temporal casts read the widened value, preserving the overflow
 * widening introduced by PR #4824 (e.g. {@code to_utc(<seconds> * 1000000)}).
 */
public class IntArithmeticOverflowFoldingTest extends AbstractCairoTest {

    @Test
    public void testComparisonAgreesBetweenConstantAndColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x INT)");
            execute("INSERT INTO t VALUES (839759)");

            // plain INT projection wraps mod 2^32 on both paths
            assertQuery("SELECT 839759::INT * 330972L::SHORT AS v").expectSize().returns("v\n-1530480668\n");
            assertQuery("SELECT x::INT * 330972L::SHORT AS v FROM t").expectSize().returns("v\n-1530480668\n");

            // the offending comparison: false on both paths (was true for the folded LONG)
            assertQuery("SELECT (839759::INT * 330972L::SHORT) > 2 AS v").expectSize().returns("v\nfalse\n");
            assertQuery("SELECT (x::INT * 330972L::SHORT) > 2 AS v FROM t").expectSize().returns("v\nfalse\n");
        });
    }

    @Test
    public void testWiderCastsWidenOnBothPaths() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE u (y INT)");
            execute("INSERT INTO u VALUES (2147483647)");

            // LONG / DOUBLE / DATE targets hold the un-wrapped value, constant and column alike
            assertQuery("SELECT (2147483647 + 3)::LONG AS v").expectSize().returns("v\n2147483650\n");
            assertQuery("SELECT (y + 3)::LONG AS v FROM u").expectSize().returns("v\n2147483650\n");

            assertQuery("SELECT (2147483647 + 3)::DOUBLE AS v").expectSize().returns("v\n2.14748365E9\n");
            assertQuery("SELECT (y + 3)::DOUBLE AS v FROM u").expectSize().returns("v\n2.14748365E9\n");

            assertQuery("SELECT (2147483647 + 3)::DATE AS v").expectSize().returns("v\n1970-01-25T20:31:23.650Z\n");
            assertQuery("SELECT (y + 3)::DATE AS v FROM u").expectSize().returns("v\n1970-01-25T20:31:23.650Z\n");

            // FLOAT widens too: the wrapped INT (-2147483646) would print negative; the widened
            // value (2147483650) rounds to the nearest float, +2.14748365E9.
            assertQuery("SELECT (2147483647 + 3)::FLOAT AS v").expectSize().returns("v\n2.14748365E9\n");
            assertQuery("SELECT (y + 3)::FLOAT AS v FROM u").expectSize().returns("v\n2.14748365E9\n");
        });
    }

    @Test
    public void testWiderCastsWidenWhenProductWrapsToIntNull() throws Exception {
        // Boundary case: the wrapped INT product lands EXACTLY on the INT_NULL
        // sentinel (-2^31). functionToConstant0 must not mistake that for a real
        // null. Before the fix it folded the literal to IntConstant.NULL (the
        // intConst == INT_NULL disjunct fired before the leave-unfolded branch),
        // so a wider cast read NULL while the column path widened to the true
        // value. The fold now keys off longConst == LONG_NULL, which only a
        // genuine null has, so both paths widen alike.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE u (y INT)");
            execute("INSERT INTO u VALUES (2147483647)");

            // 2147483647 + 1 wraps to -2147483648 == INT_NULL; getLong() holds 2147483648.
            assertQuery("SELECT (2147483647 + 1)::LONG AS v").expectSize().returns("v\n2147483648\n");
            assertQuery("SELECT (y + 1)::LONG AS v FROM u").expectSize().returns("v\n2147483648\n");

            // A different product that also wraps to exactly -2^31: 65536 * 32768.
            assertQuery("SELECT (65536 * 32768)::LONG AS v").expectSize().returns("v\n2147483648\n");

            // The plain INT projection still wraps mod 2^32 on both paths.
            assertQuery("SELECT 2147483647 + 1 AS v").expectSize().returns("v\nnull\n");
            assertQuery("SELECT y + 1 AS v FROM u").expectSize().returns("v\nnull\n");
        });
    }

    @Test
    public void testImplicitDoublePromotionWrapsLikeConstantAndColumn() throws Exception {
        // (intCol + intConst) + floatConst: the constant-reassociation pass used to
        // regroup the column form into intCol + (intConst + floatConst), folding the
        // two constants to a single DOUBLE and evaluating intCol + intConst at double
        // width -- so an overflowing INT addition widened instead of wrapping. The
        // literal form folds the inner INT arithmetic first (wrapping) and never
        // regroups, so the two diverged. The reassociation now leaves an integer/
        // floating-point constant pair un-regrouped, so both paths wrap alike. An
        // explicit cast still widens, and a LONG constant still widens on both paths.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE u (y INT, a INT)");
            execute("INSERT INTO u VALUES (2147483647, 100000)");

            // implicit DOUBLE promotion wraps on both the constant and column paths
            assertQuery("SELECT (2147483647 + 3) + 0.0 AS v").expectSize().returns("v\n-2.147483646E9\n");
            assertQuery("SELECT (y + 3) + 0.0 AS v FROM u").expectSize().returns("v\n-2.147483646E9\n");

            // commutative (constant on the left) and the mirror shape agree too
            assertQuery("SELECT (3 + 2147483647) + 0.0 AS v").expectSize().returns("v\n-2.147483646E9\n");
            assertQuery("SELECT (3 + y) + 0.0 AS v FROM u").expectSize().returns("v\n-2.147483646E9\n");
            assertQuery("SELECT 0.0 + (2147483647 + 3) AS v").expectSize().returns("v\n-2.147483646E9\n");
            assertQuery("SELECT 0.0 + (y + 3) AS v FROM u").expectSize().returns("v\n-2.147483646E9\n");

            // FLOAT promotion wraps alike (-2147483646 rounds to -2.1474836E9)
            assertQuery("SELECT (2147483647 + 3) + 0.0f AS v").expectSize().returns("v\n-2.1474836E9\n");
            assertQuery("SELECT (y + 3) + 0.0f AS v FROM u").expectSize().returns("v\n-2.1474836E9\n");

            // multiplication overflow under a DOUBLE promotion wraps alike
            assertQuery("SELECT (100000 * 100000) * 2.0 AS v").expectSize().returns("v\n2.820130816E9\n");
            assertQuery("SELECT (a * 100000) * 2.0 AS v FROM u").expectSize().returns("v\n2.820130816E9\n");

            // an explicit wider cast still widens on both paths (unchanged)
            assertQuery("SELECT (2147483647 + 3)::DOUBLE AS v").expectSize().returns("v\n2.14748365E9\n");
            assertQuery("SELECT (y + 3)::DOUBLE AS v FROM u").expectSize().returns("v\n2.14748365E9\n");

            // a LONG constant still combines and widens identically on both paths
            assertQuery("SELECT (2147483647 + 3) + 0L AS v").expectSize().returns("v\n2147483650\n");
            assertQuery("SELECT (y + 3) + 0L AS v FROM u").expectSize().returns("v\n2147483650\n");
        });
    }

    @Test
    public void testRuntimeConstDivisionWrapsLikeColumnAndLiteral() throws Exception {
        // IntRuntimeConstFunction memoizes a composite runtime-const INT subtree. For + - *
        // (int) getLong() == getInt() (a modular ring homomorphism: the low 32 bits of the
        // widened result equal the wrapped result), but division breaks it: getInt() divides
        // the per-op-wrapped INT operands while getLong() divides the full-width ones. Here
        // (1000000 * 1000000) wraps to -727379968 at INT width, so the INT division is
        // -727379968 / 7 == -103911424, while the LONG division is 10^12 / 7 == 142857142857
        // (whose low 32 bits, 1123222089, are a different number). Before the fix the wrapper
        // derived the INT value from getLong() and served 1123222089 from getInt(); it must
        // cache each getter at its own width instead.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (a INT, b INT, c INT, ai INT)");
            execute("INSERT INTO x VALUES (1000000, 1000000, 7, 0)");

            // literal and column forms read the division at INT width: -103911424
            assertQuery("SELECT ((1000000 * 1000000) / 7) + ai AS v FROM x").expectSize().returns("v\n-103911424\n");
            assertQuery("SELECT ((a * b) / c) + ai AS v FROM x").expectSize().returns("v\n-103911424\n");

            // the bind (runtime-const) form must agree, not serve the widened low 32 bits
            bindVariableService.clear();
            bindVariableService.setStr("b0", "1000000");
            bindVariableService.setStr("b1", "1000000");
            bindVariableService.setStr("b2", "7");
            assertQuery("SELECT ((:b0::INT * :b1::INT) / :b2::INT) + ai AS v FROM x").expectSize().returns("v\n-103911424\n");

            // a LONG-promoting context still reads the widened division on every path
            assertQuery("SELECT ((1000000 * 1000000) / 7)::LONG AS v FROM x").expectSize().returns("v\n142857142857\n");
            assertQuery("SELECT ((a * b) / c)::LONG AS v FROM x").expectSize().returns("v\n142857142857\n");
            bindVariableService.clear();
            bindVariableService.setStr("b0", "1000000");
            bindVariableService.setStr("b1", "1000000");
            bindVariableService.setStr("b2", "7");
            assertQuery("SELECT ((:b0::INT * :b1::INT) / :b2::INT)::LONG AS v FROM x").expectSize().returns("v\n142857142857\n");

            // NULL flows through unchanged: a null divisor yields a null quotient
            bindVariableService.clear();
            bindVariableService.setStr("b0", "1000000");
            bindVariableService.setStr("b1", "1000000");
            bindVariableService.setStr("b2", null);
            assertQuery("SELECT ((:b0::INT * :b1::INT) / :b2::INT) + ai AS v FROM x").expectSize().returns("v\nnull\n");
        });
    }

    @Test
    public void testRuntimeConstWidensLikeColumnAndLiteral() throws Exception {
        // A runtime-constant (but not compile-time-constant) overflowing INT arithmetic subtree --
        // here a string bind variable cast to INT -- gets memoized by IntRuntimeConstFunction. The
        // wrapper must preserve the dual getInt()-wraps / getLong()-widens behavior of its arg, so
        // that a LONG-promoting context over the memoized constant agrees with the literal and the
        // column forms. Before the fix the wrapper cached only the INT (wrapped) value and re-widened
        // it, so (:b0::INT * 17161::SHORT) * nL wrapped to +1438038338 first and the product diverged
        // from the literal/column result by an exact multiple of 2^32. The query fuzzer's literal-vs-
        // bind oracle surfaced this through an nth_value() window projection.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (nL LONG, a INT)");
            execute("INSERT INTO x VALUES (6, -166478)");

            // (-166478 * 17161) overflows INT: getInt() wraps to +1438038338, getLong() widens to
            // -2856928958; the outer LONG multiply must see the widened value, so * 6 == -17141573748.
            assertQuery("SELECT (-166478 * 17161::SHORT) * nL AS v FROM x").expectSize().returns("v\n-17141573748\n");
            assertQuery("SELECT (a * 17161::SHORT) * nL AS v FROM x").expectSize().returns("v\n-17141573748\n");
            bindVariableService.clear();
            bindVariableService.setStr("b0", "-166478");
            assertQuery("SELECT (:b0::INT * 17161::SHORT) * nL AS v FROM x").expectSize().returns("v\n-17141573748\n");

            // A direct ::LONG cast of the memoized constant widens identically on both paths.
            assertQuery("SELECT (a * 17161::SHORT)::LONG AS v FROM x").expectSize().returns("v\n-2856928958\n");
            bindVariableService.clear();
            bindVariableService.setStr("b0", "-166478");
            assertQuery("SELECT (:b0::INT * 17161::SHORT)::LONG AS v FROM x").expectSize().returns("v\n-2856928958\n");

            // NULL flows through the wrapper unchanged: a null operand yields a null product.
            bindVariableService.clear();
            bindVariableService.setStr("b0", null);
            assertQuery("SELECT (:b0::INT * 17161::SHORT) * nL AS v FROM x").expectSize().returns("v\nnull\n");
        });
    }

    @Test
    public void testTimezoneConversionWidensOnBothPaths() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE s (sec INT)");
            execute("INSERT INTO s VALUES (1720468802)");

            // PR #4824 use case: seconds * 1_000_000 must not overflow into the timestamp
            assertQuery("SELECT to_utc(1720468802 * 1000000, 'Europe/Berlin') AS v")
                    .expectSize().returns("v\n2024-07-08T18:00:02.000000Z\n");
            assertQuery("SELECT to_utc(sec * 1000000, 'Europe/Berlin') AS v FROM s")
                    .expectSize().returns("v\n2024-07-08T18:00:02.000000Z\n");
        });
    }
}
