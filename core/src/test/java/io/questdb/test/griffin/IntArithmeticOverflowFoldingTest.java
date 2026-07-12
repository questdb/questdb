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
            assertQuery("SELECT 839759::INT * 330972L::SHORT AS v").noLeakCheck().expectSize().returns("v\n-1530480668\n");
            assertQuery("SELECT x::INT * 330972L::SHORT AS v FROM t").noLeakCheck().expectSize().returns("v\n-1530480668\n");

            // the offending comparison: false on both paths (was true for the folded LONG)
            assertQuery("SELECT (839759::INT * 330972L::SHORT) > 2 AS v").noLeakCheck().expectSize().returns("v\nfalse\n");
            assertQuery("SELECT (x::INT * 330972L::SHORT) > 2 AS v FROM t").noLeakCheck().expectSize().returns("v\nfalse\n");
        });
    }

    @Test
    public void testFloatingPointPairAgreesBetweenConstantAndColumn() throws Exception {
        // A floating-point constant pair used to regroup: (dblCol * 1e300) * 1e-300 became
        // dblCol * (1e300 * 1e-300) = dblCol * 1.0. IEEE-754 * is not associative - the
        // un-regrouped form overflows to Infinity at the intermediate and stays there - and
        // reassociateConstants never regroups the all-literal form, so only the column form
        // moved and the two diverged. Neither form regroups now, so both overflow alike.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE u (d DOUBLE, f FLOAT)");
            execute("INSERT INTO u VALUES (1e10, 1e10)");

            // multiplication: the intermediate overflows to Infinity, which prints as null
            assertQuery("SELECT 1e10 * 1e300 * 1e-300 AS v").noLeakCheck().expectSize().returns("v\nnull\n");
            assertQuery("SELECT d * 1e300 * 1e-300 AS v FROM u").noLeakCheck().expectSize().returns("v\nnull\n");

            // addition: 1e16 + 1.0 rounds back to 1e16 (ulp is 2), so the left-associative form
            // yields 1.0000000000000002E16, while regrouping to d + 3.0 would yield ...04E16
            assertQuery("SELECT 1e16 + 1.0 + 2.0 AS v").noLeakCheck().expectSize().returns("v\n1.0000000000000002E16\n");
            assertQuery("SELECT (d * 1e6) + 1.0 + 2.0 AS v FROM u").noLeakCheck().expectSize().returns("v\n1.0000000000000002E16\n");

            // a FLOAT pair diverges the same way: 1e10f * 1e30f overflows the float range
            assertQuery("SELECT 1e10f * 1e30f * 1e-30f AS v").noLeakCheck().expectSize().returns("v\nnull\n");
            assertQuery("SELECT f * 1e30f * 1e-30f AS v FROM u").noLeakCheck().expectSize().returns("v\nnull\n");
        });
    }

    @Test
    public void testImplicitDoublePromotionWrapsLikeConstantAndColumn() throws Exception {
        // (intCol + intConst) + floatConst: the constant-reassociation pass used to
        // regroup the column form into intCol + (intConst + floatConst), folding the
        // two constants to a single DOUBLE and evaluating intCol + intConst at double
        // width - so an overflowing INT addition widened instead of wrapping. The
        // literal form folds the inner INT arithmetic first (wrapping) and never
        // regroups, so the two diverged. The reassociation now leaves an integer/
        // floating-point constant pair un-regrouped, so both paths wrap alike. An
        // explicit ::LONG cast (and a LONG constant) still widens on both paths, while
        // an explicit ::DOUBLE cast wraps like the implicit promotion it must agree with.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE u (y INT, a INT)");
            execute("INSERT INTO u VALUES (2147483647, 100000)");

            // implicit DOUBLE promotion wraps on both the constant and column paths
            assertQuery("SELECT (2147483647 + 3) + 0.0 AS v").noLeakCheck().expectSize().returns("v\n-2.147483646E9\n");
            assertQuery("SELECT (y + 3) + 0.0 AS v FROM u").noLeakCheck().expectSize().returns("v\n-2.147483646E9\n");

            // commutative (constant on the left) and the mirror shape agree too
            assertQuery("SELECT (3 + 2147483647) + 0.0 AS v").noLeakCheck().expectSize().returns("v\n-2.147483646E9\n");
            assertQuery("SELECT (3 + y) + 0.0 AS v FROM u").noLeakCheck().expectSize().returns("v\n-2.147483646E9\n");
            assertQuery("SELECT 0.0 + (2147483647 + 3) AS v").noLeakCheck().expectSize().returns("v\n-2.147483646E9\n");
            assertQuery("SELECT 0.0 + (y + 3) AS v FROM u").noLeakCheck().expectSize().returns("v\n-2.147483646E9\n");

            // FLOAT promotion wraps alike (-2147483646 rounds to -2.1474836E9)
            assertQuery("SELECT (2147483647 + 3) + 0.0f AS v").noLeakCheck().expectSize().returns("v\n-2.1474836E9\n");
            assertQuery("SELECT (y + 3) + 0.0f AS v FROM u").noLeakCheck().expectSize().returns("v\n-2.1474836E9\n");

            // multiplication overflow under a DOUBLE promotion wraps alike
            assertQuery("SELECT (100000 * 100000) * 2.0 AS v").noLeakCheck().expectSize().returns("v\n2.820130816E9\n");
            assertQuery("SELECT (a * 100000) * 2.0 AS v FROM u").noLeakCheck().expectSize().returns("v\n2.820130816E9\n");

            // an explicit ::LONG cast widens on both paths, matching an implicit LONG read
            assertQuery("SELECT (2147483647 + 3)::LONG AS v").noLeakCheck().expectSize().returns("v\n2147483650\n");
            assertQuery("SELECT (y + 3)::LONG AS v FROM u").noLeakCheck().expectSize().returns("v\n2147483650\n");

            // an explicit ::DOUBLE cast wraps, matching the implicit DOUBLE promotion above
            assertQuery("SELECT (2147483647 + 3)::DOUBLE AS v").noLeakCheck().expectSize().returns("v\n-2.147483646E9\n");
            assertQuery("SELECT (y + 3)::DOUBLE AS v FROM u").noLeakCheck().expectSize().returns("v\n-2.147483646E9\n");

            // a LONG constant still combines and widens identically on both paths
            assertQuery("SELECT (2147483647 + 3) + 0L AS v").noLeakCheck().expectSize().returns("v\n2147483650\n");
            assertQuery("SELECT (y + 3) + 0L AS v FROM u").noLeakCheck().expectSize().returns("v\n2147483650\n");
        });
    }

    @Test
    public void testReassociationDivModPairWrappingToIntNullWrapsLikeColumnAndLiteral() throws Exception {
        // (intCol + C1) + (C2 / C3) - the inner constant element uses '/' (DivInt) or
        // '%' (RemInt). Both are INT-typed and propagate INT_NULL exactly like + - *, so a
        // regrouped pair (C1 + (C2 op C3)) can land on the INT_NULL sentinel and poison the
        // column to NULL. reassociateConstants modeled only + - * & | ^ when folding the
        // pair, so a '/' or '%' element made the fold bail and the guard missed the poison.
        // The fold now covers '/' and '%' (zero divisor -> INT_NULL), so the pair stays
        // un-regrouped and both paths keep the real wrapped value.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x INT)");
            execute("INSERT INTO t VALUES (2)");

            // division: 2147483647 + (2 / 2) = 2147483647 + 1 wraps to -2^31 == INT_NULL
            assertQuery("SELECT (2 + 2147483647) + (2 / 2) AS v").noLeakCheck().expectSize().returns("v\n-2147483646\n");
            assertQuery("SELECT (x + 2147483647) + (2 / 2) AS v FROM t").noLeakCheck().expectSize().returns("v\n-2147483646\n");

            // modulo: the regrouped pair 2147483647 + (7 % 2) = 2147483647 + 1 wraps to
            // -2^31 == INT_NULL, while the left-associative x + 2147483647 = -2147483647 does
            // NOT hit the sentinel, so the un-regrouped form keeps the real wrapped value.
            assertQuery("SELECT (2 + 2147483647) + (7 % 2) AS v").noLeakCheck().expectSize().returns("v\n-2147483646\n");
            assertQuery("SELECT (x + 2147483647) + (7 % 2) AS v FROM t").noLeakCheck().expectSize().returns("v\n-2147483646\n");

            // control: a '/' element pair that does NOT hit the sentinel still regroups and stays correct
            assertQuery("SELECT (2 + 1000) + (6 / 2) AS v").noLeakCheck().expectSize().returns("v\n1005\n");
            assertQuery("SELECT (x + 1000) + (6 / 2) AS v FROM t").noLeakCheck().expectSize().returns("v\n1005\n");
        });
    }

    @Test
    public void testReassociationIntDecimalMixWrapsLikeColumnAndLiteral() throws Exception {
        // (intCol + intConst) + decimalConst: the constant-reassociation pass used to regroup
        // the column form into intCol + (intConst + decimalConst), folding the two constants to
        // a single DECIMAL and evaluating intCol + intConst at DECIMAL width - so an
        // overflowing INT addition widened instead of wrapping. The literal form folds the
        // inner INT arithmetic first (wrapping) and never regroups, so the two diverged. The
        // widening guard classified only DOUBLE / FLOAT; it now recognizes DECIMAL ('m' suffix)
        // too, so both paths wrap alike. A DECIMAL-only pair still combines.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ic INT)");
            execute("INSERT INTO t VALUES (2)");

            // implicit DECIMAL promotion wraps on both the constant and column paths
            assertQuery("SELECT (2 + 2147483647) + 1.5m AS v").noLeakCheck().expectSize().returns("v\n-2147483645.5\n");
            assertQuery("SELECT (ic + 2147483647) + 1.5m AS v FROM t").noLeakCheck().expectSize().returns("v\n-2147483645.5\n");

            // commutative (constant on the left) and the mirror shape agree too
            assertQuery("SELECT (2147483647 + ic) + 1.5m AS v FROM t").noLeakCheck().expectSize().returns("v\n-2147483645.5\n");
            assertQuery("SELECT 1.5m + (ic + 2147483647) AS v FROM t").noLeakCheck().expectSize().returns("v\n-2147483645.5\n");

            // a DECIMAL-only pair (no integer mixing) still combines and stays correct
            assertQuery("SELECT (ic + 1.5m) + 2.5m AS v FROM t").noLeakCheck().expectSize().returns("v\n6.0\n");
        });
    }

    @Test
    public void testReassociationIntPairWrappingToIntNullWrapsLikeColumnAndLiteral() throws Exception {
        // (intCol op C1) op C2 where the regrouped constant pair (C1 op C2) wraps exactly
        // onto the INT_NULL sentinel (-2^31). The constant-reassociation pass used to hoist
        // that pair under the column - intCol op (C1 op C2) = intCol op INT_NULL - and
        // AddInt/MulInt then return INT_NULL for every row, so the column poisoned to NULL.
        // The fully-constant literal folds left-associatively and never regroups, keeping the
        // real wrapped value. reassociateConstants now refuses to regroup an integer pair that
        // folds to INT_NULL, so both paths agree. A pair that does not hit the sentinel still
        // regroups and is unaffected.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE u (i INT, j INT)");
            execute("INSERT INTO u VALUES (5, 2)");

            // addition: 2147483647 + 1 wraps to -2^31 == INT_NULL
            // Pattern A: (A op C1) op C2
            assertQuery("SELECT (5 + 2147483647) + 1 AS v").noLeakCheck().expectSize().returns("v\n-2147483643\n");
            assertQuery("SELECT (i + 2147483647) + 1 AS v FROM u").noLeakCheck().expectSize().returns("v\n-2147483643\n");
            // Pattern B: (C1 op A) op C2 (commutative)
            assertQuery("SELECT (2147483647 + 5) + 1 AS v").noLeakCheck().expectSize().returns("v\n-2147483643\n");
            assertQuery("SELECT (2147483647 + i) + 1 AS v FROM u").noLeakCheck().expectSize().returns("v\n-2147483643\n");
            // Mirror A: C2 op (A op C1) (commutative)
            assertQuery("SELECT 1 + (5 + 2147483647) AS v").noLeakCheck().expectSize().returns("v\n-2147483643\n");
            assertQuery("SELECT 1 + (i + 2147483647) AS v FROM u").noLeakCheck().expectSize().returns("v\n-2147483643\n");
            // Mirror B: C2 op (C1 op A)
            assertQuery("SELECT 1 + (2147483647 + 5) AS v").noLeakCheck().expectSize().returns("v\n-2147483643\n");
            assertQuery("SELECT 1 + (2147483647 + i) AS v FROM u").noLeakCheck().expectSize().returns("v\n-2147483643\n");

            // multiplication: 65536 * 32768 wraps to -2^31 == INT_NULL
            assertQuery("SELECT (2 * 65536) * 32768 AS v").noLeakCheck().expectSize().returns("v\n0\n");
            assertQuery("SELECT (j * 65536) * 32768 AS v FROM u").noLeakCheck().expectSize().returns("v\n0\n");

            // negative-constant pair: -2147483647 + -1 also folds to -2^31 == INT_NULL, but each minus
            // binds as a unary operator, so neither operand is marked constant and the pair never
            // reaches the reassociation guard. It stays un-regrouped for that reason, so both paths keep
            // the real wrapped value (-2147483643) rather than poisoning every row to NULL.
            assertQuery("SELECT (5 + -2147483647) + -1 AS v").noLeakCheck().expectSize().returns("v\n-2147483643\n");
            assertQuery("SELECT (i + -2147483647) + -1 AS v FROM u").noLeakCheck().expectSize().returns("v\n-2147483643\n");

            // control: a pair that does not fold to INT_NULL still regroups and stays correct
            assertQuery("SELECT (5 + 3) + 4 AS v").noLeakCheck().expectSize().returns("v\n12\n");
            assertQuery("SELECT (i + 3) + 4 AS v FROM u").noLeakCheck().expectSize().returns("v\n12\n");
        });
    }

    @Test
    public void testReassociationLongPairWrappingToLongNullWrapsLikeColumnAndLiteral() throws Exception {
        // (longCol + C1) + C2 where the regrouped LONG constant pair (C1 + C2) wraps exactly
        // onto the LONG_NULL sentinel (-2^63). The INT-width fold rejects LONG-range /
        // L-suffixed literals (parseInt throws), so integerPairFoldsToNull never saw a LONG pair
        // and the poison slipped through: the column read LONG_NULL while the left-associative literal
        // kept the real wrapped value. The guard now also folds at LONG width and blocks a pair
        // that lands on LONG_NULL, so both paths agree. (This shape diverged before this PR too;
        // it is the LONG-width sibling of the INT_NULL guard, fixed in the same place.)
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (lc LONG)");
            execute("INSERT INTO t VALUES (5)");

            // 9223372036854775807 + 1 wraps to -2^63 == LONG_NULL
            assertQuery("SELECT (5 + 9223372036854775807) + 1 AS v").noLeakCheck().expectSize().returns("v\n-9223372036854775803\n");
            assertQuery("SELECT (lc + 9223372036854775807) + 1 AS v FROM t").noLeakCheck().expectSize().returns("v\n-9223372036854775803\n");

            // control: a LONG pair that does NOT hit the sentinel still regroups and stays correct
            assertQuery("SELECT (5 + 9000000000000000000) + 100 AS v").noLeakCheck().expectSize().returns("v\n9000000000000000105\n");
            assertQuery("SELECT (lc + 9000000000000000000) + 100 AS v FROM t").noLeakCheck().expectSize().returns("v\n9000000000000000105\n");
        });
    }

    @Test
    public void testProductOverflowingToLongNullWrapsLikeColumn() throws Exception {
        // functionToConstant0 folded a constant INT expression to the shared NULL constant whenever
        // its getLong() read LONG_NULL. That test is not a test for nullness: getLong() computes at
        // long width, so a deep enough INT chain lands on -2^63 without being null at all.
        // 1073741824 * 8 * 1073741824 is exactly 2^63 there, while getInt() wraps it to a plain 0 -
        // the same 0 the column path produces. Folding it to NULL printed null for the literal form
        // and 0 for the column form. The fold now needs the sentinel at BOTH widths, which only a
        // genuine null carries.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, b INT, c INT)");
            execute("INSERT INTO t VALUES (1_073_741_824, 8, 1_073_741_824)");

            // (1073741824 * 8) wraps to 0 at INT width, and 0 * 1073741824 stays 0.
            assertQuery("SELECT 1_073_741_824 * 8 * 1_073_741_824 AS v").noLeakCheck().expectSize().returns("v\n0\n");
            assertQuery("SELECT a * b * c AS v FROM t").noLeakCheck().expectSize().returns("v\n0\n");

            // The wide cast reads getLong(), which is -2^63 == LONG_NULL on both paths.
            assertQuery("SELECT (1_073_741_824 * 8 * 1_073_741_824)::LONG AS v").noLeakCheck().expectSize().returns("v\nnull\n");
            assertQuery("SELECT (a * b * c)::LONG AS v FROM t").noLeakCheck().expectSize().returns("v\nnull\n");

            // A genuine NULL still folds to the NULL constant: the sentinel shows at both widths.
            assertQuery("SELECT NULL::INT * 8 AS v").noLeakCheck().expectSize().returns("v\nnull\n");
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
            assertQuery("SELECT ((1000000 * 1000000) / 7) + ai AS v FROM x").noLeakCheck().expectSize().returns("v\n-103911424\n");
            assertQuery("SELECT ((a * b) / c) + ai AS v FROM x").noLeakCheck().expectSize().returns("v\n-103911424\n");

            // the bind (runtime-const) form must agree, not serve the widened low 32 bits
            bindVariableService.clear();
            bindVariableService.setStr("b0", "1000000");
            bindVariableService.setStr("b1", "1000000");
            bindVariableService.setStr("b2", "7");
            assertQuery("SELECT ((:b0::INT * :b1::INT) / :b2::INT) + ai AS v FROM x").noLeakCheck().expectSize().returns("v\n-103911424\n");

            // a LONG-promoting context still reads the widened division on every path
            assertQuery("SELECT ((1000000 * 1000000) / 7)::LONG AS v FROM x").noLeakCheck().expectSize().returns("v\n142857142857\n");
            assertQuery("SELECT ((a * b) / c)::LONG AS v FROM x").noLeakCheck().expectSize().returns("v\n142857142857\n");
            bindVariableService.clear();
            bindVariableService.setStr("b0", "1000000");
            bindVariableService.setStr("b1", "1000000");
            bindVariableService.setStr("b2", "7");
            assertQuery("SELECT ((:b0::INT * :b1::INT) / :b2::INT)::LONG AS v FROM x").noLeakCheck().expectSize().returns("v\n142857142857\n");

            // NULL flows through unchanged: a null divisor yields a null quotient
            bindVariableService.clear();
            bindVariableService.setStr("b0", "1000000");
            bindVariableService.setStr("b1", "1000000");
            bindVariableService.setStr("b2", null);
            assertQuery("SELECT ((:b0::INT * :b1::INT) / :b2::INT) + ai AS v FROM x").noLeakCheck().expectSize().returns("v\nnull\n");
        });
    }

    @Test
    public void testRuntimeConstWidensLikeColumnAndLiteral() throws Exception {
        // A runtime-constant (but not compile-time-constant) overflowing INT arithmetic subtree -
        // here a string bind variable cast to INT - gets memoized by IntRuntimeConstFunction. The
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
            assertQuery("SELECT (-166478 * 17161::SHORT) * nL AS v FROM x").noLeakCheck().expectSize().returns("v\n-17141573748\n");
            assertQuery("SELECT (a * 17161::SHORT) * nL AS v FROM x").noLeakCheck().expectSize().returns("v\n-17141573748\n");
            bindVariableService.clear();
            bindVariableService.setStr("b0", "-166478");
            assertQuery("SELECT (:b0::INT * 17161::SHORT) * nL AS v FROM x").noLeakCheck().expectSize().returns("v\n-17141573748\n");

            // A direct ::LONG cast of the memoized constant widens identically on both paths.
            assertQuery("SELECT (a * 17161::SHORT)::LONG AS v FROM x").noLeakCheck().expectSize().returns("v\n-2856928958\n");
            bindVariableService.clear();
            bindVariableService.setStr("b0", "-166478");
            assertQuery("SELECT (:b0::INT * 17161::SHORT)::LONG AS v FROM x").noLeakCheck().expectSize().returns("v\n-2856928958\n");

            // NULL flows through the wrapper unchanged: a null operand yields a null product.
            bindVariableService.clear();
            bindVariableService.setStr("b0", null);
            assertQuery("SELECT (:b0::INT * 17161::SHORT) * nL AS v FROM x").noLeakCheck().expectSize().returns("v\nnull\n");
        });
    }

    @Test
    public void testTimezoneConversionWidensOnBothPaths() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE s (sec INT)");
            execute("INSERT INTO s VALUES (1720468802)");

            // PR #4824 use case: seconds * 1_000_000 must not overflow into the timestamp
            assertQuery("SELECT to_utc(1720468802 * 1000000, 'Europe/Berlin') AS v")
                    .noLeakCheck().expectSize().returns("v\n2024-07-08T18:00:02.000000Z\n");
            assertQuery("SELECT to_utc(sec * 1000000, 'Europe/Berlin') AS v FROM s")
                    .noLeakCheck().expectSize().returns("v\n2024-07-08T18:00:02.000000Z\n");
        });
    }

    @Test
    public void testWiderCastsAgreeWhenValueIsExactlyIntNull() throws Exception {
        // Sibling of testWiderCastsWidenWhenProductWrapsToIntNull: there getInt()
        // wraps to -2^31 while getLong() holds a different value; here the genuine
        // full-width value IS exactly -2^31, so getInt() == getLong() == INT_NULL.
        // Before the fix the intConst == longConst disjunct folded the literal to
        // IntConstant.NULL, so a ::LONG cast read LONG_NULL while the column path
        // widened to -2147483648. The fold now folds only when both getters agree
        // AND the value is not the sentinel, so the literal and column paths agree
        // for every cast target.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE u (y INT, m INT, p INT, q INT)");
            // y=2147483647 for ~y; m=-1073741824 for m*2; p&q = 0x80000001 & 0xFFFFFFFE = -2^31
            execute("INSERT INTO u VALUES (2147483647, -1073741824, -2147483647, -2)");

            // ~2147483647 = -2147483648 exactly: full 5-cast matrix, literal vs column
            assertQuery("SELECT (~2147483647)::LONG AS v").noLeakCheck().expectSize().returns("v\n-2147483648\n");
            assertQuery("SELECT (~y)::LONG AS v FROM u").noLeakCheck().expectSize().returns("v\n-2147483648\n");

            assertQuery("SELECT (~2147483647)::TIMESTAMP AS v").noLeakCheck().expectSize().returns("v\n1969-12-31T23:24:12.516352Z\n");
            assertQuery("SELECT (~y)::TIMESTAMP AS v FROM u").noLeakCheck().expectSize().returns("v\n1969-12-31T23:24:12.516352Z\n");

            // ::DOUBLE / ::FLOAT / ::DATE read getInt(), which returns the value itself - and
            // that value IS the INT_NULL sentinel, so they read it as NULL. They agree with the
            // plain INT projection below and with an implicit DOUBLE promotion, which is the
            // point: a cast never disagrees with the implicit read of the same expression.
            assertQuery("SELECT (~2147483647)::DOUBLE AS v").noLeakCheck().expectSize().returns("v\nnull\n");
            assertQuery("SELECT (~y)::DOUBLE AS v FROM u").noLeakCheck().expectSize().returns("v\nnull\n");
            assertQuery("SELECT (~y) + 0.0 AS v FROM u").noLeakCheck().expectSize().returns("v\nnull\n");

            assertQuery("SELECT CAST(~2147483647 AS FLOAT) AS v").noLeakCheck().expectSize().returns("v\nnull\n");
            assertQuery("SELECT CAST(~y AS FLOAT) AS v FROM u").noLeakCheck().expectSize().returns("v\nnull\n");

            assertQuery("SELECT (~2147483647)::DATE AS v").noLeakCheck().expectSize().returns("v\n\n");
            assertQuery("SELECT (~y)::DATE AS v FROM u").noLeakCheck().expectSize().returns("v\n\n");

            // -1073741824 * 2 = -2147483648 exactly (no overflow, exact product)
            assertQuery("SELECT (-1073741824 * 2)::LONG AS v").noLeakCheck().expectSize().returns("v\n-2147483648\n");
            assertQuery("SELECT (m * 2)::LONG AS v FROM u").noLeakCheck().expectSize().returns("v\n-2147483648\n");
            assertQuery("SELECT (-1073741824 * 2)::DOUBLE AS v").noLeakCheck().expectSize().returns("v\nnull\n");
            assertQuery("SELECT (m * 2)::DOUBLE AS v FROM u").noLeakCheck().expectSize().returns("v\nnull\n");

            // bitwise AND landing exactly on 0x80000000: -2147483647 & -2 = -2147483648
            assertQuery("SELECT (-2147483647 & -2)::LONG AS v").noLeakCheck().expectSize().returns("v\n-2147483648\n");
            assertQuery("SELECT (p & q)::LONG AS v FROM u").noLeakCheck().expectSize().returns("v\n-2147483648\n");
            assertQuery("SELECT (-2147483647 & -2)::TIMESTAMP AS v").noLeakCheck().expectSize().returns("v\n1969-12-31T23:24:12.516352Z\n");
            assertQuery("SELECT (p & q)::TIMESTAMP AS v FROM u").noLeakCheck().expectSize().returns("v\n1969-12-31T23:24:12.516352Z\n");

            // control: the plain INT projection displays as null on both paths (getInt() == INT_NULL)
            assertQuery("SELECT ~2147483647 AS v").noLeakCheck().expectSize().returns("v\nnull\n");
            assertQuery("SELECT ~y AS v FROM u").noLeakCheck().expectSize().returns("v\nnull\n");
            assertQuery("SELECT -1073741824 * 2 AS v").noLeakCheck().expectSize().returns("v\nnull\n");
            assertQuery("SELECT m * 2 AS v FROM u").noLeakCheck().expectSize().returns("v\nnull\n");
            assertQuery("SELECT -2147483647 & -2 AS v").noLeakCheck().expectSize().returns("v\nnull\n");
            assertQuery("SELECT p & q AS v FROM u").noLeakCheck().expectSize().returns("v\nnull\n");
        });
    }

    @Test
    public void testWiderCastsAgreeWithImplicitReadsOnBothPaths() throws Exception {
        // Each INT cast reads the same getter its IntFunction counterpart reads, so an
        // explicit cast never disagrees with an implicit read of the same expression:
        // getLong() / getTimestamp() widen, getDouble() / getFloat() / getDate() wrap.
        // A cast that widened where the implicit read wraps would make (y * z)::DOUBLE
        // and (y * z) + 0.0 - and round(), and ::DECIMAL - return different values for
        // the same overflowing product.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE u (y INT, z INT)");
            execute("INSERT INTO u VALUES (1000000, 1000000)");

            // 1000000 * 1000000 = 10^12, which wraps to -727379968 at INT width
            assertQuery("SELECT 1000000 * 1000000 AS v").noLeakCheck().expectSize().returns("v\n-727379968\n");
            assertQuery("SELECT y * z AS v FROM u").noLeakCheck().expectSize().returns("v\n-727379968\n");

            // ::DOUBLE wraps, and agrees with the implicit DOUBLE reads (+ 0.0, round())
            assertQuery("SELECT (1000000 * 1000000)::DOUBLE AS v").noLeakCheck().expectSize().returns("v\n-7.27379968E8\n");
            assertQuery("SELECT (y * z)::DOUBLE AS v FROM u").noLeakCheck().expectSize().returns("v\n-7.27379968E8\n");
            assertQuery("SELECT (y * z)::DOUBLE = (y * z) + 0.0 AS v FROM u").noLeakCheck().expectSize().returns("v\ntrue\n");
            assertQuery("SELECT (y * z)::DOUBLE = round(y * z, 0) AS v FROM u").noLeakCheck().expectSize().returns("v\ntrue\n");

            // ::DECIMAL keeps INT width too, so ::DOUBLE must not disagree with it
            assertQuery("SELECT (y * z)::DECIMAL(20,0)::DOUBLE = (y * z)::DOUBLE AS v FROM u").noLeakCheck().expectSize().returns("v\ntrue\n");

            // the FLOAT cast wraps, and agrees with the implicit FLOAT read. CAST(x AS FLOAT),
            // not x::FLOAT: the :: form resolves FLOAT to DOUBLE and would not reach
            // CastIntToFloatFunctionFactory at all.
            assertQuery("SELECT CAST(1000000 * 1000000 AS FLOAT) AS v").noLeakCheck().expectSize().returns("v\n-7.2738E8\n");
            assertQuery("SELECT CAST(y * z AS FLOAT) AS v FROM u").noLeakCheck().expectSize().returns("v\n-7.2738E8\n");
            assertQuery("SELECT CAST(y * z AS FLOAT) = (y * z) + 0.0f AS v FROM u").noLeakCheck().expectSize().returns("v\ntrue\n");

            // ::DATE wraps: -727379968 ms before the epoch
            assertQuery("SELECT (1000000 * 1000000)::DATE AS v").noLeakCheck().expectSize().returns("v\n1969-12-23T13:57:00.032Z\n");
            assertQuery("SELECT (y * z)::DATE AS v FROM u").noLeakCheck().expectSize().returns("v\n1969-12-23T13:57:00.032Z\n");

            // ::LONG and ::TIMESTAMP widen, matching IntFunction.getLong() / getTimestamp()
            assertQuery("SELECT (1000000 * 1000000)::LONG AS v").noLeakCheck().expectSize().returns("v\n1000000000000\n");
            assertQuery("SELECT (y * z)::LONG AS v FROM u").noLeakCheck().expectSize().returns("v\n1000000000000\n");
            assertQuery("SELECT (y * z)::LONG = (y * z) + 0L AS v FROM u").noLeakCheck().expectSize().returns("v\ntrue\n");

            assertQuery("SELECT (1000000 * 1000000)::TIMESTAMP AS v").noLeakCheck().expectSize().returns("v\n1970-01-12T13:46:40.000000Z\n");
            assertQuery("SELECT (y * z)::TIMESTAMP AS v FROM u").noLeakCheck().expectSize().returns("v\n1970-01-12T13:46:40.000000Z\n");

            // a WHERE clause sees the same wrapped value through the cast as without it,
            // so a bound above the wrapped product filters both forms out alike
            assertQuery("SELECT y FROM u WHERE (y * z)::DOUBLE > 5e11").noLeakCheck().returns("y\n");
            assertQuery("SELECT y FROM u WHERE (y * z) > 5e11").noLeakCheck().returns("y\n");
        });
    }

    @Test
    public void testWiderCastsWidenOnBothPaths() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE u (y INT)");
            execute("INSERT INTO u VALUES (2147483647)");

            // LONG / TIMESTAMP targets hold the un-wrapped value, constant and column alike
            assertQuery("SELECT (2147483647 + 3)::LONG AS v").noLeakCheck().expectSize().returns("v\n2147483650\n");
            assertQuery("SELECT (y + 3)::LONG AS v FROM u").noLeakCheck().expectSize().returns("v\n2147483650\n");

            // TIMESTAMP (micros) widens directly, not only via to_utc()
            assertQuery("SELECT (2147483647 + 3)::TIMESTAMP AS v").noLeakCheck().expectSize().returns("v\n1970-01-01T00:35:47.483650Z\n");
            assertQuery("SELECT (y + 3)::TIMESTAMP AS v FROM u").noLeakCheck().expectSize().returns("v\n1970-01-01T00:35:47.483650Z\n");

            // DOUBLE / FLOAT / DATE keep INT width and wrap, on both paths alike: they must
            // agree with IntFunction.getDouble() / getFloat() / getDate(), which read getInt().
            // See testWiderCastsAgreeWithImplicitReadsOnBothPaths.
            assertQuery("SELECT (2147483647 + 3)::DOUBLE AS v").noLeakCheck().expectSize().returns("v\n-2.147483646E9\n");
            assertQuery("SELECT (y + 3)::DOUBLE AS v FROM u").noLeakCheck().expectSize().returns("v\n-2.147483646E9\n");

            assertQuery("SELECT CAST(2147483647 + 3 AS FLOAT) AS v").noLeakCheck().expectSize().returns("v\n-2.1474836E9\n");
            assertQuery("SELECT CAST(y + 3 AS FLOAT) AS v FROM u").noLeakCheck().expectSize().returns("v\n-2.1474836E9\n");

            assertQuery("SELECT (2147483647 + 3)::DATE AS v").noLeakCheck().expectSize().returns("v\n1969-12-07T03:28:36.354Z\n");
            assertQuery("SELECT (y + 3)::DATE AS v FROM u").noLeakCheck().expectSize().returns("v\n1969-12-07T03:28:36.354Z\n");
        });
    }

    @Test
    public void testWiderCastsWidenRemBitwiseAbsOnBothPaths() throws Exception {
        // BF9 made explicit widening casts read getLong(), so +, -, *, / and unary
        // minus widen an overflowing INT operand under ::LONG. %, & | ^ ~ and abs
        // inherited the wrapping getLong() and stayed narrow, leaving ((a*b)/c)::LONG
        // widened but ((a*b)%c)::LONG wrapped. Those operators now override getLong()
        // to recurse at long width too, so every integer operator widens alike under
        // a widening cast, constant and column paths identical.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE u (y INT)");
            execute("INSERT INTO u VALUES (1000000)");

            // reference: '/' already widened (DivInt overrides getLong)
            assertQuery("SELECT ((1000000 * 1000000) / 7)::LONG AS v").noLeakCheck().expectSize().returns("v\n142857142857\n");
            assertQuery("SELECT ((y * y) / 7)::LONG AS v FROM u").noLeakCheck().expectSize().returns("v\n142857142857\n");

            // '%' now widens to match '/': 10^12 % 7 = 1 (was 0 from the wrapped product)
            assertQuery("SELECT ((1000000 * 1000000) % 7)::LONG AS v").noLeakCheck().expectSize().returns("v\n1\n");
            assertQuery("SELECT ((y * y) % 7)::LONG AS v FROM u").noLeakCheck().expectSize().returns("v\n1\n");

            // '&' with an all-ones mask is identity: the widened value holds the full product
            assertQuery("SELECT ((1000000 * 1000000) & -1)::LONG AS v").noLeakCheck().expectSize().returns("v\n1000000000000\n");
            assertQuery("SELECT ((y * y) & -1)::LONG AS v FROM u").noLeakCheck().expectSize().returns("v\n1000000000000\n");

            // '|' with 0 is identity
            assertQuery("SELECT ((1000000 * 1000000) | 0)::LONG AS v").noLeakCheck().expectSize().returns("v\n1000000000000\n");
            assertQuery("SELECT ((y * y) | 0)::LONG AS v FROM u").noLeakCheck().expectSize().returns("v\n1000000000000\n");

            // '^' with 0 is identity
            assertQuery("SELECT ((1000000 * 1000000) ^ 0)::LONG AS v").noLeakCheck().expectSize().returns("v\n1000000000000\n");
            assertQuery("SELECT ((y * y) ^ 0)::LONG AS v FROM u").noLeakCheck().expectSize().returns("v\n1000000000000\n");

            // '~' complements the full-width product: ~10^12 = -(10^12) - 1
            assertQuery("SELECT (~(1000000 * 1000000))::LONG AS v").noLeakCheck().expectSize().returns("v\n-1000000000001\n");
            assertQuery("SELECT (~(y * y))::LONG AS v FROM u").noLeakCheck().expectSize().returns("v\n-1000000000001\n");

            // abs() widens: the wrapped product is negative, so a narrow abs would flip
            // its sign; the widened value keeps the true magnitude.
            assertQuery("SELECT (abs(1000000 * 1000000))::LONG AS v").noLeakCheck().expectSize().returns("v\n1000000000000\n");
            assertQuery("SELECT (abs(y * y))::LONG AS v FROM u").noLeakCheck().expectSize().returns("v\n1000000000000\n");

            // control: the plain INT projection (no widening cast) still wraps mod 2^32
            // on both paths - getInt() is unchanged.
            assertQuery("SELECT (1000000 * 1000000) % 7 AS v").noLeakCheck().expectSize().returns("v\n0\n");
            assertQuery("SELECT (y * y) % 7 AS v FROM u").noLeakCheck().expectSize().returns("v\n0\n");
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
            assertQuery("SELECT (2147483647 + 1)::LONG AS v").noLeakCheck().expectSize().returns("v\n2147483648\n");
            assertQuery("SELECT (y + 1)::LONG AS v FROM u").noLeakCheck().expectSize().returns("v\n2147483648\n");

            // A different product that also wraps to exactly -2^31: 65536 * 32768.
            assertQuery("SELECT (65536 * 32768)::LONG AS v").noLeakCheck().expectSize().returns("v\n2147483648\n");

            // The plain INT projection still wraps mod 2^32 on both paths.
            assertQuery("SELECT 2147483647 + 1 AS v").noLeakCheck().expectSize().returns("v\nnull\n");
            assertQuery("SELECT y + 1 AS v FROM u").noLeakCheck().expectSize().returns("v\nnull\n");
        });
    }
}
