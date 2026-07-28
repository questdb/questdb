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

import io.questdb.PropertyKey;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

/**
 * Pins the agreement between the constant-folded path and the column / bind path for INT
 * arithmetic that overflows. The query fuzzer's literal-vs-bind oracle surfaced the divergence
 * this is about: a constant {@code INT * INT} product that overflowed folded to a wider LONG
 * (2764486628) while the same expression over a column wrapped to INT (-1530480668), which
 * flipped a {@code > 2} comparison and changed a row count.
 * <p>
 * The fold now keeps the wrap, so all three spellings of an INT expression carry the same value.
 * {@link IntWidthWrapTest} pins that rule across every context; this class covers the folding and
 * constant-reassociation machinery underneath it, where the hazard is an INTERMEDIATE landing on a
 * NULL sentinel rather than the width of the result.
 */
public class IntArithmeticOverflowFoldingTest extends AbstractCairoTest {

    @Test
    public void testComparisonAgreesBetweenConstantAndColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x INT)");
            execute("INSERT INTO t VALUES (839_759)");

            // plain INT projection wraps mod 2^32 on both paths
            assertQuery("SELECT 839_759::INT * 330_972L::SHORT AS v").noLeakCheck().expectSize().returns("v\n-1530480668\n");
            assertQuery("SELECT x::INT * 330_972L::SHORT AS v FROM t").noLeakCheck().expectSize().returns("v\n-1530480668\n");

            // the offending comparison: false on both paths (was true for the folded LONG)
            assertQuery("SELECT (839_759::INT * 330_972L::SHORT) > 2 AS v").noLeakCheck().expectSize().returns("v\nfalse\n");
            assertQuery("SELECT (x::INT * 330_972L::SHORT) > 2 AS v FROM t").noLeakCheck().expectSize().returns("v\nfalse\n");
        });
    }

    @Test
    public void testConstantReassociationGuardCoversQuotedNumericLiterals() throws Exception {
        // The reassociation guard screened constants by their first character, and a string
        // literal keeps its quotes, so '02' took the non-numeric early-out with both fold flags
        // clear and the pair regrouped anyway. Overload resolution then cast it to a number, so
        // l * '02' * 4 regrouped to l * 8 and produced 0 where the unquoted l * 2 * 4 produced
        // null - spelling-dependent semantics for the same arithmetic.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (l LONG, d DOUBLE)");
            execute("INSERT INTO t VALUES (4_611_686_018_427_387_904, 1.0)");

            // the quoted spelling now agrees with the unquoted one, on the column and the literal
            assertQuery("SELECT l * 2 * 4 AS v FROM t").noLeakCheck().expectSize().returns("v\nnull\n");
            assertQuery("SELECT l * '02' * 4 AS v FROM t").noLeakCheck().expectSize().returns("v\nnull\n");
            assertQuery("SELECT 4_611_686_018_427_387_904 * '02' * 4 AS v").noLeakCheck().expectSize().returns("v\nnull\n");

            // the regrouping is gone from the plan, so the multiplication stays left-associative
            assertQuery("SELECT l * '02' * 4 AS v FROM t").noLeakCheck().expectSize()
                    .withPlanContaining("l*'02'*4").returns("v\nnull\n");

            // a quoted floating-point literal resolves against the DOUBLE column it sits next to
            // instead of being folded into an INT pair, which used to raise ImplicitCastException
            assertQuery("SELECT d + '0.1' + 1 AS v FROM t").noLeakCheck().expectSize().returns("v\n2.1\n");
            assertQuery("SELECT 1.0 + '0.1' + 1 AS v").noLeakCheck().expectSize().returns("v\n2.1\n");

            // unquoted non-numeric constants stay reassociable: boolean logic still regroups
            assertQuery("SELECT true AND true AND false AS v").noLeakCheck().expectSize().returns("v\nfalse\n");
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

            // division: 2_147_483_647 + (2 / 2) = 2_147_483_647 + 1 wraps to -2^31 == INT_NULL
            assertQuery("SELECT (2 + 2_147_483_647) + (2 / 2) AS v").noLeakCheck().expectSize().returns("v\n-2147483646\n");
            assertQuery("SELECT (x + 2_147_483_647) + (2 / 2) AS v FROM t").noLeakCheck().expectSize().returns("v\n-2147483646\n");

            // modulo: the regrouped pair 2_147_483_647 + (7 % 2) = 2_147_483_647 + 1 wraps to
            // -2^31 == INT_NULL, while the left-associative x + 2_147_483_647 = -2147483647 does
            // NOT hit the sentinel, so the un-regrouped form keeps the real wrapped value.
            assertQuery("SELECT (2 + 2_147_483_647) + (7 % 2) AS v").noLeakCheck().expectSize().returns("v\n-2147483646\n");
            assertQuery("SELECT (x + 2_147_483_647) + (7 % 2) AS v FROM t").noLeakCheck().expectSize().returns("v\n-2147483646\n");

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
            assertQuery("SELECT (2 + 2_147_483_647) + 1.5m AS v").noLeakCheck().expectSize().returns("v\n-2147483645.5\n");
            assertQuery("SELECT (ic + 2_147_483_647) + 1.5m AS v FROM t").noLeakCheck().expectSize().returns("v\n-2147483645.5\n");

            // commutative (constant on the left) and the mirror shape agree too
            assertQuery("SELECT (2_147_483_647 + ic) + 1.5m AS v FROM t").noLeakCheck().expectSize().returns("v\n-2147483645.5\n");
            assertQuery("SELECT 1.5m + (ic + 2_147_483_647) AS v FROM t").noLeakCheck().expectSize().returns("v\n-2147483645.5\n");

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

            // addition: 2_147_483_647 + 1 wraps to -2^31 == INT_NULL
            // Pattern A: (A op C1) op C2
            assertQuery("SELECT (5 + 2_147_483_647) + 1 AS v").noLeakCheck().expectSize().returns("v\n-2147483643\n");
            assertQuery("SELECT (i + 2_147_483_647) + 1 AS v FROM u").noLeakCheck().expectSize().returns("v\n-2147483643\n");
            // Pattern B: (C1 op A) op C2 (commutative)
            assertQuery("SELECT (2_147_483_647 + 5) + 1 AS v").noLeakCheck().expectSize().returns("v\n-2147483643\n");
            assertQuery("SELECT (2_147_483_647 + i) + 1 AS v FROM u").noLeakCheck().expectSize().returns("v\n-2147483643\n");
            // Mirror A: C2 op (A op C1) (commutative)
            assertQuery("SELECT 1 + (5 + 2_147_483_647) AS v").noLeakCheck().expectSize().returns("v\n-2147483643\n");
            assertQuery("SELECT 1 + (i + 2_147_483_647) AS v FROM u").noLeakCheck().expectSize().returns("v\n-2147483643\n");
            // Mirror B: C2 op (C1 op A)
            assertQuery("SELECT 1 + (2_147_483_647 + 5) AS v").noLeakCheck().expectSize().returns("v\n-2147483643\n");
            assertQuery("SELECT 1 + (2_147_483_647 + i) AS v FROM u").noLeakCheck().expectSize().returns("v\n-2147483643\n");

            // multiplication: 65_536 * 32_768 wraps to -2^31 == INT_NULL
            assertQuery("SELECT (2 * 65_536) * 32_768 AS v").noLeakCheck().expectSize().returns("v\n0\n");
            assertQuery("SELECT (j * 65_536) * 32_768 AS v FROM u").noLeakCheck().expectSize().returns("v\n0\n");

            // negative-constant pair: -2_147_483_647 + -1 also folds to -2^31 == INT_NULL, but each minus
            // binds as a unary operator, so neither operand is marked constant and the pair never
            // reaches the reassociation guard. It stays un-regrouped for that reason, so both paths keep
            // the real wrapped value (-2147483643) rather than poisoning every row to NULL.
            assertQuery("SELECT (5 + -2_147_483_647) + -1 AS v").noLeakCheck().expectSize().returns("v\n-2147483643\n");
            assertQuery("SELECT (i + -2_147_483_647) + -1 AS v FROM u").noLeakCheck().expectSize().returns("v\n-2147483643\n");

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

            // 9_223_372_036_854_775_807 + 1 wraps to -2^63 == LONG_NULL
            assertQuery("SELECT (5 + 9_223_372_036_854_775_807) + 1 AS v").noLeakCheck().expectSize().returns("v\n-9223372036854775803\n");
            assertQuery("SELECT (lc + 9_223_372_036_854_775_807) + 1 AS v FROM t").noLeakCheck().expectSize().returns("v\n-9223372036854775803\n");

            // control: a LONG pair that does NOT hit the sentinel still regroups and stays correct
            assertQuery("SELECT (5 + 9_000_000_000_000_000_000) + 100 AS v").noLeakCheck().expectSize().returns("v\n9000000000000000105\n");
            assertQuery("SELECT (lc + 9_000_000_000_000_000_000) + 100 AS v FROM t").noLeakCheck().expectSize().returns("v\n9000000000000000105\n");
        });
    }
}
