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

import io.questdb.griffin.ExpressionTreeBuilder;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.IQueryModel;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class ConstantReassociationTest extends AbstractCairoTest {

    @Test
    public void testBindVariableIsNotConstant() throws Exception {
        assertReassociation("d + $1 + 4", "d + $1 + 4");
    }

    @Test
    public void testConcatenationFlattensNumericLookingConstants() throws Exception {
        // The constant shapes that close the regroup for the arithmetic operators never reach that
        // guard through '||': rewriteConcat has already flattened the chain into a single concat()
        // argument list, and reassociateConstants leaves an n-ary node's argument list in place.
        // So none of the guards below has any say over what a '||' chain compiles to.

        // A quoted numeric-looking literal is marked widening, because '*' and '+' resolve it
        // against a numeric operand (l * '02' * 4 is integer arithmetic).
        assertPostRewriteReassociation("d || '02' || '4'", "concat(d, '02', '4')");
        assertPostRewriteReassociation("'02' || ('4' || d)", "concat('02', '4', d)");

        // Integer literals are excluded from arithmetic regrouping because an intermediate can
        // wrap onto the INT_NULL / LONG_NULL sentinel.
        assertPostRewriteReassociation("d || 2_147_483_647 || 1", "concat(d, 2_147_483_647, 1)");

        // Floating-point and DECIMAL literals are excluded from arithmetic regrouping because
        // rounding and scale are not associative.
        assertPostRewriteReassociation("d || 1.5 || 2.5", "concat(d, 1.5, 2.5)");
        assertPostRewriteReassociation("d || 1.5m || 2.5m", "concat(d, 1.5m, 2.5m)");
    }

    @Test
    public void testConcatenationIsAssociativeButNotCommutative() throws Exception {
        // SqlParser.rewriteConcat turns every '||' OPERATION node into a concat() FUNCTION node and
        // folds a nested one into its parent's argument list, so reassociateConstants never sees a
        // '||' pair to regroup. The flattening is what carries associativity: both bracketings of
        // the same operand order collapse to the same argument list.
        assertPostRewriteReassociation("('hello' || d) || 'world'", "concat('hello', d, 'world')");
        assertPostRewriteReassociation("'hello' || (d || 'world')", "concat('hello', d, 'world')");

        // Commutativity is a separate question and the answer is no: swapping the operands changes
        // the argument order, and concat appends its arguments in that order.
        assertPostRewriteReassociation("'world' || (d || 'hello')", "concat('world', d, 'hello')");
        assertPostRewriteReassociation("d || 'hello' || 'world'", "concat(d, 'hello', 'world')");
        assertPostRewriteReassociation("'world' || ('hello' || d)", "concat('world', 'hello', d)");
    }

    @Test
    public void testDeepConstantChainFoldsAndGuardsViaCachedFold() throws Exception {
        // A long left-associative chain drives the O(n) cached-fold guard path: each level reads the
        // accumulating constant subtree's cached triple in O(1) rather than re-walking it, so the
        // whole chain is checked in O(n) instead of O(n^2). The chain itself stays in source order
        // (the guard leaves it a no-op) because an integer intermediate may wrap onto a NULL sentinel.
        assertReassociationNoOp("d + 1 + 2 + 3 + 4 + 5 + 6 + 7 + 8");

        // Integer chains stay unchanged even when only a deep intermediate reaches INT_NULL; the
        // optimizer has no row-value range with which to prove an earlier prefix safe.
        assertReassociationNoOp("d + 1 + 2 + 3 + 2_147_483_641 + 1");
    }

    @Test
    public void testDivisionModuloPairWrappingToIntNullIsNotReassociated() throws Exception {
        // DivInt / RemInt are INT-typed and propagate INT_NULL just like + - *, so a
        // constant pair element built with '/' or '%' that folds to the INT_NULL sentinel
        // (-2^31) must keep the pair un-regrouped. The fold used to model only + - * & | ^,
        // Integer reassociation is disabled for all modeled operators because a row-dependent
        // intermediate may hit INT_NULL even when the constant pair does not.

        // division: 2_147_483_647 + (2 / 2) = 2_147_483_647 + 1 wraps to -2^31 == INT_NULL
        assertReassociationNoOp("d + 2_147_483_647 + (2 / 2)");
        // modulo: 2_147_483_646 + (5 % 3) = 2_147_483_646 + 2 wraps to -2^31 == INT_NULL
        assertReassociationNoOp("d + 2_147_483_646 + (5 % 3)");

        // A non-sentinel constant fold also stays in source order because a row-dependent
        // intermediate may still hit the sentinel.
        assertReassociationNoOp("d + 1000 + (6 / 2)");
    }

    @Test
    public void testFloatingPointPairIsNotReassociated() throws Exception {
        // IEEE-754 + and * are not associative under rounding, overflow and underflow, and
        // reassociateConstants never regroups the all-literal form (it returns early once both
        // sides are constant, so the fold runs left-associatively). Regrouping the column form
        // would therefore make it diverge from the literal one - see
        // IntArithmeticOverflowFoldingTest.testFloatingPointPairAgreesBetweenConstantAndColumn.
        assertReassociationNoOp("d + 1.0 + 2.0");
        assertReassociationNoOp("d + 1.0 + 2.0f");
        assertReassociationNoOp("d * 1e300 * 1e-300");
        // Pattern B (commutative): (C1 op col) op floatConst
        assertReassociationNoOp("(1.0 + d) + 2.0");
        // Mirror A (commutative): floatConst op (col op C1)
        assertReassociationNoOp("2.0 + (d + 1.0)");
        // Mirror B (associative): floatConst op (C1 op col)
        assertReassociationNoOp("2.0 + (1.0 + d)");
    }

    @Test
    public void testIntegerDecimalMixIsNotReassociated() throws Exception {
        // Regrouping an integer constant with a DECIMAL one widens the inner operation to
        // DECIMAL. For an INT column that overflows, (col + intConst) wraps mod 2^32, but
        // col + (intConst + decimalConst) does not - it evaluates at DECIMAL width. The
        // widening guard classified only DOUBLE / FLOAT, so a DECIMAL literal ('m' suffix)
        // looked non-widening and the int/decimal pair regrouped. It now recognizes DECIMAL.
        assertReassociationNoOp("d + 3 + 1.5m");
        assertReassociationNoOp("d * 3 * 2.0m");
        // Pattern B (commutative): (C1 op col) op decimalConst
        assertReassociationNoOp("(3 + d) + 1.5m");
        // Mirror A (commutative): decimalConst op (col op C1)
        assertReassociationNoOp("1.5m + (d + 3)");
        // Mirror B (associative): decimalConst op (C1 op col)
        assertReassociationNoOp("1.5m + (3 + d)");

        // A same-category DECIMAL pair does not regroup either: a DECIMAL fold carries
        // precision and scale, which regrouping shifts, and the all-literal form is never
        // regrouped - so regrouping only the column form makes the two diverge.
        assertReassociationNoOp("d + 1.5m + 2.5m");
    }

    @Test
    public void testIntegerFloatingPointMixIsNotReassociated() throws Exception {
        // Regrouping an integer constant with a floating-point one widens the
        // inner operation to floating point. For an INT column that overflows,
        // (col + intConst) wraps mod 2^32, but col + (intConst + floatConst) does
        // not - it evaluates at double width. The literal form folds the inner
        // INT arithmetic and wraps, so reassociating only the column form makes
        // the two disagree. These shapes must therefore stay un-regrouped.
        assertReassociationNoOp("d + 3 + 0.0");
        assertReassociationNoOp("d + 3 + 0.0f");
        assertReassociationNoOp("d * 3 * 2.0");
        // Pattern B (commutative): (C1 op col) op floatConst
        assertReassociationNoOp("(3 + d) + 0.0");
        // Mirror A (commutative): floatConst op (col op C1)
        assertReassociationNoOp("0.0 + (d + 3)");
        // Mirror B (associative): floatConst op (C1 op col)
        assertReassociationNoOp("0.0 + (3 + d)");

        // Same-category integer pairs stay in source order because a row-dependent intermediate
        // can wrap onto INT_NULL.
        assertReassociationNoOp("d + 3 + 4");
        assertReassociationNoOp("d + 3 + 4L");
    }

    @Test
    public void testIntegerPairWrappingToIntNullIsNotReassociated() throws Exception {
        // An integer constant pair whose INT-width fold lands exactly on the INT_NULL
        // sentinel (-2^31) must not be regrouped. Hoisting it under the column as
        // col op (C1 op C2) = col op INT_NULL makes AddInt / MulInt return INT_NULL for
        // every row, while the left-associative (and fully-constant literal) form keeps
        // the real wrapped value. The query-level oracle lives in
        // IntArithmeticOverflowFoldingTest.testReassociationIntPairWrappingToIntNullWrapsLikeColumnAndLiteral;
        // this pins the same guard structurally, one level closer to the rewrite.

        // addition: 2_147_483_647 + 1 wraps to -2^31 == INT_NULL - stays un-regrouped in every
        // pattern.
        // Pattern A: (A op C1) op C2
        assertReassociation("d + 2_147_483_647 + 1", "d + 2_147_483_647 + 1");
        // Pattern B: (C1 op A) op C2 (commutative) - the column is NOT moved to the front
        assertReassociation("(2_147_483_647 + d) + 1", "2_147_483_647 + d + 1");
        // Mirror A: C2 op (A op C1) (commutative)
        assertReassociation("1 + (d + 2_147_483_647)", "1 + (d + 2_147_483_647)");
        // Mirror B: C2 op (C1 op A)
        assertReassociation("1 + (2_147_483_647 + d)", "1 + (2_147_483_647 + d)");

        // multiplication: 65_536 * 32_768 wraps to -2^31 == INT_NULL
        assertReassociation("d * 65_536 * 32_768", "d * 65_536 * 32_768");

        // negative-constant pair: -2_147_483_647 + -1 also folds to -2^31 == INT_NULL, but the
        // minus binds as a unary operator, so neither operand is marked constant and the pair
        // never reaches the INT_NULL guard. It stays un-regrouped for that reason instead, and
        // still avoids the poison. (See the "unary-minus escapes the guard" note: safe because
        // reassociation never fires here.)
        assertReassociation("d + -2_147_483_647 + -1", "d + -(2_147_483_647) + -(1)");

        // A constant-pair-only guard cannot see that d + 2_147_483_647 reaches INT_NULL
        // when d == 1, so all integer pairs stay in their original evaluation order.
        assertReassociationNoOp("d + 2_147_483_647 + 2");
        assertReassociationNoOp("(2_147_483_647 + d) + 2");
    }

    @Test
    public void testIntegerReassociationPreservesIntermediateNull() throws Exception {
        assertQuery("SELECT (i + 2_147_483_647) + 2 result FROM x")
                .ddl("CREATE TABLE x AS (SELECT 1::int i)")
                .expectSize()
                .returns("""
                        result
                        null
                        """);
    }

    @Test
    public void testLogicalAndOrReassociation() throws Exception {
        // AND — Pattern A: (col AND C1) AND C2
        assertReassociation("a and true and true", "a and (true and true)");

        // OR — Pattern A: (col OR C1) OR C2
        assertReassociation("a or true or false", "a or (true or false)");

        // AND — Pattern B: (C1 AND col) AND C2 (commutative)
        assertReassociation("(true and a) and false", "a and (true and false)");

        // OR — Mirror A: C2 OR (col OR C1) (commutative)
        assertReassociation("true or (a or false)", "a or (true or false)");

        // AND - Mirror B: C2 AND (C1 AND col). Pure regrouping, so associativity alone carries it
        // and the column stays in the outer right operand. The unquoted true / false constants are
        // neither widening nor integer-valid, so isReassociationSafe opens the guard.
        assertReassociation("true and (false and a)", "true and false and a");
    }

    @Test
    public void testLong256HexPairIsNotReassociated() throws Exception {
        // A LONG256 (0x...) hex constant pair must not be regrouped. Hex literals fail all four
        // numeric parses (int/long/double/float), so before the guard they landed with
        // isConstFoldWidening=false and isConstFoldLongValid=false, and isReassociationSafe returned
        // true. Hoisting them under the column as col256 op (C1 op C2) folds the inner pair with
        // Long256Impl.add, which propagates NULL_LONG256 only when an OPERAND is the sentinel: a pair
        // summing (mod 2^256) to the sentinel becomes a NULL operand -> every row NULL, while the
        // left-associative (and fully-constant literal) form keeps the real value. LONG256 '+' is
        // registered associative, so only this guard stops the rewrite.

        // Pattern A: (col op C1) op C2
        assertReassociationNoOp("h256 + 0x0f + 0x01");
        assertReassociationNoOp("h256 + 0x1234567890abcdef + 0x00fedcba09876543");
        // Pattern B: (C1 op col) op C2 (commutative)
        assertReassociationNoOp("(0x0f + h256) + 0x01");
        // Mirror A: C2 op (col op C1)
        assertReassociationNoOp("0x01 + (h256 + 0x0f)");
        // Mirror B: C2 op (C1 op col)
        assertReassociationNoOp("0x01 + (0x0f + h256)");
        // A three-constant chain, like the deep-fold guard cases.
        assertReassociationNoOp("h256 + 0x0f + 0x01 + 0x02");
    }

    @Test
    public void testLongNullOperandUnderUnmodeledOperatorIsNotReassociated() throws Exception {
        // CHARACTERIZATION, not pinned semantics. applyLongFold returns LONG_NULL for a LONG_NULL
        // operand BEFORE it inspects the operator, so a constant subtree rooted at an operator the
        // fold does not model still counts as a valid long-width fold once either side has folded
        // to the sentinel. isReassociationSafe then stays closed over the pair. The contrast case
        // below shows the same shape WITHOUT a sentinel does regroup, so what these assert is a
        // conservative accident of the sentinel short-circuit rather than a required outcome -
        // these are boolean subtrees, outside the integer-wrap hazard the guard exists for.
        //
        // They are here because collapsing isConstFoldLongValid to "both children valid AND the
        // operator is modeled" looks equivalent and is not: it drops the sentinel short-circuit,
        // both operands stop counting as folds, and the guard opens. Nothing else in this class
        // covers that, so such a change would otherwise land green.

        // Zero-divisor arm: 1 / 0 folds to LONG_NULL, then '=' is unmodeled but the sentinel arm
        // returns through it anyway.
        assertReassociationNoOp("b and 1 / 0 = 5 and 1 / 0 = 6");
        // '%' by zero reaches the sentinel the same way, under a length-2 unmodeled operator.
        assertReassociationNoOp("b and 1 % 0 <> 5 and 1 % 0 <> 6");
        // Overflow arm: 2^63-1 + 1 wraps onto the sentinel through a modeled operator, which the
        // unmodeled '=' above it then returns through. No LEAF can carry LONG_NULL - the parser
        // emits a prefix '-' as a unary operation, so a negative literal never reaches the fold as
        // one token - which is why the sentinel has to be manufactured by a fold here.
        assertReassociationNoOp("b and 9_223_372_036_854_775_807 + 1 = 5 and 9_223_372_036_854_775_807 + 1 = 6");

        // Contrast: drop the sentinel and the identical shape regroups today.
        assertReassociation("b and 1 = 5 and 1 = 6", "b and (1 = 5 and 1 = 6)");
    }

    @Test
    public void testLongPairWrappingToLongNullIsNotReassociated() throws Exception {
        // A LONG (or INT+LONG) constant pair whose LONG-width fold lands exactly on the
        // LONG_NULL sentinel (-2^63) must not be regrouped: col op (C1 op C2) = col op
        // LONG_NULL poisons every row to NULL, while the left-associative form keeps the
        // real wrapped value. The INT-width fold rejects LONG-range / L-suffixed literals, so
        // LONG pairs stay in source order because a row-dependent intermediate may hit LONG_NULL.

        // 9_223_372_036_854_775_807 + 1 wraps to -2^63 == LONG_NULL
        assertReassociationNoOp("l + 9_223_372_036_854_775_807 + 1");
        // L-suffixed operands fold the same way: 9_223_372_036_854_775_806L + 2L -> LONG_NULL
        assertReassociationNoOp("l + 9_223_372_036_854_775_806L + 2L");

        // A non-sentinel LONG pair also stays in source order because a row-dependent
        // intermediate can still hit LONG_NULL.
        assertReassociationNoOp("l + 9_000_000_000_000_000_000 + 100");
    }

    @Test
    public void testMismatchedOperatorsAreNotReassociated() throws Exception {
        // Inner operator differs from outer — lhs.token != token, no reassociation
        assertReassociation("d * 2 + 3", "d * 2 + 3");
        assertReassociation("3 + d * 2", "3 + d * 2");
    }

    @Test
    public void testNaryConstantArgsSkipFoldParsing() throws Exception {
        // reassociateConstants marks a CONSTANT argument of an n-ary node constant WITHOUT building
        // its fold cache. isReassociationSafe is the cache's only reader and only ever receives an
        // operand of a binary pair, which an args slot never is, so IN (1, ..., 10_000) parses no
        // tokens instead of ten thousand - and a LONG-range element skips a guaranteed-failing
        // parseInt whose NumericException fills a stack trace under -ea, which is how the tests run.
        //
        // The skip leaves the rendered tree byte-identical, which is all
        // testReassociationMultiParamsMixedLiteralKinds can observe - restoring the cacheConstantFold
        // call would keep that test green. The fold-cache state is the only thing that tells the two
        // apart, so it is what this test asserts.
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            ExpressionTreeBuilder listener = new ExpressionTreeBuilder();
            compiler.testParseExpression("l in (1, 5_000_000_000, null)", listener);
            ExpressionNode node = listener.poll();
            assert node != null;
            node.reassociateConstants(false);

            int constantArgs = 0;
            for (int i = 0; i < node.paramCount; i++) {
                final ExpressionNode arg = node.args.getQuick(i);
                if (arg.type != ExpressionNode.CONSTANT) {
                    continue;
                }
                constantArgs++;
                Assert.assertTrue("args constant must carry the mark: " + arg.token, arg.isConstantExpression);
                // The two integer tokens carry this one: cacheConstantFold parses both (parseInt
                // takes 1, parseLong takes 5_000_000_000) and sets it true, so false here is proof
                // no parse ran. 'null' cannot - it is not integer-shaped either way.
                Assert.assertFalse("args constant must skip the parse: " + arg.token, arg.isConstFoldLongValid());
                // Fail closed. All three tokens carry this one, but 'null' is the only one that
                // carries it ALONE: cacheConstantFold classifies an unquoted non-numeric token as
                // non-widening, i.e. safe to regroup. The skip has to say the opposite, so a future
                // reader of the unbuilt cache refuses rather than trusting a cleared default.
                Assert.assertTrue("args constant must fail closed: " + arg.token, arg.isConstFoldWidening());
            }
            Assert.assertEquals("expected three CONSTANT args", 3, constantArgs);
        }

        // Contrast: a binary pair's operands ARE on isReassociationSafe's route, so they keep the
        // fold. Without this the assertions above would also pass if reassociateConstants stopped
        // caching altogether.
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            ExpressionTreeBuilder listener = new ExpressionTreeBuilder();
            compiler.testParseExpression("1 + 5_000_000_000", listener);
            ExpressionNode node = listener.poll();
            assert node != null;
            node.reassociateConstants(false);
            Assert.assertTrue(node.lhs.isConstFoldLongValid());
            Assert.assertTrue(node.rhs.isConstFoldLongValid());
            Assert.assertFalse(node.lhs.isConstFoldWidening());
            Assert.assertFalse(node.rhs.isConstFoldWidening());
        }
    }

    @Test
    public void testNoReassociationForNonConstantSubtree() throws Exception {
        // Reassociation is only applied to constant subtrees, so if the subtree contains a non-constant node, it should be left unchanged.
        assertReassociation("1 + (a + b)", "1 + (a + b)");
        assertReassociation("(a + b) + 1", "a + b + 1");
    }

    @Test
    public void testReassociationMultiParams() throws Exception {
        assertReassociation("d + coalesce(d + 1 + 2, 3, 4, 5)", "d + coalesce(d + 1 + 2, 3, 4, 5)");
    }

    @Test
    public void testReassociationMultiParamsMixedLiteralKinds() throws Exception {
        // An n-ary argument list is never reassociated, whatever its arguments are made of, so every
        // literal kind has to come back verbatim: INT, out-of-INT-range LONG, float, DECIMAL with its
        // 'm' suffix, LONG256 hex, a quoted numeric and a keyword. reassociateConstants marks such an
        // argument constant without parsing it - the fold cache it would build is read only for the
        // two operands of a binary pair - so this pins that skipping the parse changes nothing.
        assertReassociationNoOp("l in (1, 5_000_000_000, 0.5, 1.5m, 0x1f, '02', null)");
        assertReassociationNoOp("coalesce(l, 1, 5_000_000_000, 0.5, 1.5m, 0x1f, '02')");
        // A subtree nested inside such a list still reassociates by its own rules: the integer
        // arithmetic keeps its source order, and a '||' chain is not a binary pair by the time the
        // pass runs - rewriteConcat has folded it into one nested concat() argument list.
        assertPostRewriteReassociation(
                "coalesce(s, s || 'b' || 'c', 'x')",
                "coalesce(s, concat(s, 'b', 'c'), 'x')"
        );
        assertReassociationNoOp("coalesce(l, l + 2 + 3, 4)");
    }

    @Test
    public void testReassociationOfModeledArithmeticOperators() throws Exception {
        assertReassociationNoOp("d + 1 + 4");
        assertReassociationNoOp("d * 2 * 3");
        assertReassociationNoOp("l & 3 & 5");
        assertReassociationNoOp("l | 1 | 4");
        assertReassociationNoOp("l ^ 3 ^ 5");
        assertReassociation("d + NULL + 4", "d + (NULL + 4)");

        assertReassociationNoOp("d - 1 - 4");
        assertReassociationNoOp("d / 2 / 5");
        assertReassociationNoOp("d % 7 % 3");
    }

    @Test
    public void testReturnValueIndicatesConstantSubtree() throws Exception {
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            ExpressionNode allConst = compiler.testParseExpression("1 + 2 + 3", (IQueryModel) null);
            Assert.assertTrue(allConst.reassociateConstants(false));

            ExpressionNode hasColumn = compiler.testParseExpression("d + 1 + 4", (IQueryModel) null);
            Assert.assertFalse(hasColumn.reassociateConstants(false));

            ExpressionNode leaf = compiler.testParseExpression("42", (IQueryModel) null);
            Assert.assertTrue(leaf.reassociateConstants(false));
        }
    }

    @Test
    public void testUnaryOperatorsAreNotReassociated() throws Exception {
        // Unary operators (paramCount == 1) are left unchanged, but
        // reassociation still applies inside their operand subtree.
        assertReassociation("-d + 1 + 2", "-(d) + 1 + 2");
        assertReassociation("-(d + 1 + 2)", "-(d + 1 + 2)");
        assertReassociation("3 + (-d)", "3 + -(d)");
    }

    /**
     * Asserts what production does to {@code inputExpr}. Unlike {@link #assertReassociation}, this
     * routes through {@link SqlCompiler#testParseExpression(CharSequence, IQueryModel)},
     * which runs {@code SqlParser.rewriteKnownStatements} - and with it {@code rewriteConcat} -
     * exactly as the production {@code expr(...)} overloads do. The tree handed to
     * {@link ExpressionNode#reassociateConstants} here is therefore the one {@code FunctionParser}
     * sees. The helper renders {@code expectedExpr} twice, once before and once after the pass.
     *
     * <p>The first assertion carries the coverage: it pins the shape {@code rewriteConcat} flattens
     * {@code inputExpr} into. The second assertion re-renders after the pass and is a forward guard
     * only - it cannot fail against the current implementation. {@code reassociateConstants} regroups
     * a node only when that node satisfies {@code type == OPERATION && paramCount == 2} (see the early
     * return near the top of {@link ExpressionNode#reassociateConstants}), and a flattened
     * {@code concat} is a FUNCTION node hanging its operands off an n-ary {@code args} list; neither
     * that node nor any argument these call sites hand it meets the condition. Keep the assertion
     * anyway: it goes red the day the n-ary arm starts restructuring {@code args}.</p>
     */
    private void assertPostRewriteReassociation(String inputExpr, String expectedExpr) throws SqlException {
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            ExpressionNode node = compiler.testParseExpression(inputExpr, (IQueryModel) null);
            Assert.assertNotNull(node);
            sink.clear();
            node.toSink(sink);
            TestUtils.assertEquals("parser rewrite for: " + inputExpr, expectedExpr, sink);
            node.reassociateConstants(false);
            sink.clear();
            node.toSink(sink);
            TestUtils.assertEquals("reassociation for: " + inputExpr, expectedExpr, sink);
        }
    }

    /**
     * Parses {@code inputExpr} through the {@link io.questdb.griffin.ExpressionParser} alone and
     * asserts the canonical rendering after {@link ExpressionNode#reassociateConstants}. This helper
     * deliberately skips {@code SqlParser.rewriteKnownStatements}, so it is faithful only for
     * operators that rewrite leaves untouched - the arithmetic and boolean ones. An operator the
     * parser rewrites, {@code ||} above all, must use {@link #assertPostRewriteReassociation}
     * instead, or the assertion pins a tree production never builds.
     */
    private void assertReassociation(String inputExpr, String expectedExpr) throws SqlException {
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            ExpressionTreeBuilder listener = new ExpressionTreeBuilder();
            compiler.testParseExpression(inputExpr, listener);
            ExpressionNode node = listener.poll();
            assert node != null;
            node.reassociateConstants(false);
            sink.clear();
            node.toSink(sink);
            TestUtils.assertEquals(expectedExpr, sink);
        }
    }

    /**
     * Asserts that {@link ExpressionNode#reassociateConstants} leaves the parsed tree
     * structurally unchanged (a no-op), comparing its canonical rendering before and
     * after. Used for the guard cases that must NOT regroup; avoids hand-predicting the
     * canonical string, and goes RED if the guard ever lets the regroup through.
     *
     * <p>This helper carries the same hazard as {@link #assertReassociation}: it reaches the parser
     * through the identical listener overload, so an operator that {@code SqlParser.rewriteConcat}
     * or a sibling rewrite reshapes, {@code ||} above all, belongs in
     * {@link #assertPostRewriteReassociation} instead.</p>
     */
    private void assertReassociationNoOp(String inputExpr) throws SqlException {
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            ExpressionTreeBuilder listener = new ExpressionTreeBuilder();
            compiler.testParseExpression(inputExpr, listener);
            ExpressionNode node = listener.poll();
            assert node != null;
            sink.clear();
            node.toSink(sink);
            final String before = sink.toString();
            node.reassociateConstants(false);
            sink.clear();
            node.toSink(sink);
            TestUtils.assertEquals("reassociation must be a no-op for: " + inputExpr, before, sink);
        }
    }
}
