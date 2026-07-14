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
    public void testConcatenationIsAssociativeButNotCommutative() throws Exception {
        // || is associative but not commutative, so Pattern B and Mirror A
        // (which require commutativity) are skipped.
        assertReassociation("('hello' || d) || 'world'", "'hello' || d || 'world'");
        assertReassociation("'world' || (d || 'hello')", "'world' || (d || 'hello')");

        // Pattern A and Mirror B don't require commutativity, so they still apply.
        assertReassociation("d || 'hello' || 'world'", "d || ('hello' || 'world')");
        assertReassociation("'world' || ('hello' || d)", "'world' || 'hello' || d");
    }

    @Test
    public void testDeepConstantChainFoldsAndGuardsViaCachedFold() throws Exception {
        // A long left-associative chain drives the O(n) cached-fold path: each level reads the
        // accumulating constant subtree's cached triple in O(1) rather than re-walking it, so the
        // whole chain reassociates in O(n) instead of O(n^2). Integer chains stay in source order because an intermediate may wrap onto a NULL sentinel.
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
    public void testReassociationReordersTree() throws Exception {
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
