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
    public void testIntegerFloatingPointMixIsNotReassociated() throws Exception {
        // Regrouping an integer constant with a floating-point one widens the
        // inner operation to floating point. For an INT column that overflows,
        // (col + intConst) wraps mod 2^32, but col + (intConst + floatConst) does
        // not -- it evaluates at double width. The literal form folds the inner
        // INT arithmetic and wraps, so reassociating only the column form makes
        // the two disagree. These shapes must therefore stay un-regrouped.
        assertReassociation("d + 3 + 0.0", "d + 3 + 0.0");
        assertReassociation("d + 3 + 0.0f", "d + 3 + 0.0f");
        assertReassociation("d * 3 * 2.0", "d * 3 * 2.0");
        // Pattern B (commutative): (C1 op col) op floatConst
        assertReassociation("(3 + d) + 0.0", "3 + d + 0.0");
        // Mirror A (commutative): floatConst op (col op C1)
        assertReassociation("0.0 + (d + 3)", "0.0 + (d + 3)");
        // Mirror B (associative): floatConst op (C1 op col)
        assertReassociation("0.0 + (3 + d)", "0.0 + (3 + d)");

        // Same-category pairs still regroup. Integer pairs are safe because
        // integer addition is associative modulo 2^32 and INT-to-LONG widening
        // reads the same value via getLong(); floating pairs evaluate at floating
        // point regardless of grouping.
        assertReassociation("d + 3 + 4", "d + (3 + 4)");
        assertReassociation("d + 3 + 4L", "d + (3 + 4L)");
        assertReassociation("d + 1.0 + 2.0", "d + (1.0 + 2.0)");
        assertReassociation("d + 1.0 + 2.0f", "d + (1.0 + 2.0f)");
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
        assertReassociation("d + coalesce(d + 1 + 2, 3, 4, 5)", "d + coalesce(d + (1 + 2), 3, 4, 5)");
    }

    @Test
    public void testReassociationReordersTree() throws Exception {
        // Pattern A: (A op C1) op C2 — natural left-associative chain
        assertReassociation("d + 1 + 4", "d + (1 + 4)");
        assertReassociation("d * 2 * 3", "d * (2 * 3)");
        assertReassociation("l & 3 & 5", "l & (3 & 5)");
        assertReassociation("l | 1 | 4", "l | (1 | 4)");
        assertReassociation("l ^ 3 ^ 5", "l ^ (3 ^ 5)");
        assertReassociation("d + 1 + 2 + 3", "d + (1 + 2 + 3)");
        assertReassociation("d + (1 + 2) + 4", "d + (1 + 2 + 4)");
        assertReassociation("d + 1 * 2 + 3", "d + (1 * 2 + 3)");

        // Pattern B: (C1 op A) op C2 — needs associative + commutative
        assertReassociation("(1 + d) + 4", "d + (1 + 4)");
        assertReassociation("(2 * d) * 3", "d * (2 * 3)");

        // Mirror A: C2 op (A op C1) — needs associative + commutative
        assertReassociation("4 + (d + 1)", "d + (4 + 1)");
        assertReassociation("3 * (d * 2)", "d * (3 * 2)");

        // Mirror B: C2 op (C1 op A) — needs only associative
        assertReassociation("4 + (1 + d)", "4 + 1 + d");
        assertReassociation("3 * (2 * d)", "3 * 2 * d");

        // NULL handling
        assertReassociation("d + NULL + 4", "d + (NULL + 4)");

        // Negative cases: NOT reassociated (non-commutative operators)
        assertReassociation("d - 1 - 4", "d - 1 - 4");
        assertReassociation("d / 2 / 5", "d / 2 / 5");
        assertReassociation("d % 7 % 3", "d % 7 % 3");
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
        assertReassociation("-d + 1 + 2", "-(d) + (1 + 2)");
        assertReassociation("-(d + 1 + 2)", "-(d + (1 + 2))");
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
}
