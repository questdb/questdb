/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2017 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.parser.sql;

import com.questdb.ex.ParserException;
import com.questdb.parser.sql.model.ExprNode;
import com.questdb.std.Lexer;
import com.questdb.std.ObjectPool;
import com.questdb.test.tools.AbstractTest;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class InvertedBooleanOptimisationTest extends AbstractTest {

    private final RpnBuilder rpn = new RpnBuilder();
    private final ObjectPool<ExprNode> exprNodeObjectPool = new ObjectPool<>(ExprNode.FACTORY, 128);
    private final Lexer lexer = new Lexer();
    private final ExprParser p = new ExprParser(exprNodeObjectPool);
    private final ExprAstBuilder ast = new ExprAstBuilder();
    private final PostOrderTreeTraversalAlgo traversalAlgo = new PostOrderTreeTraversalAlgo();
    private final PostOrderTreeTraversalAlgo.Visitor rpnBuilderVisitor = rpn::onNode;
    private final QueryCompiler compiler = new QueryCompiler();

    @Before
    public void setUp() {
        exprNodeObjectPool.clear();
        ExprParser.configureLexer(lexer);
    }

    @Test
    public void testDoubleInversion() throws Exception {
        assertOk("ba>", "not (not (a > b))");
    }

    @Test
    public void testFunction() throws Exception {
        assertOk("'b''a'ainnot", "not(a in ('a', 'b'))");
    }

    @Test
    public void testInvertAnd() throws Exception {
        assertOk("dc!=ba=or", "not (a != b and c = d)");
    }

    @Test
    public void testInvertAnd2() throws Exception {
        assertOk("'xyz'x~notdc!=ba=oror", "not (a != b and c = d and x ~ 'xyz')");
    }

    @Test
    public void testInvertEquals() throws Exception {
        assertOk("ba!=", "not (a = b)");
    }

    @Test
    public void testInvertGreater() throws Exception {
        assertOk("ba<=", "not (a > b)");
    }

    @Test
    public void testInvertGreaterOrEqual() throws Exception {
        assertOk("ba<", "not (a >= b)");
    }

    @Test
    public void testInvertLess() throws Exception {
        assertOk("ba>=", "not (a < b)");
    }

    @Test
    public void testInvertLessOrEqual() throws Exception {
        assertOk("ba>", "not (a <= b)");
    }

    @Test
    public void testInvertNotEquals() throws Exception {
        assertOk("ba=", "not (a != b)");
    }

    @Test
    public void testInvertOr() throws Exception {
        assertOk("dc<=ba<=and", "not (a > b or c > d)");
    }

    @Test
    public void testInvertOrAndAnd() throws Exception {
        assertOk("10c!=dc>ba<=andor", "not (a > b or not(c > d) and c = 10)");
    }

    @Test
    public void testLiteral() throws Exception {
        assertOk("anot", "not(a)");
    }

    @Test
    public void testNoInversion() throws Exception {
        assertOk("cb+a>", "(a > (b + c))");
    }

    private void assertOk(CharSequence expected, String expression) throws ParserException {
        lexer.setContent(expression);
        p.parseExpr(lexer, ast);
        ExprNode n = compiler.optimiseInvertedBooleans(ast.poll(), false);
        Assert.assertNotNull(n);
        TestUtils.assertEquals(expected, toRpn(n));
    }

    private CharSequence toRpn(ExprNode node) throws ParserException {
        rpn.reset();
        traversalAlgo.traverse(node, rpnBuilderVisitor);
        return rpn.rpn();
    }
}
