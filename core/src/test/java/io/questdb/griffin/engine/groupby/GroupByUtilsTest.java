/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.griffin.engine.groupby;

import io.questdb.griffin.model.ExpressionNode;
import io.questdb.std.ObjectFactory;
import org.junit.Test;

import static io.questdb.griffin.model.ExpressionNode.FUNCTION;
import static io.questdb.griffin.model.ExpressionNode.LITERAL;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class GroupByUtilsTest {

    public static final ObjectFactory<ExpressionNode> FACTORY = ExpressionNode.FACTORY;

    @Test
    public void testNodeTraversal1() {
        ExpressionNode n1 = FACTORY.newInstance();
        ExpressionNode n2 = FACTORY.newInstance();
        n1.type = FUNCTION;
        n2.type = LITERAL;
        assertFalse(ExpressionNode.compareNodesGroupBy(n1, n2));
    }

    @Test
    public void testNodeTraversal10() {
        ExpressionNode n1 = FACTORY.newInstance();
        ExpressionNode n2 = FACTORY.newInstance();
        //
        n1.type = FUNCTION;
        n1.token = "func";
        n1.rhs = FACTORY.newInstance();
        n1.rhs.type = LITERAL;
        //
        n2.type = FUNCTION;
        n2.token = "func";
        n2.rhs = FACTORY.newInstance();
        n2.rhs.type = LITERAL;
        assertTrue(ExpressionNode.compareNodesGroupBy(n1, n2));
    }

    @Test
    public void testNodeTraversal11() {
        ExpressionNode n1 = FACTORY.newInstance();
        ExpressionNode n2 = FACTORY.newInstance();
        //
        n1.type = FUNCTION;
        n1.token = "func";
        n1.rhs = FACTORY.newInstance();
        n1.rhs.type = LITERAL;
        //
        n2.type = FUNCTION;
        n2.token = "diff";
        n2.rhs = FACTORY.newInstance();
        n2.rhs.type = LITERAL;
        assertFalse(ExpressionNode.compareNodesGroupBy(n1, n2));
    }

    @Test
    public void testNodeTraversal12() {
        ExpressionNode n1 = FACTORY.newInstance();
        ExpressionNode n2 = FACTORY.newInstance();
        //
        n1.type = FUNCTION;
        n1.token = "func";
        n1.rhs = FACTORY.newInstance();
        n1.rhs.type = LITERAL;
        n1.rhs.token = "baa";
        //
        n2.type = FUNCTION;
        n2.token = "func";
        n2.rhs = FACTORY.newInstance();
        n2.rhs.type = LITERAL;
        n2.rhs.token = "boo";
        assertFalse(ExpressionNode.compareNodesGroupBy(n1, n2));
    }

    @Test
    public void testNodeTraversal13() {
        ExpressionNode n1 = FACTORY.newInstance();
        ExpressionNode n2 = FACTORY.newInstance();
        //
        n1.type = FUNCTION;
        n1.token = "func";
        n1.rhs = FACTORY.newInstance();
        n1.rhs.type = LITERAL;
        n1.rhs.token = "boo";
        //
        n2.type = FUNCTION;
        n2.token = "func";
        n2.rhs = FACTORY.newInstance();
        n2.rhs.type = LITERAL;
        n2.rhs.token = "boo";
        assertTrue(ExpressionNode.compareNodesGroupBy(n1, n2));
    }

    @Test
    public void testNodeTraversal14() {
        ExpressionNode n1 = FACTORY.newInstance();
        ExpressionNode n2 = FACTORY.newInstance();
        n1.type = FUNCTION;
        n2.type = FUNCTION;
        n1.rhs = FACTORY.newInstance();
        assertFalse(ExpressionNode.compareNodesGroupBy(n1, n2));
    }

    @Test
    public void testNodeTraversal15() {
        ExpressionNode n1 = FACTORY.newInstance();
        ExpressionNode n2 = FACTORY.newInstance();
        //
        n1.type = FUNCTION;
        n1.token = "func";
        n1.lhs = FACTORY.newInstance();
        n1.lhs.type = LITERAL;
        n1.lhs.token = "A";
        n1.rhs = FACTORY.newInstance();
        n1.rhs.type = LITERAL;
        n1.rhs.token = "B";
        //
        n2.type = FUNCTION;
        n2.token = "func";
        n2.lhs = FACTORY.newInstance();
        n2.lhs.type = LITERAL;
        n2.lhs.token = "B";
        n2.rhs = FACTORY.newInstance();
        n2.rhs.type = LITERAL;
        n2.rhs.token = "A";
        assertFalse(ExpressionNode.compareNodesGroupBy(n1, n2));
    }

    @Test
    public void testNodeTraversal16() {
        ExpressionNode n1 = FACTORY.newInstance();
        ExpressionNode n2 = FACTORY.newInstance();
        //
        n1.type = FUNCTION;
        n1.token = "func";
        n1.lhs = FACTORY.newInstance();
        n1.lhs.type = LITERAL;
        n1.lhs.token = "B";
        n1.rhs = FACTORY.newInstance();
        n1.rhs.type = LITERAL;
        n1.rhs.token = "A";
        //
        n2.type = FUNCTION;
        n2.token = "func";
        n2.lhs = FACTORY.newInstance();
        n2.lhs.type = LITERAL;
        n2.lhs.token = "B";
        n2.rhs = FACTORY.newInstance();
        n2.rhs.type = LITERAL;
        n2.rhs.token = "A";
        assertTrue(ExpressionNode.compareNodesGroupBy(n1, n2));
    }

    @Test
    public void testNodeTraversal17() {
        ExpressionNode n1 = FACTORY.newInstance();
        ExpressionNode n2 = FACTORY.newInstance();
        //
        n1.type = FUNCTION;
        n1.token = "func";
        n1.args.add(FACTORY.newInstance());
        //
        n2.type = FUNCTION;
        n2.token = "func";
        n2.lhs = FACTORY.newInstance();
        n2.lhs.type = LITERAL;
        n2.lhs.token = "B";
        n2.rhs = FACTORY.newInstance();
        n2.rhs.type = LITERAL;
        n2.rhs.token = "A";
        assertFalse(ExpressionNode.compareNodesGroupBy(n1, n2));
    }

    @Test
    public void testNodeTraversal18() {
        ExpressionNode n1 = FACTORY.newInstance();
        ExpressionNode n2 = FACTORY.newInstance();
        //
        n1.type = FUNCTION;
        n1.token = "func";
        n1.lhs = FACTORY.newInstance();
        n1.lhs.type = LITERAL;
        n1.lhs.token = "B";
        n1.rhs = FACTORY.newInstance();
        n1.rhs.type = LITERAL;
        n1.rhs.token = "A";
        //
        n2.type = FUNCTION;
        n2.token = "func";
        n2.args.add(FACTORY.newInstance());
        assertFalse(ExpressionNode.compareNodesGroupBy(n1, n2));
    }

    @Test
    public void testNodeTraversal19() {
        ExpressionNode n1 = FACTORY.newInstance();
        ExpressionNode n2 = FACTORY.newInstance();
        //
        n1.type = FUNCTION;
        n1.token = "func";
        n1.args.add(FACTORY.newInstance());
        //
        n2.type = FUNCTION;
        n2.token = "func";
        n2.args.add(FACTORY.newInstance());
        assertTrue(ExpressionNode.compareNodesGroupBy(n1, n2));
    }

    @Test
    public void testNodeTraversal2() {
        ExpressionNode n1 = FACTORY.newInstance();
        ExpressionNode n2 = FACTORY.newInstance();
        n1.type = FUNCTION;
        n2.type = FUNCTION;
        assertTrue(ExpressionNode.compareNodesGroupBy(n1, n2));
    }

    @Test
    public void testNodeTraversal20() {
        ExpressionNode n1 = FACTORY.newInstance();
        ExpressionNode n2 = FACTORY.newInstance();
        //
        n1.type = FUNCTION;
        n1.token = "func";
        n1.args.add(FACTORY.newInstance());
        n1.args.add(FACTORY.newInstance());
        //
        n2.type = FUNCTION;
        n2.token = "func";
        n2.args.add(FACTORY.newInstance());
        assertFalse(ExpressionNode.compareNodesGroupBy(n1, n2));
    }

    @Test
    public void testNodeTraversal21() {
        ExpressionNode n1 = FACTORY.newInstance();
        ExpressionNode n2 = FACTORY.newInstance();
        ExpressionNode arg = FACTORY.newInstance();

        arg.type = FUNCTION;
        arg.token = "func";
        arg.lhs = FACTORY.newInstance();
        arg.lhs.type = LITERAL;
        arg.lhs.token = "B";
        arg.rhs = FACTORY.newInstance();
        arg.rhs.type = LITERAL;
        arg.rhs.token = "A";
        //
        n1.type = FUNCTION;
        n1.token = "func";
        n1.args.add(arg);
        //
        n2.type = FUNCTION;
        n2.token = "func";
        n2.args.add(arg);
        assertTrue(ExpressionNode.compareNodesGroupBy(n1, n2));
    }

    @Test
    public void testNodeTraversal22() {
        ExpressionNode n1 = FACTORY.newInstance();
        ExpressionNode n2 = FACTORY.newInstance();
        ExpressionNode arg1 = createNode(LITERAL, "BOO", null, null);
        ExpressionNode arg2 = createNode(LITERAL, "BOO", null, null);
        ExpressionNode arg3 = createNode(LITERAL, "BOO", null, null);
        ExpressionNode arg4 = createNode(LITERAL, "BOO", null, null);
        ExpressionNode arg5 = createNode(LITERAL, "BOO", null, null);
        ExpressionNode arg6 = createNode(LITERAL, "BOO", null, null);
        //
        n1.type = FUNCTION;
        n1.token = "func";
        n1.args.add(arg1);
        n1.args.add(arg2);
        n1.args.add(arg3);
        //
        n2.type = FUNCTION;
        n2.token = "func";
        n2.args.add(arg4);
        n2.args.add(arg5);
        n2.args.add(arg6);
        assertTrue(ExpressionNode.compareNodesGroupBy(n1, n2));
    }

    @Test
    public void testNodeTraversal23() {
        ExpressionNode n1 = FACTORY.newInstance();
        ExpressionNode n2 = FACTORY.newInstance();
        ExpressionNode arg1 = createNode(LITERAL, "BOO", null, null);
        ExpressionNode arg2 = createNode(LITERAL, "BOO", null, null);
        ExpressionNode arg3 = createNode(LITERAL, "BOO", null, null);
        ExpressionNode arg4 = createNode(LITERAL, "BOO", null, null);
        ExpressionNode arg5 = createNode(LITERAL, "BOO", null, null);
        ExpressionNode arg6 = createNode(LITERAL, "YYY", null, null);
        //
        n1.type = FUNCTION;
        n1.token = "func";
        n1.args.add(arg1);
        n1.args.add(arg2);
        n1.args.add(arg3);
        //
        n2.type = FUNCTION;
        n2.token = "func";
        n2.args.add(arg4);
        n2.args.add(arg5);
        n2.args.add(arg6);
        assertFalse(ExpressionNode.compareNodesGroupBy(n1, n2));
    }

    @Test
    public void testNodeTraversal3() {
        ExpressionNode n1 = FACTORY.newInstance();
        ExpressionNode n2 = FACTORY.newInstance();
        n1.type = FUNCTION;
        n2.type = FUNCTION;
        n1.lhs = FACTORY.newInstance();
        assertFalse(ExpressionNode.compareNodesGroupBy(n1, n2));
    }

    @Test
    public void testNodeTraversal4() {
        ExpressionNode n1 = FACTORY.newInstance();
        ExpressionNode n2 = FACTORY.newInstance();
        //
        n1.type = FUNCTION;
        n1.lhs = FACTORY.newInstance();
        n1.lhs.type = LITERAL;
        //
        n2.type = FUNCTION;
        n2.lhs = FACTORY.newInstance();
        n2.lhs.type = LITERAL;
        assertTrue(ExpressionNode.compareNodesGroupBy(n1, n2));
    }

    @Test
    public void testNodeTraversal5() {
        ExpressionNode n1 = FACTORY.newInstance();
        ExpressionNode n2 = FACTORY.newInstance();
        //
        n1.type = FUNCTION;
        n1.token = "func";
        n1.lhs = FACTORY.newInstance();
        n1.lhs.type = LITERAL;
        //
        n2.type = FUNCTION;
        n2.token = "func";
        n2.lhs = FACTORY.newInstance();
        n2.lhs.type = LITERAL;
        assertTrue(ExpressionNode.compareNodesGroupBy(n1, n2));
    }

    @Test
    public void testNodeTraversal6() {
        ExpressionNode n1 = FACTORY.newInstance();
        ExpressionNode n2 = FACTORY.newInstance();
        //
        n1.type = FUNCTION;
        n1.token = "func";
        n1.lhs = FACTORY.newInstance();
        n1.lhs.type = LITERAL;
        //
        n2.type = FUNCTION;
        n2.token = "diff";
        n2.lhs = FACTORY.newInstance();
        n2.lhs.type = LITERAL;
        assertFalse(ExpressionNode.compareNodesGroupBy(n1, n2));
    }

    @Test
    public void testNodeTraversal7() {
        ExpressionNode n1 = FACTORY.newInstance();
        ExpressionNode n2 = FACTORY.newInstance();
        //
        n1.type = FUNCTION;
        n1.token = "func";
        n1.lhs = FACTORY.newInstance();
        n1.lhs.type = LITERAL;
        n1.lhs.token = "baa";
        //
        n2.type = FUNCTION;
        n2.token = "func";
        n2.lhs = FACTORY.newInstance();
        n2.lhs.type = LITERAL;
        n2.lhs.token = "boo";
        assertFalse(ExpressionNode.compareNodesGroupBy(n1, n2));
    }

    @Test
    public void testNodeTraversal8() {
        ExpressionNode n1 = FACTORY.newInstance();
        ExpressionNode n2 = FACTORY.newInstance();
        //
        n1.type = FUNCTION;
        n1.token = "func";
        n1.lhs = FACTORY.newInstance();
        n1.lhs.type = LITERAL;
        n1.lhs.token = "boo";
        //
        n2.type = FUNCTION;
        n2.token = "func";
        n2.lhs = FACTORY.newInstance();
        n2.lhs.type = LITERAL;
        n2.lhs.token = "boo";
        assertTrue(ExpressionNode.compareNodesGroupBy(n1, n2));
    }

    @Test
    public void testNodeTraversal9() {
        ExpressionNode n1 = FACTORY.newInstance();
        ExpressionNode n2 = FACTORY.newInstance();
        //
        n1.type = FUNCTION;
        n1.rhs = FACTORY.newInstance();
        n1.rhs.type = LITERAL;
        //
        n2.type = FUNCTION;
        n2.rhs = FACTORY.newInstance();
        n2.rhs.type = LITERAL;
        assertTrue(ExpressionNode.compareNodesGroupBy(n1, n2));
    }

    static ExpressionNode createNode(int type, String token, ExpressionNode l, ExpressionNode r) {
        ExpressionNode node = FACTORY.newInstance();
        node.type = type;
        node.token = token;
        node.lhs = l;
        node.rhs = r;
        return node;
    }
}