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

package io.questdb.test.griffin.model;

import io.questdb.griffin.model.ExpressionNode;
import io.questdb.std.ObjectPool;
import org.junit.Assert;
import org.junit.Test;

public class ExpressionNodeTest {

    @Test
    public void testClearResetsInheritedTimestampOrder() {
        final ObjectPool<ExpressionNode> pool = new ObjectPool<>(ExpressionNode.FACTORY, 1);
        final ExpressionNode node = pool.next().of(ExpressionNode.LITERAL, "ts", 0, 0);
        Assert.assertFalse(node.isTimestampOrderInherited);

        node.isTimestampOrderInherited = true;
        node.clear();
        Assert.assertFalse(node.isTimestampOrderInherited);

        node.isTimestampOrderInherited = true;
        node.of(ExpressionNode.LITERAL, "other_ts", 0, 0);
        Assert.assertFalse(node.isTimestampOrderInherited);

        node.isTimestampOrderInherited = true;
        pool.clear();
        Assert.assertSame(node, pool.next());
        Assert.assertFalse(node.isTimestampOrderInherited);
    }

    @Test
    public void testOperandsFollowParamCount() {
        final ObjectPool<ExpressionNode> pool = new ObjectPool<>(ExpressionNode.FACTORY, 8);
        final ExpressionNode a = pool.next().of(ExpressionNode.LITERAL, "a", 0, 0);
        final ExpressionNode b = pool.next().of(ExpressionNode.LITERAL, "b", 0, 0);
        final ExpressionNode c = pool.next().of(ExpressionNode.LITERAL, "c", 0, 0);
        final ExpressionNode d = pool.next().of(ExpressionNode.LITERAL, "d", 0, 0);

        final ExpressionNode leaf = pool.next().of(ExpressionNode.LITERAL, "x", 0, 0);
        Assert.assertEquals(2, leaf.operandCount());
        Assert.assertNull(leaf.operandAt(0));
        Assert.assertNull(leaf.operandAt(1));

        final ExpressionNode unary = pool.next().of(ExpressionNode.OPERATION, "-", 0, 0);
        unary.paramCount = 1;
        unary.rhs = a;
        Assert.assertEquals(2, unary.operandCount());
        Assert.assertNull(unary.operandAt(0));
        Assert.assertSame(a, unary.operandAt(1));

        final ExpressionNode binary = pool.next().of(ExpressionNode.OPERATION, "+", 0, 0);
        binary.paramCount = 2;
        binary.lhs = a;
        binary.rhs = b;
        Assert.assertEquals(2, binary.operandCount());
        Assert.assertSame(a, binary.operandAt(0));
        Assert.assertSame(b, binary.operandAt(1));
        binary.setOperandAt(0, c);
        binary.setOperandAt(1, d);
        Assert.assertSame(c, binary.lhs);
        Assert.assertSame(d, binary.rhs);

        final ExpressionNode call = pool.next().of(ExpressionNode.FUNCTION, "f", 0, 0);
        call.paramCount = 3;
        call.args.add(a);
        call.args.add(b);
        call.args.add(c);
        Assert.assertEquals(3, call.operandCount());
        Assert.assertSame(a, call.operandAt(0));
        Assert.assertSame(c, call.operandAt(2));
        call.setOperandAt(1, d);
        Assert.assertSame(d, call.args.getQuick(1));
        Assert.assertNull(call.lhs);
        Assert.assertNull(call.rhs);
    }

    @Test
    public void testDeepCloneAndCopyFromCarryInheritedTimestampOrder() {
        final ObjectPool<ExpressionNode> pool = new ObjectPool<>(ExpressionNode.FACTORY, 4);
        final ExpressionNode node = pool.next().of(ExpressionNode.LITERAL, "ts", 0, 0);
        node.isTimestampOrderInherited = true;

        final ExpressionNode clone = ExpressionNode.deepClone(pool, node);
        Assert.assertNotSame(node, clone);
        Assert.assertTrue(clone.isTimestampOrderInherited);

        final ExpressionNode copy = pool.next().copyFrom(node);
        Assert.assertTrue(copy.isTimestampOrderInherited);

        node.isTimestampOrderInherited = false;
        Assert.assertTrue(clone.isTimestampOrderInherited);
        copy.copyFrom(node);
        Assert.assertFalse(copy.isTimestampOrderInherited);
        Assert.assertFalse(ExpressionNode.deepClone(pool, node).isTimestampOrderInherited);
    }

    @Test
    public void testEmptyLiteral() {
        ExpressionNode node = ExpressionNode.FACTORY.newInstance();
        node.of(ExpressionNode.LITERAL, "", 0, 0);
        Assert.assertEquals(ExpressionNode.LITERAL, node.type);
    }

}
