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
import io.questdb.griffin.model.WindowExpression;
import io.questdb.std.ObjectPool;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;

public class WindowExpressionTest {
    private final ObjectPool<ExpressionNode> nodes = new ObjectPool<>(ExpressionNode.FACTORY, 32);
    private final ObjectPool<WindowExpression> windows = new ObjectPool<>(WindowExpression.FACTORY, 4);

    @Test
    public void testPendingSubsampleClear() {
        final WindowExpression window = pending();
        final ExpressionNode raw = window.getPendingSubsample();
        window.of("renamed", window.getAst());
        Assert.assertSame(raw, window.getPendingSubsample());
        Assert.assertTrue(window.isSubsampleProjectionPending());
        window.clear();
        Assert.assertNull(window.getPendingSubsample());
        Assert.assertFalse(window.isSubsampleProjectionPending());
        Assert.assertFalse(window.hasSubsampleSourceTimestamp());
        Assert.assertFalse(window.isSubsampleKeepFlag());
        Assert.assertEquals(0, window.getSubsamplePosition());
        Assert.assertNull(window.getAst());
        Assert.assertTrue(window.isIncludeIntoWildcard());
        Assert.assertEquals(0, window.getOrderBy().size());
        Assert.assertEquals(0, window.getPartitionBy().size());
    }

    @Test
    public void testPendingSubsampleDeepClone() throws Exception {
        final WindowExpression source = pending();
        source.setSubsampleProjectionPending(false);
        final ExpressionNode target = source.getPendingSubsample().args.getQuick(1);
        target.reassociateConstants(false);
        final WindowExpression clone = source.deepClone(windows, nodes);
        Assert.assertNotSame(source, clone);
        Assert.assertSame(clone, clone.getAst().windowExpression);
        Assert.assertSame(source, source.getAst().windowExpression);
        Assert.assertFalse(clone.isIncludeIntoWildcard());
        Assert.assertTrue(clone.isSubsampleKeepFlag());
        Assert.assertFalse(clone.isSubsampleProjectionPending());
        Assert.assertTrue(clone.hasSubsampleSourceTimestamp());
        Assert.assertEquals(17, clone.getSubsamplePosition());
        Assert.assertNotSame(source.getPendingSubsample(), clone.getPendingSubsample());
        Assert.assertNotSame(source.getPendingSubsample().args, clone.getPendingSubsample().args);
        final ExpressionNode clonedTarget = clone.getPendingSubsample().args.getQuick(1);
        Assert.assertNotSame(target, clonedTarget);
        Assert.assertEquals(31, clonedTarget.position);
        Assert.assertTrue(clonedTarget.isConstantExpression);
        Assert.assertTrue(clonedTarget.isConstFoldLongValid());
        Assert.assertEquals(target.isConstFoldWidening(), clonedTarget.isConstFoldWidening());
        Assert.assertEquals(2, readConstFoldLongValue(target));
        Assert.assertEquals(readConstFoldLongValue(target), readConstFoldLongValue(clonedTarget));
        Assert.assertNotSame(source.getOrderBy(), clone.getOrderBy());
        Assert.assertNotSame(source.getOrderBy().getQuick(0), clone.getOrderBy().getQuick(0));
        Assert.assertNotSame(source.getPartitionBy().getQuick(0), clone.getPartitionBy().getQuick(0));
        clonedTarget.clear();
        Assert.assertEquals(0, readConstFoldLongValue(clonedTarget));
        Assert.assertEquals(2, readConstFoldLongValue(target));
        clone.getAst().rhs.token = "changed";
        clone.getOrderBy().clear();
        clone.getPartitionBy().getQuick(0).token = "other";
        Assert.assertEquals("2", target.token);
        Assert.assertTrue(target.isConstantExpression);
        Assert.assertTrue(target.isConstFoldLongValid());
        Assert.assertEquals("value", source.getAst().rhs.token);
        Assert.assertEquals(1, source.getOrderBy().size());
        Assert.assertEquals("partition", source.getPartitionBy().getQuick(0).token);
    }

    @Test
    public void testPendingSubsampleSpecCopy() {
        final WindowExpression source = pending();
        final WindowExpression ordinary = windows.next();
        ordinary.copySpecFrom(source, nodes);
        Assert.assertNull(ordinary.getPendingSubsample());
        Assert.assertFalse(ordinary.isSubsampleProjectionPending());
        Assert.assertFalse(ordinary.isSubsampleKeepFlag());
        final WindowExpression other = pending();
        final ExpressionNode ownRecipe = other.getPendingSubsample();
        other.copySpecFrom(source, nodes);
        Assert.assertSame(ownRecipe, other.getPendingSubsample());
        Assert.assertTrue(other.isSubsampleProjectionPending());
        Assert.assertNotSame(source.getOrderBy().getQuick(0), other.getOrderBy().getQuick(0));
        Assert.assertNotSame(source.getPartitionBy().getQuick(0), other.getPartitionBy().getQuick(0));
    }

    private static long readConstFoldLongValue(ExpressionNode node) throws Exception {
        final Field field = ExpressionNode.class.getDeclaredField("constFoldLongValue");
        field.setAccessible(true);
        return field.getLong(node);
    }

    private WindowExpression pending() {
        final WindowExpression window = windows.next();
        final ExpressionNode call = nodes.next().of(ExpressionNode.FUNCTION, "minmax", 0, 20);
        call.paramCount = 2;
        call.lhs = nodes.next().of(ExpressionNode.LITERAL, "ts", 0, 20);
        call.rhs = nodes.next().of(ExpressionNode.LITERAL, "value", 0, 27);
        call.windowExpression = window;
        window.of("__keep_subsample", call);
        window.setIncludeIntoWildcard(false);
        window.setSubsampleKeepFlag(true);
        window.addOrderBy(nodes.next().of(ExpressionNode.LITERAL, "ts", 0, 20), 1);
        window.getPartitionBy().add(nodes.next().of(ExpressionNode.LITERAL, "partition", 0, 9));
        final ExpressionNode raw = nodes.next().of(ExpressionNode.FUNCTION, "minmax", 0, 20);
        raw.paramCount = 2;
        raw.args.add(nodes.next().of(ExpressionNode.LITERAL, "value", 0, 27));
        raw.args.add(nodes.next().of(ExpressionNode.CONSTANT, "2", 0, 31));
        window.setPendingSubsample(raw, 17, true);
        return window;
    }
}
