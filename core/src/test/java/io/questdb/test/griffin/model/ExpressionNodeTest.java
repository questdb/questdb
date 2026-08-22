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

import io.questdb.cairo.ColumnType;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.ScalarTimestampBoundHolder;
import io.questdb.std.ObjectPool;
import io.questdb.std.str.AsciiCharSequence;
import io.questdb.std.str.Utf8String;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class ExpressionNodeTest {

    @Test
    public void testClearResetsScalarBoundHolder() {
        // ExpressionNode instances are pooled and recycled across compiles. A holder surviving
        // clear() would let a later, unrelated query resolve its sub-query node to a stale
        // ScalarSubQueryBoundRefFunction and read a bound frozen by a previous execution.
        final ObjectPool<ExpressionNode> pool = new ObjectPool<>(ExpressionNode.FACTORY, 8);
        final ExpressionNode node = pool.next().of(ExpressionNode.QUERY, "query", 0, 0);
        node.scalarBoundHolder = new ScalarTimestampBoundHolder(ColumnType.TIMESTAMP);

        node.clear();
        Assert.assertNull(node.scalarBoundHolder);
    }

    @Test
    public void testDeepCloneAndCopyFromCarryLateralDepth() {
        // LateralJoinRewriter pass 1 tags correlated refs with lateralDepth and later passes
        // branch on it (allLiteralsAreCorrelated, hasCorrelatedExprAtDepth, unqualified-ref
        // substitution). deepClone must carry the tag like copyFrom does, per the
        // "update deepClone method after adding a new field" invariant in ExpressionNode.
        final ObjectPool<ExpressionNode> pool = new ObjectPool<>(ExpressionNode.FACTORY, 8);

        final ExpressionNode node = pool.next().of(ExpressionNode.LITERAL, "x", 0, 0);
        node.lateralDepth = 2;

        final ExpressionNode clone = ExpressionNode.deepClone(pool, node);
        Assert.assertEquals(2, clone.lateralDepth);

        final ExpressionNode copy = pool.next().copyFrom(node);
        Assert.assertEquals(2, copy.lateralDepth);
    }

    @Test
    public void testDeepCloneAndCopyFromCarryScalarBoundHolder() {
        // The holder is a compile-time link shared by reference (like queryModel): the pruning
        // bound publishes a single frozen value into it and every residual re-compile - including
        // ones fed a cloned filter expression on the filter-stealing path - must read that same
        // holder. Dropping it on clone makes FunctionParser re-open the sub-query per worker.
        final ObjectPool<ExpressionNode> pool = new ObjectPool<>(ExpressionNode.FACTORY, 8);
        final ScalarTimestampBoundHolder holder = new ScalarTimestampBoundHolder(ColumnType.TIMESTAMP);

        final ExpressionNode node = pool.next().of(ExpressionNode.QUERY, "query", 0, 0);
        node.scalarBoundHolder = holder;

        final ExpressionNode clone = ExpressionNode.deepClone(pool, node);
        Assert.assertSame(holder, clone.scalarBoundHolder);

        final ExpressionNode copy = pool.next().copyFrom(node);
        Assert.assertSame(holder, copy.scalarBoundHolder);
    }

    @Test
    public void testDeepCloneCopyFromAndClearCarryFilterExpression() {
        // filterExpression holds an aggregate's FILTER (WHERE ...) condition between parsing and
        // SqlOptimiser.lowerAggregateFilters(). It obeys the same rules as every other node field:
        // deepClone copies the subtree rather than aliasing it, copyFrom carries it, and clear()
        // drops it so a recycled node cannot leak a condition into an unrelated query.
        final ObjectPool<ExpressionNode> pool = new ObjectPool<>(ExpressionNode.FACTORY, 8);

        final ExpressionNode node = pool.next().of(ExpressionNode.FUNCTION, "sum", 0, 0);
        node.filterExpression = pool.next().of(ExpressionNode.LITERAL, "cond", 0, 0);

        final ExpressionNode clone = ExpressionNode.deepClone(pool, node);
        Assert.assertNotNull(clone.filterExpression);
        Assert.assertNotSame(node.filterExpression, clone.filterExpression);
        TestUtils.assertEquals("cond", clone.filterExpression.token);

        final ExpressionNode copy = pool.next().copyFrom(node);
        Assert.assertSame(node.filterExpression, copy.filterExpression);

        node.clear();
        Assert.assertNull(node.filterExpression);
    }

    @Test
    public void testDeepHashCodeConsistentWithCompareNodesExact() {
        // AsciiCharSequence does not override hashCode(), so it uses identity-based Object.hashCode().
        // This test verifies that deepHashCode uses content-based hashing for tokens,
        // consistent with compareNodesExact which uses Chars.equals for comparison.
        AsciiCharSequence token1 = new AsciiCharSequence().of(new Utf8String("test"));
        AsciiCharSequence token2 = new AsciiCharSequence().of(new Utf8String("test"));

        // Sanity check: tokens are different instances with same content
        Assert.assertNotSame(token1, token2);

        ExpressionNode node1 = ExpressionNode.FACTORY.newInstance();
        ExpressionNode node2 = ExpressionNode.FACTORY.newInstance();

        // Use CONSTANT type which uses case-sensitive Chars.equals in compareNodesExact
        node1.of(ExpressionNode.CONSTANT, token1, 0, 0);
        node2.of(ExpressionNode.CONSTANT, token2, 0, 0);

        // Nodes should be equal by content
        Assert.assertTrue(ExpressionNode.compareNodesExact(node1, node2));

        // Hash codes must be equal for equal nodes (hash/equality contract)
        Assert.assertEquals(ExpressionNode.deepHashCode(node1), ExpressionNode.deepHashCode(node2));
    }

    @Test
    public void testEmptyLiteral() {
        ExpressionNode node = ExpressionNode.FACTORY.newInstance();
        node.of(ExpressionNode.LITERAL, "", 0, 0);
        Assert.assertEquals(ExpressionNode.LITERAL, node.type);
    }

    @Test
    public void testFilterExpressionDistinguishesNodes() {
        // Two aggregates that differ only in their FILTER condition must not compare equal, or a
        // deduplicating pass could collapse them into one column. The hash has to agree.
        final ObjectPool<ExpressionNode> pool = new ObjectPool<>(ExpressionNode.FACTORY, 8);

        final ExpressionNode a = pool.next().of(ExpressionNode.FUNCTION, "sum", 0, 0);
        a.filterExpression = pool.next().of(ExpressionNode.LITERAL, "a", 0, 0);
        final ExpressionNode b = pool.next().of(ExpressionNode.FUNCTION, "sum", 0, 0);
        b.filterExpression = pool.next().of(ExpressionNode.LITERAL, "b", 0, 0);
        final ExpressionNode c = pool.next().of(ExpressionNode.FUNCTION, "sum", 0, 0);

        Assert.assertFalse(ExpressionNode.compareNodesExact(a, b));
        Assert.assertFalse(ExpressionNode.compareNodesExact(a, c));
        Assert.assertNotEquals(ExpressionNode.deepHashCode(a), ExpressionNode.deepHashCode(b));

        final ExpressionNode a2 = pool.next().of(ExpressionNode.FUNCTION, "sum", 0, 0);
        a2.filterExpression = pool.next().of(ExpressionNode.LITERAL, "a", 0, 0);
        Assert.assertTrue(ExpressionNode.compareNodesExact(a, a2));
        Assert.assertEquals(ExpressionNode.deepHashCode(a), ExpressionNode.deepHashCode(a2));
    }

    @Test
    public void testFilterLoweredMarkerDistinguishesNodes() {
        // A CASE the lowering synthesized still owes a type rejection that FunctionParser applies
        // later, so it must not compare equal to an identical CASE the user wrote by hand - a
        // deduplicating pass would otherwise keep the unflagged one and drop the rejection with it.
        final ObjectPool<ExpressionNode> pool = new ObjectPool<>(ExpressionNode.FACTORY, 8);

        final ExpressionNode lowered = pool.next().of(ExpressionNode.FUNCTION, "case", 0, 0);
        lowered.isFilterLowered = true;
        final ExpressionNode handWritten = pool.next().of(ExpressionNode.FUNCTION, "case", 0, 0);

        Assert.assertFalse(ExpressionNode.compareNodesExact(lowered, handWritten));
        Assert.assertNotEquals(
                ExpressionNode.deepHashCode(lowered),
                ExpressionNode.deepHashCode(handWritten)
        );

        // two lowered nodes still match, so identical filtered aggregates keep deduplicating
        final ExpressionNode lowered2 = pool.next().of(ExpressionNode.FUNCTION, "case", 0, 0);
        lowered2.isFilterLowered = true;
        Assert.assertTrue(ExpressionNode.compareNodesExact(lowered, lowered2));
        Assert.assertEquals(ExpressionNode.deepHashCode(lowered), ExpressionNode.deepHashCode(lowered2));

        // and the marker does not survive recycling
        lowered.clear();
        Assert.assertFalse(lowered.isFilterLowered);
    }
}
