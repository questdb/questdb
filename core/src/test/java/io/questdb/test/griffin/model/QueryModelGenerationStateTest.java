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
import io.questdb.griffin.model.QueryModel;
import io.questdb.griffin.model.QueryModelGenerationState;
import io.questdb.griffin.model.QueryModelWrapper;
import io.questdb.std.ObjectPool;
import org.junit.Assert;
import org.junit.Test;

public class QueryModelGenerationStateTest {
    @Test
    public void testActiveSelfReentryAndLimitIdentityAcrossRepeatedRegions() {
        QueryModel parent = model();
        QueryModel child = model();
        ExpressionNode limit = node("1");
        parent.setNestedModel(new QueryModelWrapper().of(child, 1));
        parent.setLimit(limit, null);
        child.setLimitAdvice(limit, null);
        child.setWhereClause(limit);
        child.setPostJoinWhereClause(limit);
        QueryModelGenerationState state = new QueryModelGenerationState();
        ObjectPool<ExpressionNode> pool = pool();
        state.begin(parent, pool);
        Assert.assertNotSame(limit, child.getWhereClause());
        state.enterModel(parent);
        Assert.assertFalse(state.enterRegion(parent, pool));
        ExpressionNode retained = child.getWhereClause();
        retained.token = "consumed";
        for (int i = 0; i < 4; i++) {
            boolean hasEntered = state.enterRegion(child, pool);
            Assert.assertTrue(hasEntered);
            Assert.assertFalse(limit.implemented);
            Assert.assertSame(limit, parent.getLimitLo());
            Assert.assertSame(limit, child.getLimitAdviceLo());
            Assert.assertSame(child.getWhereClause(), child.getPostJoinWhereClause());
            Assert.assertEquals("1", child.getWhereClause().token);
            Assert.assertEquals("consumed", retained.token);
            Assert.assertNotSame(retained, child.getWhereClause());
            state.enterModel(child);
            ExpressionNode working = child.getWhereClause();
            Assert.assertFalse(state.enterRegion(child, pool));
            state.enterModel(child);
            state.exitModel(child);
            Assert.assertSame(working, child.getWhereClause());
            child.getLimitAdviceLo().implemented = true;
            Assert.assertTrue(parent.getLimitLo().implemented);
            state.exitModel(child);
            state.exitRegion(hasEntered);
        }
        state.exitModel(parent);
        state.clear();
        Assert.assertEquals(0, state.getRetainedNodeCount());
    }

    @Test
    public void testAllocationFailureDoesNotPublishCaptureAndAllowsFreshAttempt() {
        QueryModel model = model();
        model.setWhereClause(node("first"));
        QueryModelWrapper wrapper = new QueryModelWrapper().of(model, 1);
        QueryModelGenerationState state = new QueryModelGenerationState();
        OutOfMemoryError failure = new OutOfMemoryError("injected working-copy allocation");
        ObjectPool<ExpressionNode> failingPool = new ObjectPool<>(ExpressionNode.FACTORY, 1) {
            @Override
            public ExpressionNode next() {
                throw failure;
            }
        };
        try {
            state.begin(wrapper, failingPool);
            Assert.fail();
        } catch (OutOfMemoryError e) {
            Assert.assertSame(failure, e);
        }
        Assert.assertEquals(0, state.getRetainedNodeCount());
        model.setWhereClause(node("second"));
        state.begin(wrapper, pool());
        Assert.assertEquals("second", model.getWhereClause().token);
        state.clear();
        Assert.assertEquals(0, state.getRetainedNodeCount());
    }

    @Test
    public void testCompleteLogicalFieldsAndAttemptReuse() {
        QueryModel root = model();
        QueryModel child = model();
        root.setNestedModel(new QueryModelWrapper().of(child, 1));
        child.setWhereClause(node("where"));
        child.setBackupWhereClause(node("backup"));
        child.setConstWhereClause(node("const"));
        child.setPostJoinWhereClause(node("post"));
        child.setOuterJoinExpressionClause(node("outer"));
        child.addLatestBy(node("latest"));
        QueryModelGenerationState state = new QueryModelGenerationState();
        ObjectPool<ExpressionNode> pool = pool();
        state.begin(root, pool);
        for (int i = 0; i < 3; i++) {
            child.setWhereClause(null);
            child.setBackupWhereClause(null);
            child.setConstWhereClause(null);
            child.setPostJoinWhereClause(null);
            child.getOuterJoinExpressionClause().token = "stolen";
            child.getLatestBy().clear();
            child.setSkipped(true);
            boolean hasEntered = state.enterRegion(child, pool);
            Assert.assertEquals("where", child.getWhereClause().token);
            Assert.assertEquals("backup", child.getBackupWhereClause().token);
            Assert.assertEquals("const", child.getConstWhereClause().token);
            Assert.assertEquals("post", child.getPostJoinWhereClause().token);
            Assert.assertEquals("outer", child.getOuterJoinExpressionClause().token);
            Assert.assertEquals("latest", child.getLatestBy().getQuick(0).token);
            Assert.assertFalse(child.isSkipped());
            state.exitRegion(hasEntered);
        }
        state.clear();
        pool.clear();
        child.setWhereClause(node("new attempt"));
        state.begin(root, pool);
        Assert.assertEquals("new attempt", child.getWhereClause().token);
        state.clear();
    }

    @Test
    public void testNaryQueryEdgesSavedBeforePredicatesAreCleared() {
        QueryModel root = model();
        QueryModel scalar = model();
        QueryModel tail = model();
        root.setUnionModel(tail);
        ExpressionNode args = node("between");
        ExpressionNode query = node("query");
        query.queryModel = new QueryModelWrapper().of(scalar, 1);
        args.args.add(query);
        args.args.add(query);
        args.paramCount = 2;
        root.setWhereClause(args);
        scalar.setWhereClause(node("scalar"));
        tail.setWhereClause(node("tail"));
        QueryModelGenerationState state = new QueryModelGenerationState();
        ObjectPool<ExpressionNode> pool = pool();
        state.begin(root, pool);
        // Include the shared default SAMPLE BY ZERO_OFFSET expression once.
        Assert.assertEquals(8, state.getDiscoveryCount());
        Assert.assertEquals(8, state.getPreparationCount());
        Assert.assertEquals(4, state.getWorkingCopyCount());
        root.setWhereClause(null);
        scalar.setWhereClause(null);
        tail.setWhereClause(null);
        // Public nested generation can still find a model whose original expression vanished.
        Assert.assertTrue(state.enterRegion(scalar, pool));
        Assert.assertEquals("scalar", scalar.getWhereClause().token);
        state.exitRegion(true);
        Assert.assertTrue(state.enterRegion(tail, pool));
        Assert.assertEquals("tail", tail.getWhereClause().token);
        state.exitRegion(true);
        Assert.assertEquals(14, state.getPreparationCount());
        Assert.assertEquals(6, state.getWorkingCopyCount());
        state.clear();
    }

    @Test
    public void testNoSharingOnlyDiscoversWithoutReplacingPredicates() {
        QueryModel root = model();
        QueryModel child = model();
        root.setNestedModel(child);
        ExpressionNode predicate = node("true");
        root.setWhereClause(predicate);
        child.setWhereClause(predicate);
        QueryModelGenerationState state = new QueryModelGenerationState();
        state.begin(root, pool());
        Assert.assertEquals(4, state.getDiscoveryCount());
        Assert.assertEquals(0, state.getWorkingCopyCount());
        Assert.assertEquals(0, state.getPreparationCount());
        Assert.assertEquals(0, state.getRetainedNodeCount());
        Assert.assertSame(predicate, root.getWhereClause());
        Assert.assertSame(predicate, child.getWhereClause());
    }

    @Test
    public void testOverlappingRegionsCopyEachDistinctPredicateOncePerPreparation() {
        QueryModel root = model();
        QueryModel left = model();
        QueryModel right = model();
        QueryModel shared = model();
        root.setNestedModel(left);
        root.setUnionModel(right);
        left.setNestedModel(new QueryModelWrapper().of(shared, 1));
        right.setNestedModel(new QueryModelWrapper().of(shared, 2));
        ExpressionNode predicate = node("true");
        left.setWhereClause(predicate);
        right.setWhereClause(predicate);
        shared.setWhereClause(predicate);
        QueryModelGenerationState state = new QueryModelGenerationState();
        ObjectPool<ExpressionNode> pool = pool();
        state.begin(root, pool);
        Assert.assertEquals(6, state.getDiscoveryCount());
        Assert.assertEquals(1, state.getWorkingCopyCount());
        for (int i = 0; i < 5; i++) {
            Assert.assertTrue(state.enterRegion(left, pool));
            Assert.assertSame(left.getWhereClause(), shared.getWhereClause());
            state.exitRegion(true);
            Assert.assertTrue(state.enterRegion(right, pool));
            Assert.assertSame(right.getWhereClause(), shared.getWhereClause());
            state.exitRegion(true);
        }
        Assert.assertEquals(46, state.getPreparationCount());
        Assert.assertEquals(11, state.getWorkingCopyCount());
        state.clear();
    }

    private static QueryModel model() {
        return QueryModel.FACTORY.newInstance();
    }

    private static ExpressionNode node(String token) {
        return ExpressionNode.FACTORY.newInstance().of(ExpressionNode.CONSTANT, token, 0, 0);
    }

    private static ObjectPool<ExpressionNode> pool() {
        return new ObjectPool<>(ExpressionNode.FACTORY, 32);
    }
}
