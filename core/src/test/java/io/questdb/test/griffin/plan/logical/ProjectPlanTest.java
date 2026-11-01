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

package io.questdb.test.griffin.plan.logical;

import io.questdb.cairo.ColumnType;
import io.questdb.griffin.LogicalPlans;
import io.questdb.griffin.optimiser.PlanVerifier;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.ConstantExpression;
import io.questdb.griffin.plan.logical.FunctionSourcePlan;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.PlanToFactoryRules;
import io.questdb.griffin.plan.logical.ProjectPlan;
import io.questdb.griffin.plan.logical.WindowPlan;
import io.questdb.std.IntHashSet;
import org.junit.Assert;
import org.junit.Test;

public class ProjectPlanTest {
    @Test
    public void testRepeatedColumnIgnoresComputedExpressions() {
        final ProjectPlan project = project(source(), 2, 1, 3);
        final IntHashSet columnIds = new IntHashSet();
        columnIds.add(2);
        Assert.assertFalse(LogicalPlans.hasRepeatedColumn(project, columnIds));
        project.getExpressions().setQuick(0, new ConstantExpression().ofLong(1, 0));
        Assert.assertFalse(LogicalPlans.hasRepeatedColumn(project, columnIds));
        project.getExpressions().setQuick(2, new ColumnExpression().of(1, ColumnType.LONG, 0));
        Assert.assertTrue(LogicalPlans.hasRepeatedColumn(project, columnIds));
    }

    @Test
    public void testColumnReferenceCountsFollowRewrites() {
        final FunctionSourcePlan source = source();
        final ProjectPlan project = project(source, 2, 1, 3);
        final IntHashSet columnIds = new IntHashSet();
        project.deriveColumnReferenceCounts(columnIds);
        Assert.assertTrue(LogicalPlans.isColumnOnlyProjection(project));
        Assert.assertEquals(3, project.getDistinctColumnCount());
        project.getExpressions().setQuick(2, new ColumnExpression().of(1, ColumnType.LONG, 0));
        project.deriveColumnReferenceCounts(columnIds);
        Assert.assertFalse(LogicalPlans.isColumnOnlyProjection(project));
        Assert.assertEquals(2, project.getDistinctColumnCount());
        project.getExpressions().setPos(2);
        project.deriveColumnReferenceCounts(columnIds);
        Assert.assertTrue(LogicalPlans.isColumnOnlyProjection(project));
        project.clear();
        Assert.assertEquals(-1, project.getDistinctColumnCount());
        Assert.assertEquals(-1, project.getDistinctWindowColumnCount());
        Assert.assertEquals(0, columnIds.size());
    }

    @Test
    public void testVerifierLeavesMutablePlansWithoutReferenceCounts() {
        final ProjectPlan project = project(source(), 2, 1, 3);
        final PlanVerifier verifier = PlanVerifier.newStandalone();
        verifier.verifyBound(project);
        Assert.assertEquals(-1, project.getDistinctColumnCount());
        project.getExpressions().setQuick(2, new ColumnExpression().of(1, ColumnType.LONG, 0));
        Assert.assertFalse(LogicalPlans.isColumnOnlyProjection(project));
        verifier.verifyBound(project);
        Assert.assertFalse(LogicalPlans.isColumnOnlyProjection(project));
        Assert.assertEquals(-1, project.getDistinctColumnCount());
    }

    @Test
    public void testWindowProjectionCountsDistinctFunctionsAndAllowsRepeatedInputs() {
        final FunctionSourcePlan source = source();
        final WindowPlan window = new WindowPlan().of(source, 0);
        window.getOutput().copyFrom(source.getOutput());
        window.getFunctionColumnIds().add(4);
        window.getFunctionColumnIds().add(5);
        window.getOutput().add(4, "w1", ColumnType.LONG, true);
        window.getOutput().add(5, "w2", ColumnType.LONG, true);
        final IntHashSet columnIds = new IntHashSet();
        final ProjectPlan reordered = project(window, 5, 1, 4, 1);
        Assert.assertTrue(PlanToFactoryRules.isWindowOutputProjection(reordered, window));
        reordered.deriveColumnReferenceCounts(columnIds);
        Assert.assertEquals(3, reordered.getDistinctColumnCount());
        Assert.assertEquals(2, reordered.getDistinctWindowColumnCount());
        Assert.assertTrue(PlanToFactoryRules.isWindowOutputProjection(reordered, window));
        Assert.assertFalse(LogicalPlans.isColumnOnlyProjection(reordered));
        reordered.getExpressions().setQuick(2, new ColumnExpression().of(5, ColumnType.LONG, 0));
        reordered.deriveColumnReferenceCounts(columnIds);
        Assert.assertFalse(PlanToFactoryRules.isWindowOutputProjection(reordered, window));
        final ProjectPlan duplicated = project(window, 5, 4, 5);
        duplicated.deriveColumnReferenceCounts(columnIds);
        Assert.assertFalse(PlanToFactoryRules.isWindowOutputProjection(duplicated, window));
    }

    private static ProjectPlan project(LogicalPlan source, int... columnIds) {
        final ProjectPlan project = new ProjectPlan().of(source, 0);
        for (int i = 0; i < columnIds.length; i++) {
            project.getExpressions().add(new ColumnExpression().of(columnIds[i], ColumnType.LONG, 0));
            project.getOutput().add(10 + i, "c" + i, ColumnType.LONG, true);
        }
        return project;
    }

    private static FunctionSourcePlan source() {
        final FunctionSourcePlan source = new FunctionSourcePlan().of(0);
        source.getOutput().add(1, "a", ColumnType.LONG, true);
        source.getOutput().add(2, "b", ColumnType.LONG, true);
        source.getOutput().add(3, "c", ColumnType.LONG, true);
        return source;
    }
}
