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

package io.questdb.griffin;

import io.questdb.griffin.plan.logical.AggregatePlan;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.CursorExpression;
import io.questdb.griffin.plan.logical.FillPlan;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.JoinPlan;
import io.questdb.griffin.plan.logical.LimitPlan;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.ProjectPlan;
import io.questdb.griffin.plan.logical.SortPlan;
import io.questdb.griffin.plan.logical.WindowPlan;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

/**
 * Merges column projections into their consumers and moves sorts across the projections that keep
 * their keys. On request of {@link AggregateRewritePass}, also moves a LIMIT below a projection over
 * an aggregate.
 */
final class ProjectionMergePass {
    private final OptimiserContext context;

    ProjectionMergePass(OptimiserContext context) {
        this.context = context;
    }

    /**
     * A column projection that only selects columns, keeping their names and values.
     */
    private static ProjectPlan absorbableProject(LogicalPlan input) {
        if (!(input instanceof ProjectPlan inner) || !LogicalPlans.isColumnProjection(inner) || inner.hasTimestampDeclaration()) {
            return null;
        }
        final OutputSchema innerInput = inner.getInput().getOutput();
        if (innerInput.hasColumnQualifiers()) {
            return null;
        }
        for (int i = 0, n = inner.getExpressions().size(); i < n; i++) {
            final ColumnExpression column = (ColumnExpression) inner.getExpressions().getQuick(i);
            final int index = innerInput.getColumnIndexById(column.getColumnId());
            if (column.isCast() || index < 0 || !Chars.equals(inner.getOutput().getColumnName(i), innerInput.getColumnName(index))) {
                return null;
            }
        }
        return inner;
    }

    private static boolean isCompleteColumnPermutation(ProjectPlan project) {
        final OutputSchema input = project.getInput().getOutput();
        final int count = project.getExpressions().size();
        if (project.hasTimestampDeclaration() || count != input.getColumnCount() || !LogicalPlans.isColumnProjection(project)) {
            return false;
        }
        for (int i = 0; i < count; i++) {
            final ColumnExpression column = (ColumnExpression) project.getExpressions().getQuick(i);
            final int index = input.getColumnIndexById(column.getColumnId());
            if (index < 0 || column.getDataType() != input.getColumnType(index)
                    || !Chars.equals(project.getOutput().getColumnName(i), input.getColumnName(index))) {
                return false;
            }
            for (int k = 0; k < i; k++) {
                if (((ColumnExpression) project.getExpressions().getQuick(k)).getColumnId() == column.getColumnId()) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * The sort orders a column projection of grouped rows by columns the projection passes through
     * unchanged, so it can order the grouped rows instead and let a column projection above it merge
     * with this one.
     */
    private static boolean isSortableBelow(ProjectPlan project, SortPlan sort) {
        final LogicalPlan input = project.getInput();
        if (!(input instanceof AggregatePlan || input instanceof FillPlan)
                || sort.isMarkoutHorizon() || project.hasTimestampDeclaration() || !LogicalPlans.isColumnProjection(project)
                || LogicalPlans.hasRepeatedColumn(project)) {
            return false;
        }
        final IntList ids = sort.getColumnIds();
        for (int i = 0, n = ids.size(); i < n; i++) {
            final int index = project.getOutput().getColumnIndexById(ids.getQuick(i));
            if (index < 0 || !(project.getExpressions().getQuick(index) instanceof ColumnExpression column) || column.isCast()) {
                return false;
            }
        }
        return true;
    }

    /**
     * Every sort key is an aggregate output the projection selects unchanged.
     */
    private static boolean isSortedBySelectedAggregateOutputs(SortPlan sort, ProjectPlan project, AggregatePlan aggregate) {
        final IntList ids = sort.getColumnIds();
        for (int i = 0, n = ids.size(); i < n; i++) {
            final int index = project.getOutput().getColumnIndexById(ids.getQuick(i));
            if (index < 0 || !(project.getExpressions().getQuick(index) instanceof ColumnExpression column) || column.isCast()
                    || aggregate.getOutput().getColumnIndexById(column.getColumnId()) < 0) {
                return false;
            }
        }
        return true;
    }

    /**
     * The expression reads only columns the projection passes through unchanged.
     */
    private static boolean readsPassedColumns(BoundExpression expression, ProjectPlan project) {
        if (expression instanceof ColumnExpression column) {
            final int index = project.getOutput().getColumnIndexById(column.getColumnId());
            return index >= 0 && project.getExpressions().getQuick(index) instanceof ColumnExpression passed
                    && passed.getDataType() == column.getDataType();
        }
        if (expression instanceof FunctionExpression call) {
            for (int i = 0, n = call.getArgumentCount(); i < n; i++) {
                if (!readsPassedColumns(call.argumentAt(i), project)) {
                    return false;
                }
            }
            return true;
        }
        return !(expression instanceof CursorExpression);
    }

    private static LogicalPlan sortBelowProject(SortPlan sort, ProjectPlan project) {
        final IntList ids = sort.getColumnIds();
        for (int i = 0, n = ids.size(); i < n; i++) {
            ids.setQuick(i, ((ColumnExpression) project.getExpressions().getQuick(project.getOutput().getColumnIndexById(ids.getQuick(i)))).getColumnId());
        }
        sort.replaceInput(0, project.getInput());
        project.replaceInput(0, sort);
        if (sort.getOutput().getTimestampIndex() >= 0) {
            project.getOutput().setTimestampIndex(LogicalPlans.projectedColumnIndex(project, ids.getQuick(0)));
        }
        return project;
    }

    /**
     * A computing projection reads the input of a column projection beneath it directly.
     */
    private void absorbColumnProject(ProjectPlan project) {
        if (project.hasUpdateConversions() || project.hasPrunedComputedColumns()) {
            return;
        }
        final ProjectPlan inner = absorbableProject(project.getInput());
        if (inner == null) {
            return;
        }
        for (int i = 0, n = project.getExpressions().size(); i < n; i++) {
            if (!LogicalPlans.readsOnly(project.getExpressions().getQuick(i), inner.getOutput())) {
                return;
            }
        }
        for (int i = 0, n = project.getExpressions().size(); i < n; i++) {
            project.getExpressions().setQuick(i, context.getRewriter().remapColumns(project.getExpressions().getQuick(i), inner));
        }
        project.replaceInput(0, inner.getInput());
    }

    /**
     * An aggregate reads the input of a column projection beneath it directly.
     */
    private void absorbColumnProject(AggregatePlan aggregate) {
        final ProjectPlan inner = absorbableProject(aggregate.getInput());
        if (inner == null) {
            return;
        }
        final ObjList<BoundExpression> keys = aggregate.getGroupingExpressions();
        final ObjList<FunctionExpression> calls = aggregate.getAggregates();
        for (int i = 0, n = keys.size(); i < n; i++) {
            if (!LogicalPlans.readsOnly(keys.getQuick(i), inner.getOutput())) {
                return;
            }
        }
        for (int i = 0, n = calls.size(); i < n; i++) {
            if (!LogicalPlans.readsOnly(calls.getQuick(i), inner.getOutput())) {
                return;
            }
        }
        for (int i = 0, n = keys.size(); i < n; i++) {
            keys.setQuick(i, context.getRewriter().remapColumns(keys.getQuick(i), inner));
        }
        for (int i = 0, n = calls.size(); i < n; i++) {
            calls.setQuick(i, (FunctionExpression) context.getRewriter().remapColumns(calls.getQuick(i), inner));
        }
        final IntList sharedIds = aggregate.getSharedInputIds();
        for (int i = 0, n = sharedIds.size(); i < n; i++) {
            final int index = inner.getOutput().getColumnIndexById(sharedIds.getQuick(i));
            if (index >= 0) {
                sharedIds.setQuick(i, ((ColumnExpression) inner.getExpressions().getQuick(index)).getColumnId());
            }
        }
        aggregate.replaceInput(0, inner.getInput());
    }

    private LogicalPlan sortOverProject(ProjectPlan project, SortPlan sort) {
        for (int i = 0, n = sort.getColumnIds().size(); i < n; i++) {
            sort.getColumnIds().setQuick(i, project.getOutput().getColumnId(LogicalPlans.projectedUncastColumnIndex(project, sort.getColumnIds().getQuick(i))));
        }
        project.replaceInput(0, sort.getInput());
        sort.replaceInput(0, collapseColumnProjects(project));
        return sort;
    }

    static boolean projectsSortKeys(ProjectPlan project, SortPlan sort) {
        for (int i = 0, n = sort.getColumnIds().size(); i < n; i++) {
            if (LogicalPlans.projectedUncastColumnIndex(project, sort.getColumnIds().getQuick(i)) < 0) {
                return false;
            }
        }
        return true;
    }

    LogicalPlan collapseColumnProjects(LogicalPlan plan) {
        for (int i = 0, n = plan.inputCount(); i < n; i++) {
            final LogicalPlan input = plan.inputAt(i);
            final LogicalPlan collapsed = collapseColumnProjects(input);
            if (collapsed != input) {
                plan.replaceInput(i, collapsed);
            }
        }
        if (plan instanceof AggregatePlan aggregate) {
            absorbColumnProject(aggregate);
            return plan;
        }
        if (!(plan instanceof ProjectPlan project)) {
            return plan;
        }
        if (!LogicalPlans.isColumnProjection(project)) {
            absorbColumnProject(project);
            if (project.getInput() instanceof SortPlan sort
                    && !project.hasTimestampDeclaration()
                    && sort.getInput() instanceof JoinPlan
                    && !sort.isMarkoutHorizon()
                    && projectsSortKeys(project, sort)) {
                return sortOverProject(project, sort);
            }
            return plan;
        }
        if (project.getInput() instanceof SortPlan sort) {
            LogicalPlan input = sort.getInput();
            while (input instanceof ProjectPlan permutation && isCompleteColumnPermutation(permutation)) {
                input = permutation.getInput();
            }
            if (isCompleteColumnPermutation(project) && input instanceof AggregatePlan aggregate
                    && aggregate.getGroupingExpressions().size() > 0
                    || (input instanceof JoinPlan || sort.getInput() instanceof WindowPlan)
                    && !sort.isMarkoutHorizon() && projectsSortKeys(project, sort)) {
                return sortOverProject(project, sort);
            }
            if (sort.getInput() instanceof ProjectPlan grouped && isSortableBelow(grouped, sort)) {
                project.replaceInput(0, sortBelowProject(sort, grouped));
            }
        }
        while (true) {
            LogicalPlan input = project.getInput();
            // LIMIT observes rows and order, not a pure column mapping. Preserve
            // its position while composing projections on either side of it.
            LimitPlan boundary = null;
            while (input instanceof LimitPlan limit) {
                boundary = limit;
                input = limit.getInput();
            }
            if (!(input instanceof ProjectPlan inner) || !LogicalPlans.isColumnProjection(inner)) {
                return project;
            }
            // Generation elides an identity declaration over an undesignated input, which
            // leaves the result undesignated. Merging would designate it.
            if (project.hasTimestampDeclaration() && inner.getOutput().getTimestampIndex() < 0) {
                return project;
            }
            if (boundary != null) {
                boolean isIdentity = project.getExpressions().size() == inner.getExpressions().size()
                        && project.getOutput().getTimestampIndex() == inner.getOutput().getTimestampIndex();
                boolean hasComputedReference = false;
                for (int i = 0, n = project.getExpressions().size(); i < n; i++) {
                    final ColumnExpression column = (ColumnExpression) project.getExpressions().getQuick(i);
                    isIdentity = isIdentity && column.getColumnId() == inner.getOutput().getColumnId(i)
                            && Chars.equals(project.getOutput().getColumnName(i), inner.getOutput().getColumnName(i));
                    if (!column.isDirectReference()) {
                        hasComputedReference = true;
                        for (int k = 0; k < i; k++) {
                            if (((ColumnExpression) project.getExpressions().getQuick(k)).getColumnId() == column.getColumnId()) {
                                return project;
                            }
                        }
                    }
                }
                // Identity projections disappear during generation. Moving their alias
                // mapping through LIMIT would instead move the surviving factory.
                if (isIdentity && hasComputedReference) {
                    return project;
                }
            }
            for (int i = 0, n = project.getExpressions().size(); i < n; i++) {
                project.getExpressions().setQuick(i, context.getRewriter().remapColumns(project.getExpressions().getQuick(i), inner));
            }
            if (inner.hasTimestampDeclaration() && project.getOutput().getTimestampIndex() >= 0) {
                project.markTimestampDeclaration();
            }
            // Pruning already resolved duplicate timestamp aliases. Keep the outer
            // schema exactly. Order-sensitive rules have finished, so a declaration
            // boundary can now merge without changing its observable metadata.
            if (boundary == null) {
                project.replaceInput(0, inner.getInput());
            } else {
                boundary.replaceInput(0, inner.getInput());
                LogicalPlans.deriveLimits(project.getInput());
            }
        }
    }

    /**
     * Moves a LIMIT, with the sort beneath it, below a projection over an aggregate, so the
     * projection computes only the rows the LIMIT keeps. A sort moves only when its keys are aggregate
     * outputs the projection selects.
     */
    LogicalPlan limitBelowProjection(LimitPlan limit, SortPlan sort, ProjectPlan project, AggregatePlan aggregate) {
        if (sort != null && !isSortedBySelectedAggregateOutputs(sort, project, aggregate)) {
            return limit;
        }
        final LogicalPlan below = sort == null ? aggregate : sort;
        if (sort != null) {
            final IntList ids = sort.getColumnIds();
            for (int i = 0, n = ids.size(); i < n; i++) {
                ids.setQuick(i, ((ColumnExpression) project.getExpressions().getQuick(project.getOutput().getColumnIndexById(ids.getQuick(i)))).getColumnId());
            }
            sort.replaceInput(0, aggregate);
        }
        limit.replaceInput(0, below);
        project.replaceInput(0, limit);
        return project;
    }

    /**
     * A computing projection over another computing projection reads the inner one's input directly
     * when it selects each value the inner one computes exactly once, unchanged, and computes its own
     * values only from columns the inner one passes through.
     */
    void mergeComputingProject(ProjectPlan project, ProjectPlan inner) {
        if (project.hasUpdateConversions() || project.hasPrunedComputedColumns() || project.hasTimestampDeclaration()
                || inner.hasUpdateConversions() || inner.hasPrunedComputedColumns() || inner.hasTimestampDeclaration()) {
            return;
        }
        final ObjList<BoundExpression> expressions = project.getExpressions();
        final ObjList<BoundExpression> innerExpressions = inner.getExpressions();
        final OutputSchema innerOutput = inner.getOutput();
        for (int i = 0, n = expressions.size(); i < n; i++) {
            final BoundExpression expression = expressions.getQuick(i);
            if (expression instanceof ColumnExpression column) {
                final int index = innerOutput.getColumnIndexById(column.getColumnId());
                if (index < 0 || !(innerExpressions.getQuick(index) instanceof ColumnExpression)
                        && (column.isCast() || column.getDataType() != innerExpressions.getQuick(index).getDataType())) {
                    return;
                }
            } else if (!readsPassedColumns(expression, inner)) {
                return;
            }
        }
        for (int k = 0, m = innerExpressions.size(); k < m; k++) {
            if (!(innerExpressions.getQuick(k) instanceof ColumnExpression)) {
                final int columnId = innerOutput.getColumnId(k);
                int references = 0;
                for (int i = 0, n = expressions.size(); i < n; i++) {
                    if (expressions.getQuick(i) instanceof ColumnExpression column && column.getColumnId() == columnId) {
                        references++;
                    }
                }
                if (references != 1) {
                    return;
                }
            }
        }
        for (int i = 0, n = expressions.size(); i < n; i++) {
            final BoundExpression expression = expressions.getQuick(i);
            final int index = expression instanceof ColumnExpression column ? innerOutput.getColumnIndexById(column.getColumnId()) : -1;
            expressions.setQuick(i, index >= 0 && !(innerExpressions.getQuick(index) instanceof ColumnExpression)
                    ? innerExpressions.getQuick(index) : context.getRewriter().remapColumns(expression, inner));
        }
        project.replaceInput(0, inner.getInput());
    }
}
