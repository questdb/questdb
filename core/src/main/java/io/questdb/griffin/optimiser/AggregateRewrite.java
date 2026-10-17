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

package io.questdb.griffin.optimiser;

import io.questdb.griffin.LogicalPlans;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.plan.logical.AggregatePlan;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.ConstantExpression;
import io.questdb.griffin.plan.logical.DistinctPlan;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.HorizonJoinPlan;
import io.questdb.griffin.plan.logical.JoinPlan;
import io.questdb.griffin.plan.logical.LimitPlan;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.ProjectPlan;
import io.questdb.griffin.plan.logical.SortPlan;
import io.questdb.griffin.plan.logical.WindowJoinPlan;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

/**
 * Rewrites an aggregate read by a projection, directly or through a sort of its rows.
 * <p>
 * Drops a grouping key that is arithmetic over constants and one column the aggregate also groups by:
 * {@code GROUP BY a, a + 1} forms the same groups as {@code GROUP BY a}. The projection above the
 * aggregate computes the dropped key from the surviving one in place of its references, so the
 * projection's columns keep their ids.
 * <p>
 * Applies to a selected key, other than the key of the first GROUP BY expression, of a plain aggregate
 * (no SAMPLE BY, DISTINCT or join input) that groups explicitly or selects a computed value. A
 * projection over the computing one merges into it, see {@link ProjectionMerge#mergeComputingProject}.
 * A LIMIT over the projection of an aggregate with two keys over one column moves below the projection,
 * see {@link ProjectionMerge#limitBelowProjection}.
 */
final class AggregateRewrite implements OptimiserPass {
    private final OptimiserContext context;
    private final ProjectPlan mapping;
    private final ColumnExpression mappingColumn;
    private final ProjectionMerge projectionMerge;
    private final IntList sortKeyIndexes;
    // The projection whose aggregate the last rewriteProjectedAggregate call lifted keys from.
    private ProjectPlan liftedProject;
    // The last aggregate, in postorder, with two keys over one column; a LIMIT above its projection moves below it.
    private AggregatePlan repeatedKeyAggregate;

    AggregateRewrite(
            OptimiserContext context,
            ProjectionMerge projectionMerge,
            ProjectPlan mapping,
            ColumnExpression mappingColumn,
            IntList sortKeyIndexes
    ) {
        this.context = context;
        this.projectionMerge = projectionMerge;
        this.mapping = mapping;
        this.mappingColumn = mappingColumn;
        this.sortKeyIndexes = sortKeyIndexes;
    }

    @Override
    public LogicalPlan apply(LogicalPlan plan) throws SqlException {
        return rewriteAggregates(plan);
    }

    @Override
    public void clear() {
        liftedProject = null;
        repeatedKeyAggregate = null;
    }

    @Override
    public String getName() {
        return "aggregate rewrite";
    }

    /**
     * Counts the columns of arithmetic over columns and constants, or -1 for any other expression.
     * A folded constant counts as the operator call it was folded from.
     */
    private static int arithmeticColumnCount(BoundExpression expression) {
        if (expression instanceof ColumnExpression) {
            return 1;
        }
        if (expression instanceof ConstantExpression constant) {
            if (constant.getSource() != null) {
                return arithmeticColumnCount(constant.getSource());
            }
            return constant.isLiteral() ? 0 : -1;
        }
        if (!(expression instanceof FunctionExpression call) || !isArithmetic(call)) {
            return -1;
        }
        int count = 0;
        for (int i = 0, n = call.getArgumentCount(); i < n; i++) {
            final int argumentCount = arithmeticColumnCount(call.argumentAt(i));
            if (argumentCount < 0) {
                return -1;
            }
            count += argumentCount;
        }
        return count;
    }

    private static ColumnExpression firstColumn(BoundExpression expression) {
        if (expression instanceof ColumnExpression column) {
            return column;
        }
        if (expression instanceof ConstantExpression constant) {
            return constant.getSource() != null ? firstColumn(constant.getSource()) : null;
        }
        if (expression instanceof FunctionExpression call) {
            for (int i = 0, n = call.getArgumentCount(); i < n; i++) {
                final ColumnExpression column = firstColumn(call.argumentAt(i));
                if (column != null) {
                    return column;
                }
            }
        }
        return null;
    }

    private static int groupingColumnIndex(AggregatePlan aggregate, int columnId) {
        final ObjList<BoundExpression> keys = aggregate.getGroupingExpressions();
        for (int i = 0, n = keys.size(); i < n; i++) {
            if (keys.getQuick(i) instanceof ColumnExpression column && column.getColumnId() == columnId) {
                return i;
            }
        }
        return -1;
    }

    /**
     * The projection reads the column inside an expression or through a cast.
     */
    private static boolean hasComputedColumn(ProjectPlan project) {
        final ObjList<BoundExpression> expressions = project.getExpressions();
        for (int i = 0, n = expressions.size(); i < n; i++) {
            if (!(expressions.getQuick(i) instanceof ColumnExpression column) || column.isCast()) {
                return true;
            }
        }
        return false;
    }

    private static boolean hasRepeatedKeyColumn(AggregatePlan aggregate) {
        final ObjList<BoundExpression> keys = aggregate.getGroupingExpressions();
        final OutputSchema input = aggregate.getChild().getOutput();
        for (int i = 1, n = keys.size(); i < n; i++) {
            final int columnId = keyColumnId(keys.getQuick(i), input);
            if (columnId >= 0) {
                for (int k = 0; k < i; k++) {
                    if (keyColumnId(keys.getQuick(k), input) == columnId) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    private static boolean isArithmetic(FunctionExpression call) {
        final String name = call.getName();
        if (name.length() != 1) {
            return false;
        }
        final int count = call.getArgumentCount();
        return switch (name.charAt(0)) {
            case '*', '/', '%' -> count == 2;
            case '+', '-' -> count == 1 || count == 2;
            default -> false;
        };
    }

    private static boolean isJoinInput(AggregatePlan aggregate) {
        return switch (LogicalPlans.skipFilters(aggregate.getChild())) {
            case JoinPlan _, WindowJoinPlan _, HorizonJoinPlan _ -> true;
            default -> false;
        };
    }

    private static boolean isSelected(ProjectPlan project, int columnId) {
        final OutputSchema output = project.getOutput();
        for (int i = 0, n = project.getExpressions().size(); i < n; i++) {
            if (output.isVisible(i) && project.getExpressions().getQuick(i) instanceof ColumnExpression column
                    && column.getColumnId() == columnId) {
                return true;
            }
        }
        return false;
    }

    /**
     * The single column a key reads: the key itself, or the column of binary arithmetic over it and
     * constants ({@code a + 1}, {@code 2 * a - 3}); otherwise null.
     */
    private static ColumnExpression keyColumn(BoundExpression key) {
        if (key instanceof ColumnExpression column) {
            return column;
        }
        final BoundExpression call = key instanceof ConstantExpression constant ? constant.getSource() : key;
        return call instanceof FunctionExpression function && function.getArgumentCount() == 2 && arithmeticColumnCount(function) == 1
                ? firstColumn(function) : null;
    }

    private static int keyColumnId(BoundExpression key, OutputSchema input) {
        final ColumnExpression column = keyColumn(key);
        if (column == null || key instanceof ColumnExpression && input.getColumnIndexById(column.getColumnId()) < 0) {
            return -1;
        }
        return column.getColumnId();
    }

    /**
     * The key the grouping lists first: the key of the first GROUP BY expression, or without GROUP BY
     * the key of the first selected value; -1 when that expression forms no key.
     */
    private static int leadingKeyIndex(ProjectPlan project, AggregatePlan aggregate) {
        if (aggregate.hasExplicitGrouping()) {
            return aggregate.hasConstantLeadingGroupBy() ? -1 : 0;
        }
        if (project.getExpressions().size() > 0 && project.getExpressions().getQuick(0) instanceof ColumnExpression column) {
            final int index = aggregate.getOutput().getColumnIndexById(column.getColumnId());
            return index < aggregate.getGroupingExpressions().size() ? index : -1;
        }
        return -1;
    }

    private void liftKey(ProjectPlan project, AggregatePlan aggregate, int keyIndex, ColumnExpression column, int columnKeyIndex) {
        final OutputSchema output = aggregate.getOutput();
        mapping.clear();
        mapping.of(aggregate, aggregate.getPosition());
        mapping.getExpressions().add(mappingColumn.of(output.getColumnId(columnKeyIndex), column.getDataType(), column.getPosition()));
        mapping.getOutput().add(column.getColumnId(), output.getColumnName(columnKeyIndex), column.getDataType(), false);
        final BoundExpression key = aggregate.getGroupingExpressions().getQuick(keyIndex);
        final int keyId = output.getColumnId(keyIndex);
        final ObjList<BoundExpression> expressions = project.getExpressions();
        boolean isMoved = false;
        for (int i = 0, n = expressions.size(); i < n; i++) {
            if (LogicalPlans.readsColumn(expressions.getQuick(i), keyId)) {
                final BoundExpression value = isMoved ? context.getRewriter().copyRemappedColumns(key, mapping) : context.getRewriter().remapColumns(key, mapping);
                isMoved = true;
                expressions.setQuick(i, context.getRewriter().moveToColumn(expressions.getQuick(i), keyId, value));
            }
        }
        aggregate.getGroupingExpressions().remove(keyIndex);
        output.remove(keyIndex);
    }

    private boolean liftKeys(ProjectPlan project, AggregatePlan aggregate) {
        if (aggregate.hasSampleByBucket() || isJoinInput(aggregate)
                || !aggregate.hasExplicitGrouping() && !hasComputedColumn(project)
                || !hasRepeatedKeyColumn(aggregate)) {
            return false;
        }
        repeatedKeyAggregate = aggregate;
        final ObjList<BoundExpression> keys = aggregate.getGroupingExpressions();
        final OutputSchema input = aggregate.getChild().getOutput();
        final int leadingIndex = leadingKeyIndex(project, aggregate);
        boolean isLifted = false;
        for (int i = keys.size() - 1; i >= 0; i--) {
            final BoundExpression key = keys.getQuick(i);
            if (i == leadingIndex || key instanceof ColumnExpression || !isSelected(project, aggregate.getOutput().getColumnId(i))) {
                continue;
            }
            final ColumnExpression column = keyColumn(key);
            if (column == null || input.getColumnIndexById(column.getColumnId()) < 0) {
                continue;
            }
            final int columnKeyIndex = groupingColumnIndex(aggregate, column.getColumnId());
            if (columnKeyIndex >= 0) {
                liftKey(project, aggregate, i, column, columnKeyIndex);
                isLifted = true;
            }
        }
        return isLifted;
    }

    private LogicalPlan rewrite(LogicalPlan plan) throws SqlException {
        for (int i = 0, n = plan.inputCount(); i < n; i++) {
            final LogicalPlan input = plan.inputAt(i);
            if (input != null) {
                liftedProject = null;
                final LogicalPlan rewritten = rewriteProjectedAggregate(rewrite(input), !(plan instanceof DistinctPlan));
                if (rewritten != input) {
                    plan.replaceInput(i, rewritten);
                }
                if (liftedProject != null && rewritten == liftedProject && plan instanceof ProjectPlan project) {
                    projectionMerge.mergeComputingProject(project, liftedProject);
                }
            }
        }
        if (!(plan instanceof LimitPlan limit) || repeatedKeyAggregate == null) {
            return plan;
        }
        final LogicalPlan input = limit.getChild();
        final SortPlan sort = input instanceof SortPlan sorted ? sorted : null;
        final LogicalPlan projected = sort == null ? input : sort.getChild();
        return projected instanceof ProjectPlan project && project.getChild() == repeatedKeyAggregate
                ? projectionMerge.limitBelowProjection(limit, sort, project, repeatedKeyAggregate) : limit;
    }

    private LogicalPlan rewriteAggregates(LogicalPlan root) throws SqlException {
        repeatedKeyAggregate = null;
        liftedProject = null;
        return rewriteProjectedAggregate(rewrite(root), true);
    }

    /**
     * Lifts the derived keys of an aggregate read by a projection, directly or through a sort of the
     * aggregate's rows. A projection that now computes a value no longer passes the sort its keys
     * unchanged, so the sort moves above it. Returns the plan that replaces {@code plan}.
     */
    private LogicalPlan rewriteProjectedAggregate(LogicalPlan plan, boolean isKeyLiftable) {
        if (!isKeyLiftable || !(plan instanceof ProjectPlan project)) {
            return plan;
        }
        final LogicalPlan input = project.getChild();
        if (input instanceof AggregatePlan aggregate) {
            if (liftKeys(project, aggregate)) {
                liftedProject = project;
            }
            return project;
        }
        if (!(input instanceof SortPlan sort) || !(sort.getChild() instanceof AggregatePlan aggregate)
                || !LogicalPlans.projectsSortKeys(project, sort)) {
            return project;
        }
        LogicalPlans.collectSortKeyIndexes(project, sort, sortKeyIndexes);
        project.replaceInput(0, aggregate);
        if (!liftKeys(project, aggregate)) {
            project.replaceInput(0, sort);
            return project;
        }
        liftedProject = project;
        return LogicalPlans.sortAboveProjection(sort, project, aggregate, sortKeyIndexes);
    }

}
