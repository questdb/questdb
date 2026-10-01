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
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.FilterPlan;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.ProjectPlan;
import io.questdb.griffin.plan.logical.SetOperationPlan;
import io.questdb.griffin.plan.logical.SortPlan;
import io.questdb.griffin.plan.logical.UnaryPlan;
import io.questdb.griffin.plan.logical.WindowPlan;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;

/**
 * Drops the ordering operators beneath an aggregate whose result does not depend on its input order.
 */
final class AggregateInputOrderPass implements Mutable {
    private final ObjList<LogicalPlan> orderedBranchAggregates = new ObjList<>();

    @Override
    public void clear() {
        orderedBranchAggregates.clear();
    }

    private static boolean isUnorderedAggregate(AggregatePlan aggregate) {
        for (int i = 0, n = aggregate.getGroupingExpressions().size(); i < n; i++) {
            if (!LogicalPlans.isOrderIndependent(aggregate.getGroupingExpressions().getQuick(i))) {
                return false;
            }
        }
        // Key-only grouping and an explicit GROUP BY clause keep their input order, including
        // beneath an outer ORDER BY. Only an aggregate with inferred keys drops it.
        if (aggregate.getAggregates().size() == 0 || aggregate.hasExplicitGrouping()) {
            return false;
        }
        for (int i = 0, n = aggregate.getAggregates().size(); i < n; i++) {
            final FunctionExpression expression = aggregate.getAggregates().getQuick(i);
            if (!expression.isAggregate()
                    || expression.getOverload().isOrderSensitiveAggregate()) {
                return false;
            }
            for (int k = 0, count = expression.getArgumentCount(); k < count; k++) {
                if (!LogicalPlans.isOrderIndependent(expression.argumentAt(k))) {
                    return false;
                }
            }
        }
        return true;
    }

    private LogicalPlan removeAggregateInputOrder(LogicalPlan plan) {
        switch (plan) {
            case SortPlan sort -> {
                return LogicalPlans.skipProjects(sort.getInput()).getType() == LogicalPlan.Type.WINDOW
                        ? plan : removeAggregateInputOrder(sort.getInput());
            }
            case WindowPlan window -> {
                for (int i = 0, n = window.getSpecs().size(); i < n; i++) {
                    if (window.getSpecs().getQuick(i).getOrderByColumnIds().size() == 0) {
                        return plan;
                    }
                }
                final LogicalPlan input = window.getInput();
                final LogicalPlan replacement = removeAggregateInputOrder(input);
                if (replacement.getOutput().getTimestampColumnId() == input.getOutput().getTimestampColumnId()) {
                    window.replaceInput(0, replacement);
                }
                return plan;
            }
            case FilterPlan filter -> {
                if (LogicalPlans.isOrderIndependent(filter.getPredicate()) && replaceWithUnorderedInput(filter)) {
                    filter.getOutput().copyFrom(filter.getInput().getOutput());
                }
                return plan;
            }
            case ProjectPlan project -> {
                if (project.hasTimestampDeclaration() || project.hasUpdateConversions()) {
                    return plan;
                }
                for (int i = 0, n = project.getExpressions().size(); i < n; i++) {
                    if (!LogicalPlans.isOrderIndependent(project.getExpressions().getQuick(i))) {
                        return plan;
                    }
                }
                if (replaceWithUnorderedInput(project)) {
                    project.getOutput().setTimestampIndex(-1);
                    final int replacementTimestampId = project.getInput().getOutput().getTimestampColumnId();
                    for (int i = 0, n = project.getExpressions().size(); i < n; i++) {
                        if (project.getExpressions().getQuick(i) instanceof ColumnExpression column
                                && column.getColumnId() == replacementTimestampId) {
                            project.getOutput().setTimestampIndex(i);
                            break;
                        }
                    }
                }
                return plan;
            }
            default -> {
                // LIMIT observes order, DISTINCT selects an implementation from it,
                // and sets have their own branch-order contracts.
                return plan;
            }
        }
    }

    /** Returns whether the input or its designated timestamp changed. */
    private boolean replaceWithUnorderedInput(UnaryPlan plan) {
        final LogicalPlan input = plan.getInput();
        final int timestampId = input.getOutput().getTimestampColumnId();
        final LogicalPlan replacement = removeAggregateInputOrder(input);
        plan.replaceInput(0, replacement);
        return input != replacement || timestampId != replacement.getOutput().getTimestampColumnId();
    }

    /**
     * When the first branch of a set operation is neither an aggregate nor a sort, it emits rows in
     * its input order; an aggregate in a later branch then keeps the ORDER BY below it as well.
     */
    void collectOrderedBranchAggregates(LogicalPlan plan) {
        if (plan instanceof SetOperationPlan operation) {
            LogicalPlan first = operation;
            while (first instanceof SetOperationPlan left) {
                first = left.getLeft();
            }
            first = LogicalPlans.skipProjects(first);
            final LogicalPlan branch = LogicalPlans.skipProjects(operation.getRight());
            if (branch.getType() == LogicalPlan.Type.AGGREGATE && first.getType() != LogicalPlan.Type.AGGREGATE
                    && first.getType() != LogicalPlan.Type.SORT) {
                orderedBranchAggregates.add(branch);
            }
        }
        for (int i = 0, n = plan.inputCount(); i < n; i++) {
            collectOrderedBranchAggregates(plan.inputAt(i));
        }
    }

    void removeInputOrder(AggregatePlan aggregate) {
        if (isUnorderedAggregate(aggregate) && orderedBranchAggregates.indexOf(aggregate) < 0) {
            aggregate.replaceInput(0, removeAggregateInputOrder(aggregate.getInput()));
        }
    }
}
