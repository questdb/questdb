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
import io.questdb.griffin.plan.logical.AggregatePlan;
import io.questdb.griffin.plan.logical.FilterPlan;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.PlanVisitor;
import io.questdb.griffin.plan.logical.ProjectPlan;
import io.questdb.griffin.plan.logical.SetOperationPlan;
import io.questdb.griffin.plan.logical.SortPlan;
import io.questdb.griffin.plan.logical.TreeWalk;
import io.questdb.griffin.plan.logical.WindowPlan;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;

/**
 * Drops the ordering operators beneath an aggregate whose result does not depend on its input order.
 */
final class AggregateInputOrder implements Mutable {
    private final ObjList<LogicalPlan> orderedBranchAggregates = new ObjList<>();
    private final PlanVisitor orderedBranches = this::collectOrderedBranchAggregate;

    @Override
    public void clear() {
        orderedBranchAggregates.clear();
    }

    private static boolean isUnorderedAggregate(AggregatePlan aggregate) {
        // Key-only grouping and an explicit GROUP BY clause keep their input order, including
        // beneath an outer ORDER BY. Only an aggregate with inferred keys drops it.
        return aggregate.getAggregates().size() > 0 && !aggregate.hasExplicitGrouping()
                && LogicalPlans.isAggregateOrderIndependent(aggregate);
    }

    private int collectOrderedBranchAggregate(LogicalPlan plan) {
        if (plan instanceof SetOperationPlan operation) {
            LogicalPlan first = operation;
            while (first instanceof SetOperationPlan left) {
                first = left.getLeft();
            }
            first = LogicalPlans.skipProjects(first);
            final LogicalPlan branch = LogicalPlans.skipProjects(operation.getRight());
            if (branch instanceof AggregatePlan && !(first instanceof AggregatePlan) && !(first instanceof SortPlan)) {
                orderedBranchAggregates.add(branch);
            }
        }
        return TreeWalk.CONTINUE;
    }

    private LogicalPlan removeAggregateInputOrder(LogicalPlan plan) {
        switch (plan) {
            case SortPlan sort -> {
                return LogicalPlans.skipProjects(sort.getChild()) instanceof WindowPlan
                        ? plan : removeAggregateInputOrder(sort.getChild());
            }
            case WindowPlan window -> {
                if (!LogicalPlans.isInputOrderIgnored(window)) {
                    return plan;
                }
                final LogicalPlan input = window.getChild();
                final LogicalPlan replacement = removeAggregateInputOrder(input);
                if (replacement.getOutput().getTimestampColumnId() == input.getOutput().getTimestampColumnId()) {
                    window.replaceInput(0, replacement);
                }
                return plan;
            }
            case FilterPlan filter -> {
                if (LogicalPlans.isOrderIndependent(filter.getPredicate())) {
                    filter.replaceInput(0, removeAggregateInputOrder(filter.getChild()));
                }
                return plan;
            }
            case ProjectPlan project -> {
                if (project.hasUpdateConversions() || !LogicalPlans.isOrderTransparent(project)) {
                    return plan;
                }
                if (replaceWithUnorderedInput(project)) {
                    project.getOutput().setTimestampIndex(LogicalPlans.projectedColumnIndex(project,
                            project.getChild().getOutput().getTimestampColumnId()));
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

    /**
     * Returns whether the input or its designated timestamp changed.
     */
    private boolean replaceWithUnorderedInput(ProjectPlan project) {
        final LogicalPlan input = project.getChild();
        final int timestampId = input.getOutput().getTimestampColumnId();
        final LogicalPlan replacement = removeAggregateInputOrder(input);
        project.replaceInput(0, replacement);
        return input != replacement || timestampId != replacement.getOutput().getTimestampColumnId();
    }

    /**
     * When the first branch of a set operation is neither an aggregate nor a sort, it emits rows in
     * its input order; an aggregate in a later branch then keeps the ORDER BY below it as well.
     */
    void collectOrderedBranchAggregates(LogicalPlan plan) {
        plan.walkTopDown(orderedBranches);
    }

    void removeInputOrder(AggregatePlan aggregate) {
        if (isUnorderedAggregate(aggregate) && orderedBranchAggregates.indexOf(aggregate) < 0) {
            aggregate.replaceInput(0, removeAggregateInputOrder(aggregate.getChild()));
        }
    }
}
