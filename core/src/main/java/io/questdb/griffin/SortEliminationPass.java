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
import io.questdb.griffin.plan.logical.FillPlan;
import io.questdb.griffin.plan.logical.FilterPlan;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.JoinInput;
import io.questdb.griffin.plan.logical.JoinKind;
import io.questdb.griffin.plan.logical.JoinPlan;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.ProjectPlan;
import io.questdb.griffin.plan.logical.SetOperationPlan;
import io.questdb.griffin.plan.logical.SortDirection;
import io.questdb.griffin.plan.logical.SortPlan;
import io.questdb.griffin.plan.logical.WindowPlan;
import io.questdb.std.Chars;
import io.questdb.std.ObjList;

/**
 * Marks markout-horizon sorts, then drops the sorts whose order a consumer re-sorts or discards, and the
 * sorts of a single row that keep its designated timestamp.
 */
final class SortEliminationPass {

    private static boolean isHiddenSortKeyPrefix(ProjectPlan project, ProjectPlan inner) {
        final int n = project.getExpressions().size();
        if (!LogicalPlans.isColumnProjection(project) || project.hasTimestampDeclaration() || inner.hasTimestampDeclaration()
                || inner.hasUpdateConversions() || n >= inner.getExpressions().size()) {
            return false;
        }
        for (int i = 0; i < n; i++) {
            final ColumnExpression column = (ColumnExpression) project.getExpressions().getQuick(i);
            if (!column.isDirectReference() || column.isCast() || column.getColumnId() != inner.getOutput().getColumnId(i)
                    || !Chars.equals(project.getOutput().getColumnName(i), inner.getOutput().getColumnName(i))) {
                return false;
            }
        }
        return true;
    }

    private static boolean isSingleRow(LogicalPlan plan) {
        plan = LogicalPlans.skipProjectsAndFilters(plan);
        return plan instanceof AggregatePlan aggregate && aggregate.getGroupingExpressions().size() == 0;
    }

    private static void markMarkoutHorizon(SortPlan sort) {
        if (sort.getColumnIds().size() != 1 || sort.getDirections().getQuick(0) != SortDirection.ASCENDING) {
            return;
        }
        int columnId = sort.getColumnIds().getQuick(0);
        LogicalPlan plan = sort.getInput();
        while (plan instanceof ProjectPlan project) {
            final int index = project.getOutput().getColumnIndexById(columnId);
            if (index < 0) {
                return;
            }
            final BoundExpression expression = project.getExpressions().getQuick(index);
            plan = project.getInput();
            if (expression instanceof ColumnExpression column) {
                columnId = column.getColumnId();
                continue;
            }
            if (expression instanceof FunctionExpression call && call.getArgumentCount() == 2
                    && Chars.equals(call.getName(), '+')
                    && call.argumentAt(0) instanceof ColumnExpression left
                    && call.argumentAt(1) instanceof ColumnExpression right) {
                markMarkoutHorizon(sort, plan, left.getColumnId(), right.getColumnId());
            }
            return;
        }
    }

    private static void markMarkoutHorizon(SortPlan sort, LogicalPlan plan, int leftId, int rightId) {
        while (plan instanceof ProjectPlan project) {
            final int leftIndex = project.getOutput().getColumnIndexById(leftId);
            final int rightIndex = project.getOutput().getColumnIndexById(rightId);
            if (leftIndex < 0 || rightIndex < 0
                    || !(project.getExpressions().getQuick(leftIndex) instanceof ColumnExpression left)
                    || !(project.getExpressions().getQuick(rightIndex) instanceof ColumnExpression right)) {
                return;
            }
            leftId = left.getColumnId();
            rightId = right.getColumnId();
            plan = project.getInput();
        }
        if (!(plan instanceof JoinPlan join)) {
            return;
        }
        final ObjList<JoinInput> inputs = join.getOrderedInputs();
        if (inputs.size() != 2) {
            return;
        }
        final JoinInput slave = inputs.getQuick(1);
        if (slave.getJoinType() != JoinKind.CROSS || (slave.getHints() & JoinInput.HINT_MARKOUT_HORIZON) == 0
                || inputs.getQuick(0).getInput() == null || slave.getInput() == null) {
            return;
        }
        final int timestampId = inputs.getQuick(0).getInput().getOutput().getTimestampColumnId();
        final OutputSchema slaveOutput = slave.getInput().getOutput();
        if (timestampId < 0) {
            return;
        }
        if (leftId == timestampId && slaveOutput.getColumnIndexById(rightId) >= 0) {
            slave.setMarkout(timestampId, rightId);
        } else if (rightId == timestampId && slaveOutput.getColumnIndexById(leftId) >= 0) {
            slave.setMarkout(timestampId, leftId);
        } else {
            return;
        }
        sort.markMarkoutHorizon();
    }

    private static void replaceReorderedInput(LogicalPlan plan, boolean isReordered, boolean isSetBranchReordered) {
        final int previousTimestampId = plan.inputAt(0).getOutput().getTimestampColumnId();
        final LogicalPlan replacement = removeReorderedSorts(plan.inputAt(0), isReordered, isSetBranchReordered);
        plan.replaceInput(0, replacement);
        final int timestampId = replacement.getOutput().getTimestampColumnId();
        if (timestampId == previousTimestampId || plan.getOutput().getTimestampIndex() >= 0) {
            return;
        }
        if (plan instanceof ProjectPlan project) {
            for (int i = 0, n = project.getExpressions().size(); i < n; i++) {
                if (project.getExpressions().getQuick(i) instanceof ColumnExpression column && column.getColumnId() == timestampId) {
                    project.getOutput().setTimestampIndex(i);
                    return;
                }
            }
        } else {
            plan.getOutput().setTimestampIndex(plan.getOutput().getColumnIndexById(timestampId));
        }
    }

    static void markMarkoutHorizons(LogicalPlan plan) {
        for (int i = 0, n = plan.inputCount(); i < n; i++) {
            final LogicalPlan input = plan.inputAt(i);
            if (input != null) {
                markMarkoutHorizons(input);
            }
        }
        if (plan instanceof SortPlan sort) {
            markMarkoutHorizon(sort);
        }
    }

    static LogicalPlan removeReorderedSorts(LogicalPlan plan, boolean isReordered, boolean isSetBranchReordered) {
        switch (plan) {
            case SortPlan sort -> {
                final LogicalPlan source = LogicalPlans.skipProjects(sort.getInput());
                final LogicalPlan input = removeReorderedSorts(sort.getInput(), true, false);
                if (source instanceof WindowPlan) {
                    sort.replaceInput(0, input);
                    return sort;
                }
                if (isReordered && !sort.isMarkoutHorizon() && (isSetBranchReordered && !(source instanceof FillPlan) || sort.getOutput().getTimestampIndex() < 0
                        || sort.getOutput().getTimestampIndex() == input.getOutput().getTimestampIndex())) {
                    return input;
                }
                if (isSingleRow(input) && sort.getOutput().getTimestampIndex() == input.getOutput().getTimestampIndex()) {
                    return input;
                }
                sort.replaceInput(0, input);
                return sort;
            }
            case ProjectPlan project -> {
                final boolean isSorted = project.getInput() instanceof SortPlan;
                replaceReorderedInput(project, isReordered && !project.hasTimestampDeclaration(), isSetBranchReordered);
                if (isSorted && project.getInput() instanceof ProjectPlan inner && isHiddenSortKeyPrefix(project, inner)) {
                    inner.getExpressions().setPos(project.getExpressions().size());
                    inner.getOutput().copyFrom(project.getOutput());
                    return inner;
                }
            }
            case FilterPlan filter -> replaceReorderedInput(filter, isReordered, isSetBranchReordered);
            case WindowPlan window -> {
                boolean isInputReordered = isReordered;
                for (int i = 0, n = window.getSpecs().size(); i < n; i++) {
                    isInputReordered &= window.getSpecs().getQuick(i).getOrderByColumnIds().size() > 0
                            && !window.getSpecs().getQuick(i).isSubsampleKeepFlag();
                }
                replaceReorderedInput(window, isInputReordered, false);
            }
            case JoinPlan join -> {
                final ObjList<JoinInput> ordered = join.getOrderedInputs();
                boolean isMasterReordered = isReordered;
                for (int i = 1, n = ordered.size(); i < n; i++) {
                    switch (ordered.getQuick(i).getJoinType()) {
                        case INNER, CROSS, LEFT_OUTER -> {
                        }
                        default -> isMasterReordered = false;
                    }
                }
                for (int i = 0, n = ordered.size(); i < n; i++) {
                    final JoinInput input = ordered.getQuick(i);
                    if (input.getInput() != null) {
                        input.setInput(removeReorderedSorts(input.getInput(), i == 0 && isMasterReordered, false));
                    }
                }
            }
            case SetOperationPlan _ -> {
                for (int i = 0, n = plan.inputCount(); i < n; i++) {
                    plan.replaceInput(i, removeReorderedSorts(plan.inputAt(i), isReordered, isReordered));
                }
            }
            default -> {
                for (int i = 0, n = plan.inputCount(); i < n; i++) {
                    plan.replaceInput(i, removeReorderedSorts(plan.inputAt(i), false, false));
                }
            }
        }
        return plan;
    }
}
