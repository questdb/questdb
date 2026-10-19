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
import io.questdb.griffin.SqlKeywords;
import io.questdb.griffin.plan.logical.AggregatePlan;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.DistinctPlan;
import io.questdb.griffin.plan.logical.ExpressionVisitor;
import io.questdb.griffin.plan.logical.FillMode;
import io.questdb.griffin.plan.logical.FillPlan;
import io.questdb.griffin.plan.logical.FilterPlan;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.FunctionSourcePlan;
import io.questdb.griffin.plan.logical.GroupingPlan;
import io.questdb.griffin.plan.logical.HorizonJoinPlan;
import io.questdb.griffin.plan.logical.JoinInput;
import io.questdb.griffin.plan.logical.JoinPlan;
import io.questdb.griffin.plan.logical.LatestByPlan;
import io.questdb.griffin.plan.logical.LimitPlan;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.PlanExpressionVisitor;
import io.questdb.griffin.plan.logical.ProjectPlan;
import io.questdb.griffin.plan.logical.SampleByPlan;
import io.questdb.griffin.plan.logical.ScanPlan;
import io.questdb.griffin.plan.logical.SetOperationKind;
import io.questdb.griffin.plan.logical.SetOperationPlan;
import io.questdb.griffin.plan.logical.SortPlan;
import io.questdb.griffin.plan.logical.TreeWalk;
import io.questdb.griffin.plan.logical.WindowJoinPlan;
import io.questdb.griffin.plan.logical.WindowPlan;
import io.questdb.griffin.plan.logical.WindowSpec;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;

/**
 * The columns a plan reads, collected from the root's output down to the scans: a node keeps the output columns its
 * consumers read and those it computes for itself, see {@link #retainOutputs}, and reads the input columns that compute
 * them, see {@link #collectInputReads}. A walk collects a node once it has collected every consumer of the node, so the
 * read set then holds each output column of the node that the query reads. Column pruning removes the columns outside
 * the set, and join ordering lets a node require the designated timestamp of its input, and judges whether a join
 * input yields the same rows on every evaluation, only through the columns in it, so a computation that join ordering
 * finds unread is one that pruning removes.
 */
final class ColumnReads implements Mutable {
    private final ObjList<LogicalPlan> sharedSetOperations = new ObjList<>();
    private IntHashSet readColumnIds;
    private final ExpressionVisitor expressionReads = expression -> {
        if (expression instanceof ColumnExpression column) {
            readColumnIds.add(column.getColumnId());
        }
        return TreeWalk.CONTINUE;
    };
    private final PlanExpressionVisitor planReads = new PlanExpressionVisitor() {
        @Override
        public int visitColumnId(int columnId, int position) {
            readColumnIds.add(columnId);
            return columnId;
        }

        @Override
        public BoundExpression visitExpression(BoundExpression expression) {
            expression.walk(expressionReads);
            return expression;
        }
    };

    private static boolean hasDependentStep(ObjList<JoinInput> steps) {
        for (int i = 0, n = steps.size(); i < n; i++) {
            if (steps.getQuick(i).isDependent()) {
                return true;
            }
        }
        return false;
    }

    /**
     * A count reads the columns of an input that would otherwise compute them anyway: a set operation compares them,
     * and a computing projection evaluates them.
     */
    private static boolean isCountInputMaterialized(LogicalPlan input) {
        while (true) {
            switch (input) {
                case SetOperationPlan _ -> {
                    return true;
                }
                case ProjectPlan project -> {
                    if (!LogicalPlans.isColumnProjection(project)) {
                        return true;
                    }
                }
                case LimitPlan _, SortPlan _, FilterPlan _ -> {
                }
                default -> {
                    return false;
                }
            }
            input = input.inputAt(0);
        }
    }

    private static boolean isUnionAllChain(SetOperationPlan plan) {
        while (true) {
            if (plan.getRight() instanceof SetOperationPlan right
                    && (right.getOperation() != SetOperationKind.UNION_ALL || !isUnionAllChain(right))) {
                return false;
            }
            if (!(plan.getLeft() instanceof SetOperationPlan left)) {
                return true;
            }
            if (left.getOperation() != SetOperationKind.UNION_ALL) {
                return false;
            }
            plan = left;
        }
    }

    /**
     * The timestamp a time series join binds its master by: one that a sort by another key hides stays read, since
     * order planning may drop that sort and expose it again.
     */
    private static int temporalMasterTimestampId(LogicalPlan master) {
        final int timestampId = master.getOutput().getTimestampColumnId();
        return timestampId < 0 ? LogicalPlans.unorderedTimestampColumnId(master) : timestampId;
    }

    @Override
    public void clear() {
        sharedSetOperations.clear();
        readColumnIds = null;
    }

    /**
     * Adds the input columns the plan reads to compute the output columns the read set holds. A projection column that
     * reads an earlier column of the projection by alias reads that column too.
     */
    void collectInputReads(LogicalPlan plan) {
        switch (plan) {
            case ScanPlan _, FunctionSourcePlan _, LimitPlan _ -> {
            }
            case SampleByPlan sample -> {
                readColumnIds.add(sample.getTimestampColumnId());
                sample.visitReads(planReads);
            }
            case AggregatePlan aggregate -> {
                if (isRowCount(aggregate) && isCountInputMaterialized(aggregate.getChild())) {
                    readAllColumns(aggregate.getChild().getOutput());
                }
                for (int i = 0, n = aggregate.getGroupingExpressions().size(); i < n; i++) {
                    aggregate.getGroupingExpressions().getQuick(i).walk(expressionReads);
                }
                final int keyCount = aggregate.getGroupingExpressions().size();
                for (int i = 0, n = aggregate.getAggregates().size(); i < n; i++) {
                    if (readColumnIds.contains(aggregate.getOutput().getColumnId(keyCount + i))) {
                        aggregate.getAggregates().getQuick(i).walk(expressionReads);
                    }
                }
            }
            case WindowPlan window -> {
                for (int i = 0, n = window.getFunctionColumnIds().size(); i < n; i++) {
                    if (readColumnIds.contains(window.getFunctionColumnIds().getQuick(i))) {
                        window.getFunctions().getQuick(i).walk(expressionReads);
                        final WindowSpec spec = window.getSpecs().getQuick(i);
                        for (int k = 0, m = spec.getPartitionBy().size(); k < m; k++) {
                            spec.getPartitionBy().getQuick(k).walk(expressionReads);
                        }
                        readColumnIds.addAll(spec.getOrderByColumnIds());
                    }
                }
            }
            case JoinPlan join -> {
                final ObjList<JoinInput> steps = LogicalPlans.orderedSteps(join);
                for (int i = 0, n = steps.size(); i < n; i++) {
                    final JoinInput step = steps.getQuick(i);
                    if (step.getJoinType().isTemporal()) {
                        readColumnIds.add(temporalMasterTimestampId(steps.getQuick(0).getInput()));
                        readColumnIds.add(step.getInput().getOutput().getTimestampColumnId());
                    }
                }
                if (hasDependentStep(steps)) {
                    // Decorrelation rewrites the join and may re-read an input whole as a shared source.
                    for (int i = 0, n = steps.size(); i < n; i++) {
                        if (steps.getQuick(i).getInput() != null) {
                            readAllColumns(steps.getQuick(i).getSourceOutput());
                        }
                    }
                }
                join.visitReads(planReads);
            }
            case WindowJoinPlan windowJoin -> {
                readColumnIds.add(windowJoin.getMaster().getOutput().getTimestampColumnId());
                for (int i = 0, n = windowJoin.getSteps().size(); i < n; i++) {
                    readColumnIds.add(windowJoin.getSteps().getQuick(i).getSlave().getOutput().getTimestampColumnId());
                }
                windowJoin.visitReads(planReads);
            }
            case HorizonJoinPlan horizon -> {
                readColumnIds.add(temporalMasterTimestampId(horizon.getMaster()));
                for (int i = 0, n = horizon.getSlaves().size(); i < n; i++) {
                    readColumnIds.add(horizon.getSlaves().getQuick(i).getInput().getOutput().getTimestampColumnId());
                }
                horizon.visitReads(planReads);
            }
            case SetOperationPlan operation -> {
                final boolean isNarrowed = isNarrowedUnionAll(operation);
                final OutputSchema output = operation.getOutput();
                for (int i = 0, n = operation.inputCount(); i < n; i++) {
                    final OutputSchema input = operation.inputAt(i).getOutput();
                    for (int k = 0, m = input.getColumnCount(); k < m; k++) {
                        if (!isNarrowed || readColumnIds.contains(output.getColumnId(k))) {
                            readColumnIds.add(input.getColumnId(k));
                        }
                    }
                }
            }
            case ProjectPlan project -> {
                final OutputSchema output = project.getOutput();
                for (int readCount = -1, count = readOutputCount(output); count != readCount; count = readOutputCount(output)) {
                    readCount = count;
                    for (int i = output.getColumnCount() - 1; i >= 0; i--) {
                        if (readColumnIds.contains(output.getColumnId(i))) {
                            project.getExpressions().getQuick(i).walk(expressionReads);
                        }
                    }
                }
            }
            case FilterPlan filter -> filter.visitReads(planReads);
            case SortPlan sort -> sort.visitReads(planReads);
            case LatestByPlan latest -> {
                final LogicalPlan input = latest.getChild();
                final boolean isNativeScan = input instanceof ScanPlan
                        || input instanceof FilterPlan filter && filter.getChild() instanceof ScanPlan;
                if (latest.getTimestampColumnId() >= 0 && !isNativeScan) {
                    readColumnIds.add(latest.getTimestampColumnId());
                }
                readColumnIds.addAll(latest.getKeyColumnIds());
            }
            case FillPlan fill -> {
                readColumnIds.add(fill.getTimestampColumnId());
                final IntList targets = fill.getTargetColumnIds();
                final IntList sources = fill.getSourceColumnIds();
                final ObjList<FillMode> modes = fill.getModes();
                for (int i = 0, n = targets.size(); i < n; i++) {
                    if (readColumnIds.contains(targets.getQuick(i))) {
                        int entry = i;
                        while (entry >= 0 && modes.getQuick(entry) == FillMode.PREV_COLUMN) {
                            final int source = sources.getQuick(entry);
                            if (!readColumnIds.add(source)) {
                                break;
                            }
                            entry = targets.indexOf(source, 0, targets.size());
                        }
                    }
                }
            }
            // Every value contributes to tuple equality, including hidden columns.
            case DistinctPlan distinct -> readAllColumns(distinct.getChild().getOutput());
        }
    }

    /**
     * True when the read set holds the column.
     */
    boolean isRead(int columnId) {
        return readColumnIds.contains(columnId);
    }

    /**
     * True when column pruning narrows the set operation to the columns its consumers read: a chain of UNION ALL
     * whose rows no shared relation re-reads.
     */
    boolean isNarrowedUnionAll(SetOperationPlan operation) {
        return operation.getOperation() == SetOperationKind.UNION_ALL && sharedSetOperations.indexOf(operation) < 0
                && isUnionAllChain(operation);
    }

    /**
     * True when the aggregate computes only the count of its input rows.
     */
    boolean isRowCount(AggregatePlan aggregate) {
        if (aggregate.getGroupingExpressions().size() > 0) {
            return false;
        }
        FunctionExpression count = null;
        for (int i = 0, n = aggregate.getAggregates().size(); i < n; i++) {
            if (readColumnIds.contains(aggregate.getOutput().getColumnId(i))) {
                if (count != null) {
                    return false;
                }
                count = aggregate.getAggregates().getQuick(i);
            }
        }
        return count != null && count.getArgumentCount() == 0 && SqlKeywords.isCountKeyword(count.getName());
    }

    /**
     * True when the aggregate counts its input rows without reading any of the input's columns.
     */
    boolean isUnreadCountInput(AggregatePlan aggregate) {
        return isRowCount(aggregate) && !isCountInputMaterialized(aggregate.getChild());
    }

    /**
     * Starts a walk from the root, whose consumer reads all of its output columns, into the given read set.
     */
    void of(LogicalPlan root, IntHashSet readColumnIds) {
        clear();
        this.readColumnIds = readColumnIds;
        readColumnIds.clear();
        readAllColumns(root.getOutput());
    }

    /**
     * Adds the output columns the plan keeps whether or not a consumer reads them: an aggregate or a SAMPLE BY keeps its
     * grouping keys, which define its groups, and a projection keeps its declared designated timestamp.
     */
    void retainOutputs(LogicalPlan plan) {
        if (plan instanceof GroupingPlan grouping) {
            if (grouping instanceof AggregatePlan aggregate && aggregate.getSharedSource() != null) {
                LogicalPlan input = aggregate.getChild();
                while (input instanceof ProjectPlan project && LogicalPlans.isColumnProjection(project)) {
                    input = input.inputAt(0);
                }
                if (input instanceof SetOperationPlan) {
                    sharedSetOperations.add(input);
                }
            }
            final OutputSchema output = grouping.getOutput();
            for (int i = 0, n = grouping.getGroupingExpressions().size(); i < n; i++) {
                readColumnIds.add(output.getColumnId(i));
            }
        } else if (plan instanceof ProjectPlan project && project.hasTimestampDeclaration()) {
            readColumnIds.add(project.getOutput().getTimestampColumnId());
        }
    }

    private int readOutputCount(OutputSchema output) {
        int count = 0;
        for (int i = 0, n = output.getColumnCount(); i < n; i++) {
            if (readColumnIds.contains(output.getColumnId(i))) {
                count++;
            }
        }
        return count;
    }

    private void readAllColumns(OutputSchema output) {
        for (int i = 0, n = output.getColumnCount(); i < n; i++) {
            readColumnIds.add(output.getColumnId(i));
        }
    }
}
