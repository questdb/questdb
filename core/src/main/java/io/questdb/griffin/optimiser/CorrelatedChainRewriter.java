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
import io.questdb.griffin.plan.logical.FilterPlan;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.GroupingPlan;
import io.questdb.griffin.plan.logical.LatestByPlan;
import io.questdb.griffin.plan.logical.LimitPlan;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.ProjectPlan;
import io.questdb.griffin.plan.logical.SortDirection;
import io.questdb.griffin.plan.logical.SortPlan;
import io.questdb.griffin.plan.logical.WindowPlan;
import io.questdb.griffin.plan.logical.WindowSpec;
import io.questdb.std.ObjList;

import static io.questdb.griffin.optimiser.DecorrelationContext.keyColumnId;
import static io.questdb.griffin.optimiser.ScalarCompensation.isImplicitlyKeyedByOuterColumns;

/**
 * Rewrites the chain of a body block to read mapped columns: aggregates group by them, windows partition by
 * them, a LIMIT and LATEST ON rank per them and projections expose them.
 */
final class CorrelatedChainRewriter {
    private final ScalarCompensation compensation;
    private final DecorrelationContext ctx;
    private final CorrelationKeys keys;

    CorrelatedChainRewriter(DecorrelationContext ctx, CorrelationKeys keys, ScalarCompensation compensation) {
        this.ctx = ctx;
        this.keys = keys;
        this.compensation = compensation;
    }

    private static int groupingIndex(GroupingPlan aggregate, int columnId) {
        final ObjList<BoundExpression> keys = aggregate.getGroupingExpressions();
        for (int i = 0, n = keys.size(); i < n; i++) {
            if (keys.getQuick(i) instanceof ColumnExpression column && column.getColumnId() == columnId) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Groups the aggregate by every mapped column, as a hidden key in front of the aggregates, and maps the
     * outer columns to those keys. Records a keyless aggregate that becomes keyed.
     */
    private void addMappedKeys(GroupingPlan aggregate, int base) {
        final OutputSchema input = aggregate.getInput().getOutput();
        final OutputSchema output = aggregate.getOutput();
        if (aggregate instanceof AggregatePlan scalar && compensation.scalarAggregates.indexOf(scalar) > -1) {
            compensation.scalarAggregate = scalar;
        }
        for (int i = base, n = ctx.mappedOuterIds.size(); i < n; i++) {
            final int columnId = ctx.mappedColumnIds.getQuick(i);
            int keyIndex = groupingIndex(aggregate, columnId);
            if (keyIndex < 0) {
                final int type = input.getColumnType(input.getColumnIndexById(columnId));
                aggregate.getGroupingExpressions().add(ctx.planNodes.columns.next().of(columnId, type, aggregate.getPosition()));
                keyIndex = aggregate.getGroupingExpressions().size() - 1;
                ctx.insertColumn(output, keyIndex, ctx.context.newColumnId(), ctx.outerRefName(ctx.mappedOuterIds.getQuick(i)), type);
            }
            ctx.mappedColumnIds.setQuick(i, output.getColumnId(keyIndex));
        }
    }

    private void prependPartitions(WindowSpec spec, OutputSchema input, int base, int position) {
        final ObjList<BoundExpression> partitionBy = spec.getPartitionBy();
        int inserted = 0;
        for (int k = base, n = ctx.mappedOuterIds.size(); k < n; k++) {
            final int columnId = ctx.mappedColumnIds.getQuick(k);
            boolean isPresent = false;
            for (int p = 0, m = partitionBy.size(); p < m && !isPresent; p++) {
                isPresent = partitionBy.getQuick(p) instanceof ColumnExpression column && column.getColumnId() == columnId;
            }
            if (!isPresent) {
                partitionBy.insert(inserted++, 1, ctx.planNodes.columns.next().of(columnId, input.getColumnType(input.getColumnIndexById(columnId)), position));
            }
        }
    }

    /**
     * LATEST ON per outer row: ranks the rows of each latest key and mapped key by descending timestamp and
     * keeps the first.
     */
    private LogicalPlan rankLatest(LatestByPlan latest, LogicalPlan input, int base) throws SqlException {
        final int position = latest.getPosition();
        final OutputSchema output = input.getOutput();
        final WindowSpec spec = ctx.planNodes.windowSpecs.next().of(ctx.planNodes.unboundedWindow);
        for (int i = 0, n = latest.getKeyColumnIds().size(); i < n; i++) {
            final int columnId = latest.getKeyColumnIds().getQuick(i);
            spec.getPartitionBy().add(ctx.planNodes.columns.next().of(columnId, output.getColumnType(output.getColumnIndexById(columnId)), position));
        }
        for (int k = base, n = ctx.mappedOuterIds.size(); k < n; k++) {
            final int columnId = ctx.mappedColumnIds.getQuick(k);
            spec.getPartitionBy().add(ctx.planNodes.columns.next().of(columnId, output.getColumnType(output.getColumnIndexById(columnId)), position));
        }
        final int timestampId = latest.getTimestampColumnId();
        spec.getOrderByColumnIds().add(timestampId);
        spec.getOrderByDirections().add(SortDirection.DESCENDING);
        spec.getOrderByPositions().add(position);
        spec.getOrderByNames().add(output.getColumnName(output.getColumnIndexById(timestampId)));
        return rowNumberFilter(input, spec, "_latest_rn", position, null, null);
    }

    /**
     * A LIMIT per outer row: numbers the rows of each mapped key in the order of the sort below and keeps the
     * rows the limit selects. Below a column projection of the sort, the numbering reads the sort's input.
     */
    private LogicalPlan rankLimit(LimitPlan limit, LogicalPlan input, int base) throws SqlException {
        final int position = limit.getPosition();
        final ProjectPlan wrapper = input instanceof ProjectPlan project && project.getInput() instanceof SortPlan
                && LogicalPlans.isColumnProjection(project) ? project : null;
        LogicalPlan ordered = wrapper != null ? wrapper.getInput() : input;
        final OutputSchema output = ordered.getOutput();
        final WindowSpec spec = ctx.planNodes.windowSpecs.next().of(ctx.planNodes.unboundedWindow);
        for (int k = base, n = ctx.mappedOuterIds.size(); k < n; k++) {
            final int columnId = wrapper != null ? keyColumnId(wrapper, ctx.mappedColumnIds.getQuick(k)) : ctx.mappedColumnIds.getQuick(k);
            spec.getPartitionBy().add(ctx.planNodes.columns.next().of(columnId, output.getColumnType(output.getColumnIndexById(columnId)), position));
        }
        if (ordered instanceof SortPlan sort) {
            for (int i = 0, n = sort.getColumnIds().size(); i < n; i++) {
                final int columnId = sort.getColumnIds().getQuick(i);
                spec.getOrderByColumnIds().add(columnId);
                spec.getOrderByDirections().add(sort.getDirections().getQuick(i));
                spec.getOrderByPositions().add(position);
                spec.getOrderByNames().add(output.getColumnName(output.getColumnIndexById(columnId)));
            }
            ordered = sort.getInput();
        }
        BoundExpression lo = limit.getLo();
        BoundExpression hi = limit.getHi();
        if (wrapper != null) {
            ctx.substitution.clear();
            for (int i = 0, n = wrapper.getExpressions().size(); i < n; i++) {
                ctx.substitution.put(wrapper.getOutput().getColumnId(i), ((ColumnExpression) wrapper.getExpressions().getQuick(i)).getColumnId());
            }
            lo = lo == null ? null : ctx.context.getRewriter().remapColumns(lo, ctx.substitution);
            hi = hi == null ? null : ctx.context.getRewriter().remapColumns(hi, ctx.substitution);
        }
        final LogicalPlan ranked = rowNumberFilter(ordered, spec, "__lateral_rn", position, lo, hi);
        if (wrapper == null) {
            return ranked;
        }
        wrapper.replaceInput(0, ranked);
        return wrapper;
    }

    private LogicalPlan rewriteChainNode(LogicalPlan node, LogicalPlan input, int base) throws SqlException {
        final boolean isCorrelated = ctx.mappedOuterIds.size() > base;
        switch (node) {
            case ProjectPlan project -> {
                for (int k = base, n = ctx.mappedOuterIds.size(); k < n; k++) {
                    ctx.exposeColumn(project, input.getOutput(), ctx.mappedColumnIds.getQuick(k), ctx.outerRefName(ctx.mappedOuterIds.getQuick(k)), project.getPosition());
                    ctx.mappedColumnIds.setQuick(k, project.getOutput().getColumnId(project.getOutput().getColumnCount() - 1));
                }
                return project;
            }
            case GroupingPlan aggregate -> {
                if (isCorrelated) {
                    addMappedKeys(aggregate, base);
                }
                return aggregate;
            }
            case WindowPlan window -> {
                if (isCorrelated) {
                    for (int s = 0, n = window.getSpecs().size(); s < n; s++) {
                        prependPartitions(window.getSpecs().getQuick(s), input.getOutput(), base, window.getPosition());
                    }
                }
                ctx.alignColumns(window.getOutput(), input.getOutput());
                return window;
            }
            case LimitPlan limit when isCorrelated -> {
                return rankLimit(limit, input, base);
            }
            case LatestByPlan latest when isCorrelated -> {
                return rankLatest(latest, input, base);
            }
            default -> {
                return node;
            }
        }
    }

    private LogicalPlan rowNumberFilter(LogicalPlan input, WindowSpec spec, CharSequence name, int position, BoundExpression lo, BoundExpression hi)
            throws SqlException {
        final WindowPlan window = ctx.planNodes.windowPlans.next().of(input, position);
        window.getOutput().copyFrom(input.getOutput());
        final FunctionExpression rowNumber = ctx.context.getRewriter().describeWindowCall("row_number", position);
        final int rowNumberId = ctx.context.newColumnId();
        window.getFunctions().add(rowNumber);
        window.getSpecs().add(spec);
        window.getFunctionColumnIds().add(rowNumberId);
        window.getOutput().add(rowNumberId, name, rowNumber.getDataType(), false);
        final ColumnExpression rank = ctx.planNodes.columns.next().of(rowNumberId, rowNumber.getDataType(), position);
        final BoundExpression predicate;
        if (lo == null && hi == null) {
            predicate = ctx.bindCall("=", position, rank, ctx.planNodes.constants.next().ofInt(1, position), window.getOutput());
        } else {
            final BoundExpression upper = compensation.limitComparison(">=", hi == null ? lo : hi, rank, window.getOutput(), position);
            predicate = hi != null && lo != null
                    ? ctx.context.getRewriter().combineConjunction(upper, compensation.limitComparison("<", lo, rank, window.getOutput(), position), position) : upper;
        }
        final FilterPlan filter = ctx.planNodes.filters.next().of(window, predicate, position);
        filter.deriveOutput();
        return filter;
    }

    /**
     * Rewrites the chain above the block source: outer columns read the mapped columns, aggregates group by
     * them, windows partition by them, a LIMIT and LATEST ON rank per them and the projection exposes them.
     */
    LogicalPlan rewriteChain(LogicalPlan source, int chainBase, int base, boolean isUncompensated) throws SqlException {
        LogicalPlan input = source;
        for (int i = ctx.chain.size() - 1; i >= chainBase; i--) {
            final LogicalPlan node = ctx.chain.getQuick(i);
            node.replaceInput(0, input);
            if (node instanceof FilterPlan filter) {
                final BoundExpression predicate = keys.dropEqualities(filter.getPredicate());
                if (predicate == null || predicate instanceof ConstantExpression constant && constant.getLongValue() != 0) {
                    continue;
                }
                filter.of(input, predicate, filter.getPosition());
            }
            if (node instanceof AggregatePlan aggregate && isImplicitlyKeyedByOuterColumns(aggregate)) {
                compensation.scalarAggregates.add(aggregate);
            }
            final ProjectPlan scalar = isUncompensated || i != chainBase ? null : compensation.compensableScalar(input);
            if (node instanceof LimitPlan limit && (scalar != null || compensation.compensatedScalars.indexOf(input) > -1)) {
                input = compensation.limitGuardFilter(limit, scalar != null ? compensation.compensateBlock(scalar, base) : input);
                continue;
            }
            ctx.remapMapped(node, base);
            input = rewriteChainNode(node, input, base);
            if (!isUncompensated && i == chainBase && compensation.compensableScalar(input) != null) {
                input = compensation.compensateBlock((ProjectPlan) input, base);
            }
        }
        return input;
    }
}
