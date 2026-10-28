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
import io.questdb.griffin.PlanNodePools;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.plan.logical.AggregatePlan;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.DistinctPlan;
import io.questdb.griffin.plan.logical.FillPlan;
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
import io.questdb.griffin.plan.logical.UnaryPlan;
import io.questdb.griffin.plan.logical.WindowPlan;
import io.questdb.griffin.plan.logical.WindowSpec;
import io.questdb.std.IntIntHashMap;
import io.questdb.std.ObjList;

import static io.questdb.griffin.optimiser.DecorrelationContext.isTrue;
import static io.questdb.griffin.optimiser.ScalarCompensation.isImplicitlyKeyedByOuterColumns;
import static io.questdb.griffin.optimiser.ScalarCompensation.isTailLimit;

/**
 * Rewrites the chain of a body block to read mapped columns. A correlation enters a block at its source or at a
 * filter, whose equalities between outer and inner columns become join keys; every chain node sits above that
 * point. Below a filter, {@link Decorrelation} ends the chain at the first node that does not commute with it: that
 * node starts the block's source and never sees the filter's equalities, as in the equivalent join. Each kind of
 * chain node takes one rule when the mapping enters below it:
 * <ul>
 * <li>Project: exposes each mapped column as a hidden column; ends a filter's chain.</li>
 * <li>Filter: reads the mapped columns and drops the equalities they satisfy; commutes with a filter.</li>
 * <li>Sort: none, a LIMIT above it, across filters and distincts, ranks in its order; commutes with a filter.</li>
 * <li>Distinct: none, its tuples hold the mapped columns; commutes with a filter.</li>
 * <li>Fill: none, it fills per the keys of the grouping below it; ends a filter's chain.</li>
 * <li>Limit: numbers the rows of each mapped key, counts them too for a negative constant bound, and keeps the ones
 * the limit selects, while the step checks a non-constant bound per outer row; over a scalar body of the step, none,
 * the step guards the body's row; ends a filter's chain.</li>
 * <li>LatestBy: ranks the rows of each latest key and mapped key by timestamp; ends a filter's chain.</li>
 * <li>Aggregate and SampleBy: group by the mapped columns; end a filter's chain.</li>
 * <li>Window: partitions by the mapped columns; ends a filter's chain.</li>
 * </ul>
 * Every other kind is a block source, which {@link Decorrelation} rewrites through its inputs or rejects.
 */
final class CorrelatedChainRewriter {
    private final ScalarCompensation compensation;
    private final OuterColumnMapping mapping;
    private final OptimiserContext context;
    private final PlanNodePools planNodes;
    private final ObjList<LogicalPlan> chain;
    private final IntIntHashMap substitution;
    private final DecorrelationContext ctx;
    private final CorrelationKeys keys;

    CorrelatedChainRewriter(
            DecorrelationContext ctx,
            CorrelationKeys keys,
            ScalarCompensation compensation,
            OuterColumnMapping mapping,
            OptimiserContext context,
            PlanNodePools planNodes,
            ObjList<LogicalPlan> chain,
            IntIntHashMap substitution
    ) {
        this.ctx = ctx;
        this.mapping = mapping;
        this.context = context;
        this.planNodes = planNodes;
        this.chain = chain;
        this.substitution = substitution;
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
     * The sort that orders the rows of the plan, under the filters and distincts between them, or null.
     */
    private static SortPlan orderingSort(LogicalPlan plan) {
        while (plan instanceof FilterPlan || plan instanceof DistinctPlan) {
            plan = plan.inputAt(0);
        }
        return plan instanceof SortPlan sort ? sort : null;
    }

    /**
     * Groups the aggregate by every mapped column, as a hidden key in front of the aggregates, and maps the
     * outer columns to those keys. Records a keyless aggregate that becomes keyed.
     */
    private void addMappedKeys(GroupingPlan aggregate, int base) {
        final OutputSchema input = aggregate.getChild().getOutput();
        final OutputSchema output = aggregate.getOutput();
        compensation.keyAggregate(aggregate);
        for (int i = base, n = mapping.mark(); i < n; i++) {
            final int columnId = mapping.columnIdAt(i);
            int keyIndex = groupingIndex(aggregate, columnId);
            if (keyIndex < 0) {
                final int type = input.getColumnType(input.getColumnIndexById(columnId));
                aggregate.getGroupingExpressions().add(planNodes.columns.next().of(columnId, type, aggregate.getPosition()));
                keyIndex = aggregate.getGroupingExpressions().size() - 1;
                ctx.insertColumn(output, keyIndex, context.newColumnId(), ctx.outerRefName(mapping.outerIdAt(i)), type);
            }
            mapping.setColumnIdAt(i, output.getColumnId(keyIndex));
        }
    }

    private void prependPartitions(WindowSpec spec, OutputSchema input, int base, int position) {
        final ObjList<BoundExpression> partitionBy = spec.getPartitionBy();
        int inserted = 0;
        for (int k = base, n = mapping.mark(); k < n; k++) {
            final int columnId = mapping.columnIdAt(k);
            boolean isPresent = false;
            for (int p = 0, m = partitionBy.size(); p < m && !isPresent; p++) {
                isPresent = partitionBy.getQuick(p) instanceof ColumnExpression column && column.getColumnId() == columnId;
            }
            if (!isPresent) {
                partitionBy.insert(inserted++, 1, planNodes.columns.next().of(columnId, input.getColumnType(input.getColumnIndexById(columnId)), position));
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
        final WindowSpec spec = planNodes.windowSpecs.next().of(planNodes.unboundedWindow);
        for (int i = 0, n = latest.getKeyColumnIds().size(); i < n; i++) {
            final int columnId = latest.getKeyColumnIds().getQuick(i);
            spec.getPartitionBy().add(planNodes.columns.next().of(columnId, output.getColumnType(output.getColumnIndexById(columnId)), position));
        }
        for (int k = base, n = mapping.mark(); k < n; k++) {
            final int columnId = mapping.columnIdAt(k);
            spec.getPartitionBy().add(planNodes.columns.next().of(columnId, output.getColumnType(output.getColumnIndexById(columnId)), position));
        }
        final int timestampId = latest.getTimestampColumnId();
        spec.addOrderBy(timestampId, SortDirection.DESCENDING, output.getColumnName(output.getColumnIndexById(timestampId)), position);
        return rowNumberFilter(input, spec, "_latest_rn", position, null, null);
    }

    /**
     * A LIMIT per outer row: numbers the rows of each mapped key in the order of the sort below, under the filters
     * and distincts between them, which the numbering replaces, and keeps the rows the limit selects. Below a column
     * projection of the sort, the numbering reads the projection's input.
     */
    private LogicalPlan rankLimit(LimitPlan limit, LogicalPlan input, int base) throws SqlException {
        final int position = limit.getPosition();
        final ProjectPlan wrapper = input instanceof ProjectPlan project && orderingSort(project.getChild()) != null
                && LogicalPlans.isColumnProjection(project) ? project : null;
        LogicalPlan ordered = wrapper != null ? wrapper.getChild() : input;
        final OutputSchema output = ordered.getOutput();
        final WindowSpec spec = planNodes.windowSpecs.next().of(planNodes.unboundedWindow);
        for (int k = base, n = mapping.mark(); k < n; k++) {
            final int columnId = wrapper != null ? LogicalPlans.projectedSourceColumnId(wrapper, mapping.columnIdAt(k)) : mapping.columnIdAt(k);
            spec.getPartitionBy().add(planNodes.columns.next().of(columnId, output.getColumnType(output.getColumnIndexById(columnId)), position));
        }
        final SortPlan sort = orderingSort(ordered);
        if (sort != null) {
            for (int i = 0, n = sort.getColumnIds().size(); i < n; i++) {
                final int columnId = sort.getColumnIds().getQuick(i);
                spec.addOrderBy(columnId, sort.getDirections().getQuick(i), output.getColumnName(output.getColumnIndexById(columnId)), position);
            }
            if (ordered == sort) {
                ordered = sort.getChild();
            } else {
                LogicalPlan parent = ordered;
                while (parent.inputAt(0) != sort) {
                    parent = parent.inputAt(0);
                }
                parent.replaceInput(0, sort.getChild());
                ctx.realignAbove(ordered, sort.getChild());
            }
        }
        BoundExpression lo = limit.getLo();
        BoundExpression hi = limit.getHi();
        if (wrapper != null) {
            substitution.clear();
            for (int i = 0, n = wrapper.getExpressions().size(); i < n; i++) {
                substitution.put(wrapper.getOutput().getColumnId(i), ((ColumnExpression) wrapper.getExpressions().getQuick(i)).getColumnId());
            }
            lo = lo == null ? null : context.getRewriter().remapColumns(lo, substitution);
            hi = hi == null ? null : context.getRewriter().remapColumns(hi, substitution);
        }
        final LogicalPlan ranked = rowNumberFilter(ordered, spec, "__lateral_rn", position, lo, hi);
        if (wrapper == null) {
            return ranked;
        }
        wrapper.replaceInput(0, ranked);
        return wrapper;
    }

    private LogicalPlan rewriteChainNode(UnaryPlan node, LogicalPlan input, int base) throws SqlException {
        final boolean isCorrelated = mapping.mark() > base;
        return switch (node) {
            case ProjectPlan project -> {
                for (int k = base, n = mapping.mark(); k < n; k++) {
                    mapping.setColumnIdAt(k, ctx.exposeColumn(project, input.getOutput(), mapping.columnIdAt(k),
                            ctx.outerRefName(mapping.outerIdAt(k)), project.getPosition()));
                }
                yield project;
            }
            case FilterPlan _, SortPlan _, DistinctPlan _, FillPlan _ -> node;
            case LimitPlan limit -> isCorrelated ? rankLimit(limit, input, base) : limit;
            case LatestByPlan latest -> isCorrelated ? rankLatest(latest, input, base) : latest;
            case GroupingPlan aggregate -> {
                if (isCorrelated) {
                    addMappedKeys(aggregate, base);
                }
                yield aggregate;
            }
            case WindowPlan window -> {
                if (isCorrelated) {
                    for (int s = 0, n = window.getSpecs().size(); s < n; s++) {
                        prependPartitions(window.getSpecs().getQuick(s), input.getOutput(), base, window.getPosition());
                    }
                }
                ctx.realign(window);
                yield window;
            }
        };
    }

    private LogicalPlan rowNumberFilter(LogicalPlan input, WindowSpec spec, CharSequence name, int position, BoundExpression lo, BoundExpression hi)
            throws SqlException {
        final WindowPlan window = planNodes.windowPlans.next().of(input, position);
        window.getOutput().copyFrom(input.getOutput());
        final ColumnExpression rank = windowColumn(window, "row_number", 0, spec, name, position);
        if (lo == null) {
            return planNodes.filters.next().of(window,
                    context.bindCall("=", position, rank, planNodes.constants.next().ofInt(1, position), window.getOutput()), position);
        }
        ColumnExpression size = null;
        if (isTailLimit(lo, hi)) {
            final WindowSpec partition = planNodes.windowSpecs.next().ofFrame(spec);
            partition.getPartitionBy().addAll(spec.getPartitionBy());
            size = windowColumn(window, "count", BoundExpression.MULTI_PASS, partition, "__lateral_count", position);
        }
        return planNodes.filters.next().of(window, compensation.rankPredicate(lo, hi, rank, size, window.getOutput()), position);
    }

    /**
     * Adds the argument-free window function with the function flags to the window as a hidden column; returns that
     * column.
     */
    private ColumnExpression windowColumn(WindowPlan window, CharSequence function, int flags, WindowSpec spec, CharSequence name, int position) {
        final FunctionExpression call = context.getRewriter().describeWindowCall(function, flags, position);
        final int columnId = context.newColumnId();
        window.getFunctions().add(call);
        window.getSpecs().add(spec);
        window.getFunctionColumnIds().add(columnId);
        window.getOutput().add(columnId, name, call.getDataType(), false);
        return planNodes.columns.next().of(columnId, call.getDataType(), position);
    }

    /**
     * True when the node commutes with a filter above it: the filter's equalities can move below the node.
     */
    static boolean isCommutingWithFilter(UnaryPlan node) {
        return switch (node) {
            case FilterPlan _, SortPlan _, DistinctPlan _ -> true;
            case ProjectPlan _, FillPlan _, LimitPlan _, LatestByPlan _, GroupingPlan _, WindowPlan _ -> false;
        };
    }

    /**
     * Rewrites the chain above the block source: outer columns read the mapped columns, aggregates group by
     * them, windows partition by them, a LIMIT and LATEST ON rank per them and the projection exposes them. Unless
     * the step restores them, a keyless aggregate restores its values over no rows, see
     * {@link ScalarCompensation#compensateBlock}.
     */
    LogicalPlan rewriteChain(LogicalPlan source, int chainBase, int base, boolean isUncompensated, boolean isBranch) throws SqlException {
        LogicalPlan input = source;
        for (int i = chain.size() - 1; i >= chainBase; i--) {
            final LogicalPlan node = chain.getQuick(i);
            node.replaceInput(0, input);
            if (node instanceof FilterPlan filter) {
                final BoundExpression predicate = keys.dropEqualities(filter.getPredicate());
                if (isTrue(predicate)) {
                    continue;
                }
                filter.of(input, predicate, filter.getPosition());
            }
            if (node instanceof AggregatePlan aggregate && isImplicitlyKeyedByOuterColumns(aggregate)) {
                compensation.recordScalar(aggregate);
            }
            final ProjectPlan scalar = isUncompensated || i != chainBase ? null : compensation.compensableScalar(input);
            if (node instanceof LimitPlan limit) {
                if (scalar != null || compensation.isCompensated(input)) {
                    final LogicalPlan compensated = scalar != null ? compensation.compensateBlock(scalar, base, isBranch) : input;
                    ctx.remapMapped(limit, base);
                    input = compensation.limitGuardFilter(limit, compensated);
                    continue;
                }
                if (compensation.isStepGuarded(limit, input)) {
                    continue;
                }
                if (mapping.mark() > base) {
                    compensation.addBoundChecks(limit);
                }
            }
            ctx.remapMapped(node, base);
            input = rewriteChainNode((UnaryPlan) node, input, base);
            if (!isUncompensated && i == chainBase && compensation.compensableScalar(input) != null) {
                input = compensation.compensateBlock((ProjectPlan) input, base, isBranch);
            }
        }
        return input;
    }
}
