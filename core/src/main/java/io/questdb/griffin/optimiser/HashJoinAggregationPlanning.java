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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.IndexType;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.BoundExpressionRewriter;
import io.questdb.griffin.HashJoinGroupByKeys;
import io.questdb.griffin.HashJoinGroupByMetadata;
import io.questdb.griffin.LogicalPlans;
import io.questdb.griffin.PlanNodePools;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlKeywords;
import io.questdb.griffin.engine.functions.groupby.HashJoinGroupByAggregates;
import io.questdb.griffin.plan.logical.AggregatePlan;
import io.questdb.griffin.plan.logical.BindVariableExpression;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.ConstantExpression;
import io.questdb.griffin.plan.logical.CursorExpression;
import io.questdb.griffin.plan.logical.ExpressionVisitor;
import io.questdb.griffin.plan.logical.FilterPlan;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.HashJoinAggregation;
import io.questdb.griffin.plan.logical.JoinInput;
import io.questdb.griffin.plan.logical.JoinKey;
import io.questdb.griffin.plan.logical.JoinKind;
import io.questdb.griffin.plan.logical.JoinPlan;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.OuterColumnExpression;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.PlanToFactoryRules;
import io.questdb.griffin.plan.logical.ProjectPlan;
import io.questdb.griffin.plan.logical.ScanPlan;
import io.questdb.griffin.plan.logical.TreeWalk;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectPool;

/**
 * Decides for {@link OperatorPlanning} whether the GROUP BY of an aggregate fuses into the INNER, LEFT or RIGHT hash
 * join of two tables under it, and records how on a {@link HashJoinAggregation}. Each input is a table scan, under a
 * filter and column projections; the generator reads its page frames, or steals its parallel filter and reads the
 * frames under it. The build input is fixed for an outer join, the input it NULL-extends, and is the table with fewer
 * rows for an INNER join; when an interval scan can change which input holds fewer rows, the decision also records the
 * other build, which each execution may pick. The filters between the aggregate and the join run as the ordinary plan
 * runs them: a runtime constant that gates the join gates both inputs once per execution, every other filter runs
 * over the joined rows, and an outer join's ON condition on the build alone runs over the build rows.
 */
final class HashJoinAggregationPlanning implements Mutable {
    private static final ExpressionVisitor PARALLEL_STABLE = node -> node instanceof CursorExpression || node instanceof OuterColumnExpression
            ? TreeWalk.STOP : TreeWalk.CONTINUE;
    private final ObjectPool<AggregatePlan> aggregates;
    private final ExpressionVisitor castsKept = this::visitCast;
    private final OptimiserContext context;
    private final ObjList<FilterPlan> filters = new ObjList<>();
    private final ExpressionVisitor frameReads = this::visitFrameRead;
    private final ObjectPool<HashJoinAggregation> hashJoins;
    private final OutputSchema joined = new OutputSchema();
    private final GenericRecordMetadata joinedMetadata = new GenericRecordMetadata();
    private final ExpressionVisitor payloadReads = this::visitPayloadRead;
    private final IntList projectedIds = new IntList();
    private final ObjList<ProjectPlan> projections = new ObjList<>();
    private final ObjList<BoundExpression> rowFilters = new ObjList<>();
    private final ExpressionVisitor schemaReads = this::visitSchemaRead;
    private BoundExpression gate;
    private BoundExpression keyFilter;
    private OutputSchema masterOutput;
    private BoundExpression onFilter;
    private IntList payloadColumns;
    private OutputSchema payloadOutput;
    private BoundExpression postJoinFilter;
    private ProjectPlan projection;
    private OutputSchema readSchema;
    private OutputSchema slaveOutput;

    HashJoinAggregationPlanning(OptimiserContext context, PlanNodePools planNodes) {
        this.context = context;
        this.aggregates = planNodes.aggregates;
        this.hashJoins = planNodes.hashJoinAggregations;
    }

    @Override
    public void clear() {
        filters.clear();
        projections.clear();
        rowFilters.clear();
        joined.clear();
        joinedMetadata.clear();
        gate = null;
        keyFilter = null;
        masterOutput = null;
        onFilter = null;
        payloadColumns = null;
        payloadOutput = null;
        postJoinFilter = null;
        projection = null;
        readSchema = null;
        slaveOutput = null;
    }

    /**
     * True when the RIGHT join's preserved input carries a filter, an interval of its own or the intervals of a
     * window join, which shrink the rows the ordinary plan hashes.
     */
    private static boolean hasFilter(LogicalPlan source) {
        final ScanPlan scan = frameScan(source);
        return source instanceof FilterPlan || scan.getResidual() != null || scan.getJoinIntervals().size() > 0;
    }

    private static boolean hasVariableArgument(BoundExpression expression, boolean isArgument) {
        if (!(expression instanceof FunctionExpression call)) {
            return false;
        }
        final boolean isBooleanOperation = isBooleanOperation(call);
        if (isArgument && !isBooleanOperation && ColumnType.isVarSize(call.getDataType())
                && !call.walk(node -> node instanceof ColumnExpression ? TreeWalk.STOP : TreeWalk.CONTINUE)) {
            return true;
        }
        for (int i = 0, n = call.getArgumentCount(); i < n; i++) {
            if (hasVariableArgument(call.argumentAt(i), !isBooleanOperation)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isBooleanOperation(FunctionExpression call) {
        return call.isAnd() || call.isOr() || SqlKeywords.isNotKeyword(call.getName()) || isComparison(call.getName());
    }

    /**
     * True when the condition combines with AND, OR and NOT only comparisons whose operands are each a column, a
     * constant or a bind variable.
     */
    private static boolean isCheap(BoundExpression expression) {
        if (!(expression instanceof FunctionExpression call)) {
            return false;
        }
        if (call.isAnd() || call.isOr() || SqlKeywords.isNotKeyword(call.getName())) {
            for (int i = 0, n = call.getArgumentCount(); i < n; i++) {
                if (!isCheap(call.argumentAt(i))) {
                    return false;
                }
            }
            return true;
        }
        if (call.getArgumentCount() != 2 || !isComparison(call.getName())) {
            return false;
        }
        for (int i = 0; i < 2; i++) {
            final BoundExpression argument = call.argumentAt(i);
            if (!(argument instanceof ColumnExpression || argument instanceof ConstantExpression || argument instanceof BindVariableExpression)) {
                return false;
            }
        }
        return true;
    }

    private static boolean isComparison(CharSequence name) {
        return Chars.equals(name, "=") || Chars.equals(name, "!=") || Chars.equals(name, "<>")
                || Chars.equals(name, "<") || Chars.equals(name, "<=") || Chars.equals(name, ">") || Chars.equals(name, ">=");
    }

    /**
     * True when the ON condition on the build can cost far more on a build row than the hash insert it spares: it does
     * more than compare columns, constants and bind variables, and reads a variable-size column or passes a
     * variable-size value it computes to anything but a comparison.
     */
    private static boolean isExpensive(BoundExpression expression) {
        if (isCheap(expression)) {
            return false;
        }
        return !expression.walk(node -> node instanceof ColumnExpression && !HashJoinGroupByMetadata.isSupportedValueType(node.getDataType())
                ? TreeWalk.STOP : TreeWalk.CONTINUE) || hasVariableArgument(expression, false);
    }

    /**
     * True when the workers can evaluate the expression in parallel within one execution: it supports parallelism, is
     * stable within the execution and reads neither a sub-query nor an outer column.
     */
    private static boolean isParallelStable(BoundExpression expression) {
        return (expression.getFunctionFlags() & (BoundExpression.NO_PARALLELISM | BoundExpression.STABLE_WITHIN_EXECUTION))
                == BoundExpression.STABLE_WITHIN_EXECUTION && expression.walk(PARALLEL_STABLE);
    }

    private boolean addFilter(BoundExpression predicate) throws SqlException {
        if (predicate == null || predicate instanceof ConstantExpression constant && constant.getLongValue() != 0) {
            return true;
        }
        if (OperatorPlanning.isGate(predicate)) {
            if (!isParallelStable(predicate)) {
                return false;
            }
            gate = context.getRewriter().combineConjunction(gate, predicate, predicate.getPosition());
            return true;
        }
        final BoundExpression resolved = resolve(predicate);
        if (resolved == null || !isParallelStable(resolved) || resolved instanceof ConstantExpression constant && constant.getLongValue() == 0
                || !isPostConjunct(resolved)) {
            return false;
        }
        rowFilters.add(resolved);
        return true;
    }

    private boolean collectPayload(BoundExpression expression, OutputSchema build, IntList required) {
        payloadOutput = build;
        payloadColumns = required;
        return expression.walk(payloadReads);
    }

    /**
     * The id under which the frame source of a join input outputs the column {@code columnId} of the input, which the
     * column projections over the source rename; -1 when a projection computes it.
     */
    private int frameColumnId(int columnId) {
        for (int i = 0, n = projections.size(); i < n; i++) {
            final ProjectPlan project = projections.getQuick(i);
            final int index = project.getOutput().getColumnIndexById(columnId);
            if (index >= 0) {
                if (!(project.getExpressions().getQuick(index) instanceof ColumnExpression column)) {
                    return -1;
                }
                columnId = column.getColumnId();
            }
        }
        return columnId;
    }

    /**
     * The table scan of the join input, under a filter, past the column projections over them, which the resolution of
     * the expressions reads through; null for any other input.
     */
    private LogicalPlan frameSource(LogicalPlan input) {
        while (input instanceof ProjectPlan project) {
            if (LogicalPlans.isComputedProjection(project)) {
                return null;
            }
            projections.add(project);
            input = project.getChild();
        }
        return frameScan(input) != null ? input : null;
    }

    private boolean isPostConjunct(BoundExpression expression) {
        if (expression instanceof FunctionExpression call && call.isAnd()) {
            for (int i = 0, n = call.getArgumentCount(); i < n; i++) {
                if (!isPostConjunct(call.argumentAt(i))) {
                    return false;
                }
            }
            return true;
        }
        return onlyReads(expression, masterOutput) || onlyReads(expression, slaveOutput);
    }

    private boolean onlyReads(BoundExpression expression, OutputSchema schema) {
        readSchema = schema;
        return expression.walk(schemaReads);
    }

    /**
     * The fusion that builds the input on {@code side}, or null when one of its keys, aggregates or filters keeps the
     * ordinary plan.
     */
    private HashJoinAggregation orientation(
            AggregatePlan aggregate,
            JoinPlan join,
            JoinInput.BuildSide side,
            LogicalPlan master,
            HashJoinAggregation.InputRead masterRead,
            long masterRows,
            LogicalPlan slave,
            HashJoinAggregation.InputRead slaveRead,
            long slaveRows
    ) throws SqlException {
        final JoinInput first = join.getOrderedInputs().getQuick(0);
        final JoinInput second = join.getOrderedInputs().getQuick(1);
        final JoinKind kind = second.getJoinType();
        final boolean isMasterBuilt = side == JoinInput.BuildSide.MASTER;
        final HashJoinAggregation hashJoin = hashJoins.next().of(aggregates.next().of(join, aggregate.getPosition()), kind, side,
                isMasterBuilt ? slave : master, isMasterBuilt ? slaveRead : masterRead, (isMasterBuilt ? second : first).getBindingAlias(),
                isMasterBuilt ? master : slave, isMasterBuilt ? masterRead : slaveRead, (isMasterBuilt ? first : second).getBindingAlias());
        final OutputSchema probeOutput = hashJoin.getProbeOutput();
        final OutputSchema buildOutput = hashJoin.getBuildOutput();
        final HashJoinGroupByKeys keys = hashJoin.getKeys();
        for (int i = 0, n = second.getKeys().size(); i < n; i++) {
            final JoinKey key = second.getKeys().getQuick(i);
            final int masterId = frameColumnId(key.getMasterColumnId());
            final int slaveId = frameColumnId(key.getSlaveColumnId());
            final int masterIndex = masterOutput.getColumnIndexById(masterId);
            final int slaveIndex = slaveOutput.getColumnIndexById(slaveId);
            if (masterIndex < 0 || slaveIndex < 0) {
                return null;
            }
            final int probeIndex = isMasterBuilt ? slaveIndex : masterIndex;
            final int buildIndex = isMasterBuilt ? masterIndex : slaveIndex;
            final int probeType = probeOutput.getColumnType(probeIndex);
            final int buildType = buildOutput.getColumnType(buildIndex);
            if (!keys.add(probeOutput.getColumnId(probeIndex), probeType, buildOutput.getColumnId(buildIndex), buildType)
                    || ColumnType.isSymbol(probeType) && !probeOutput.isSymbolTableStatic(probeIndex)
                    || ColumnType.isSymbol(buildType) && !buildOutput.isSymbolTableStatic(buildIndex)) {
                return null;
            }
        }
        if (keys.hasTranslatedSymbol() && frameScan(hashJoin.getBuildInput()).getTableToken().isLiveView()) {
            return null;
        }
        final AggregatePlan resolved = hashJoin.getAggregate();
        resolved.getOutput().copyFrom(aggregate.getOutput());
        final IntList required = hashJoin.getRequiredBuildColumns();
        for (int i = 0, n = aggregate.getGroupingExpressions().size(); i < n; i++) {
            final BoundExpression key = resolve(aggregate.getGroupingExpressions().getQuick(i));
            if (key == null || !isParallelStable(key) || !HashJoinGroupByKeys.isSupportedKeyType(key.getDataType())
                    || !collectPayload(key, buildOutput, required)) {
                return null;
            }
            resolved.getGroupingExpressions().add(key);
        }
        joined.copyFrom(probeOutput);
        for (int i = 0, n = buildOutput.getColumnCount(); i < n; i++) {
            joined.addColumnFrom(buildOutput, i);
        }
        joinedMetadata.clear();
        for (int i = 0, n = joined.getColumnCount(); i < n; i++) {
            joinedMetadata.add(new TableColumnMetadata("c" + joined.getColumnId(i), joined.getColumnType(i), IndexType.NONE, 0,
                    joined.isSymbolTableStatic(i), null));
        }
        for (int i = 0, n = aggregate.getAggregates().size(); i < n; i++) {
            if (!(resolve(aggregate.getAggregates().getQuick(i)) instanceof FunctionExpression call)
                    || !isParallelStable(call) || !collectPayload(call, buildOutput, required)) {
                return null;
            }
            try (Function function = context.getInstantiator().instantiateAggregate(call, joined, joinedMetadata, context.getExecutionContext())) {
                if (!HashJoinGroupByAggregates.isSupportedAggregate(function)) {
                    return null;
                }
            }
            resolved.getAggregates().add(call);
        }
        for (int i = 0, n = rowFilters.size(); i < n; i++) {
            final BoundExpression filter = rowFilters.getQuick(i);
            if (!collectPayload(filter, buildOutput, required) || kind == JoinKind.RIGHT_OUTER && !onlyReads(filter, buildOutput)) {
                return null;
            }
        }
        // A slave-only key constraint is a build predicate only when the physical build is that slave.
        if (keyFilter != null && (!isParallelStable(keyFilter) || !onlyReads(keyFilter, buildOutput))) {
            return null;
        }
        final boolean isPresized = (isMasterBuilt ? masterRows : slaveRows) <= (isMasterBuilt ? slaveRows : masterRows);
        if (onFilter != null && (!isParallelStable(onFilter) || !onlyReads(onFilter, buildOutput) || !isPresized && isExpensive(onFilter))) {
            return null;
        }
        hashJoin.setFilters(onFilter, postJoinFilter, isPresized);
        return hashJoin;
    }

    /**
     * Sorts the filters between the aggregate and the join, and those the join applies to its pairs, as the ordinary
     * plan evaluates them. A runtime constant there gates the join in the ordinary plan, so it goes into the gate of
     * both inputs: false, neither input serves a row, as the gated join returns none, and no filter of an input runs.
     * An INNER join applies its ON condition together with its post-join filter, as one filter, so a runtime constant
     * in it gates both inputs too. An outer join's ON condition runs on the build alone, after the build's own filter,
     * as the ordinary join applies it to the rows it hashed: false, every probe row comes out NULL-extended. Every
     * other filter runs over the joined rows and reads each conjunct from one input. Returns false for a filter the
     * fused operator cannot run.
     */
    private boolean planFilters(JoinPlan join, JoinKind kind) throws SqlException {
        final BoundExpressionRewriter rewriter = context.getRewriter();
        final JoinInput second = join.getOrderedInputs().getQuick(1);
        if (!addFilter(join.getOrderedInputs().getQuick(0).getPostJoinFilter())) {
            return false;
        }
        BoundExpression on = second.getOnResidual();
        if (second.getKeyFilter() != null) {
            if ((keyFilter = resolve(second.getKeyFilter())) == null) {
                return false;
            }
            on = rewriter.combineConjunction(on, second.getKeyFilter(), on != null ? on.getPosition() : second.getKeyFilter().getPosition());
        }
        if (kind == JoinKind.INNER) {
            if (!addFilter(on == null ? second.getPostJoinFilter() : rewriter.combineConjunction(on, second.getPostJoinFilter(), on.getPosition()))) {
                return false;
            }
        } else if (on != null && (onFilter = resolve(on)) == null || !addFilter(second.getPostJoinFilter())) {
            return false;
        }
        for (int i = filters.size() - 1; i >= 0; i--) {
            if (!addFilter(filters.getQuick(i).getPredicate())) {
                return false;
            }
        }
        for (int i = 0, n = rowFilters.size(); i < n; i++) {
            final BoundExpression filter = rowFilters.getQuick(i);
            postJoinFilter = rewriter.combineConjunction(postJoinFilter, filter, postJoinFilter == null ? filter.getPosition() : postJoinFilter.getPosition());
        }
        return true;
    }

    /**
     * The expression over the columns of the frame sources: each column a projection between the aggregate and the
     * frame sources computes reads the projected expression instead, in place of a cast column whose type it keeps.
     * Null when the expression reads a column of neither frame source.
     */
    private BoundExpression resolve(BoundExpression expression) {
        final BoundExpressionRewriter rewriter = context.getRewriter();
        for (int i = 0, n = projections.size(); i < n; i++) {
            projection = projections.getQuick(i);
            if (!expression.walk(castsKept)) {
                return null;
            }
            final OutputSchema output = projection.getOutput();
            projectedIds.clear();
            for (int j = 0, m = output.getColumnCount(); j < m; j++) {
                projectedIds.add(output.getColumnId(j));
            }
            expression = rewriter.substituteColumns(expression, projectedIds, projection.getExpressions());
        }
        return expression.walk(frameReads) ? expression : null;
    }

    private int visitCast(BoundExpression node) {
        if (node instanceof ColumnExpression column && column.isCast()) {
            final int index = projection.getOutput().getColumnIndexById(column.getColumnId());
            if (index >= 0 && projection.getExpressions().getQuick(index).getDataType() != column.getDataType()) {
                return TreeWalk.STOP;
            }
        }
        return TreeWalk.CONTINUE;
    }

    private int visitFrameRead(BoundExpression node) {
        if (node instanceof ColumnExpression column) {
            final int columnId = column.getColumnId();
            final OutputSchema output = masterOutput.getColumnIndexById(columnId) >= 0 ? masterOutput : slaveOutput;
            final int index = output.getColumnIndexById(columnId);
            if (index < 0 || column.isCast() && output.getColumnType(index) != column.getDataType()) {
                return TreeWalk.STOP;
            }
        }
        return TreeWalk.CONTINUE;
    }

    private int visitPayloadRead(BoundExpression node) {
        if (node instanceof ColumnExpression column && payloadOutput.getColumnIndexById(column.getColumnId()) >= 0) {
            if (!HashJoinGroupByMetadata.isSupportedValueType(column.getDataType())) {
                return TreeWalk.STOP;
            }
            if (!payloadColumns.contains(column.getColumnId())) {
                payloadColumns.add(column.getColumnId());
            }
        }
        return TreeWalk.CONTINUE;
    }

    private int visitSchemaRead(BoundExpression node) {
        return node instanceof ColumnExpression column && readSchema.getColumnIndexById(column.getColumnId()) < 0
                ? TreeWalk.STOP : TreeWalk.CONTINUE;
    }

    /**
     * The table scan of a frame source, read directly or under a filter; null for any other plan.
     */
    static ScanPlan frameScan(LogicalPlan source) {
        return source instanceof ScanPlan scan ? scan : source instanceof FilterPlan filter && filter.getChild() instanceof ScanPlan scan ? scan : null;
    }

    /**
     * How the generator reads the page frames of the frame source: those of the factory it builds for a scan under a
     * folded or gating filter, or those under the parallel filter it steals; null when it can read neither, as for an
     * access path other than page frames, whose frames the fused operator cannot read, and a scan a filter empties.
     */
    static HashJoinAggregation.InputRead inputRead(LogicalPlan source) {
        final ScanPlan scan = frameScan(source);
        if (scan == null || scan.getAccessPath() != ScanPlan.AccessPath.PAGE_FRAMES || scan.getLimitLo() != null || scan.getLimitHi() != null) {
            return null;
        }
        final BoundExpression predicate = source instanceof FilterPlan filter && !PlanToFactoryRules.isFusedFilter(filter)
                ? filter.getPredicate() : scan.getResidual();
        if (predicate != null && !isParallelStable(predicate)) {
            return null;
        }
        if (OperatorPlanning.isPageFrameSource(source)) {
            return HashJoinAggregation.InputRead.PAGE_FRAMES;
        }
        return OperatorPlanning.isStolenFilter(source, OperatorPlanning.FilterConsumer.HASH_JOIN_INPUT)
                ? HashJoinAggregation.InputRead.STOLEN_FILTER : null;
    }

    /**
     * The fusion of the aggregate's GROUP BY into the hash join under it, or null when the shape keeps the ordinary
     * plan. Reads the row counts of the tables from the access facts {@link OptimiserContext#getTableAccessInfo} holds.
     */
    HashJoinAggregation plan(AggregatePlan aggregate) throws SqlException {
        clear();
        if (aggregate.getSharedSource() != null || aggregate.getSharedConsumerCount() > 0 || aggregate.getAggregates().size() == 0
                || !OperatorPlanning.hasParallelFunctions(aggregate)) {
            return null;
        }
        LogicalPlan input = aggregate.getChild();
        while (input instanceof ProjectPlan || input instanceof FilterPlan) {
            if (input instanceof ProjectPlan project) {
                projections.add(project);
            } else {
                final FilterPlan filter = (FilterPlan) input;
                if (LogicalPlans.skipRenames(filter.getChild()) instanceof ProjectPlan project && LogicalPlans.isComputedProjection(project)) {
                    return null;
                }
                filters.add(filter);
            }
            input = input.inputAt(0);
        }
        if (!(input instanceof JoinPlan join) || join.getOrderedInputs().size() != 2) {
            return null;
        }
        final JoinInput first = join.getOrderedInputs().getQuick(0);
        final JoinInput second = join.getOrderedInputs().getQuick(1);
        final JoinKind kind = second.getJoinType();
        if (kind != JoinKind.INNER && kind != JoinKind.LEFT_OUTER && kind != JoinKind.RIGHT_OUTER
                || second.getKeys().size() == 0 || first.isDependent() || second.isDependent()
                || PlanToFactoryRules.isSlaveEmptied(second) || PlanToFactoryRules.isMasterEmptied(second)) {
            return null;
        }
        final LogicalPlan master = frameSource(first.getInput());
        final LogicalPlan slave = master != null ? frameSource(second.getInput()) : null;
        if (slave == null) {
            return null;
        }
        final HashJoinAggregation.InputRead masterRead = inputRead(master);
        final HashJoinAggregation.InputRead slaveRead = inputRead(slave);
        if (masterRead == null || slaveRead == null) {
            return null;
        }
        final ScanPlan masterScan = frameScan(master);
        final ScanPlan slaveScan = frameScan(slave);
        final long masterRows = context.getTableAccessInfo(masterScan).getRowCount();
        final long slaveRows = context.getTableAccessInfo(slaveScan).getRowCount();
        // A RIGHT join builds its master, while the ordinary plan hashes its slave, the preserved input, after the
        // filters and intervals of the slave: the fusion holds only when its build holds no more rows than those.
        if (kind == JoinKind.RIGHT_OUTER && (masterRows > slaveRows || hasFilter(slave)
                || context.getExecutionContext().isOverriddenIntrinsics(slaveScan.getTableToken()) || masterScan.getTableToken().isLiveView())) {
            return null;
        }
        masterOutput = HashJoinAggregation.frameOutput(master);
        slaveOutput = HashJoinAggregation.frameOutput(slave);
        if (!planFilters(join, kind)) {
            return null;
        }
        final JoinInput.BuildSide side = switch (kind) {
            case RIGHT_OUTER -> JoinInput.BuildSide.MASTER;
            case INNER -> JoinInput.BuildSide.smaller(masterRows, slaveRows);
            default -> JoinInput.BuildSide.SLAVE;
        };
        final HashJoinAggregation hashJoin = orientation(aggregate, join, side, master, masterRead, masterRows, slave, slaveRead, slaveRows);
        if (hashJoin == null) {
            return null;
        }
        hashJoin.setGate(gate);
        // A SYMBOL key translates every distinct key the probe reads into the build's dictionary, so probing the
        // input with more rows costs about what building it would: SYMBOL keys keep the one build.
        if (kind == JoinKind.INNER && !hashJoin.getKeys().hasTranslatedSymbol() && (masterScan.isIntervalScan() || slaveScan.isIntervalScan())) {
            hashJoin.setAlternate(orientation(aggregate, join, side == JoinInput.BuildSide.MASTER
                    ? JoinInput.BuildSide.SLAVE : JoinInput.BuildSide.MASTER, master, masterRead, masterRows, slave, slaveRead, slaveRows));
        }
        return hashJoin;
    }
}
